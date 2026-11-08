using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Mvc.Testing;
using StackExchange.Redis;

namespace IntegrationHost.Tests;

/// <summary>Event Hubs as both source and destination, against the Event Hubs emulator.</summary>
[Collection("eventhubs")]
public class EventHubsPipeTests(EventHubsFixture eh, RedisFixture redis)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static string Unique(string p) => p + "-" + Guid.NewGuid().ToString("N")[..8];

    private static WebApplicationFactory<Program> Host(params (string Key, string Value)[] settings) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Host:PollIntervalMs", "20");
            b.UseSetting("Pipes:0:Name", "eh-test");
            b.UseSetting("Pipes:0:Retry:InitialDelayMs", "10");
            b.UseSetting("Pipes:0:Retry:MaxDelayMs", "50");
            foreach (var (k, v) in settings) b.UseSetting(k, v);
        });

    private (string, string)[] EhSource(string hub, string group, string container) =>
    [
        ("Pipes:0:Source:Transport", "eventhubs"),
        ("Pipes:0:Source:ConnectionString", EventHubsFixture.EmulatorConnectionString),
        ("Pipes:0:Source:EventHub", hub),
        ("Pipes:0:Source:ConsumerGroup", group),
        ("Pipes:0:Source:CheckpointConnectionString", eh.StorageConnectionString),
        ("Pipes:0:Source:CheckpointContainer", container),
    ];

    [Fact]
    public async Task Redis_to_eventhubs_publishes_the_payload_with_the_source_id_as_the_dedupe_key()
    {
        var stream = Unique("s");
        var mux = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        var id = (string)(await mux.GetDatabase().StreamAddAsync(stream, [new("data", """{"n":1}"""), new("type", "OrderPlaced")]))!;

        await using var consumer = new EventHubConsumerClient(EventHubsFixture.Group, EventHubsFixture.EmulatorConnectionString, "eh2");
        await using var host = Host(
            ("Host:RedisConnectionString", redis.ConnectionString),
            ("Pipes:0:Source:Transport", "redis"), ("Pipes:0:Source:Stream", stream), ("Pipes:0:Source:ConsumerGroup", "g"),
            ("Pipes:0:Source:StartFrom", "Beginning"),
            ("Pipes:0:Destination:Transport", "eventhubs"),
            ("Pipes:0:Destination:ConnectionString", EventHubsFixture.EmulatorConnectionString),
            ("Pipes:0:Destination:EventHub", "eh2"));
        using var client = host.CreateClient();

        using var cts = new CancellationTokenSource(Timeout);
        await foreach (var e in consumer.ReadEventsAsync(startReadingAtEarliestEvent: true, cancellationToken: cts.Token))
        {
            if (e.Data is null || !e.Data.Properties.TryGetValue("idempotency-key", out var key) || (string)key != id) continue;
            Assert.Equal("""{"n":1}""", Encoding.UTF8.GetString(e.Data.EventBody.ToArray()));
            Assert.Equal("OrderPlaced", e.Data.Properties["type"]);
            return;
        }
        Assert.Fail("event not received");
    }

    [Fact]
    public async Task Eventhubs_to_http_delivers_checkpoints_and_does_not_redeliver_after_restart()
    {
        var container = Unique("cp");
        var marker = Unique("m");
        await using var receiver = await Receiver.StartAsync();
        await using var producer = new EventHubProducerClient(EventHubsFixture.EmulatorConnectionString, "eh1");
        await producer.SendAsync([new EventData(Encoding.UTF8.GetBytes($$"""{"m":"{{marker}}"}"""))]);

        var settings = EhSource("eh1", "cg1", container).Concat(
        [
            ("Pipes:0:Destination:Transport", "http"), ("Pipes:0:Destination:Url", receiver.BaseUrl + "/hook"),
        ]).ToArray();

        await using (var host = Host(settings))
        {
            using var client = host.CreateClient();
            Assert.True(await Receiver.WaitFor(() => receiver.Calls.Any(c => c.Body.Contains(marker)), Timeout));
            var call = receiver.Calls.First(c => c.Body.Contains(marker));
            Assert.StartsWith("eh1/", call.Headers["Idempotency-Key"]); // hub/partition/sequence
            await Task.Delay(1500); // let the checkpoint land before stopping
        }

        var before = receiver.Calls.Count(c => c.Body.Contains(marker));
        await using (var host2 = Host(settings))
        {
            using var client = host2.CreateClient();
            await Task.Delay(8000); // ample time for a redelivery, if there were one
        }
        Assert.Equal(before, receiver.Calls.Count(c => c.Body.Contains(marker)));
    }

    [Fact]
    public async Task Eventhubs_to_redis_round_trips_payload_and_properties()
    {
        var stream = Unique("out");
        var marker = Unique("m");
        var mux = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        await using var producer = new EventHubProducerClient(EventHubsFixture.EmulatorConnectionString, "eh1");
        var data = new EventData(Encoding.UTF8.GetBytes($$"""{"m":"{{marker}}"}"""));
        data.Properties["type"] = "OrderPlaced";
        await producer.SendAsync([data]);

        await using var host = Host(EhSource("eh1", "cg2", Unique("cp")).Concat(
        [
            ("Host:RedisConnectionString", redis.ConnectionString),
            ("Pipes:0:Destination:Transport", "redis"), ("Pipes:0:Destination:Stream", stream),
        ]).ToArray());
        using var client = host.CreateClient();

        // The hub is shared with other tests, so the consumer group may also see their events: look for ours.
        StreamEntry? found = null;
        Assert.True(await Receiver.WaitFor(() =>
        {
            found = mux.GetDatabase().StreamRange(stream).Cast<StreamEntry?>().FirstOrDefault(e => ((string?)e!.Value["data"])?.Contains(marker) == true);
            return found is not null;
        }, Timeout));
        var entry = found!.Value;
        Assert.Contains(marker, (string)entry["data"]!);
        Assert.Equal("OrderPlaced", (string)entry["type"]!);
        Assert.StartsWith("eh1/", (string)entry["source-id"]!);
    }
}
