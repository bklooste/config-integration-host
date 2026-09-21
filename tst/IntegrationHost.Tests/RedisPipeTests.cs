using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.AspNetCore.Mvc.Testing;
using StackExchange.Redis;

namespace IntegrationHost.Tests;

public sealed class RedisFixture : IAsyncLifetime
{
    private readonly IContainer container = new ContainerBuilder()
        .WithImage("redis:7-alpine")
        .WithPortBinding(6379, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("redis-cli", "ping"))
        .Build();

    public string ConnectionString => $"{container.Hostname}:{container.GetMappedPublicPort(6379)}";
    public async ValueTask InitializeAsync() => await container.StartAsync();
    public async ValueTask DisposeAsync() => await container.DisposeAsync();
}

/// <summary>The whole host against a real Redis and a real HTTP receiver.</summary>
public class RedisPipeTests(RedisFixture redis) : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private WebApplicationFactory<Program> Host(Receiver receiver, string stream, string group, params (string Key, string Value)[] extra) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Host:RedisConnectionString", redis.ConnectionString);
            b.UseSetting("Host:PollIntervalMs", "20");
            b.UseSetting("Pipes:0:Name", "test");
            b.UseSetting("Pipes:0:Source:Transport", "redis");
            b.UseSetting("Pipes:0:Source:Stream", stream);
            b.UseSetting("Pipes:0:Source:ConsumerGroup", group);
            b.UseSetting("Pipes:0:Source:StartFrom", "Beginning");
            b.UseSetting("Pipes:0:Destination:Transport", "http");
            b.UseSetting("Pipes:0:Destination:Url", receiver.BaseUrl + "/hook");
            b.UseSetting("Pipes:0:Retry:InitialDelayMs", "10");
            b.UseSetting("Pipes:0:Retry:MaxDelayMs", "50");
            foreach (var (k, v) in extra) b.UseSetting(k, v);
        });

    private async Task<IDatabase> Db()
    {
        var mux = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        return mux.GetDatabase();
    }

    private static string Unique(string p) => p + "-" + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    public async Task A_stream_message_arrives_at_the_http_destination_with_its_id_as_the_dedupe_key()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        var id = (string)(await db.StreamAddAsync(stream, [new("data", """{"hello":"world"}"""), new("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")]))!;

        await using var host = Host(receiver, stream, "g");
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        var call = receiver.Calls.Single();
        Assert.Equal("POST", call.Method);
        Assert.Equal("""{"hello":"world"}""", call.Body);
        Assert.Equal(id, call.Headers["Idempotency-Key"]);
        // same trace, new span: the outgoing call continues the producer's trace
        Assert.StartsWith("00-0af7651916cd43dd8448eb211c80319c-", call.Headers["traceparent"]);
        // acked: nothing left pending
        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0, Timeout));
    }

    [Fact]
    public async Task Entries_without_a_payload_field_are_sent_as_a_json_object_of_all_fields()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("a", "1"), new("b", "2")]);

        await using var host = Host(receiver, stream, "g");
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        using var doc = JsonDocument.Parse(receiver.Calls.Single().Body);
        Assert.Equal("1", doc.RootElement.GetProperty("a").GetString());
        Assert.Equal("2", doc.RootElement.GetProperty("b").GetString());
    }

    [Fact]
    public async Task An_outage_blocks_the_pipe_turns_health_unhealthy_and_nothing_is_lost_when_it_recovers()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        receiver.StatusCode = 503;
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"n":1}""")]);
        await db.StreamAddAsync(stream, [new("data", """{"n":2}""")]);

        await using var host = Host(receiver, stream, "g", ("Pipes:0:Retry:MaxAttempts", "2"));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => Health(client).Result == System.Net.HttpStatusCode.ServiceUnavailable, Timeout));
        Assert.Equal(System.Net.HttpStatusCode.OK, (await client.GetAsync("/health/live")).StatusCode); // liveness unaffected
        var body = await (await client.GetAsync("/health")).Content.ReadAsStringAsync();
        Assert.Contains("Blocked", body);

        receiver.StatusCode = 200;

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count(c => c.Body.Contains("\"n\":2")) >= 1, Timeout));
        Assert.Equal(System.Net.HttpStatusCode.OK, await Health(client));
        // in order, and message 1 was not skipped
        var okBodies = receiver.Calls.Select(c => c.Body).Distinct().ToList();
        Assert.Equal(["""{"n":1}""", """{"n":2}"""], okBodies);
    }

    [Fact]
    public async Task Skip_and_alert_drops_the_poison_message_and_carries_on()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        receiver.StatusCode = 400;
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"n":1}""")]);

        await using var host = Host(receiver, stream, "g", ("Pipes:0:OnFailure", "skip-and-alert"), ("Pipes:0:Retry:MaxAttempts", "2"));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0 && receiver.Calls.Count >= 2, Timeout));
        receiver.StatusCode = 200;
        await db.StreamAddAsync(stream, [new("data", """{"n":2}""")]);

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Any(c => c.Body.Contains("\"n\":2")), Timeout));
        Assert.Equal(System.Net.HttpStatusCode.OK, await Health(client));
    }

    [Fact]
    public async Task A_message_delivered_but_never_acked_is_redelivered_after_restart_with_the_same_dedupe_key()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"n":1}""")]);
        // Simulate a crash between "destination accepted" and "ack": read into the pending list, never ack.
        await db.StreamCreateConsumerGroupAsync(stream, "g", "0-0");
        var consumer = "crashed-replica";
        var stranded = await db.StreamReadGroupAsync(stream, "g", consumer, ">", 10);
        Assert.Single(stranded);

        // A new replica with the same consumer name re-reads its own pending entries first.
        await using var host = Host(receiver, stream, "g", ("Host:ConsumerName", consumer));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        Assert.Equal(stranded[0].Id.ToString(), receiver.Calls.First().Headers["Idempotency-Key"]);
        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0, Timeout));
    }

    [Fact]
    public async Task Entries_stranded_on_a_dead_replica_are_claimed_once_idle()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"n":1}""")]);
        await db.StreamCreateConsumerGroupAsync(stream, "g", "0-0");
        await db.StreamReadGroupAsync(stream, "g", "dead-replica", ">", 10);

        await using var host = Host(receiver, stream, "g", ("Host:ConsumerName", "live-replica"), ("Pipes:0:Source:ClaimIdleSeconds", "1"));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0, Timeout));
    }

    [Fact]
    public async Task Type_filter_only_delivers_listed_types()
    {
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("type", "Other"), new("data", """{"n":1}""")]);
        await db.StreamAddAsync(stream, [new("type", "Audit"), new("data", """{"n":2}""")]);

        await using var host = Host(receiver, stream, "g", ("Pipes:0:Source:TypeFilter:0", "Audit"));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0, Timeout));
        Assert.Equal("""{"n":2}""", Assert.Single(receiver.Calls).Body);
    }

    [Fact]
    public async Task An_unreachable_redis_is_unhealthy_not_a_crash()
    {
        await using var receiver = await Receiver.StartAsync();
        await using var host = Host(receiver, "s", "g", ("Host:RedisConnectionString", "127.0.0.1:1,connectTimeout=500"));
        using var client = host.CreateClient();

        Assert.Equal(System.Net.HttpStatusCode.ServiceUnavailable, await Health(client));
        Assert.Equal(System.Net.HttpStatusCode.OK, (await client.GetAsync("/health/live")).StatusCode);
    }

    private static async Task<System.Net.HttpStatusCode> Health(HttpClient c) => (await c.GetAsync("/health")).StatusCode;
}
