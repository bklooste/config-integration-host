using System.Net;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using IntegrationHost.Pipes;
using IntegrationHost.Runtime;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace IntegrationHost.Tests;

/// <summary>The published rule-engine-service image (public GHCR, pulled anonymously) plus its Redis.</summary>
public sealed class RuleEngineFixture : IAsyncLifetime
{
    private readonly INetwork network = new NetworkBuilder().Build();
    private IContainer redis = null!;
    private IContainer engine = null!;

    public string RedisConnectionString => $"{redis.Hostname}:{redis.GetMappedPublicPort(6379)}";
    public string EngineUrl => $"http://{engine.Hostname}:{engine.GetMappedPublicPort(8080)}";

    public async ValueTask InitializeAsync()
    {
        await network.CreateAsync();
        redis = new ContainerBuilder().WithImage("redis:7-alpine").WithNetwork(network).WithNetworkAliases("rules-redis")
            .WithPortBinding(6379, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("redis-cli", "ping")).Build();
        await redis.StartAsync();

        engine = new ContainerBuilder().WithImage("ghcr.io/bklooste/rule-engine-service/service:latest").WithNetwork(network)
            .WithPortBinding(8080, true)
            .WithEnvironment("ConnectionStrings__Redis", "rules-redis:6379")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8080).ForPath("/health")))
            .Build();
        await engine.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await engine.DisposeAsync();
        await redis.DisposeAsync();
        await network.DisposeAsync();
    }

    public async Task PutTemplateAsync(string id, string json)
    {
        using var http = new HttpClient();
        var r = await http.PutAsync($"{EngineUrl}/v2/templates/{id}", new StringContent(json, Encoding.UTF8, "application/json"));
        r.EnsureSuccessStatusCode();
    }
}

public class RuleEngineMapTests(RuleEngineFixture rules) : IClassFixture<RuleEngineFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static string Unique(string p) => p + "-" + Guid.NewGuid().ToString("N")[..8];

    // A partner-facing reshaping: only bets are forwarded, renamed, with internal fields dropped.
    private const string BetTemplate = """
        {"aggregationField":"id","templates":[
          {"name":"bets","dataToMatch":{"eventType":"bet"},"matchFragment":{"betId":"{betId}","stake":"{amount}","kind":"warehouse"}}]}
        """;

    private WebApplicationFactory<Program> Host(Receiver receiver, string stream, string template, params (string, string)[] extra) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Host:RedisConnectionString", rules.RedisConnectionString);
            b.UseSetting("Host:RuleEngineUrl", rules.EngineUrl);
            b.UseSetting("Host:PollIntervalMs", "20");
            b.UseSetting("Pipes:0:Name", "mapped");
            b.UseSetting("Pipes:0:Source:Transport", "redis");
            b.UseSetting("Pipes:0:Source:Stream", stream);
            b.UseSetting("Pipes:0:Source:ConsumerGroup", "g");
            b.UseSetting("Pipes:0:Source:StartFrom", "Beginning");
            b.UseSetting("Pipes:0:Map:Template", template);
            b.UseSetting("Pipes:0:Destination:Transport", "http");
            b.UseSetting("Pipes:0:Destination:Url", receiver.BaseUrl + "/hook");
            b.UseSetting("Pipes:0:Retry:InitialDelayMs", "10");
            b.UseSetting("Pipes:0:Retry:MaxDelayMs", "100");
            foreach (var (k, v) in extra) b.UseSetting(k, v);
        });

    private async Task<IDatabase> Db() => (await ConnectionMultiplexer.ConnectAsync(rules.RedisConnectionString)).GetDatabase();

    [Fact]
    public async Task The_real_rule_engine_reshapes_the_payload_before_delivery()
    {
        var template = Unique("t");
        await rules.PutTemplateAsync(template, BetTemplate);
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        var id = (string)(await db.StreamAddAsync(stream, [new("data", """{"eventType":"bet","betId":"b1","amount":"12.5","customerEmail":"secret@x.com"}""")]))!;

        await using var host = Host(receiver, stream, template);
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        var call = receiver.Calls.Single();
        Assert.Contains("\"betId\":\"b1\"", call.Body);
        Assert.Contains("\"kind\":\"warehouse\"", call.Body);
        Assert.DoesNotContain("secret@x.com", call.Body);   // the internal field never leaves
        Assert.Equal(id, call.Headers["Idempotency-Key"]);   // dedupe key unchanged by mapping
    }

    [Fact]
    public async Task Messages_no_rule_matches_are_skipped_not_sent_as_empty_objects()
    {
        var template = Unique("t");
        await rules.PutTemplateAsync(template, BetTemplate);
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"eventType":"login","user":"u"}""")]);
        await db.StreamAddAsync(stream, [new("data", """{"eventType":"bet","betId":"b2","amount":"3"}""")]);

        await using var host = Host(receiver, stream, template);
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Count >= 1, Timeout));
        Assert.True(await Receiver.WaitFor(() => db.StreamPending(stream, "g").PendingMessageCount == 0, Timeout));
        Assert.Contains("b2", Assert.Single(receiver.Calls).Body);
    }

    [Fact]
    public async Task A_missing_template_makes_the_host_unhealthy_with_a_clear_reason_then_recovers_when_it_appears()
    {
        var template = Unique("late");
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"eventType":"bet","betId":"b3","amount":"1"}""")]);

        await using var host = Host(receiver, stream, template);
        using var client = host.CreateClient();

        var body = "";
        Assert.True(await Receiver.WaitFor(() =>
        {
            var r = client.GetAsync("/health").Result;
            body = r.Content.ReadAsStringAsync().Result;
            return r.StatusCode == HttpStatusCode.ServiceUnavailable && body.Contains("not found");
        }, Timeout));
        Assert.Contains($"template '{template}' not found", body);
        Assert.Empty(receiver.Calls);

        await rules.PutTemplateAsync(template, BetTemplate); // fixing config needs no host restart

        Assert.True(await Receiver.WaitFor(() => receiver.Calls.Any(c => c.Body.Contains("b3")), TimeSpan.FromSeconds(60)));
        Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/health")).StatusCode);
    }

    [Fact]
    public async Task OnNoMatch_fail_applies_the_failure_policy()
    {
        var template = Unique("t");
        await rules.PutTemplateAsync(template, BetTemplate);
        var stream = Unique("s");
        await using var receiver = await Receiver.StartAsync();
        var db = await Db();
        await db.StreamAddAsync(stream, [new("data", """{"eventType":"login"}""")]);

        await using var host = Host(receiver, stream, template,
            ("Pipes:0:Map:OnNoMatch", "fail"), ("Pipes:0:Retry:MaxAttempts", "2"));
        using var client = host.CreateClient();

        Assert.True(await Receiver.WaitFor(() => client.GetAsync("/health").Result.StatusCode == HttpStatusCode.ServiceUnavailable, Timeout));
        Assert.Empty(receiver.Calls);
        Assert.Equal(1, (await db.StreamPendingAsync(stream, "g")).PendingMessageCount); // block: never acked, never lost
    }

    [Fact]
    public async Task RuleEngineMap_treats_empty_object_as_no_match_and_errors_as_failures()
    {
        var handler = new StubHandler();
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rules/") };
        var map = new RuleEngineMap(http, "t 1");
        var msg = new Envelope("1", "{}", null, new Dictionary<string, string>());

        handler.Respond = (HttpStatusCode.OK, "{}");
        Assert.Null(await map.MapAsync(msg, default));
        handler.Respond = (HttpStatusCode.OK, """{"a":1}""");
        Assert.Equal("""{"a":1}""", await map.MapAsync(msg, default));
        Assert.Equal("/v2/templates/t%201/evaluate", handler.LastPath); // template id is escaped
        handler.Respond = (HttpStatusCode.InternalServerError, "");
        await Assert.ThrowsAsync<HttpRequestException>(() => map.MapAsync(msg, default));
        handler.Respond = (HttpStatusCode.NotFound, "");
        await Assert.ThrowsAsync<InvalidOperationException>(() => map.CheckAsync(default));
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        public (HttpStatusCode Code, string Body) Respond = (HttpStatusCode.OK, "{}");
        public string? LastPath;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            LastPath = request.RequestUri!.AbsolutePath;
            return Task.FromResult(new HttpResponseMessage(Respond.Code) { Content = new StringContent(Respond.Body) });
        }
    }
}
