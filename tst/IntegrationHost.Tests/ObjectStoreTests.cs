using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using IntegrationHost.Pipes;
using IntegrationHost.Runtime;
using Microsoft.AspNetCore.Mvc.Testing;
using StackExchange.Redis;

namespace IntegrationHost.Tests;

public sealed class AzuriteFixture : IAsyncLifetime
{
    private readonly IContainer container = new ContainerBuilder()
        .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
        .WithPortBinding(10000, true)
        .WithCommand("azurite", "--blobHost", "0.0.0.0", "--skipApiVersionCheck")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Azurite Blob service is successfully listening"))
        .Build();

    public string ConnectionString =>
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        $"BlobEndpoint=http://{container.Hostname}:{container.GetMappedPublicPort(10000)}/devstoreaccount1;";

    public async ValueTask InitializeAsync() => await container.StartAsync();
    public async ValueTask DisposeAsync() => await container.DisposeAsync();
}

public class ObjectStoreTests(AzuriteFixture azurite, RedisFixture redis) : IClassFixture<AzuriteFixture>, IClassFixture<RedisFixture>
{
    private static string Unique(string p) => p + "-" + Guid.NewGuid().ToString("N")[..8];

    private static Envelope Msg(string id, string body = """{"n":1}""", string? type = "Orange.Models.Bet.Placed", string? correlation = "corr-1", string? partition = "pk-") =>
        new(id, body, type, new Dictionary<string, string>
        {
            ["correlationId"] = correlation ?? "", ["partitionKey"] = partition ?? "",
        }.Where(kv => kv.Value != "").ToDictionary());

    private DestinationConfig BlobConfig(string container, string template = "{type}-{correlationId}-{partitionKey}{id}", string strip = "Orange.Models.") => new()
    {
        Transport = "objectstore", Backend = "azure-blob", ConnectionString = azurite.ConnectionString,
        Container = container, NameTemplate = template, StripTypePrefix = strip,
    };

    /// <summary>The plat-events2blob naming formula, verbatim, as the parity oracle.</summary>
    private static string LegacyName(string type, string correlationId, string partitionKey, string offset) =>
        type.Replace("Orange.Models.", string.Empty) + "-" + correlationId + "-" + partitionKey + offset;

    [Fact]
    public async Task Parity_with_events2blob_same_object_name_and_bytes()
    {
        var container = Unique("parity");
        var cfg = BlobConfig(container);
        var dest = new ObjectStoreDestination(new AzureBlobStore(cfg), cfg);
        var m = Msg("1712345678901-0", body: """{"bet":"abc","stake":12.5,"note":"héllo ✓"}""");

        await dest.SendAsync(m, default);

        var blob = new BlobContainerClient(azurite.ConnectionString, container).GetBlobClient(LegacyName(m.Type!, "corr-1", "pk-", "1712345678901-0"));
        Assert.True(await blob.ExistsAsync(TestContext.Current.CancellationToken));
        var stored = (await blob.DownloadContentAsync(TestContext.Current.CancellationToken)).Value.Content.ToArray();
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(m.Payload), stored);
    }

    [Fact]
    public async Task A_redelivery_is_a_no_op_and_never_overwrites()
    {
        var container = Unique("idem");
        var cfg = BlobConfig(container);
        var dest = new ObjectStoreDestination(new AzureBlobStore(cfg), cfg);

        await dest.SendAsync(Msg("5-0", body: "first"), default);
        await dest.SendAsync(Msg("5-0", body: "second"), default); // must not throw (legacy swallowed 409) nor overwrite

        var blob = new BlobContainerClient(azurite.ConnectionString, container).GetBlobClient(LegacyName("Orange.Models.Bet.Placed", "corr-1", "pk-", "5-0"));
        Assert.Equal("first", (await blob.DownloadContentAsync(TestContext.Current.CancellationToken)).Value.Content.ToString());
    }

    [Fact]
    public async Task Redis_to_blob_through_the_whole_host()
    {
        var container = Unique("e2e");
        var stream = Unique("s");
        var mux = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        var id = (string)(await mux.GetDatabase().StreamAddAsync(stream,
            [new("data", """{"x":1}"""), new("type", "Orange.Models.Bet.Placed"), new("correlationId", "c9"), new("partitionKey", "p")]))!;

        await using var host = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("Host:RedisConnectionString", redis.ConnectionString);
            b.UseSetting("Host:PollIntervalMs", "20");
            b.UseSetting("Pipes:0:Name", "to-blob");
            b.UseSetting("Pipes:0:Source:Transport", "redis");
            b.UseSetting("Pipes:0:Source:Stream", stream);
            b.UseSetting("Pipes:0:Source:ConsumerGroup", "g");
            b.UseSetting("Pipes:0:Source:StartFrom", "Beginning");
            b.UseSetting("Pipes:0:Destination:Transport", "objectstore");
            b.UseSetting("Pipes:0:Destination:Backend", "azure-blob");
            b.UseSetting("Pipes:0:Destination:ConnectionString", azurite.ConnectionString);
            b.UseSetting("Pipes:0:Destination:Container", container);
            b.UseSetting("Pipes:0:Destination:NameTemplate", "{type}-{correlationId}-{partitionKey}{id}");
            b.UseSetting("Pipes:0:Destination:StripTypePrefix", "Orange.Models.");
        });
        using var client = host.CreateClient();

        var blob = new BlobContainerClient(azurite.ConnectionString, container).GetBlobClient(LegacyName("Orange.Models.Bet.Placed", "c9", "p", id));
        Assert.True(await Receiver.WaitFor(() => blob.Exists().Value, TimeSpan.FromSeconds(20)));
        Assert.Equal("""{"x":1}""", (await blob.DownloadContentAsync(TestContext.Current.CancellationToken)).Value.Content.ToString());
    }

    [Fact]
    public async Task File_backend_writes_and_is_idempotent()
    {
        var dir = Path.Combine(Path.GetTempPath(), Unique("objs"));
        var cfg = new DestinationConfig { Transport = "objectstore", Backend = "file", Container = dir, NameTemplate = "{date}/{type}-{id}" };
        var dest = new ObjectStoreDestination(new FileStore(dir), cfg);

        await dest.SendAsync(Msg("7-0", body: "a", type: "T"), default);
        await dest.SendAsync(Msg("7-0", body: "b", type: "T"), default);

        var file = Assert.Single(Directory.GetFiles(dir, "*", SearchOption.AllDirectories));
        Assert.Equal("a", File.ReadAllText(file));
        Assert.EndsWith("T-7-0", file);
        Directory.Delete(dir, true);
    }

    [Fact]
    public async Task File_backend_cannot_be_steered_outside_its_root_by_message_data()
    {
        var dir = Path.Combine(Path.GetTempPath(), Unique("objs"));
        var cfg = new DestinationConfig { Transport = "objectstore", Backend = "file", Container = dir, NameTemplate = "{type}/{id}" };
        var dest = new ObjectStoreDestination(new FileStore(dir), cfg);

        await dest.SendAsync(Msg("8-0", type: "../../escape"), default);

        Assert.All(Directory.GetFiles(dir, "*", SearchOption.AllDirectories), f => Assert.StartsWith(Path.GetFullPath(dir), Path.GetFullPath(f)));
        Assert.False(File.Exists(Path.Combine(dir, "..", "..", "escape", "8-0")));
        Directory.Delete(dir, true);
    }

    [Fact]
    public void Validation_covers_the_objectstore_options()
    {
        PipeConfig P(Action<DestinationConfig> t)
        {
            var d = new DestinationConfig { Transport = "objectstore", Backend = "azure-blob", ConnectionString = "x", Container = "c" };
            t(d);
            return new() { Name = "o", Source = new() { Transport = "redis", Stream = "s", ConsumerGroup = "g" }, Destination = d };
        }

        Assert.Empty(PipeValidator.Validate([P(_ => { })], new() { RedisConnectionString = "x" }));
        Assert.Contains(PipeValidator.Validate([P(d => d.Backend = "s3")]), e => e.Contains("Backend"));
        Assert.Contains(PipeValidator.Validate([P(d => d.Container = "")]), e => e.Contains("Container"));
        Assert.Contains(PipeValidator.Validate([P(d => d.ServiceUri = "https://a")]), e => e.Contains("exactly one"));
        Assert.Contains(PipeValidator.Validate([P(d => d.NameTemplate = "{type}")]), e => e.Contains("{id}"));
        Assert.Contains(PipeValidator.Validate([P(d => d.NameTemplate = "{id}{bogus}")]), e => e.Contains("{bogus}"));
    }
}
