using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace IntegrationHost.Tests;

/// <summary>Event Hubs emulator + Azurite (for checkpoints). The emulator's AMQP port is fixed at 5672, so tests using it share one collection.</summary>
public sealed class EventHubsFixture : IAsyncLifetime
{
    public const string Hub = "eh1";
    public const string Group = "cg1";
    public const string EmulatorConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private const string Config = """
        {"UserConfig":{"NamespaceConfig":[{"Type":"EventHub","Name":"emulator","Entities":[
          {"Name":"eh1","PartitionCount":"2","ConsumerGroups":[{"Name":"cg1"},{"Name":"cg2"},{"Name":"cg3"}]},
          {"Name":"eh2","PartitionCount":"2","ConsumerGroups":[{"Name":"cg1"}]}]}],
          "LoggingConfig":{"Type":"File"}}}
        """;

    private readonly INetwork network = new NetworkBuilder().Build();
    private IContainer azurite = null!;
    private IContainer emulator = null!;

    public string StorageConnectionString =>
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        $"BlobEndpoint=http://{azurite.Hostname}:{azurite.GetMappedPublicPort(10000)}/devstoreaccount1;";

    public async ValueTask InitializeAsync()
    {
        await network.CreateAsync();
        azurite = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .WithNetwork(network).WithNetworkAliases("azurite")
            .WithPortBinding(10000, true)
            .WithCommand("azurite", "--blobHost", "0.0.0.0", "--queueHost", "0.0.0.0", "--tableHost", "0.0.0.0", "--skipApiVersionCheck")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Azurite Blob service is successfully listening"))
            .Build();
        await azurite.StartAsync();

        emulator = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
            .WithNetwork(network)
            .WithPortBinding(5672, 5672)
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("BLOB_SERVER", "azurite")
            .WithEnvironment("METADATA_SERVER", "azurite")
            .WithResourceMapping(Encoding.UTF8.GetBytes(Config), "/Eventhubs_Emulator/ConfigFiles/Config.json")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Emulator Service is Successfully Up"))
            .Build();
        await emulator.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await emulator.DisposeAsync();
        await azurite.DisposeAsync();
        await network.DisposeAsync();
    }
}

[CollectionDefinition("eventhubs")]
public class EventHubsCollection : ICollectionFixture<EventHubsFixture>, ICollectionFixture<RedisFixture>;
