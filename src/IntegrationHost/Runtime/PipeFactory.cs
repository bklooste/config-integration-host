using IntegrationHost.Pipes;
using StackExchange.Redis;

namespace IntegrationHost.Runtime;

/// <summary>Builds the transports named in a pipe's config. Validation has already guaranteed the names are known.</summary>
public sealed class PipeFactory(IServiceProvider services, IntegrationHostOptions host)
{
    public IMessageSource CreateSource(PipeConfig pipe, ILogger logger) => pipe.Source.Transport.ToLowerInvariant() switch
    {
        "redis" => new RedisStreamSource(services.GetRequiredService<IConnectionMultiplexer>(), pipe.Source, host.ConsumerName),
        "eventhubs" => new EventHubsSource(pipe.Source, logger),
        var t => throw new InvalidOperationException($"Unknown source transport '{t}'"),
    };

    public IDestination CreateDestination(PipeConfig pipe) => pipe.Destination.Transport.ToLowerInvariant() switch
    {
        "http" => new HttpDestination(services.GetRequiredService<IHttpClientFactory>().CreateClient("pipe:" + pipe.Name), pipe.Destination),
        "redis" => new RedisDestination(services.GetRequiredService<IConnectionMultiplexer>(), pipe.Destination),
        "eventhubs" => new EventHubsDestination(
            EventHubsClients.Producer(pipe.Destination.ConnectionString, pipe.Destination.Namespace, pipe.Destination.EventHub), pipe.Destination),
        "objectstore" => new ObjectStoreDestination(
            pipe.Destination.Backend.Equals("file", StringComparison.OrdinalIgnoreCase) ? new FileStore(pipe.Destination.Container) : new AzureBlobStore(pipe.Destination),
            pipe.Destination),
        var t => throw new InvalidOperationException($"Unknown destination transport '{t}'"),
    };
}
