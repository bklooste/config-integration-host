using Azure.Identity;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;

namespace IntegrationHost.Runtime;

/// <summary>Builds Event Hubs clients from either a connection string or a namespace + DefaultAzureCredential (managed identity).</summary>
internal static class EventHubsClients
{
    public static EventHubProducerClient Producer(string connectionString, string ns, string hub) =>
        string.IsNullOrWhiteSpace(connectionString)
            ? new EventHubProducerClient(ns, hub, new DefaultAzureCredential())
            : new EventHubProducerClient(connectionString, hub);

    public static EventHubConsumerClient Consumer(string group, string connectionString, string ns, string hub) =>
        string.IsNullOrWhiteSpace(connectionString)
            ? new EventHubConsumerClient(group, ns, hub, new DefaultAzureCredential())
            : new EventHubConsumerClient(group, connectionString, hub);
}
