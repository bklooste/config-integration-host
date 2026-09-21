using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using IntegrationHost.Pipes;

namespace IntegrationHost.Runtime;

/// <summary>
/// Publishes each message as one event. Event Hubs has no idempotent producer, so the source message id travels as the
/// event's <c>MessageId</c> and an <c>idempotency-key</c> property — consumers must dedupe on it.
/// </summary>
public sealed class EventHubsDestination(EventHubProducerClient producer, DestinationConfig config) : IDestination
{
    public async Task SendAsync(Envelope message, CancellationToken ct)
    {
        var data = new EventData(Encoding.UTF8.GetBytes(message.Payload)) { ContentType = "application/json", MessageId = message.Id };
        data.Properties["idempotency-key"] = message.Id;
        if (message.Type is not null) data.Properties["type"] = message.Type;
        foreach (var (k, v) in message.Headers) data.Properties[k] = v;

        var options = string.IsNullOrEmpty(config.PartitionKey) ? new SendEventOptions() : new SendEventOptions { PartitionKey = config.PartitionKey };
        await producer.SendAsync([data], options, ct);
    }
}
