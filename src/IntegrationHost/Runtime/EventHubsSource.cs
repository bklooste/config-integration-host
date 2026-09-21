using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Storage.Blobs;
using IntegrationHost.Pipes;

namespace IntegrationHost.Runtime;

/// <summary>
/// Reads an event hub through an <see cref="EventProcessorClient"/> (partition ownership shared across replicas, checkpoints
/// in blob storage). Adapts its push model to the host's pull/ack contract: each event waits in a channel until the runner
/// has delivered it, and the checkpoint is written only on <see cref="AckAsync"/>. A partition therefore has at most one
/// event in flight, and a crash redelivers from the last checkpoint — at-least-once, deduped on <c>hub/partition/sequence</c>.
/// </summary>
public sealed class EventHubsSource(SourceConfig config, ILogger logger) : IMessageSource, IAsyncDisposable
{
    private readonly Channel<Envelope> queue = Channel.CreateUnbounded<Envelope>();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> inFlight = new();
    private EventProcessorClient? processor;

    public async Task StartAsync(CancellationToken ct)
    {
        // Fail with a clear error if the hub is unreachable or unknown, instead of starting a processor that silently retries.
        await using (var probe = EventHubsClients.Consumer(config.ConsumerGroup, config.ConnectionString, config.Namespace, config.EventHub))
            _ = await probe.GetPartitionIdsAsync(ct);

        BlobContainerClient container = string.IsNullOrWhiteSpace(config.CheckpointContainerUri)
            ? new BlobContainerClient(config.CheckpointConnectionString, config.CheckpointContainer)
            : new BlobContainerClient(new Uri(config.CheckpointContainerUri), new DefaultAzureCredential());
        await container.CreateIfNotExistsAsync(cancellationToken: ct);

        var client = string.IsNullOrWhiteSpace(config.ConnectionString)
            ? new EventProcessorClient(container, config.ConsumerGroup, config.Namespace, config.EventHub, new DefaultAzureCredential())
            : new EventProcessorClient(container, config.ConsumerGroup, config.ConnectionString, config.EventHub);
        client.ProcessEventAsync += OnEventAsync;
        client.ProcessErrorAsync += args =>
        {
            logger.LogError(args.Exception, "Event Hubs {Operation} error on partition {Partition}", args.Operation, args.PartitionId);
            return Task.CompletedTask;
        };
        await client.StartProcessingAsync(ct);
        processor = client;
    }

    private async Task OnEventAsync(ProcessEventArgs args)
    {
        if (!args.HasEvent) return;
        var e = args.Data;
        var id = $"{config.EventHub}/{args.Partition.PartitionId}/{e.SequenceNumber}";

        var headers = new Dictionary<string, string>();
        foreach (var h in new[] { "traceparent", "correlationId", "correlation_id" })
            if (e.Properties.TryGetValue(h, out var v) && v is not null) headers[h] = v.ToString()!;
        var type = e.Properties.TryGetValue("type", out var t) ? t?.ToString() : null;

        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        inFlight[id] = done;
        try
        {
            await queue.Writer.WriteAsync(new Envelope(id, Encoding.UTF8.GetString(e.EventBody.ToMemory().Span), type, headers), args.CancellationToken);
            await done.Task.WaitAsync(args.CancellationToken); // released by AckAsync; cancelled on shutdown/rebalance => not checkpointed
            await args.UpdateCheckpointAsync(args.CancellationToken);
        }
        finally { inFlight.TryRemove(id, out _); }
    }

    public Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct)
    {
        var batch = new List<Envelope>();
        while (batch.Count < config.BatchSize && queue.Reader.TryRead(out var m)) batch.Add(m);
        return Task.FromResult<IReadOnlyList<Envelope>>(batch);
    }

    public Task AckAsync(Envelope message, CancellationToken ct)
    {
        if (inFlight.TryGetValue(message.Id, out var done)) done.TrySetResult();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (processor is not null) await processor.StopProcessingAsync();
    }
}
