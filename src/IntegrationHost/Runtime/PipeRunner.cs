using System.Diagnostics;
using IntegrationHost.Pipes;

namespace IntegrationHost.Runtime;

/// <summary>
/// Drives one pipe: read → filter → (map) → deliver with retry → ack. The ack is the last step, so a crash at any
/// earlier point redelivers the message (at-least-once). The map step is passthrough in this version.
/// </summary>
public sealed class PipeRunner(
    PipeConfig pipe, IMessageSource source, IDestination destination,
    PipeState state, PipeMetrics metrics, ILogger logger, TimeSpan pollInterval, IMapper? map = null) : BackgroundService
{
    private static readonly TimeSpan MaxLoopBackoff = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var started = false;
        var loopBackoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!started) { if (map is not null) await map.CheckAsync(ct); await source.StartAsync(ct); started = true; state.Running(); }
                var batch = await source.ReadAsync(ct);
                if (batch.Count == 0) { state.Running(); await Task.Delay(pollInterval, ct); continue; }
                if (pipe.Concurrency <= 1)
                    foreach (var message in batch) await ProcessAsync(message, ct);
                else
                    await Parallel.ForEachAsync(batch, new ParallelOptions { MaxDegreeOfParallelism = pipe.Concurrency, CancellationToken = ct },
                        async (message, token) => await ProcessAsync(message, token));
                loopBackoff = TimeSpan.FromSeconds(1);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                // Broker unreachable, ack failed, ...: surface it in health, back off, and try again. Unacked
                // messages stay pending and are redelivered, never lost.
                state.Faulted(ex.Message);
                logger.LogError(ex, "Pipe {Pipe}: source error, retrying in {Delay}", pipe.Name, loopBackoff);
                try { await Task.Delay(loopBackoff, ct); } catch (OperationCanceledException) { break; }
                loopBackoff = TimeSpan.FromSeconds(Math.Min(loopBackoff.TotalSeconds * 2, MaxLoopBackoff.TotalSeconds));
            }
        }
    }

    internal async Task ProcessAsync(Envelope message, CancellationToken ct)
    {
        metrics.Consumed();

        // Continue the producer's trace: the outgoing call becomes a child of the message's traceparent.
        ActivityContext.TryParse(message.Headers.GetValueOrDefault("traceparent"), null, out var parent);
        using var activity = PipeMetrics.Tracing.StartActivity($"{pipe.Name} process", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.message.id", message.Id);

        var filter = pipe.Source.TypeFilter;
        if (filter.Count > 0 && (message.Type is null || !filter.Contains(message.Type)))
        {
            metrics.Filtered();
            await source.AckAsync(message, ct);
            return;
        }

        if (await DeliverAsync(message, ct))
            await source.AckAsync(message, ct);
    }

    /// <summary>Returns true if the message is finished with (delivered or deliberately skipped) and should be acked.</summary>
    private async Task<bool> DeliverAsync(Envelope message, CancellationToken ct)
    {
        var retry = pipe.Retry;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var outgoing = message;
                if (map is not null)
                {
                    var mapped = await map.MapAsync(message, ct);
                    if (mapped is null)
                    {
                        if (pipe.Map!.OnNoMatch.Equals("fail", StringComparison.OrdinalIgnoreCase))
                            throw new InvalidOperationException($"no rule in template '{pipe.Map.Template}' matched");
                        metrics.Unmapped();
                        logger.LogWarning("Pipe {Pipe}: message {MessageId} matched no rule in template {Template}; skipped", pipe.Name, message.Id, pipe.Map.Template);
                        return true;
                    }
                    metrics.Mapped();
                    outgoing = message with { Payload = mapped, RawBody = null };
                }
                await destination.SendAsync(outgoing, ct);
                metrics.Sent();
                state.Delivered();
                return true;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                metrics.Failed();
                if (attempt >= retry.MaxAttempts)
                {
                    if (pipe.Policy == FailurePolicy.SkipAndAlert)
                    {
                        metrics.Skipped();
                        logger.LogError(ex, "Pipe {Pipe}: message {MessageId} skipped after {Attempts} attempts", pipe.Name, message.Id, attempt);
                        return true;
                    }
                    // Block: never ack, keep retrying at the ceiling, and report unhealthy until it gets through.
                    state.Blocked($"message {message.Id}: {ex.Message}");
                    logger.LogError(ex, "Pipe {Pipe}: blocked on message {MessageId} after {Attempts} attempts", pipe.Name, message.Id, attempt);
                }
                else
                {
                    logger.LogWarning(ex, "Pipe {Pipe}: attempt {Attempt}/{Max} for message {MessageId} failed", pipe.Name, attempt, retry.MaxAttempts, message.Id);
                }
                await Task.Delay(Backoff(retry, attempt), ct);
            }
        }
    }

    internal static TimeSpan Backoff(RetryConfig retry, int attempt)
    {
        var ms = retry.InitialDelayMs * Math.Pow(2, Math.Min(attempt - 1, 20));
        return TimeSpan.FromMilliseconds(Math.Min(ms, retry.MaxDelayMs));
    }
}
