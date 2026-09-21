namespace IntegrationHost.Runtime;

/// <summary>
/// One message in flight. <see cref="Id"/> is the source's own identifier (the Redis stream id) and is the
/// dedupe key propagated to the destination — delivery is at-least-once, so receivers must dedupe on it.
/// </summary>
public sealed record Envelope(string Id, string Payload, string? Type, IReadOnlyDictionary<string, string> Headers);

/// <summary>A pull source with explicit acknowledgement. Nothing is acked until the destination has accepted the message.</summary>
public interface IMessageSource
{
    /// <summary>Idempotent set-up (create the consumer group, etc.). May throw if the broker is unreachable; the runner retries.</summary>
    Task StartAsync(CancellationToken ct);

    /// <summary>Returns the next batch, previously-unacked messages first. Empty means nothing available right now.</summary>
    Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct);

    Task AckAsync(Envelope message, CancellationToken ct);
}

/// <summary>Delivers one message. Any failure — including a non-success response — must throw.</summary>
public interface IDestination
{
    Task SendAsync(Envelope message, CancellationToken ct);
}
