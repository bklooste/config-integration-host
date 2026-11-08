namespace IntegrationHost.Runtime;

/// <summary>
/// One message in flight. <see cref="Id"/> is the source's own identifier (the Redis stream id) and is the
/// dedupe key propagated to the destination — delivery is at-least-once, so receivers must dedupe on it.
/// </summary>
public sealed record Envelope(string Id, string Payload, string? Type, IReadOnlyDictionary<string, string> Headers)
{
    /// <summary>The message body exactly as it arrived, when it may not be valid UTF-8 text. Cleared by a map (the payload is then new).</summary>
    public byte[]? RawBody { get; init; }

    /// <summary>The source's own id, unqualified (the Redis entry id, without any partition prefix). Available to object names as <c>{entryId}</c>.</summary>
    public string? EntryId { get; init; }

    /// <summary>Which source stream/partition this came from, so it can be acked there.</summary>
    public string? SourceStream { get; init; }

    /// <summary>The bytes to store or send verbatim: the raw body if there is one, else the payload as UTF-8.</summary>
    public byte[] Body => RawBody ?? System.Text.Encoding.UTF8.GetBytes(Payload);
}

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

/// <summary>Reshapes a message payload between source and destination.</summary>
public interface IMapper
{
    /// <summary>Verifies the map can work (e.g. the template exists). Throws with a clear message if not; the runner retries and reports the pipe unhealthy.</summary>
    Task CheckAsync(CancellationToken ct);

    /// <summary>Returns the new payload, or null when the mapping produced nothing (no rule matched). Throws on any failure.</summary>
    Task<string?> MapAsync(Envelope message, CancellationToken ct);
}
