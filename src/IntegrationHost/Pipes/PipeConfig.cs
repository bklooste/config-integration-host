namespace IntegrationHost.Pipes;

/// <summary>What to do when a message still fails after all retries.</summary>
public enum FailurePolicy
{
    /// <summary>Never ack; keep retrying at the maximum backoff and report the pipe unhealthy. Nothing is lost, the pipe stalls.</summary>
    Block,
    /// <summary>Log the failure with the message id, count it, ack, and move on. Nothing stalls, the message is dropped.</summary>
    SkipAndAlert,
}

/// <summary>One config-defined pipe: source → filter → map → destination, with a failure policy.</summary>
public sealed class PipeConfig
{
    public string Name { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public SourceConfig Source { get; set; } = new();
    public MapConfig? Map { get; set; }
    public DestinationConfig Destination { get; set; } = new();

    /// <summary><c>block</c> (default) or <c>skip-and-alert</c>.</summary>
    public string OnFailure { get; set; } = "block";

    /// <summary><see cref="OnFailure"/> parsed; unrecognised values are reported by <see cref="PipeValidator"/> and treated as Block.</summary>
    public FailurePolicy Policy => TryParsePolicy(OnFailure, out var p) ? p : FailurePolicy.Block;

    public static bool TryParsePolicy(string? value, out FailurePolicy policy)
    {
        switch ((value ?? "").Replace("-", "").Replace("_", "").ToLowerInvariant())
        {
            case "block": policy = FailurePolicy.Block; return true;
            case "skipandalert": policy = FailurePolicy.SkipAndAlert; return true;
            default: policy = FailurePolicy.Block; return false;
        }
    }
    public RetryConfig Retry { get; set; } = new();
}

public sealed class SourceConfig
{
    /// <summary><c>redis</c>. (<c>eventhubs</c> and <c>kafka</c> are planned.)</summary>
    public string Transport { get; set; } = "";

    /// <summary>Redis stream key to read, exactly as stored (include any environment prefix).</summary>
    public string Stream { get; set; } = "";

    /// <summary>Consumer group for this pipe. One group per pipe; replicas share it.</summary>
    public string ConsumerGroup { get; set; } = "";

    /// <summary>Where a brand-new group starts: <c>End</c> (default, only new messages) or <c>Beginning</c> (the whole stream).</summary>
    public string StartFrom { get; set; } = "End";

    /// <summary>Stream field holding the message payload. If absent, all fields are sent as a JSON object.</summary>
    public string PayloadField { get; set; } = "data";

    /// <summary>Stream field holding the message type, used by <see cref="TypeFilter"/>.</summary>
    public string TypeField { get; set; } = "type";

    /// <summary>If non-empty, only messages whose type is listed are delivered; others are acked and counted as filtered.</summary>
    public List<string> TypeFilter { get; set; } = [];

    /// <summary>Seconds a delivered-but-unacked message may sit idle before another replica claims it (crash recovery).</summary>
    public int ClaimIdleSeconds { get; set; } = 60;

    /// <summary>Max messages per read.</summary>
    public int BatchSize { get; set; } = 50;
}

public sealed class MapConfig
{
    /// <summary>Template name for a template map (rule-engine-service v2). Not yet supported by this version.</summary>
    public string? Template { get; set; }

    /// <summary>Compiled handler name for a code map. Not yet supported by this version.</summary>
    public string? Handler { get; set; }
}

public sealed class DestinationConfig
{
    /// <summary><c>http</c>. (<c>eventhubs</c>, <c>kafka</c> and <c>objectstore</c> are planned.)</summary>
    public string Transport { get; set; } = "";

    /// <summary>Absolute http(s) URL the payload is sent to.</summary>
    public string Url { get; set; } = "";

    public string Method { get; set; } = "POST";
    public Dictionary<string, string> Headers { get; set; } = new();
    public int TimeoutSeconds { get; set; } = 30;
}

public sealed class RetryConfig
{
    /// <summary>Delivery attempts before <see cref="PipeConfig.OnFailure"/> applies.</summary>
    public int MaxAttempts { get; set; } = 5;
    public int InitialDelayMs { get; set; } = 200;
    public int MaxDelayMs { get; set; } = 30_000;
}
