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
    /// <summary><c>redis</c> or <c>eventhubs</c>. (<c>kafka</c> is planned.)</summary>
    public string Transport { get; set; } = "";

    /// <summary>[redis] Stream key to read, exactly as stored (include any environment prefix).</summary>
    public string Stream { get; set; } = "";

    /// <summary>[redis] Number of partitioned streams. With more than one, <see cref="Stream"/> must contain <c>{partition}</c>, replaced by 0..N-1.</summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// [redis] Extra stream-field → header-name mappings, added to the defaults (<c>traceparent</c>, <c>correlationId</c>,
    /// <c>correlation_id</c>, <c>partitionKey</c>, <c>partition_key</c> map to themselves). Headers feed <c>{correlationId}</c>
    /// / <c>{partitionKey}</c> in object names, trace propagation and outgoing properties. Example: <c>{"c":"correlationId","k":"partitionKey","p":"traceparent"}</c>.
    /// </summary>
    public Dictionary<string, string> HeaderFields { get; set; } = new();

    /// <summary>[eventhubs] Connection string. Use this or <see cref="Namespace"/> (managed identity / DefaultAzureCredential).</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>[eventhubs] Fully-qualified namespace, e.g. <c>myns.servicebus.windows.net</c>.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>[eventhubs] Event hub name.</summary>
    public string EventHub { get; set; } = "";

    /// <summary>[eventhubs] Blob storage connection string for checkpoints. Use this or <see cref="CheckpointContainerUri"/>.</summary>
    public string CheckpointConnectionString { get; set; } = "";

    /// <summary>[eventhubs] Full blob container URI for checkpoints, accessed with DefaultAzureCredential.</summary>
    public string CheckpointContainerUri { get; set; } = "";

    /// <summary>[eventhubs] Checkpoint container name (with <see cref="CheckpointConnectionString"/>); created if missing.</summary>
    public string CheckpointContainer { get; set; } = "integration-host-checkpoints";

    /// <summary>Consumer group for this pipe. One group per pipe; replicas share it. For eventhubs use an event hub consumer group (e.g. <c>$Default</c>).</summary>
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
    /// <summary>
    /// Template id in a rule-engine-service (v2). Each message payload is evaluated against it and the merged fragment
    /// becomes the outgoing payload. Needs <c>Host:RuleEngineUrl</c>.
    /// </summary>
    public string? Template { get; set; }

    /// <summary>
    /// What to do when no rule matches (the engine answers <c>{}</c>): <c>skip</c> (default — ack, count as unmapped, send nothing)
    /// or <c>fail</c> (treat as a delivery failure, so <c>OnFailure</c> applies).
    /// </summary>
    public string OnNoMatch { get; set; } = "skip";

    /// <summary>Compiled handler name for a code map. Not yet supported by this version.</summary>
    public string? Handler { get; set; }
}

public sealed class DestinationConfig
{
    /// <summary><c>http</c>, <c>redis</c>, <c>eventhubs</c> or <c>objectstore</c>. (<c>kafka</c> is planned.)</summary>
    public string Transport { get; set; } = "";

    /// <summary>[http] Absolute http(s) URL the payload is sent to.</summary>
    public string Url { get; set; } = "";

    /// <summary>[redis] Stream key to append to.</summary>
    public string Stream { get; set; } = "";

    /// <summary>[redis] Approximate max stream length (XADD MAXLEN ~). 0 = unbounded.</summary>
    public int MaxLength { get; set; }

    /// <summary>[eventhubs] Connection string. Use this or <see cref="Namespace"/> (managed identity / DefaultAzureCredential).</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>[eventhubs] Fully-qualified namespace, e.g. <c>myns.servicebus.windows.net</c>.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>[eventhubs] Event hub name.</summary>
    public string EventHub { get; set; } = "";

    /// <summary>[objectstore] <c>azure-blob</c> or <c>file</c>.</summary>
    public string Backend { get; set; } = "";

    /// <summary>[objectstore] Blob container name (azure-blob; created if missing) or root directory (file).</summary>
    public string Container { get; set; } = "";

    /// <summary>[objectstore azure-blob] Storage account URI, accessed with DefaultAzureCredential. Use this or <see cref="ConnectionString"/>.</summary>
    public string ServiceUri { get; set; } = "";

    /// <summary>[objectstore] Object name template. Tokens: <c>{id}</c> or <c>{entryId}</c> (one required), <c>{type}</c>, <c>{correlationId}</c>, <c>{partitionKey}</c>, <c>{date}</c> (yyyy/MM/dd, UTC).</summary>
    public string NameTemplate { get; set; } = "{type}-{correlationId}-{id}";

    /// <summary>[objectstore] Removed from the start of <c>{type}</c> before naming (e.g. a namespace prefix). Empty = keep.</summary>
    public string StripTypePrefix { get; set; } = "";

    /// <summary>[eventhubs] Partition key for ordering; omitted = service-chosen partition.</summary>
    public string PartitionKey { get; set; } = "";

    /// <summary>[http] Method.</summary>
    public string Method { get; set; } = "POST";
    /// <summary>[http] Extra request headers.</summary>
    public Dictionary<string, string> Headers { get; set; } = new();
    /// <summary>[http] Per-request timeout.</summary>
    public int TimeoutSeconds { get; set; } = 30;
}

public sealed class RetryConfig
{
    /// <summary>Delivery attempts before <see cref="PipeConfig.OnFailure"/> applies.</summary>
    public int MaxAttempts { get; set; } = 5;
    public int InitialDelayMs { get; set; } = 200;
    public int MaxDelayMs { get; set; } = 30_000;
}
