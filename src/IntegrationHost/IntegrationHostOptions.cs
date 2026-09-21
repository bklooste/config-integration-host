using System.ComponentModel.DataAnnotations;

namespace IntegrationHost;

/// <summary>
/// Host-level settings (env <c>Host__PropertyName</c>). Pipes are configured separately, in the top-level
/// <c>Pipes</c> array. Every property must appear in the README config table — <c>ReadmeConfigTableTests</c> enforces it.
/// </summary>
public sealed class IntegrationHostOptions
{
    public const string SectionName = "Host";

    /// <summary>Redis connection string for <c>redis</c> sources. Required when any enabled pipe uses a <c>redis</c> source.</summary>
    public string RedisConnectionString { get; set; } = "";

    /// <summary>Base URL of a rule-engine-service (v2) used by <c>Map.Template</c>. Required when any enabled pipe has a template map.</summary>
    public string RuleEngineUrl { get; set; } = "";

    /// <summary>Per-request timeout (seconds) for rule-engine calls.</summary>
    [Range(1, 300)]
    public int RuleEngineTimeoutSeconds { get; set; } = 10;

    /// <summary>Consumer name used inside every pipe's consumer group. Give each replica a distinct one; defaults to the machine name.</summary>
    public string ConsumerName { get; set; } = Environment.MachineName;

    /// <summary>How long (ms) a source waits for new messages before polling again.</summary>
    [Range(10, 60_000)]
    public int PollIntervalMs { get; set; } = 500;

    /// <summary>Seconds to wait on shutdown for in-flight messages to finish.</summary>
    [Range(1, 600)]
    public int ShutdownTimeoutSeconds { get; set; } = 30;
}
