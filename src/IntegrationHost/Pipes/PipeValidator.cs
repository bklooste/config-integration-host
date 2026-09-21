namespace IntegrationHost.Pipes;

/// <summary>
/// Validates the pipe set with no I/O, so it runs identically at startup and in <c>--validate</c> dry-run.
/// Every problem is reported, not just the first, and each names the pipe.
/// </summary>
public static class PipeValidator
{
    private static readonly HashSet<string> Methods = new(StringComparer.OrdinalIgnoreCase) { "POST", "PUT", "PATCH", "DELETE" };

    public static IReadOnlyList<string> Validate(IReadOnlyList<PipeConfig> pipes, IntegrationHostOptions? host = null)
    {
        var errors = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < pipes.Count; i++)
        {
            var p = pipes[i];
            var id = string.IsNullOrWhiteSpace(p.Name) ? $"Pipes[{i}]" : $"Pipe '{p.Name}'";
            void Err(string msg) => errors.Add($"{id}: {msg}");

            if (string.IsNullOrWhiteSpace(p.Name)) Err("Name is required.");
            else if (!seen.Add(p.Name)) Err("Name is not unique.");

            // Disabled pipes are still checked: a broken commented-out pipe should not be a surprise when it is enabled.
            var s = p.Source;
            switch (s.Transport.ToLowerInvariant())
            {
                case "redis":
                    if (string.IsNullOrWhiteSpace(s.Stream)) Err("Source.Stream is required for a redis source.");
                    if (string.IsNullOrWhiteSpace(s.ConsumerGroup)) Err("Source.ConsumerGroup is required.");
                    if (s.Partitions is < 1 or > 1024) Err("Source.Partitions must be 1-1024.");
                    if (s.Partitions > 1 && !s.Stream.Contains("{partition}", StringComparison.Ordinal)) Err("Source.Stream must contain {partition} when Partitions > 1.");
                    if (s.ClaimIdleSeconds < 1) Err("Source.ClaimIdleSeconds must be at least 1.");
                    if (!s.StartFrom.Equals("End", StringComparison.OrdinalIgnoreCase) && !s.StartFrom.Equals("Beginning", StringComparison.OrdinalIgnoreCase))
                        Err($"Source.StartFrom '{s.StartFrom}' must be End or Beginning.");
                    break;
                case "eventhubs":
                    ValidateEventHubs("Source", s.ConnectionString, s.Namespace, s.EventHub, Err);
                    if (string.IsNullOrWhiteSpace(s.ConsumerGroup)) Err("Source.ConsumerGroup is required (e.g. $Default).");
                    if (string.IsNullOrWhiteSpace(s.CheckpointConnectionString) == string.IsNullOrWhiteSpace(s.CheckpointContainerUri))
                        Err("Source needs exactly one of CheckpointConnectionString or CheckpointContainerUri (an eventhubs source must checkpoint).");
                    break;
                default:
                    Err($"Source.Transport '{s.Transport}' is not supported (supported: redis, eventhubs).");
                    break;
            }
            if (s.BatchSize is < 1 or > 1000) Err("Source.BatchSize must be 1-1000.");

            if (p.Map is { } m)
            {
                if (string.IsNullOrWhiteSpace(m.Template) && string.IsNullOrWhiteSpace(m.Handler)) Err("Map is present but names no Template; remove Map for passthrough.");
                if (!m.OnNoMatch.Equals("skip", StringComparison.OrdinalIgnoreCase) && !m.OnNoMatch.Equals("fail", StringComparison.OrdinalIgnoreCase))
                    Err($"Map.OnNoMatch '{m.OnNoMatch}' must be skip or fail.");
                if (!string.IsNullOrWhiteSpace(m.Template) && p.Enabled && host is not null
                    && !(Uri.TryCreate(host.RuleEngineUrl, UriKind.Absolute, out var re) && re.Scheme is "http" or "https"))
                    Err("uses Map.Template but Host:RuleEngineUrl is not set to an absolute http(s) URL.");
                if (!string.IsNullOrWhiteSpace(m.Handler)) Err("Map.Handler is not supported by this version (code maps are planned); remove Map for passthrough.");
            }

            var d = p.Destination;
            switch (d.Transport.ToLowerInvariant())
            {
                case "http":
                    if (!Uri.TryCreate(d.Url, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
                        Err($"Destination.Url '{d.Url}' must be an absolute http(s) URL.");
                    if (!Methods.Contains(d.Method)) Err($"Destination.Method '{d.Method}' must be one of {string.Join(", ", Methods)}.");
                    if (d.TimeoutSeconds is < 1 or > 3600) Err("Destination.TimeoutSeconds must be 1-3600.");
                    break;
                case "redis":
                    if (string.IsNullOrWhiteSpace(d.Stream)) Err("Destination.Stream is required for a redis destination.");
                    if (d.MaxLength < 0) Err("Destination.MaxLength must be >= 0.");
                    break;
                case "eventhubs":
                    ValidateEventHubs("Destination", d.ConnectionString, d.Namespace, d.EventHub, Err);
                    break;
                case "objectstore":
                    ValidateObjectStore(d, Err);
                    break;
                default:
                    Err($"Destination.Transport '{d.Transport}' is not supported (supported: http, redis, eventhubs, objectstore).");
                    break;
            }

            if (!PipeConfig.TryParsePolicy(p.OnFailure, out _)) Err($"OnFailure '{p.OnFailure}' must be block or skip-and-alert.");
            if (p.Enabled && host is not null && string.IsNullOrWhiteSpace(host.RedisConnectionString)
                && (s.Transport.Equals("redis", StringComparison.OrdinalIgnoreCase) || d.Transport.Equals("redis", StringComparison.OrdinalIgnoreCase)))
                Err("uses redis but Host:RedisConnectionString is not set.");

            var r = p.Retry;
            if (r.MaxAttempts < 1) Err("Retry.MaxAttempts must be at least 1.");
            if (r.InitialDelayMs < 0 || r.MaxDelayMs < r.InitialDelayMs) Err("Retry delays must satisfy 0 <= InitialDelayMs <= MaxDelayMs.");
        }

        return errors;
    }

    private static readonly HashSet<string> NameTokens = new(StringComparer.Ordinal) { "id", "entryId", "type", "correlationId", "partitionKey", "date" };

    private static void ValidateObjectStore(DestinationConfig d, Action<string> err)
    {
        var backend = d.Backend.ToLowerInvariant();
        if (backend is not ("azure-blob" or "file")) err($"Destination.Backend '{d.Backend}' must be azure-blob or file.");
        if (string.IsNullOrWhiteSpace(d.Container)) err("Destination.Container is required for an objectstore destination.");
        var hasCs = !string.IsNullOrWhiteSpace(d.ConnectionString);
        var hasUri = !string.IsNullOrWhiteSpace(d.ServiceUri);
        if (backend == "azure-blob" && !hasCs && !hasUri) err("Destination (azure-blob) needs ConnectionString, or ServiceUri (optionally with ConnectionString or AccountName/AccountKey for credentials).");
        if (hasCs && hasUri && !string.IsNullOrWhiteSpace(d.AccountKey)) err("Destination: give credentials as ConnectionString or AccountName/AccountKey, not both.");
        var hasName = !string.IsNullOrWhiteSpace(d.AccountName);
        var hasKey = !string.IsNullOrWhiteSpace(d.AccountKey);
        if (hasName != hasKey) err("Destination.AccountName and AccountKey must be set together.");
        if (hasKey && !hasUri) err("Destination.AccountKey needs ServiceUri (shared-key access to that account).");
        var tokens = System.Text.RegularExpressions.Regex.Matches(d.NameTemplate, @"\{([^{}]*)\}").Select(m => m.Groups[1].Value).ToList();
        foreach (var t in tokens.Where(t => !NameTokens.Contains(t)).Distinct())
            err($"Destination.NameTemplate has unknown token '{{{t}}}' (allowed: {string.Join(", ", NameTokens.Select(n => "{" + n + "}"))}).");
        if (!tokens.Contains("id") && !tokens.Contains("entryId")) err("Destination.NameTemplate must contain {id} or {entryId}, or distinct messages would overwrite/skip each other.");
    }

    private static void ValidateEventHubs(string side, string connectionString, string ns, string hub, Action<string> err)
    {
        if (string.IsNullOrWhiteSpace(hub)) err($"{side}.EventHub is required for an eventhubs {side.ToLowerInvariant()}.");
        var hasCs = !string.IsNullOrWhiteSpace(connectionString);
        var hasNs = !string.IsNullOrWhiteSpace(ns);
        if (hasCs == hasNs) err($"{side} needs exactly one of ConnectionString or Namespace (managed identity).");
    }

    /// <summary>Throws with every error listed, so a bad config fails the container at start with a readable message.</summary>
    public static void ThrowIfInvalid(IReadOnlyList<PipeConfig> pipes, IntegrationHostOptions? host = null)
    {
        var errors = Validate(pipes, host);
        if (errors.Count > 0)
            throw new PipeConfigException("Invalid pipe configuration:" + Environment.NewLine + string.Join(Environment.NewLine, errors.Select(e => "  - " + e)));
    }
}

public sealed class PipeConfigException(string message) : Exception(message);
