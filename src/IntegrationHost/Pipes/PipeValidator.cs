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
            if (!string.Equals(s.Transport, "redis", StringComparison.OrdinalIgnoreCase))
                Err($"Source.Transport '{s.Transport}' is not supported (supported: redis).");
            if (string.IsNullOrWhiteSpace(s.Stream)) Err("Source.Stream is required.");
            if (string.IsNullOrWhiteSpace(s.ConsumerGroup)) Err("Source.ConsumerGroup is required.");
            if (!s.StartFrom.Equals("End", StringComparison.OrdinalIgnoreCase) && !s.StartFrom.Equals("Beginning", StringComparison.OrdinalIgnoreCase))
                Err($"Source.StartFrom '{s.StartFrom}' must be End or Beginning.");
            if (s.BatchSize is < 1 or > 1000) Err("Source.BatchSize must be 1-1000.");

            if (p.Map is { } m)
            {
                if (!string.IsNullOrWhiteSpace(m.Template)) Err("Map.Template is not supported by this version (template maps are planned); remove Map for passthrough.");
                if (!string.IsNullOrWhiteSpace(m.Handler)) Err("Map.Handler is not supported by this version (code maps are planned); remove Map for passthrough.");
            }

            var d = p.Destination;
            if (!string.Equals(d.Transport, "http", StringComparison.OrdinalIgnoreCase))
                Err($"Destination.Transport '{d.Transport}' is not supported (supported: http).");
            else
            {
                if (!Uri.TryCreate(d.Url, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
                    Err($"Destination.Url '{d.Url}' must be an absolute http(s) URL.");
                if (!Methods.Contains(d.Method)) Err($"Destination.Method '{d.Method}' must be one of {string.Join(", ", Methods)}.");
                if (d.TimeoutSeconds is < 1 or > 3600) Err("Destination.TimeoutSeconds must be 1-3600.");
            }

            if (!PipeConfig.TryParsePolicy(p.OnFailure, out _)) Err($"OnFailure '{p.OnFailure}' must be block or skip-and-alert.");
            if (s.ClaimIdleSeconds < 1) Err("Source.ClaimIdleSeconds must be at least 1.");
            if (p.Enabled && host is not null && string.Equals(s.Transport, "redis", StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrWhiteSpace(host.RedisConnectionString))
                Err("uses a redis source but Host:RedisConnectionString is not set.");

            var r = p.Retry;
            if (r.MaxAttempts < 1) Err("Retry.MaxAttempts must be at least 1.");
            if (r.InitialDelayMs < 0 || r.MaxDelayMs < r.InitialDelayMs) Err("Retry delays must satisfy 0 <= InitialDelayMs <= MaxDelayMs.");
        }

        return errors;
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
