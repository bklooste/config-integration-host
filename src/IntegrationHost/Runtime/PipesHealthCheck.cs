using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegrationHost.Runtime;

/// <summary>Unhealthy while any pipe is starting, blocked on a failing message, or cannot reach its source.</summary>
public sealed class PipesHealthCheck(IReadOnlyList<PipeState> pipes) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var data = new Dictionary<string, object>();
        var bad = new List<string>();
        foreach (var p in pipes)
        {
            var (status, error, _) = p.Snapshot();
            data[p.Name] = error is null ? status.ToString() : $"{status}: {error}";
            if (status != PipeStatus.Running) bad.Add($"{p.Name} is {status}" + (error is null ? "" : $" ({error})"));
        }
        return Task.FromResult(bad.Count == 0
            ? HealthCheckResult.Healthy($"{pipes.Count} pipe(s) running", data)
            : HealthCheckResult.Unhealthy(string.Join("; ", bad), data: data));
    }
}
