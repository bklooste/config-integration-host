namespace IntegrationHost.Runtime;

public enum PipeStatus { Starting, Running, Blocked, Faulted }

/// <summary>Live health of one pipe, written by its runner and read by the health check.</summary>
public sealed class PipeState(string name)
{
    private readonly object gate = new();
    private PipeStatus status = PipeStatus.Starting;
    private string? lastError;
    private DateTimeOffset? lastSuccess;

    public string Name { get; } = name;

    public (PipeStatus Status, string? LastError, DateTimeOffset? LastSuccess) Snapshot()
    {
        lock (gate) return (status, lastError, lastSuccess);
    }

    public void Running() { lock (gate) { status = PipeStatus.Running; lastError = null; } }
    public void Delivered() { lock (gate) { status = PipeStatus.Running; lastError = null; lastSuccess = DateTimeOffset.UtcNow; } }
    public void Blocked(string error) { lock (gate) { status = PipeStatus.Blocked; lastError = error; } }
    public void Faulted(string error) { lock (gate) { status = PipeStatus.Faulted; lastError = error; } }
}
