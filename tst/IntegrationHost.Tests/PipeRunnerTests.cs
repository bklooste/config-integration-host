using IntegrationHost.Pipes;
using IntegrationHost.Runtime;
using Microsoft.Extensions.Logging.Abstractions;

namespace IntegrationHost.Tests;

public class PipeRunnerTests
{
    private sealed class FakeSource(params Envelope[] messages) : IMessageSource
    {
        private readonly Queue<Envelope> queue = new(messages);
        public List<string> Acked { get; } = [];
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<Envelope>>(queue.Count > 0 ? [queue.Dequeue()] : []);
        public Task AckAsync(Envelope m, CancellationToken ct) { Acked.Add(m.Id); return Task.CompletedTask; }
    }

    private sealed class FakeDestination(Func<Envelope, int, Exception?> fail) : IDestination
    {
        public List<string> Sent { get; } = [];
        public int Attempts;
        public Task SendAsync(Envelope m, CancellationToken ct)
        {
            var n = Interlocked.Increment(ref Attempts);
            if (fail(m, n) is { } ex) throw ex;
            Sent.Add(m.Id);
            return Task.CompletedTask;
        }
    }

    private static Envelope Msg(string id, string? type = null) => new(id, "{}", type, new Dictionary<string, string>());

    private static PipeConfig Pipe(FailurePolicy policy, int maxAttempts = 3, params string[] typeFilter) => new()
    {
        Name = "p",
        OnFailure = policy == FailurePolicy.Block ? "block" : "skip-and-alert",
        Retry = new() { MaxAttempts = maxAttempts, InitialDelayMs = 1, MaxDelayMs = 5 },
        Source = new() { TypeFilter = [.. typeFilter] },
    };

    private static (PipeRunner Runner, PipeState State) Runner(PipeConfig pipe, IMessageSource s, IDestination d)
    {
        var state = new PipeState(pipe.Name);
        return (new PipeRunner(pipe, s, d, state, new PipeMetrics(pipe.Name), NullLogger.Instance, TimeSpan.FromMilliseconds(5)), state);
    }

    [Fact]
    public async Task A_message_is_acked_only_after_the_destination_accepted_it()
    {
        var source = new FakeSource(Msg("1"));
        var dest = new FakeDestination((_, _) => null);
        var (runner, _) = Runner(Pipe(FailurePolicy.Block), source, dest);

        await runner.ProcessAsync((await source.ReadAsync(default))[0], default);

        Assert.Equal(["1"], dest.Sent);
        Assert.Equal(["1"], source.Acked);
    }

    [Fact]
    public async Task Transient_failures_are_retried_then_delivered()
    {
        var source = new FakeSource();
        var dest = new FakeDestination((_, n) => n < 3 ? new HttpRequestException("503") : null);
        var (runner, state) = Runner(Pipe(FailurePolicy.Block, maxAttempts: 5), source, dest);

        await runner.ProcessAsync(Msg("1"), default);

        Assert.Equal(3, dest.Attempts);
        Assert.Equal(["1"], source.Acked);
        Assert.Equal(PipeStatus.Running, state.Snapshot().Status);
    }

    [Fact]
    public async Task Skip_and_alert_acks_the_message_after_retries_are_exhausted_and_moves_on()
    {
        var source = new FakeSource();
        var dest = new FakeDestination((m, _) => m.Id == "poison" ? new HttpRequestException("400") : null);
        var (runner, state) = Runner(Pipe(FailurePolicy.SkipAndAlert, maxAttempts: 3), source, dest);

        await runner.ProcessAsync(Msg("poison"), default);
        await runner.ProcessAsync(Msg("2"), default);

        Assert.Equal(["poison", "2"], source.Acked); // poison acked without being delivered
        Assert.Equal(["2"], dest.Sent);
        Assert.Equal(PipeStatus.Running, state.Snapshot().Status);
    }

    [Fact]
    public async Task Block_never_acks_keeps_retrying_and_reports_blocked_until_it_gets_through()
    {
        var source = new FakeSource();
        var allow = false;
        var dest = new FakeDestination((_, _) => allow ? null : new HttpRequestException("down"));
        var (runner, state) = Runner(Pipe(FailurePolicy.Block, maxAttempts: 2), source, dest);

        var task = runner.ProcessAsync(Msg("1"), default);
        Assert.True(await Receiver.WaitFor(() => state.Snapshot().Status == PipeStatus.Blocked, TimeSpan.FromSeconds(5)));
        Assert.Empty(source.Acked);
        Assert.Contains("message 1", state.Snapshot().LastError);

        allow = true;
        await task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(["1"], source.Acked);
        Assert.Equal(PipeStatus.Running, state.Snapshot().Status);
    }

    [Fact]
    public async Task Cancellation_while_blocked_stops_without_acking()
    {
        var source = new FakeSource();
        var dest = new FakeDestination((_, _) => new HttpRequestException("down"));
        var (runner, state) = Runner(Pipe(FailurePolicy.Block, maxAttempts: 1), source, dest);
        using var cts = new CancellationTokenSource();

        var task = runner.ProcessAsync(Msg("1"), cts.Token);
        Assert.True(await Receiver.WaitFor(() => state.Snapshot().Status == PipeStatus.Blocked, TimeSpan.FromSeconds(5)));
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Empty(source.Acked);
    }

    [Fact]
    public async Task Type_filter_acks_non_matching_messages_without_delivering_them()
    {
        var source = new FakeSource();
        var dest = new FakeDestination((_, _) => null);
        var (runner, _) = Runner(Pipe(FailurePolicy.Block, 3, "Audit"), source, dest);

        await runner.ProcessAsync(Msg("1", "Audit"), default);
        await runner.ProcessAsync(Msg("2", "Other"), default);
        await runner.ProcessAsync(Msg("3", null), default);

        Assert.Equal(["1"], dest.Sent);
        Assert.Equal(["1", "2", "3"], source.Acked);
    }

    [Fact]
    public void Backoff_doubles_and_is_capped()
    {
        var r = new RetryConfig { InitialDelayMs = 100, MaxDelayMs = 1000 };
        Assert.Equal(TimeSpan.FromMilliseconds(100), PipeRunner.Backoff(r, 1));
        Assert.Equal(TimeSpan.FromMilliseconds(400), PipeRunner.Backoff(r, 3));
        Assert.Equal(TimeSpan.FromMilliseconds(1000), PipeRunner.Backoff(r, 30));
    }

    [Fact]
    public async Task The_loop_survives_a_source_failure_and_reports_faulted_then_recovers()
    {
        var reads = 0;
        var source = new ThrowingSource(() => ++reads == 1 ? throw new InvalidOperationException("redis down") : []);
        var (runner, state) = Runner(Pipe(FailurePolicy.Block), source, new FakeDestination((_, _) => null));

        await runner.StartAsync(default);
        Assert.True(await Receiver.WaitFor(() => state.Snapshot().Status == PipeStatus.Faulted, TimeSpan.FromSeconds(5)));
        Assert.True(await Receiver.WaitFor(() => state.Snapshot().Status == PipeStatus.Running, TimeSpan.FromSeconds(5)));
        await runner.StopAsync(default);
    }

    private sealed class ThrowingSource(Func<IReadOnlyList<Envelope>> read) : IMessageSource
    {
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct) => Task.FromResult(read());
        public Task AckAsync(Envelope m, CancellationToken ct) => Task.CompletedTask;
    }
}
