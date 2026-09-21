using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace IntegrationHost.Runtime;

/// <summary>Per-pipe counters, tagged <c>pipe</c>. Exported over OTLP when <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> is set.</summary>
public sealed class PipeMetrics
{
    public const string MeterName = "IntegrationHost";
    public const string ActivitySourceName = "IntegrationHost";
    public static readonly ActivitySource Tracing = new(ActivitySourceName);
    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> ConsumedC = Meter.CreateCounter<long>("integrationhost.consumed", description: "Messages read from the source");
    private static readonly Counter<long> FilteredC = Meter.CreateCounter<long>("integrationhost.filtered", description: "Messages dropped by TypeFilter");
    private static readonly Counter<long> SentC = Meter.CreateCounter<long>("integrationhost.sent", description: "Messages accepted by the destination");
    private static readonly Counter<long> FailedC = Meter.CreateCounter<long>("integrationhost.failed", description: "Failed delivery attempts");
    private static readonly Counter<long> SkippedC = Meter.CreateCounter<long>("integrationhost.skipped", description: "Messages dropped by OnFailure=skip-and-alert");

    private readonly KeyValuePair<string, object?> tag;
    public PipeMetrics(string pipe) => tag = new("pipe", pipe);

    public void Consumed() => ConsumedC.Add(1, tag);
    public void Filtered() => FilteredC.Add(1, tag);
    public void Sent() => SentC.Add(1, tag);
    public void Failed() => FailedC.Add(1, tag);
    public void Skipped() => SkippedC.Add(1, tag);
}
