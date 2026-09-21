using IntegrationHost;
using IntegrationHost.Pipes;
using IntegrationHost.Runtime;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using StackExchange.Redis;

// Container HEALTHCHECK: the chiselled runtime image has no shell or curl, so the app probes itself.
// It probes /health/live (process is up), not /health: a pipe blocked on a failing partner should make the
// instance unready, not get the container restarted.
if (args.Contains("--healthcheck"))
{
    var port = Environment.GetEnvironmentVariable("ASPNETCORE_HTTP_PORTS")?.Split(';')[0] ?? "8080";
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    try { return (await http.GetAsync($"http://localhost:{port}/health/live")).IsSuccessStatusCode ? 0 : 1; }
    catch { return 1; }
}

var builder = WebApplication.CreateBuilder(args.Where(a => a != "--validate").ToArray());
builder.Configuration.AddSecretFiles();

var host = builder.Configuration.GetSection(IntegrationHostOptions.SectionName).Get<IntegrationHostOptions>() ?? new();
var pipes = builder.Configuration.GetSection("Pipes").Get<List<PipeConfig>>() ?? [];

// Dry run: check the config and exit, without connecting to anything.
if (args.Contains("--validate"))
{
    var errors = PipeValidator.Validate(pipes, host);
    if (errors.Count == 0)
    {
        Console.WriteLine($"OK: {pipes.Count} pipe(s) valid, {pipes.Count(p => p.Enabled)} enabled.");
        return 0;
    }
    Console.Error.WriteLine("Invalid pipe configuration:");
    foreach (var e in errors) Console.Error.WriteLine("  - " + e);
    return 1;
}

builder.Services.AddOptions<IntegrationHostOptions>()
    .BindConfiguration(IntegrationHostOptions.SectionName)
    .ValidateDataAnnotations()
    .ValidateOnStart();
// Fail fast and loudly: a bad pipe must stop the container at start, not surface at message time.
PipeValidator.ThrowIfInvalid(pipes, host);

var enabled = pipes.Where(p => p.Enabled).ToList();
var states = enabled.Select(p => new PipeState(p.Name)).ToList();

if (enabled.Count > 0)
{
    var redisOptions = ConfigurationOptions.Parse(host.RedisConnectionString);
    redisOptions.AbortOnConnectFail = false; // an unreachable broker is a health failure, not a crash
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisOptions));
}

foreach (var (pipe, state) in enabled.Zip(states))
{
    var clientName = "pipe:" + pipe.Name;
    builder.Services.AddHttpClient(clientName, c => c.Timeout = TimeSpan.FromSeconds(pipe.Destination.TimeoutSeconds));
    builder.Services.AddSingleton<IHostedService>(sp => new PipeRunner(
        pipe,
        new RedisStreamSource(sp.GetRequiredService<IConnectionMultiplexer>(), pipe.Source, host.ConsumerName),
        new HttpDestination(sp.GetRequiredService<IHttpClientFactory>().CreateClient(clientName), pipe.Destination),
        state, new PipeMetrics(pipe.Name),
        sp.GetRequiredService<ILoggerFactory>().CreateLogger("Pipe." + pipe.Name),
        TimeSpan.FromMilliseconds(host.PollIntervalMs)));
}
builder.Services.Configure<Microsoft.Extensions.Hosting.HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(host.ShutdownTimeoutSeconds));

// Telemetry: exporters are driven entirely by the standard OTEL_* env vars (OTEL_EXPORTER_OTLP_ENDPOINT,
// OTEL_SERVICE_NAME, ...). With none set, nothing is exported.
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(builder.Environment.ApplicationName))
    .WithTracing(t => t.AddSource(PipeMetrics.ActivitySourceName).AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddMeter(PipeMetrics.MeterName).AddOtlpExporter());

builder.Services.AddHealthChecks()
    .AddCheck("pipes", new PipesHealthCheck(states), HealthStatus.Unhealthy);

var app = builder.Build();

app.MapHealthChecks("/health", new() { ResponseWriter = HealthResponse.WriteAsync });
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapGet("/", () => "config-integration-host");

app.Run();
return 0;

public partial class Program;
