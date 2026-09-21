using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace IntegrationHost;

/// <summary>Health body that names each pipe and its state, so an operator can see which one is blocked.</summary>
internal static class HealthResponse
{
    // Relaxed escaping keeps quotes/apostrophes in pipe error messages readable ('x' rather than \u0027x\u0027).
    private static readonly JsonSerializerOptions Options = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static Task WriteAsync(HttpContext ctx, HealthReport report)
    {
        ctx.Response.ContentType = "application/json";
        return ctx.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            status = report.Status.ToString(),
            checks = report.Entries.ToDictionary(e => e.Key, e => new
            {
                status = e.Value.Status.ToString(),
                description = e.Value.Description,
                pipes = e.Value.Data,
            }),
        }, Options));
    }
}
