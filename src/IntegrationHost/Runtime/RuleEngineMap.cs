using System.Diagnostics;
using System.Net;
using System.Text;

namespace IntegrationHost.Runtime;

/// <summary>
/// Template map backed by a rule-engine-service (v2): POSTs the payload to <c>v2/templates/{id}/evaluate</c> and uses the
/// merged fragment as the new payload. The engine answers <c>{}</c> when nothing matched — that is surfaced as
/// "no match" (null), never forwarded as an empty message.
/// </summary>
public sealed class RuleEngineMap(HttpClient http, string templateId) : IMapper
{
    private string EvaluatePath => $"v2/templates/{Uri.EscapeDataString(templateId)}/evaluate";

    public async Task CheckAsync(CancellationToken ct)
    {
        using var response = await http.GetAsync($"v2/templates/{Uri.EscapeDataString(templateId)}", HttpCompletionOption.ResponseHeadersRead, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
            throw new InvalidOperationException($"template '{templateId}' not found in the rule engine at {http.BaseAddress}");
        response.EnsureSuccessStatusCode();
    }

    public async Task<string?> MapAsync(Envelope message, CancellationToken ct)
    {
        using var content = new StringContent(message.Payload, Encoding.UTF8, "application/json");
        using var response = await http.PostAsync(EvaluatePath, content, ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"rule engine evaluate of '{templateId}' returned {(int)response.StatusCode} {response.ReasonPhrase}", null, response.StatusCode);

        if (response.Headers.TryGetValues("X-Template-Version", out var v)) Activity.Current?.SetTag("template.version", v.FirstOrDefault());
        var body = (await response.Content.ReadAsStringAsync(ct)).Trim();
        return body is "" or "{}" or "null" ? null : body;
    }
}
