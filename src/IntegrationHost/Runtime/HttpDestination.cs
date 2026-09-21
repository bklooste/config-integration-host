using System.Net.Http.Headers;
using System.Text;
using IntegrationHost.Pipes;

namespace IntegrationHost.Runtime;

/// <summary>
/// Sends each message as one HTTP request. The message id travels as <c>Idempotency-Key</c> so the receiver can
/// dedupe redeliveries; <c>traceparent</c> and a correlation id are forwarded when the message carries them.
/// A non-2xx response is a failure, never swallowed.
/// </summary>
public sealed class HttpDestination(HttpClient http, DestinationConfig config) : IDestination
{
    public async Task SendAsync(Envelope message, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(new HttpMethod(config.Method.ToUpperInvariant()), config.Url)
        {
            Content = new StringContent(message.Payload, Encoding.UTF8, "application/json"),
        };
        foreach (var (name, value) in config.Headers)
        {
            if (name.Equals("Content-Type", StringComparison.OrdinalIgnoreCase))
                request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(value);
            else
                request.Headers.TryAddWithoutValidation(name, value);
        }
        request.Headers.Remove("Idempotency-Key");
        request.Headers.TryAddWithoutValidation("Idempotency-Key", message.Id);
        if (message.Headers.TryGetValue("traceparent", out var tp)) request.Headers.TryAddWithoutValidation("traceparent", tp);
        var correlation = message.Headers.GetValueOrDefault("correlationId") ?? message.Headers.GetValueOrDefault("correlation_id");
        if (correlation is not null) request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlation);

        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"{request.Method} {config.Url} returned {(int)response.StatusCode} {response.ReasonPhrase}", null, response.StatusCode);
    }
}
