using IntegrationHost.Pipes;
using StackExchange.Redis;

namespace IntegrationHost.Runtime;

/// <summary>Appends each message to a Redis stream (payload in <c>data</c>, plus <c>type</c>, <c>source-id</c> and any propagated headers).</summary>
public sealed class RedisDestination(IConnectionMultiplexer redis, DestinationConfig config) : IDestination
{
    public async Task SendAsync(Envelope message, CancellationToken ct)
    {
        var fields = new List<NameValueEntry> { new("data", message.Payload), new("source-id", message.Id) };
        if (message.Type is not null) fields.Add(new("type", message.Type));
        foreach (var (k, v) in message.Headers) fields.Add(new(k, v));

        await redis.GetDatabase().StreamAddAsync(config.Stream, [.. fields],
            maxLength: config.MaxLength > 0 ? config.MaxLength : null, useApproximateMaxLength: true);
    }
}
