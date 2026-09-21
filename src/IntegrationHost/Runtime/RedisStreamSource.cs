using System.Text.Json;
using IntegrationHost.Pipes;
using StackExchange.Redis;

namespace IntegrationHost.Runtime;

/// <summary>
/// Reads a Redis stream through a consumer group. At-least-once: an entry stays in the group's pending list until
/// <see cref="AckAsync"/>, so after a restart this consumer re-reads its own unacked entries first, and entries
/// stranded on a dead replica are claimed once idle for <c>ClaimIdleSeconds</c>.
/// </summary>
public sealed class RedisStreamSource(IConnectionMultiplexer redis, SourceConfig config, string consumer) : IMessageSource
{
    private static readonly string[] PassThroughHeaders = ["traceparent", "correlationId", "correlation_id", "partitionKey", "partition_key"];

    private IDatabase Db => redis.GetDatabase();
    private bool ownPendingDrained;

    public async Task StartAsync(CancellationToken ct)
    {
        var position = config.StartFrom.Equals("Beginning", StringComparison.OrdinalIgnoreCase) ? "0-0" : "$";
        try
        {
            await Db.StreamCreateConsumerGroupAsync(config.Stream, config.ConsumerGroup, position, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
        {
            // Group already exists — the normal case after the first start.
        }
    }

    public async Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct)
    {
        try
        {
            if (!ownPendingDrained)
            {
                var pending = await Db.StreamReadGroupAsync(config.Stream, config.ConsumerGroup, consumer, "0", config.BatchSize);
                if (pending.Length > 0) return await ToEnvelopesAsync(pending);
                ownPendingDrained = true;
            }

            var fresh = await Db.StreamReadGroupAsync(config.Stream, config.ConsumerGroup, consumer, ">", config.BatchSize);
            if (fresh.Length > 0) return await ToEnvelopesAsync(fresh);

            // Idle: adopt entries a crashed replica left behind.
            var claimed = await Db.StreamAutoClaimAsync(config.Stream, config.ConsumerGroup, consumer,
                config.ClaimIdleSeconds * 1000L, "0-0", config.BatchSize);
            return await ToEnvelopesAsync(claimed.ClaimedEntries);
        }
        catch
        {
            ownPendingDrained = false; // re-read our own pending list after any error
            throw;
        }
    }

    public async Task AckAsync(Envelope message, CancellationToken ct)
    {
        try { await Db.StreamAcknowledgeAsync(config.Stream, config.ConsumerGroup, message.Id); }
        catch { ownPendingDrained = false; throw; }
    }

    private async Task<IReadOnlyList<Envelope>> ToEnvelopesAsync(StreamEntry[] entries)
    {
        var result = new List<Envelope>(entries.Length);
        foreach (var e in entries)
        {
            // An entry trimmed from the stream while pending comes back null; nothing to deliver, so clear it.
            if (e.IsNull) { if (!e.Id.IsNull) await Db.StreamAcknowledgeAsync(config.Stream, config.ConsumerGroup, e.Id); continue; }
            result.Add(ToEnvelope(e));
        }
        return result;
    }

    private Envelope ToEnvelope(StreamEntry e)
    {
        var fields = e.Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString());
        var payload = fields.TryGetValue(config.PayloadField, out var p)
            ? p
            : JsonSerializer.Serialize(fields);
        fields.TryGetValue(config.TypeField, out var type);
        var headers = PassThroughHeaders.Where(fields.ContainsKey).ToDictionary(h => h, h => fields[h]);
        return new Envelope(e.Id.ToString(), payload, type, headers);
    }
}
