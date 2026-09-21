using System.Text;
using System.Text.Json;
using IntegrationHost.Pipes;
using StackExchange.Redis;

namespace IntegrationHost.Runtime;

/// <summary>
/// Reads one or more Redis streams (partitions) through consumer groups. At-least-once: an entry stays in the group's
/// pending list until <see cref="AckAsync"/>, so after a restart this consumer re-reads its own unacked entries first,
/// and entries stranded on a dead replica are claimed once idle for <c>ClaimIdleSeconds</c>.
/// </summary>
public sealed class RedisStreamSource : IMessageSource
{
    private static readonly Dictionary<string, string> DefaultHeaders = new(StringComparer.Ordinal)
    {
        ["traceparent"] = "traceparent", ["correlationId"] = "correlationId", ["correlation_id"] = "correlationId",
        ["partitionKey"] = "partitionKey", ["partition_key"] = "partitionKey",
    };

    private readonly IConnectionMultiplexer redis;
    private readonly SourceConfig config;
    private readonly string consumer;
    private readonly string[] keys;
    private readonly bool[] ownPendingDrained;
    private readonly Dictionary<string, string> headerFields;

    public RedisStreamSource(IConnectionMultiplexer redis, SourceConfig config, string consumer)
    {
        this.redis = redis;
        this.config = config;
        this.consumer = consumer;
        keys = Enumerable.Range(0, Math.Max(1, config.Partitions))
            .Select(i => config.Stream.Replace("{partition}", i.ToString(), StringComparison.Ordinal)).ToArray();
        ownPendingDrained = new bool[keys.Length];
        headerFields = new Dictionary<string, string>(DefaultHeaders, StringComparer.Ordinal);
        foreach (var (field, header) in config.HeaderFields) headerFields[field] = header;
    }

    private IDatabase Db => redis.GetDatabase();

    public async Task StartAsync(CancellationToken ct)
    {
        var position = config.StartFrom.Equals("Beginning", StringComparison.OrdinalIgnoreCase) ? "0-0" : "$";
        foreach (var key in keys)
        {
            try
            {
                await Db.StreamCreateConsumerGroupAsync(key, config.ConsumerGroup, position, createStream: true);
            }
            catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
            {
                // Group already exists — the normal case after the first start.
            }
        }
    }

    public async Task<IReadOnlyList<Envelope>> ReadAsync(CancellationToken ct)
    {
        try
        {
            var batch = new List<Envelope>();
            for (var i = 0; i < keys.Length; i++)
            {
                if (!ownPendingDrained[i])
                {
                    var pending = await Db.StreamReadGroupAsync(keys[i], config.ConsumerGroup, consumer, "0", config.BatchSize);
                    if (pending.Length > 0) { await AddAsync(batch, i, pending); continue; }
                    ownPendingDrained[i] = true;
                }
                var fresh = await Db.StreamReadGroupAsync(keys[i], config.ConsumerGroup, consumer, ">", config.BatchSize);
                await AddAsync(batch, i, fresh);
            }
            if (batch.Count > 0) return batch;

            // Idle: adopt entries a crashed replica left behind.
            for (var i = 0; i < keys.Length; i++)
            {
                var claimed = await Db.StreamAutoClaimAsync(keys[i], config.ConsumerGroup, consumer, config.ClaimIdleSeconds * 1000L, "0-0", config.BatchSize);
                await AddAsync(batch, i, claimed.ClaimedEntries);
            }
            return batch;
        }
        catch
        {
            Array.Clear(ownPendingDrained); // re-read our own pending lists after any error
            throw;
        }
    }

    public async Task AckAsync(Envelope message, CancellationToken ct)
    {
        try { await Db.StreamAcknowledgeAsync(message.SourceStream ?? keys[0], config.ConsumerGroup, message.EntryId ?? message.Id); }
        catch { Array.Clear(ownPendingDrained); throw; }
    }

    private async Task AddAsync(List<Envelope> batch, int partition, StreamEntry[] entries)
    {
        foreach (var e in entries)
        {
            // An entry trimmed from the stream while pending comes back null; nothing to deliver, so clear it.
            if (e.IsNull) { if (!e.Id.IsNull) await Db.StreamAcknowledgeAsync(keys[partition], config.ConsumerGroup, e.Id); continue; }
            batch.Add(ToEnvelope(e, partition));
        }
    }

    private Envelope ToEnvelope(StreamEntry e, int partition)
    {
        var fields = new Dictionary<string, string>(e.Values.Length);
        byte[]? raw = null;
        foreach (var v in e.Values)
        {
            var name = v.Name.ToString();
            fields[name] = v.Value.ToString();
            if (name == config.PayloadField) raw = (byte[]?)v.Value;
        }

        var payload = fields.TryGetValue(config.PayloadField, out var p) ? p : JsonSerializer.Serialize(fields);
        fields.TryGetValue(config.TypeField, out var type);

        var headers = new Dictionary<string, string>();
        foreach (var (field, header) in headerFields)
            if (fields.TryGetValue(field, out var value) && value.Length > 0) headers[header] = value;

        var entryId = e.Id.ToString();
        return new Envelope(keys.Length > 1 ? $"{partition}-{entryId}" : entryId, payload, type, headers)
        {
            RawBody = raw, EntryId = entryId, SourceStream = keys[partition],
        };
    }
}
