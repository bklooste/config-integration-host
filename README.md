# config-integration-host

[![CI](https://github.com/bklooste/config-integration-host/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/config-integration-host/actions/workflows/ci.yml)
[![Image](https://img.shields.io/badge/ghcr.io-config--integration--host-blue)](https://github.com/bklooste/config-integration-host/pkgs/container/config-integration-host%2Fservice)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Config-defined integration pipes: move messages between Redis streams, Azure Event Hubs and HTTP endpoints with at-least-once delivery, a dedupe key on every message, retry with backoff, and a failure policy you choose per pipe. Add, change or remove a pipe by editing config — no code, no new service. (Early version: `redis` and `eventhubs` sources, `http`, `redis`, `eventhubs` and `objectstore` (Azure Blob / file) destinations, passthrough and rule-engine template maps. See [Roadmap](#roadmap).)

## Quick start

Move a message end to end — Redis stream → host → HTTP endpoint:

```bash
docker compose up -d
curl http://localhost:8080/health                     # 200 once the demo pipe is running
docker compose exec redis redis-cli XADD events '*' data '{"hello":"world"}'
docker compose logs receiver                          # shows the POST, with an Idempotency-Key header
```

Images are public on GHCR — no pull secret needed. Multi-arch (`linux/amd64`, `linux/arm64`).

## How it works

A **pipe** is `source → filter → map → destination`, plus a failure policy:

```jsonc
// appsettings.json (or env: Pipes__0__Name=..., Pipes__0__Source__Stream=...)
{
  "Host": { "RedisConnectionString": "redis:6379" },
  "Pipes": [
    {
      "Name": "orders-to-partner",
      "Source":      { "Transport": "redis", "Stream": "orders", "ConsumerGroup": "int-orders-partner", "TypeFilter": ["OrderPlaced"] },
      "Destination": { "Transport": "http",  "Url": "https://partner.example/ingest", "Headers": { "Authorization": "Bearer ..." } },
      "OnFailure":   "block",
      "Retry":       { "MaxAttempts": 5, "InitialDelayMs": 200, "MaxDelayMs": 30000 }
    }
  ]
}
```

Each pipe reads its stream through its **own consumer group**; replicas of the host share the group, so scale by adding
replicas (give each a distinct `Host__ConsumerName`).

### Delivery contract

- **At-least-once.** A message is acked (removed from the group's pending list) only *after* the destination accepted it.
  A crash at any earlier point redelivers it: on restart a consumer re-reads its own unacked entries first, and entries
  stranded on a dead replica are claimed by another once idle for `ClaimIdleSeconds`.
- **Dedupe key.** The source message id (the Redis stream id, e.g. `1712345678901-0`) is sent as the `Idempotency-Key`
  header. Receivers **must** dedupe on it — redelivery is normal, not exceptional.
- **Trace propagation.** If the stream entry has a `traceparent` field, the outgoing request continues that trace.
  A `correlationId` / `correlation_id` field is forwarded as `X-Correlation-Id`.
- **Order.** By default messages of one pipe are delivered one at a time, in read order (see `Concurrency` to trade this for throughput). With `block`, a failing message holds up
  everything behind it — that is the point.
- **Failure.** A non-2xx response or a network error is a failure, never swallowed. Retried with exponential backoff
  (`Retry`), then `OnFailure` applies:

| `OnFailure` | After `MaxAttempts` failures |
|---|---|
| `block` (default) | Never acks. Keeps retrying at `MaxDelayMs`, the pipe reports **Blocked** and `/health` returns 503. Nothing is lost; the pipe stalls until the destination recovers. |
| `skip-and-alert` | Logs an error with the message id, increments `integrationhost.skipped`, acks and moves on. The message is dropped. |

Redis stream entries: the payload is taken from the `data` field (`Source.PayloadField`), the type from `type`
(`Source.TypeField`). Entries with no payload field are sent as a JSON object of all their fields.

### Transports

| Transport | As source | As destination | Dedupe key |
|---|---|---|---|
| `redis` | Consumer group; ack after delivery; pending + dead-replica recovery | `XADD` (payload in `data`, plus `type`, `source-id`, propagated headers) | Stream id (`1712345678901-0`); destination stores it as `source-id` |
| `eventhubs` | `EventProcessorClient` with blob checkpoints; one in-flight event per partition, checkpointed only after delivery | One event per message; `MessageId` + `idempotency-key` property | `hub/partition/sequenceNumber` |
| `objectstore` | — | One object per message, named from a template; **never overwrites**, so a redelivery is a no-op. Backends: `azure-blob`, `file` | Embedded in the object name via `{id}` |
| `http` | — | One request per message; non-2xx = failure | `Idempotency-Key` header |

Any source can feed any destination. Event Hubs has no idempotent producer, so an `eventhubs` destination can
duplicate on retry — its consumers must dedupe on `idempotency-key`. An `eventhubs` source checkpoints after **every**
delivered event (one blob write each), which favours correctness over throughput.

An `objectstore` name template must contain `{id}` or `{entryId}`; tokens are `{id}`, `{entryId}`, `{type}`, `{correlationId}`, `{partitionKey}` and
`{date}`. `correlationId` / `partitionKey` come from stream fields of the same name. The object body is the message body
byte for byte, even when it is not valid UTF-8 (a binary payload passes through untouched unless a template map rewrites it). With the `file` backend, `/` in a name creates subdirectories and message data can never resolve
outside the root.

### Template maps

`Map.Template` reshapes a payload with a template stored in a
[rule-engine-service](https://github.com/bklooste/rule-engine-service) — change a partner's mapping by editing the
template, with no deploy of the host. The host `POST`s each payload to `v2/templates/{id}/evaluate` and delivers the
merged fragment. Anything not named in the template's `matchFragment` is dropped, which makes templates a natural place
to strip internal fields before data leaves.

```jsonc
"Map": { "Template": "orders2fulfilment", "OnNoMatch": "skip" }
// template rule:  { "dataToMatch": { "eventType": "order" }, "matchFragment": { "orderId": "{orderId}", "total": "{amount}" } }
```

- The engine answers `{}` when nothing matched. The host never forwards that: it is `skip`ped (and counted) or, with
  `OnNoMatch: fail`, treated as a failure — so a mis-keyed template shows up in metrics or health, not as empty messages at a partner.
- The template must exist. At start (and on every retry) the host checks it; a missing template or unreachable engine
  makes the pipe **Faulted** and `/health` 503, with the reason in the body, and the pipe starts on its own once the
  template appears.
- An engine outage during delivery is a normal delivery failure: retried, then `OnFailure` applies.
- The message id (dedupe key) and `traceparent` are unaffected by mapping. The payload must be JSON.

### Pipe reference

| Key | Default | Description |
|---|---|---|
| `Name` | _(required)_ | Unique pipe name. Appears in logs, metrics (`pipe` tag) and health. |
| `Enabled` | `true` | Disabled pipes are still validated but not run — keep them in config, switched off. |
| `Source.Transport` | _(required)_ | `redis` or `eventhubs`. |
| `Source.Stream` | _(required for redis)_ | Stream key exactly as stored (include any prefix). |
| `Source.ConsumerGroup` | _(required)_ | Consumer group for this pipe (for `eventhubs`: an event hub consumer group, e.g. `$Default`). |
| `Source.Partitions` | `1` | [redis] Number of partitioned streams. With more than 1, `Source.Stream` must contain `{partition}` (replaced by `0`..`N-1`); all partitions are read by the one pipe. |
| `Source.HeaderFields` | _(empty)_ | [redis] Extra stream-field → header-name mappings, e.g. `{"c":"correlationId","k":"partitionKey","p":"traceparent"}` for a producer that uses one-byte field names. `traceparent`, `correlationId`, `correlation_id`, `partitionKey`, `partition_key` are mapped by default. |
| `Source.StartFrom` | `End` | [redis] Where a **new** group starts: `End` (only new messages) or `Beginning` (the whole stream). |
| `Source.PayloadField` / `TypeField` | `data` / `type` | [redis] Stream fields holding payload and type. (`eventhubs`: body is the payload, `type` is an event property.) |
| `Source.ConnectionString` / `Namespace` | — | [eventhubs] Exactly one: a connection string, or a fully-qualified namespace (uses `DefaultAzureCredential` / managed identity). |
| `Source.EventHub` | _(required for eventhubs)_ | Event hub name. |
| `Source.CheckpointConnectionString` / `CheckpointContainerUri` | — | [eventhubs] Exactly one: blob connection string (+ `CheckpointContainer`, default `integration-host-checkpoints`, created if missing), or a full container URI (`DefaultAzureCredential`). |
| `Source.TypeFilter` | _(empty)_ | If set, only these types are delivered; others are acked and counted as filtered. |
| `Source.BatchSize` | `50` | Messages per read. |
| `Source.ClaimIdleSeconds` | `60` | Idle time before another replica claims an unacked message. |
| `Map.Template` | _(omitted)_ | Omit `Map` for passthrough. A template id in your rule-engine-service: each payload is evaluated against it and the merged result becomes the outgoing payload. See [Template maps](#template-maps). |
| `Map.OnNoMatch` | `skip` | When no rule matches: `skip` (ack, count `integrationhost.unmapped`, send nothing) or `fail` (a delivery failure, so `OnFailure` applies). |
| `Destination.Transport` | _(required)_ | `http`, `redis` or `eventhubs`. |
| `Destination.Url` | _(required for http)_ | Absolute http(s) URL. |
| `Destination.Method` | `POST` | [http] `POST`, `PUT`, `PATCH` or `DELETE`. |
| `Destination.Headers` | _(empty)_ | [http] Extra request headers. Supply secrets with `..._FILE` env vars. |
| `Destination.TimeoutSeconds` | `30` | [http] Per-request timeout. |
| `Destination.Stream` / `MaxLength` | _(required for redis)_ / `0` | [redis] Stream to append to; approximate max length (0 = unbounded). |
| `Destination.Backend` | _(required for objectstore)_ | `azure-blob` or `file`. |
| `Destination.Container` | _(required for objectstore)_ | Blob container name (created if missing) or, for `file`, the root directory. |
| `Destination.ConnectionString` / `ServiceUri` | — | [objectstore azure-blob] A connection string; or an account URI (alone: `DefaultAzureCredential`; **with** a connection string: that URI is the endpoint and the connection string's `AccountName`/`AccountKey` are the credentials). |
| `Destination.AccountName` / `AccountKey` | — | [objectstore azure-blob] With `ServiceUri`: shared-key access (Azurite, or an account key supplied via `..._FILE`). Set together. |
| `Destination.NameTemplate` | `{type}-{correlationId}-{id}` | [objectstore] Object name; must contain `{id}` (the dedupe id — for a multi-partition redis source it is `<partition>-<entryId>`) or `{entryId}` (the source's own id, unqualified). |
| `Destination.StripTypePrefix` | _(empty)_ | [objectstore] Removed from the start of `{type}` before naming. |
| `Destination.ConnectionString` / `Namespace` / `EventHub` / `PartitionKey` | — | [eventhubs] As for the source; `PartitionKey` (optional) keeps related events ordered. |
| `OnFailure` | `block` | `block` or `skip-and-alert`. |
| `Concurrency` | `1` | Messages of a batch delivered in parallel (1–64). `1` keeps strict read order; above 1 order is not preserved and a blocked message does not hold up the rest of its batch. For order-insensitive destinations such as an object store. |
| `Retry.MaxAttempts` / `InitialDelayMs` / `MaxDelayMs` | `5` / `200` / `30000` | Backoff doubles from initial up to max. |

### Validate before you deploy

```bash
docker run --rm -v $PWD/appsettings.json:/app/appsettings.json ghcr.io/bklooste/config-integration-host/service:latest --validate
# OK: 1 pipe(s) valid, 1 enabled.        (exit 0)
```

`--validate` checks the whole pipe set — reporting **every** problem, each naming its pipe — and exits without
connecting to anything. The same checks run at startup; a bad config stops the container with the same message
instead of failing later at message time.

## Configuration

Host settings come from environment variables (ASP.NET Core `Section__Key` form), an optional mounted
`appsettings.json`, or both — env wins. Pipes are configured the same way (see above).

| Env var | Type | Default | Description |
|---|---|---|---|
| `Host__RedisConnectionString` | string | _(empty)_ | Redis connection string for `redis` sources and destinations. Required when any enabled pipe uses redis. |
| `Host__RuleEngineUrl` | url | _(empty)_ | Base URL of a [rule-engine-service](https://github.com/bklooste/rule-engine-service) (v2). Required when any enabled pipe has `Map.Template`. |
| `Host__RuleEngineTimeoutSeconds` | int | `10` | Per-request timeout for rule-engine calls. 1–300. |
| `Host__ConsumerName` | string | machine name | Consumer name within every pipe's group. Give each replica a distinct name. |
| `Host__PollIntervalMs` | int | `500` | Wait (ms) before polling again when a stream is empty. 10–60000. |
| `Host__ShutdownTimeoutSeconds` | int | `30` | Seconds to let in-flight messages finish on shutdown. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | url | _(unset)_ | Standard OpenTelemetry. Unset = nothing exported. Also honours `OTEL_SERVICE_NAME`, etc. |
| `ASPNETCORE_HTTP_PORTS` | string | `8080` | Listening port. |

**Secrets.** Any setting can be supplied from a file by adding `_FILE` to its env var, e.g.
`Host__RedisConnectionString_FILE=/run/secrets/redis` or
`Pipes__0__Destination__Headers__Authorization_FILE=/run/secrets/partner-token`. The host never logs its configuration.

## Metrics

OTLP meter `IntegrationHost`, every instrument tagged `pipe`: `integrationhost.consumed`, `.filtered`, `.sent`,
`.mapped`, `.unmapped`, `.failed` (delivery attempts), `.skipped`. Traces: each message is a span (`<pipe> process`) continuing the producer's trace.

## API reference

| Method | Path | Description |
|---|---|---|
| GET | `/health` | `200` when every pipe is running; `503` if any pipe is starting, **blocked** on a failing message, or cannot reach its source. The body names each pipe and its state. Use for **readiness**. |
| GET | `/health/live` | `200` while the process is up. Use for **liveness** — a blocked pipe should make the instance unready, not restart it. |

## Deployment

Runs on **any Kubernetes node**; it only needs network access to its dependencies. Redis and the
destination are external endpoints, never something the service ships or pins a node for.

```bash
kubectl apply -f deploy/k8s/deployment.yaml
```

The manifest deliberately has no `nodeSelector`, `affinity` or tolerations. Pin an explicit image
version (`:1.4`), not `:latest`, in production.

## Roadmap

Not in this version — pipes using them are rejected at validation with a clear error rather than ignored:

- Sources/destinations: Kafka; object-store backend `s3`.
- Maps: compiled code handlers.
- Per-pipe lag metric.

## Development

```bash
dotnet build IntegrationHost.slnx
dotnet test IntegrationHost.slnx
docker build -t config-integration-host .
```

Versions come from [Nerdbank.GitVersioning](https://github.com/dotnet/Nerdbank.GitVersioning)
(`version.json`); every push to `main` that passes tests publishes a new patch version. Bump
`version.json` for a minor/major. See [CHANGELOG.md](CHANGELOG.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

[MIT](LICENSE). Third-party licences of the shipped image are listed in [NOTICE](NOTICE).

## Migrating a sink without a gap

Because `objectstore` writes are create-only and names are deterministic, cutting over from another sink that used the same
naming is safe: start the pipe with `Source.StartFrom: Beginning` on a **new** consumer group. Everything already written
is found and skipped (no overwrite, no error); only what the old sink had not yet written is added. Then stop the old sink.
