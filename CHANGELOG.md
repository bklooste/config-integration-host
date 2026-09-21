# Changelog

Every push to `main` publishes a new patch version automatically (see `version.json` — height-based,
not manually tagged), so not every version number gets its own entry here. This file tracks what
actually changed.

## Unreleased

- Pipe model: `redis` source (consumer groups, pending recovery, dead-replica claim) → optional type filter →
  passthrough → `http` destination. At-least-once with `Idempotency-Key`, `traceparent` propagation.
- Failure policies `block` and `skip-and-alert`, exponential-backoff retry.
- Startup validation and `--validate` dry run; per-pipe health (`/health`, `/health/live`) and OTLP metrics.
