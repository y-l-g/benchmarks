# Laravel Broadcast Benchmark

Compares Laravel broadcasting through Pogo WebSocket with Laravel Reverb. Each app exposes `/fire?count=...&size=...`; k6 opens websocket clients, subscribes to `bench-channel`, triggers HTTP publishes, and records delivery/latency metrics.

## Run

Requires Docker with Compose.

```bash
POGO_WS_HOT_PATH_METRICS=true ./laravel-websocket/run.sh
```

The runner builds the images, compares Pogo and Reverb with k6, runs a Pogo Go receiver baseline, a sharded Go receiver run, compact payload and batch sweeps, and a 4096-byte compression comparison. Console logs, image metadata, run metadata, JSON summaries, and one combined audit TSV are written to `laravel-websocket/results/`.

Set `NO_CACHE=1` to force clean image builds:

```bash
NO_CACHE=1 POGO_WS_HOT_PATH_METRICS=true ./laravel-websocket/run.sh
```

The default schedule keeps websocket listeners connected until the publisher's configured `maxDuration` plus a drain buffer has elapsed. This prevents late publish batches from being counted as expected delivery after subscribers have already shut down.

This writes `laravel-websocket/results/run-*-audit.tsv` with the selected proof, scale, payload, batch, and compression scenarios.

`benchmark.js` accepts `DRIVER`, `ROLE`, `APP_KEY`, `HTTP_HOST`, `WS_HOST`, `HTTP_PORT`, `WS_PORT`, `VUS`, `MSG_COUNT`, `PAYLOAD_SIZE`, `PUBLISH_BATCHES`, `BATCH_INTERVAL_SECONDS`, `RAMP_UP_SECONDS`, `HOLD_SECONDS`, `RAMP_DOWN_SECONDS`, `PUBLISH_START_SECONDS`, `PUBLISH_MAX_DURATION_SECONDS`, `DRAIN_SECONDS`, `LATENCY_P95_THRESHOLD_MS`, and `RESULT_FILE` overrides. `ROLE=both` is the default; `ROLE=listeners` opens websocket listeners only, and `ROLE=publisher` triggers `/fire` only.

The sharded Go receiver scenario starts five listener containers at `SHARD_VUS=100` each by default, plus one publisher container. The audit reports the maximum p95/p99 across the five listener shard summaries.

The Go receiver accepts the same core benchmark environment as k6 (`ROLE`, `VUS`, `MSG_COUNT`, `PAYLOAD_SIZE`, `PUBLISH_BATCHES`, `BATCH_INTERVAL_SECONDS`, `RAMP_UP_SECONDS`, `PUBLISH_START_SECONDS`, `PUBLISH_MAX_DURATION_SECONDS`, `DRAIN_SECONDS`, `HTTP_HOST`, `WS_HOST`, ports, `APP_KEY`, `RESULT_FILE`, `METRICS_URL`, and `METRICS_FILE`). `ROLE=both` is the default; `ROLE=listeners` opens websocket listeners only, and `ROLE=publisher` triggers `/fire` only.

The Pogo benchmark app also accepts delivery-tuning overrides: `POGO_WS_OUTBOUND_QUEUE_SIZE`, `POGO_WS_WRITE_BURST_SIZE`, and `POGO_WS_ENABLE_COMPRESSION`.

Benchmarks measure end-to-end receive latency from benchmark clients. The websocket server does not inspect or mutate user payload JSON to produce benchmark timestamp metrics.

If `HOLD_SECONDS` is not set, the benchmark derives it from `PUBLISH_START_SECONDS + PUBLISH_MAX_DURATION_SECONDS + DRAIN_SECONDS - RAMP_UP_SECONDS`. If `HOLD_SECONDS` is set too low, k6 aborts instead of writing a misleading delivery summary.

The default benchmark intentionally compares the current Pogo integrated FrankenPHP websocket setup with the current Reverb split app/websocket setup. Treat it as a topology benchmark, not an isolated websocket-engine microbenchmark.
