# Laravel Broadcast Benchmark

Compares Laravel broadcasting through Pogo WebSocket with Laravel Reverb. Each app exposes `/fire?count=...&size=...`; k6 opens websocket clients, subscribes to `bench-channel`, triggers HTTP publishes, and records delivery/latency metrics.

## Run

Requires Docker with Compose.

```bash
POGO_WS_HOT_PATH_METRICS=true ./benchmarks/laravel-broadcast/run.sh
```

The runner performs a `docker compose down -v`, fresh `--no-cache` application image builds, an isolated Pogo k6 run, an isolated Reverb k6 run, a sharded Pogo k6 run, a Go receiver Pogo run, and final cleanup. Console logs, image metadata, run metadata, k6 JSON summaries, the Go receiver JSON summary, and the proof audit are written to `benchmarks/laravel-broadcast/results/`.

The default schedule keeps websocket listeners connected until the publisher's configured `maxDuration` plus a drain buffer has elapsed. This prevents late publish batches from being counted as expected delivery after subscribers have already shut down.

For a quick smoke test:

```bash
VUS=20 MSG_COUNT=5 PUBLISH_BATCHES=3 BATCH_INTERVAL_SECONDS=1 PUBLISH_MAX_DURATION_SECONDS=15 DRAIN_SECONDS=5 ./benchmarks/laravel-broadcast/run.sh
```

For the focused Pogo diagnosis after the first proof pass:

```bash
POGO_WS_HOT_PATH_METRICS=true ./benchmarks/laravel-broadcast/run-pogo-diagnosis.sh
```

This runs only Pogo and Go receiver scenarios: one 500-client baseline, one 5x100 sharded Go receiver run, a payload-size sweep (`16`, `256`, `1024`, `4096`), and a batch-size sweep (`10`, `50`, `100`, `250`). The summary table is written to `benchmarks/laravel-broadcast/results/run-*-pogo-diagnosis-audit.tsv`.

For focused Pogo delivery tuning:

```bash
POGO_WS_HOT_PATH_METRICS=true ./benchmarks/laravel-broadcast/run-pogo-tuning.sh
```

This runs only Pogo and Go receiver scenarios across delivery knobs: write burst sizes (`1`, `8`, `16`, `64`), fanout backpressure thresholds (`1`, `4`, `16`, `64`), and compression off/on at payload sizes `1024` and `4096`. The summary table is written to `benchmarks/laravel-broadcast/results/run-*-pogo-tuning-audit.tsv`.

For the structural paced-fanout experiment:

```bash
POGO_WS_HOT_PATH_METRICS=true ./benchmarks/laravel-broadcast/run-pogo-structural.sh
```

This runs only Pogo and Go receiver scenarios for `burst` baseline and paced fanout rounds (`8`, `16`, `32`, plus `16` with `1ms` yield). The summary table is written to `benchmarks/laravel-broadcast/results/run-*-pogo-structural-audit.tsv` and includes fanout duration, queue depth, queue residence, write-complete p95, and receiver p95/p99.

Manual runs are still possible:

```bash
docker compose -f benchmarks/laravel-broadcast/compose.yaml build --no-cache pogo reverb-app reverb-ws
docker compose -f benchmarks/laravel-broadcast/compose.yaml up --force-recreate --abort-on-container-exit --exit-code-from k6-pogo k6-pogo
docker compose -f benchmarks/laravel-broadcast/compose.yaml down -v
docker compose -f benchmarks/laravel-broadcast/compose.yaml up --force-recreate --abort-on-container-exit --exit-code-from k6-reverb k6-reverb
docker compose -f benchmarks/laravel-broadcast/compose.yaml down -v
docker compose -f benchmarks/laravel-broadcast/compose.yaml up --force-recreate k6-pogo-listener-1 k6-pogo-listener-2 k6-pogo-listener-3 k6-pogo-listener-4 k6-pogo-listener-5 k6-pogo-publisher
docker compose -f benchmarks/laravel-broadcast/compose.yaml down -v
docker compose -f benchmarks/laravel-broadcast/compose.yaml up --force-recreate --abort-on-container-exit --exit-code-from go-receiver-pogo go-receiver-pogo
docker compose -f benchmarks/laravel-broadcast/compose.yaml down -v
```

`benchmark.js` accepts `DRIVER`, `ROLE`, `APP_KEY`, `HTTP_HOST`, `WS_HOST`, `HTTP_PORT`, `WS_PORT`, `VUS`, `MSG_COUNT`, `PAYLOAD_SIZE`, `PUBLISH_BATCHES`, `BATCH_INTERVAL_SECONDS`, `RAMP_UP_SECONDS`, `HOLD_SECONDS`, `RAMP_DOWN_SECONDS`, `PUBLISH_START_SECONDS`, `PUBLISH_MAX_DURATION_SECONDS`, `DRAIN_SECONDS`, `LATENCY_P95_THRESHOLD_MS`, and `RESULT_FILE` overrides. `ROLE=both` is the default; `ROLE=listeners` opens websocket listeners only, and `ROLE=publisher` triggers `/fire` only.

The sharded k6 run starts five listener containers at `SHARD_VUS=100` each by default, plus one publisher container. The audit reports `sharded_k6_receive_p95_ms` as the maximum p95 across the five listener shard summaries.

The Go receiver accepts the same core benchmark environment as k6 (`ROLE`, `VUS`, `MSG_COUNT`, `PAYLOAD_SIZE`, `PUBLISH_BATCHES`, `BATCH_INTERVAL_SECONDS`, `RAMP_UP_SECONDS`, `PUBLISH_START_SECONDS`, `PUBLISH_MAX_DURATION_SECONDS`, `DRAIN_SECONDS`, `HTTP_HOST`, `WS_HOST`, ports, `APP_KEY`, `RESULT_FILE`, `METRICS_URL`, and `METRICS_FILE`). `ROLE=both` is the default; `ROLE=listeners` opens websocket listeners only, and `ROLE=publisher` triggers `/fire` only.

The Pogo benchmark app also accepts delivery-tuning overrides: `POGO_WS_OUTBOUND_QUEUE_SIZE`, `POGO_WS_WRITE_BURST_SIZE`, `POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD`, `POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT`, `POGO_WS_FANOUT_MODE`, `POGO_WS_FANOUT_ROUND_SIZE`, `POGO_WS_FANOUT_ROUND_YIELD`, and `POGO_WS_ENABLE_COMPRESSION`.

If `HOLD_SECONDS` is not set, the benchmark derives it from `PUBLISH_START_SECONDS + PUBLISH_MAX_DURATION_SECONDS + DRAIN_SECONDS - RAMP_UP_SECONDS`. If `HOLD_SECONDS` is set too low, k6 aborts instead of writing a misleading delivery summary.

The default benchmark intentionally compares the current Pogo integrated FrankenPHP websocket setup with the current Reverb split app/websocket setup. Treat it as a topology benchmark, not an isolated websocket-engine microbenchmark.
