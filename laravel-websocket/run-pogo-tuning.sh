#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/laravel-broadcast"
COMPOSE_FILE="$BENCH_DIR/compose.yaml"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AUDIT_FILE="$RESULTS_DIR/run-$STAMP-pogo-tuning-audit.tsv"
RUNTIME_CADDYFILE="$RESULTS_DIR/run-$STAMP-pogo-tuning-Caddyfile"
COMPOSE_OVERRIDE_FILE="$RESULTS_DIR/run-$STAMP-pogo-tuning-compose.override.yaml"

mkdir -p "$RESULTS_DIR"
chmod 0777 "$RESULTS_DIR"
export K6_UID="${K6_UID:-$(id -u)}"
export K6_GID="${K6_GID:-$(id -g)}"
export POGO_WS_HOT_PATH_METRICS="${POGO_WS_HOT_PATH_METRICS:-true}"

BASE_VUS="${VUS:-500}"
BASE_MSG_COUNT="${MSG_COUNT:-100}"
BASE_PAYLOAD_SIZE="${PAYLOAD_SIZE:-1024}"
BASE_PUBLISH_BATCHES="${PUBLISH_BATCHES:-20}"
BASE_BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-2}"
BASE_RAMP_UP_SECONDS="${RAMP_UP_SECONDS:-10}"
BASE_PUBLISH_MAX_DURATION_SECONDS="${PUBLISH_MAX_DURATION_SECONDS:-}"
BASE_DRAIN_SECONDS="${DRAIN_SECONDS:-10}"
BASE_OUTBOUND_QUEUE_SIZE="${POGO_WS_OUTBOUND_QUEUE_SIZE:-256}"
BASE_WRITE_BURST_SIZE="${POGO_WS_WRITE_BURST_SIZE:-64}"
BASE_FANOUT_THRESHOLD="${POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD:-16}"
BASE_FANOUT_MAX_WAIT="${POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT:-10ms}"
BASE_FANOUT_MODE="${POGO_WS_FANOUT_MODE:-burst}"
BASE_FANOUT_ROUND_SIZE="${POGO_WS_FANOUT_ROUND_SIZE:-16}"
BASE_FANOUT_ROUND_YIELD="${POGO_WS_FANOUT_ROUND_YIELD:-0ms}"
BASE_COMPRESSION="${POGO_WS_ENABLE_COMPRESSION:-false}"

cat > "$COMPOSE_OVERRIDE_FILE" <<YAML
services:
  pogo:
    volumes:
      - "$RUNTIME_CADDYFILE:/var/www/html/Caddyfile:ro"
YAML

compose() {
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_OVERRIDE_FILE" "$@"
}

render_runtime_caddyfile() {
  local write_burst_size="$1"
  local fanout_threshold="$2"
  local compression="$3"

  sed \
    -e "s/^[[:space:]]*outbound_queue_size .*/            outbound_queue_size $BASE_OUTBOUND_QUEUE_SIZE/" \
    -e "s/^[[:space:]]*write_burst_size .*/            write_burst_size $write_burst_size/" \
    -e "s/^[[:space:]]*fanout_backpressure_threshold .*/            fanout_backpressure_threshold $fanout_threshold/" \
    -e "s/^[[:space:]]*fanout_backpressure_max_wait .*/            fanout_backpressure_max_wait $BASE_FANOUT_MAX_WAIT/" \
    -e "s/^[[:space:]]*fanout_mode .*/            fanout_mode $BASE_FANOUT_MODE/" \
    -e "s/^[[:space:]]*fanout_round_size .*/            fanout_round_size $BASE_FANOUT_ROUND_SIZE/" \
    -e "s/^[[:space:]]*fanout_round_yield .*/            fanout_round_yield $BASE_FANOUT_ROUND_YIELD/" \
    -e "s/^[[:space:]]*enable_compression .*/            enable_compression $compression/" \
    "$BENCH_DIR/pogo/Caddyfile" > "$RUNTIME_CADDYFILE"

  cat > "$COMPOSE_OVERRIDE_FILE" <<YAML
services:
  pogo:
    environment:
      POGO_WS_HOT_PATH_METRICS: "$POGO_WS_HOT_PATH_METRICS"
      POGO_WS_OUTBOUND_QUEUE_SIZE: "$BASE_OUTBOUND_QUEUE_SIZE"
      POGO_WS_WRITE_BURST_SIZE: "$write_burst_size"
      POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD: "$fanout_threshold"
      POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT: "$BASE_FANOUT_MAX_WAIT"
      POGO_WS_FANOUT_MODE: "$BASE_FANOUT_MODE"
      POGO_WS_FANOUT_ROUND_SIZE: "$BASE_FANOUT_ROUND_SIZE"
      POGO_WS_FANOUT_ROUND_YIELD: "$BASE_FANOUT_ROUND_YIELD"
      POGO_WS_ENABLE_COMPRESSION: "$compression"
    volumes:
      - "$RUNTIME_CADDYFILE:/var/www/html/Caddyfile:ro"
YAML
}

{
  printf 'timestamp=%s\n' "$STAMP"
  printf 'git_sha=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  printf 'git_status=\n'
  git -C "$ROOT_DIR" status --short || true
  printf '\nbenchmark_env=\n'
  env | sort | grep -E '^(VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|RAMP_UP_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|POGO_WS_HOT_PATH_METRICS|POGO_WS_OUTBOUND_QUEUE_SIZE|POGO_WS_WRITE_BURST_SIZE|POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD|POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT|POGO_WS_ENABLE_COMPRESSION)=' || true
} > "$RESULTS_DIR/run-$STAMP-pogo-tuning-meta.txt"

json_number() {
  local file="$1"
  local key="$2"

  if [ ! -f "$file" ]; then
    printf 'null'
    return
  fi

  awk -v key="\"$key\"" '
    index($0, key) {
      value = $0
      sub(".*: *", "", value)
      sub(",.*", "", value)
      gsub(/^ *| *$/, "", value)
      print value
      found = 1
      exit
    }
    END {
      if (!found) {
        print "null"
      }
    }
  ' "$file"
}

write_audit_row() {
  local scenario="$1"
  local write_burst_size="$2"
  local fanout_threshold="$3"
  local compression="$4"
  local payload_size="$5"
  local msg_count="$6"
  local file="$7"
  local effective_write_burst_size
  local effective_fanout_threshold
  local effective_compression
  local expected_compression
  local config_match

  effective_write_burst_size="$(json_number "$file" writeBurstSize)"
  effective_fanout_threshold="$(json_number "$file" fanoutBackpressureThreshold)"
  effective_compression="$(json_number "$file" enableCompression)"
  if [ "$compression" = "true" ]; then
    expected_compression=1
  else
    expected_compression=0
  fi
  if [ "$effective_write_burst_size" = "$write_burst_size" ] && \
    [ "$effective_fanout_threshold" = "$fanout_threshold" ] && \
    [ "$effective_compression" = "$expected_compression" ]; then
    config_match=true
  else
    config_match=false
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$write_burst_size" \
    "$fanout_threshold" \
    "$compression" \
    "$effective_write_burst_size" \
    "$effective_fanout_threshold" \
    "$effective_compression" \
    "$config_match" \
    "$payload_size" \
    "$msg_count" \
    "$(json_number "$file" writeCompleteFromSentP95Ms)" \
    "$(json_number "$file" sentToReadP95Ms)" \
    "$(json_number "$file" sentToReadP99Ms)" \
    "$(json_number "$file" deliveryCompleteness)" \
    "$(json_number "$file" connectErrors)" \
    "$(json_number "$file" parseErrors)" \
    "$(json_number "$file" readErrors)" \
    "$(json_number "$file" publishErrors)"

  if [ "$config_match" != "true" ]; then
    printf 'Scenario %s effective config mismatch: expected write_burst=%s fanout_threshold=%s compression=%s, got write_burst=%s fanout_threshold=%s compression=%s\n' \
      "$scenario" \
      "$write_burst_size" \
      "$fanout_threshold" \
      "$expected_compression" \
      "$effective_write_burst_size" \
      "$effective_fanout_threshold" \
      "$effective_compression" >&2
    return 1
  fi
}

run_tuning_scenario() {
  local scenario="$1"
  local write_burst_size="$2"
  local fanout_threshold="$3"
  local compression="$4"
  local payload_size="$5"
  local msg_count="$6"
  local result_file="/results/go-receiver-tuning-${scenario}-summary.json"
  local metrics_file="/results/go-receiver-tuning-${scenario}-metrics.prom"

  render_runtime_caddyfile "$write_burst_size" "$fanout_threshold" "$compression"
  compose down -v --remove-orphans
  POGO_WS_WRITE_BURST_SIZE="$write_burst_size" \
  POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD="$fanout_threshold" \
  POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT="$BASE_FANOUT_MAX_WAIT" \
  POGO_WS_OUTBOUND_QUEUE_SIZE="$BASE_OUTBOUND_QUEUE_SIZE" \
  POGO_WS_ENABLE_COMPRESSION="$compression" \
  POGO_WS_HOT_PATH_METRICS="$POGO_WS_HOT_PATH_METRICS" \
    compose up -d pogo
  compose run --rm \
    -e ROLE=both \
    -e VUS="$BASE_VUS" \
    -e MSG_COUNT="$msg_count" \
    -e PAYLOAD_SIZE="$payload_size" \
    -e PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
    -e BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
    -e RAMP_UP_SECONDS="$BASE_RAMP_UP_SECONDS" \
    -e PUBLISH_MAX_DURATION_SECONDS="$BASE_PUBLISH_MAX_DURATION_SECONDS" \
    -e DRAIN_SECONDS="$BASE_DRAIN_SECONDS" \
    -e METRICS_URL=http://pogo:2019/metrics \
    -e METRICS_FILE="$metrics_file" \
    -e RESULT_FILE="$result_file" \
    go-receiver-pogo 2>&1 | tee "$RESULTS_DIR/run-$STAMP-tuning-${scenario}.log"
  compose down -v --remove-orphans

  write_audit_row "$scenario" "$write_burst_size" "$fanout_threshold" "$compression" "$payload_size" "$msg_count" "$RESULTS_DIR/$(basename "$result_file")" >> "$AUDIT_FILE"
}

printf 'scenario\twrite_burst_size\tfanout_backpressure_threshold\tcompression\teffective_write_burst_size\teffective_fanout_backpressure_threshold\teffective_compression\tconfig_match\tpayload_size\tmsg_count\twrite_complete_from_sent_p95_ms\tgo_receiver_p95_ms\tgo_receiver_p99_ms\tdelivery_completeness\tconnect_errors\tparse_errors\tread_errors\tpublish_errors\n' > "$AUDIT_FILE"

render_runtime_caddyfile "$BASE_WRITE_BURST_SIZE" "$BASE_FANOUT_THRESHOLD" "$BASE_COMPRESSION"
compose down -v --remove-orphans
compose build --no-cache pogo go-receiver-pogo

run_tuning_scenario "baseline" "$BASE_WRITE_BURST_SIZE" "$BASE_FANOUT_THRESHOLD" "$BASE_COMPRESSION" "$BASE_PAYLOAD_SIZE" "$BASE_MSG_COUNT"

for write_burst_size in 1 8 16 64; do
  run_tuning_scenario "write-burst-${write_burst_size}" "$write_burst_size" "$BASE_FANOUT_THRESHOLD" "$BASE_COMPRESSION" "$BASE_PAYLOAD_SIZE" "$BASE_MSG_COUNT"
done

for fanout_threshold in 1 4 16 64; do
  run_tuning_scenario "fanout-threshold-${fanout_threshold}" "$BASE_WRITE_BURST_SIZE" "$fanout_threshold" "$BASE_COMPRESSION" "$BASE_PAYLOAD_SIZE" "$BASE_MSG_COUNT"
done

for payload_size in 1024 4096; do
  run_tuning_scenario "compression-off-payload-${payload_size}" "$BASE_WRITE_BURST_SIZE" "$BASE_FANOUT_THRESHOLD" "false" "$payload_size" "$BASE_MSG_COUNT"
  run_tuning_scenario "compression-on-payload-${payload_size}" "$BASE_WRITE_BURST_SIZE" "$BASE_FANOUT_THRESHOLD" "true" "$payload_size" "$BASE_MSG_COUNT"
done

compose down -v --remove-orphans

printf 'Wrote Pogo tuning audit to %s\n' "$AUDIT_FILE"
