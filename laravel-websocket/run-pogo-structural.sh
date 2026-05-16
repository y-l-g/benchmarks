#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/laravel-broadcast"
COMPOSE_FILE="$BENCH_DIR/compose.yaml"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AUDIT_FILE="$RESULTS_DIR/run-$STAMP-pogo-structural-audit.tsv"
RUNTIME_CADDYFILE="$RESULTS_DIR/run-$STAMP-pogo-structural-Caddyfile"
COMPOSE_OVERRIDE_FILE="$RESULTS_DIR/run-$STAMP-pogo-structural-compose.override.yaml"

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
BASE_FANOUT_ROUND_SIZE="${POGO_WS_FANOUT_ROUND_SIZE:-16}"
BASE_COMPRESSION="${POGO_WS_ENABLE_COMPRESSION:-false}"

compose() {
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_OVERRIDE_FILE" "$@"
}

render_runtime_caddyfile() {
  local fanout_mode="$1"
  local round_size="$2"
  local round_yield="$3"

  sed \
    -e "s/^[[:space:]]*outbound_queue_size .*/            outbound_queue_size $BASE_OUTBOUND_QUEUE_SIZE/" \
    -e "s/^[[:space:]]*write_burst_size .*/            write_burst_size $BASE_WRITE_BURST_SIZE/" \
    -e "s/^[[:space:]]*fanout_backpressure_threshold .*/            fanout_backpressure_threshold $BASE_FANOUT_THRESHOLD/" \
    -e "s/^[[:space:]]*fanout_backpressure_max_wait .*/            fanout_backpressure_max_wait $BASE_FANOUT_MAX_WAIT/" \
    -e "s/^[[:space:]]*fanout_mode .*/            fanout_mode $fanout_mode/" \
    -e "s/^[[:space:]]*fanout_round_size .*/            fanout_round_size $round_size/" \
    -e "s/^[[:space:]]*fanout_round_yield .*/            fanout_round_yield $round_yield/" \
    -e "s/^[[:space:]]*enable_compression .*/            enable_compression $BASE_COMPRESSION/" \
    "$BENCH_DIR/pogo/Caddyfile" > "$RUNTIME_CADDYFILE"

  cat > "$COMPOSE_OVERRIDE_FILE" <<YAML
services:
  pogo:
    environment:
      POGO_WS_HOT_PATH_METRICS: "$POGO_WS_HOT_PATH_METRICS"
      POGO_WS_OUTBOUND_QUEUE_SIZE: "$BASE_OUTBOUND_QUEUE_SIZE"
      POGO_WS_WRITE_BURST_SIZE: "$BASE_WRITE_BURST_SIZE"
      POGO_WS_FANOUT_BACKPRESSURE_THRESHOLD: "$BASE_FANOUT_THRESHOLD"
      POGO_WS_FANOUT_BACKPRESSURE_MAX_WAIT: "$BASE_FANOUT_MAX_WAIT"
      POGO_WS_FANOUT_MODE: "$fanout_mode"
      POGO_WS_FANOUT_ROUND_SIZE: "$round_size"
      POGO_WS_FANOUT_ROUND_YIELD: "$round_yield"
      POGO_WS_ENABLE_COMPRESSION: "$BASE_COMPRESSION"
    volumes:
      - "$RUNTIME_CADDYFILE:/var/www/html/Caddyfile:ro"
YAML
}

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
  local fanout_mode="$2"
  local round_size="$3"
  local round_yield="$4"
  local file="$5"
  local expected_paced=0
  local effective_paced
  local effective_round_size
  local effective_round_yield_ms
  local expected_round_yield_ms
  local config_match=false

  if [ "$fanout_mode" = "paced" ]; then
    expected_paced=1
  fi
  effective_paced="$(json_number "$file" fanoutModePaced)"
  effective_round_size="$(json_number "$file" fanoutRoundSize)"
  effective_round_yield_ms="$(json_number "$file" fanoutRoundYieldMs)"
  case "$round_yield" in
    0ms) expected_round_yield_ms=0 ;;
    1ms) expected_round_yield_ms=1 ;;
    *) expected_round_yield_ms="$round_yield" ;;
  esac
  if [ "$effective_paced" = "$expected_paced" ] && \
    [ "$effective_round_size" = "$round_size" ] && \
    [ "$effective_round_yield_ms" = "$expected_round_yield_ms" ]; then
    config_match=true
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$fanout_mode" \
    "$round_size" \
    "$round_yield" \
    "$effective_paced" \
    "$effective_round_size" \
    "$effective_round_yield_ms" \
    "$config_match" \
    "$(json_number "$file" writeCompleteFromSentP95Ms)" \
    "$(json_number "$file" sentToReadP95Ms)" \
    "$(json_number "$file" sentToReadP99Ms)" \
    "$(json_number "$file" fanoutSubscribersP95)" \
    "$(json_number "$file" clientQueueDepthP95)" \
    "$(json_number "$file" clientQueueDepthP99)" \
    "$(json_number "$file" clientQueueResidenceP95Ms)" \
    "$(json_number "$file" clientQueueResidenceP99Ms)" \
    "$(json_number "$file" deliveryCompleteness)" \
    "$(json_number "$file" connectErrors)" \
    "$(json_number "$file" parseErrors)" \
    "$(json_number "$file" readErrors)"

  if [ "$config_match" != "true" ]; then
    printf 'Scenario %s effective config mismatch: expected paced=%s round_size=%s round_yield_ms=%s, got paced=%s round_size=%s round_yield_ms=%s\n' \
      "$scenario" \
      "$expected_paced" \
      "$round_size" \
      "$expected_round_yield_ms" \
      "$effective_paced" \
      "$effective_round_size" \
      "$effective_round_yield_ms" >&2
    return 1
  fi
}

run_structural_scenario() {
  local scenario="$1"
  local fanout_mode="$2"
  local round_size="$3"
  local round_yield="$4"
  local result_file="/results/go-receiver-structural-${scenario}-summary.json"
  local metrics_file="/results/go-receiver-structural-${scenario}-metrics.prom"

  render_runtime_caddyfile "$fanout_mode" "$round_size" "$round_yield"
  compose down -v --remove-orphans
  compose up -d pogo
  compose run --rm \
    -e ROLE=both \
    -e VUS="$BASE_VUS" \
    -e MSG_COUNT="$BASE_MSG_COUNT" \
    -e PAYLOAD_SIZE="$BASE_PAYLOAD_SIZE" \
    -e PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
    -e BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
    -e RAMP_UP_SECONDS="$BASE_RAMP_UP_SECONDS" \
    -e PUBLISH_MAX_DURATION_SECONDS="$BASE_PUBLISH_MAX_DURATION_SECONDS" \
    -e DRAIN_SECONDS="$BASE_DRAIN_SECONDS" \
    -e METRICS_URL=http://pogo:2019/metrics \
    -e METRICS_FILE="$metrics_file" \
    -e RESULT_FILE="$result_file" \
    go-receiver-pogo 2>&1 | tee "$RESULTS_DIR/run-$STAMP-structural-${scenario}.log"
  compose down -v --remove-orphans

  write_audit_row "$scenario" "$fanout_mode" "$round_size" "$round_yield" "$RESULTS_DIR/$(basename "$result_file")" >> "$AUDIT_FILE"
}

{
  printf 'timestamp=%s\n' "$STAMP"
  printf 'git_sha=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  printf 'git_status=\n'
  git -C "$ROOT_DIR" status --short || true
  printf '\nbenchmark_env=\n'
  env | sort | grep -E '^(VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|RAMP_UP_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|POGO_WS_)' || true
} > "$RESULTS_DIR/run-$STAMP-pogo-structural-meta.txt"

printf 'scenario\tfanout_mode\tfanout_round_size\tfanout_round_yield\teffective_fanout_mode_paced\teffective_fanout_round_size\teffective_fanout_round_yield_ms\tconfig_match\twrite_complete_from_sent_p95_ms\tgo_receiver_p95_ms\tgo_receiver_p99_ms\tfanout_subscribers_p95\tclient_queue_depth_p95\tclient_queue_depth_p99\tclient_queue_residence_p95_ms\tclient_queue_residence_p99_ms\tdelivery_completeness\tconnect_errors\tparse_errors\tread_errors\n' > "$AUDIT_FILE"

render_runtime_caddyfile "burst" "$BASE_FANOUT_ROUND_SIZE" "0ms"
compose down -v --remove-orphans
compose build --no-cache pogo go-receiver-pogo

run_structural_scenario "baseline-burst" "burst" "$BASE_FANOUT_ROUND_SIZE" "0ms"
run_structural_scenario "paced-round-8-yield-0ms" "paced" "8" "0ms"
run_structural_scenario "paced-round-16-yield-0ms" "paced" "16" "0ms"
run_structural_scenario "paced-round-32-yield-0ms" "paced" "32" "0ms"
run_structural_scenario "paced-round-16-yield-1ms" "paced" "16" "1ms"

compose down -v --remove-orphans

printf 'Wrote Pogo structural audit to %s\n' "$AUDIT_FILE"
