#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  "")
    ;;
  -h|--help|help)
    cat <<'USAGE'
Usage: ./laravel-websocket/run.sh

Runs the compact deep benchmark suite and writes one combined audit TSV.

Set NO_CACHE=1 to force clean Docker image builds.
USAGE
    exit 0
    ;;
  *)
    printf 'Unknown argument: %s\n' "$1" >&2
    exit 2
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/laravel-websocket"
COMPOSE_FILE="$BENCH_DIR/compose.yaml"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AUDIT_FILE="$RESULTS_DIR/run-$STAMP-audit.tsv"
SUMMARY_FILE="$RESULTS_DIR/run-$STAMP-summary.txt"

mkdir -p "$RESULTS_DIR"
chmod 0777 "$RESULTS_DIR"
export K6_UID="${K6_UID:-$(id -u)}"
export K6_GID="${K6_GID:-$(id -g)}"
export POGO_WS_HOT_PATH_METRICS="${POGO_WS_HOT_PATH_METRICS:-true}"

BASE_VUS="${VUS:-500}"
BASE_SHARD_VUS="${SHARD_VUS:-100}"
BASE_MSG_COUNT="${MSG_COUNT:-100}"
BASE_PAYLOAD_SIZE="${PAYLOAD_SIZE:-1024}"
BASE_PUBLISH_BATCHES="${PUBLISH_BATCHES:-20}"
BASE_BATCH_INTERVAL_SECONDS="${BATCH_INTERVAL_SECONDS:-2}"
BASE_RAMP_UP_SECONDS="${RAMP_UP_SECONDS:-10}"
BASE_PUBLISH_START_SECONDS="${PUBLISH_START_SECONDS:-12}"
BASE_PUBLISH_MAX_DURATION_SECONDS="${PUBLISH_MAX_DURATION_SECONDS:-}"
BASE_DRAIN_SECONDS="${DRAIN_SECONDS:-10}"
BASE_COMPRESSION="${POGO_WS_ENABLE_COMPRESSION:-false}"
RUNTIME_CADDYFILE="$RESULTS_DIR/run-$STAMP-Caddyfile"
COMPOSE_OVERRIDE_FILE="$RESULTS_DIR/run-$STAMP-compose.override.yaml"

compose() {
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_OVERRIDE_FILE" "$@"
}

render_runtime_caddyfile() {
  local compression="$1"

  sed \
    -e "s/^[[:space:]]*enable_compression .*/            enable_compression $compression/" \
    "$BENCH_DIR/pogo/Caddyfile" > "$RUNTIME_CADDYFILE"

  cat > "$COMPOSE_OVERRIDE_FILE" <<YAML
services:
  pogo:
    environment:
      POGO_WS_HOT_PATH_METRICS: "$POGO_WS_HOT_PATH_METRICS"
      POGO_WS_ENABLE_COMPRESSION: "$compression"
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

max_json_number() {
  local key="$1"
  shift

  awk -v key="\"$key\"" '
    FILENAME != current {
      current = FILENAME
      seen_file = 0
    }
    !seen_file && index($0, key) {
      value = $0
      sub(".*: *", "", value)
      sub(",.*", "", value)
      gsub(/^ *| *$/, "", value)
      if (value != "null" && value != "") {
        if (!found || value + 0 > max) {
          max = value + 0
        }
        found = 1
      }
      seen_file = 1
    }
    END {
      if (found) {
        printf "%.15g\n", max
      } else {
        print "null"
      }
    }
  ' "$@"
}

min_json_number() {
  local key="$1"
  shift

  awk -v key="\"$key\"" '
    FILENAME != current {
      current = FILENAME
      seen_file = 0
    }
    !seen_file && index($0, key) {
      value = $0
      sub(".*: *", "", value)
      sub(",.*", "", value)
      gsub(/^ *| *$/, "", value)
      if (value != "null" && value != "") {
        if (!found || value + 0 < min) {
          min = value + 0
        }
        found = 1
      }
      seen_file = 1
    }
    END {
      if (found) {
        printf "%.15g\n", min
      } else {
        print "null"
      }
    }
  ' "$@"
}

assert_number_equals() {
  local label="$1"
  local got="$2"
  local want="$3"

  if [ "$got" != "$want" ]; then
    printf 'FAILED: %s expected %s, got %s\n' "$label" "$want" "$got" >&2
    exit 1
  fi
}

assert_zero() {
  local label="$1"
  local got="$2"

  if [ "$got" != "0" ]; then
    printf 'FAILED: %s expected 0, got %s\n' "$label" "$got" >&2
    exit 1
  fi
}

assert_present() {
  local label="$1"
  local got="$2"

  if [ "$got" = "null" ] || [ -z "$got" ]; then
    printf 'FAILED: %s is missing\n' "$label" >&2
    exit 1
  fi
}

write_meta() {
  {
    printf 'timestamp=%s\n' "$STAMP"
    printf 'git_sha=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf 'unknown')"
    printf 'git_status=\n'
    git -C "$ROOT_DIR" status --short || true
    printf '\nbenchmark_env=\n'
    env | sort | grep -E '^(VUS|SHARD_VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|RAMP_UP_SECONDS|HOLD_SECONDS|RAMP_DOWN_SECONDS|PUBLISH_START_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|LATENCY_P95_THRESHOLD_MS|NO_CACHE|POGO_WS_)=' || true
  } > "$RESULTS_DIR/run-$STAMP-meta.txt"
}

write_audit_header() {
  printf 'scenario\tprobe\tdriver\trole\tvus\tmsg_count\tpayload_size\tpublish_batches\tcompression\tdelivery_completeness\treceive_p95_ms\treceive_p99_ms\twrite_complete_from_sent_p95_ms\tconnect_errors\tparse_errors\tread_errors\tpublish_errors\tconfig_match\n' > "$AUDIT_FILE"
}

write_k6_row() {
  local scenario="$1"
  local driver="$2"
  local file="$3"
  local write_p95="null"
  local completeness

  completeness="$(json_number "$file" completeness)"
  if [ "$driver" = "pogo" ]; then
    write_p95="$(json_number "$file" writeCompleteFromSentP95Ms)"
    assert_present "$scenario writeCompleteFromSentP95Ms" "$write_p95"
  fi

  assert_number_equals "$scenario delivery completeness" "$completeness" "1"

  printf '%s\tk6\t%s\tboth\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t0\t0\ttrue\n' \
    "$scenario" \
    "$driver" \
    "$(json_number "$file" vus)" \
    "$(json_number "$file" msgCount)" \
    "$(json_number "$file" payloadSize)" \
    "$(json_number "$file" publishBatches)" \
    "$BASE_COMPRESSION" \
    "$completeness" \
    "$(json_number "$file" eventSentToReceivedP95Ms)" \
    "$(json_number "$file" eventSentToReceivedP99Ms)" \
    "$write_p95" >> "$AUDIT_FILE"
}

write_go_row() {
  local scenario="$1"
  local file="$2"
  local compression="$3"
  local config_match=true
  local effective_compression

  if [ "$compression" = "true" ]; then
    effective_compression="$(json_number "$file" enableCompression)"
    if [ "$effective_compression" != "1" ]; then
      config_match=false
    fi
  elif [ "$compression" = "false" ]; then
    effective_compression="$(json_number "$file" enableCompression)"
    if [ "$effective_compression" != "0" ]; then
      config_match=false
    fi
  fi

  assert_number_equals "$scenario delivery completeness" "$(json_number "$file" deliveryCompleteness)" "1"
  assert_zero "$scenario connect errors" "$(json_number "$file" connectErrors)"
  assert_zero "$scenario parse errors" "$(json_number "$file" parseErrors)"
  assert_zero "$scenario read errors" "$(json_number "$file" readErrors)"
  assert_zero "$scenario publish errors" "$(json_number "$file" publishErrors)"
  assert_present "$scenario writeCompleteFromSentP95Ms" "$(json_number "$file" writeCompleteFromSentP95Ms)"

  if [ "$config_match" != "true" ]; then
    printf 'FAILED: %s effective compression did not match %s\n' "$scenario" "$compression" >&2
    exit 1
  fi

  printf '%s\tgo-receiver\tpogo\tboth\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$(json_number "$file" vus)" \
    "$(json_number "$file" msgCount)" \
    "$(json_number "$file" payloadSize)" \
    "$(json_number "$file" publishBatches)" \
    "$compression" \
    "$(json_number "$file" deliveryCompleteness)" \
    "$(json_number "$file" sentToReadP95Ms)" \
    "$(json_number "$file" sentToReadP99Ms)" \
    "$(json_number "$file" writeCompleteFromSentP95Ms)" \
    "$(json_number "$file" connectErrors)" \
    "$(json_number "$file" parseErrors)" \
    "$(json_number "$file" readErrors)" \
    "$(json_number "$file" publishErrors)" \
    "$config_match" >> "$AUDIT_FILE"
}

write_go_sharded_row() {
  local scenario="$1"
  local publisher="$RESULTS_DIR/go-receiver-sharded-publisher-summary.json"
  local shards=(
    "$RESULTS_DIR/go-receiver-shard-1-summary.json"
    "$RESULTS_DIR/go-receiver-shard-2-summary.json"
    "$RESULTS_DIR/go-receiver-shard-3-summary.json"
    "$RESULTS_DIR/go-receiver-shard-4-summary.json"
    "$RESULTS_DIR/go-receiver-shard-5-summary.json"
  )

  assert_number_equals "$scenario delivery completeness" "$(min_json_number deliveryCompleteness "${shards[@]}")" "1"
  assert_zero "$scenario connect errors" "$(max_json_number connectErrors "${shards[@]}")"
  assert_zero "$scenario parse errors" "$(max_json_number parseErrors "${shards[@]}")"
  assert_zero "$scenario read errors" "$(max_json_number readErrors "${shards[@]}")"
  assert_zero "$scenario publish errors" "$(json_number "$publisher" publishErrors)"
  assert_present "$scenario writeCompleteFromSentP95Ms" "$(json_number "$publisher" writeCompleteFromSentP95Ms)"

  printf '%s\tgo-receiver\tpogo\tsharded\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\ttrue\n' \
    "$scenario" \
    "$((BASE_SHARD_VUS * 5))" \
    "$BASE_MSG_COUNT" \
    "$BASE_PAYLOAD_SIZE" \
    "$BASE_PUBLISH_BATCHES" \
    "$BASE_COMPRESSION" \
    "$(min_json_number deliveryCompleteness "${shards[@]}")" \
    "$(max_json_number sentToReadP95Ms "${shards[@]}")" \
    "$(max_json_number sentToReadP99Ms "${shards[@]}")" \
    "$(json_number "$publisher" writeCompleteFromSentP95Ms)" \
    "$(max_json_number connectErrors "${shards[@]}")" \
    "$(max_json_number parseErrors "${shards[@]}")" \
    "$(max_json_number readErrors "${shards[@]}")" \
    "$(json_number "$publisher" publishErrors)" >> "$AUDIT_FILE"
}

build_images() {
  local build_args=(build)

  if [ "${NO_CACHE:-0}" = "1" ]; then
    build_args+=(--no-cache)
  fi

  build_args+=(pogo reverb-app reverb-ws go-receiver-pogo)
  compose "${build_args[@]}"
  compose images > "$RESULTS_DIR/run-$STAMP-images.txt"
}

start_pogo() {
  compose up -d pogo
}

stop_all() {
  compose down -v --remove-orphans
}

run_k6_pogo() {
  compose up --force-recreate --abort-on-container-exit --exit-code-from k6-pogo k6-pogo 2>&1 \
    | tee "$RESULTS_DIR/run-$STAMP-pogo-k6.log"
  write_k6_row "pogo-k6-baseline" "pogo" "$RESULTS_DIR/pogo-summary.json"
}

run_k6_reverb() {
  compose up --force-recreate --abort-on-container-exit --exit-code-from k6-reverb k6-reverb 2>&1 \
    | tee "$RESULTS_DIR/run-$STAMP-reverb-k6.log"
  write_k6_row "reverb-k6-baseline" "reverb" "$RESULTS_DIR/reverb-summary.json"
}

run_go_receiver() {
  local scenario="$1"
  local vus="$2"
  local msg_count="$3"
  local payload_size="$4"
  local compression="$5"
  local result_file="/results/go-receiver-${scenario}-summary.json"
  local metrics_file="/results/go-receiver-${scenario}-metrics.prom"

  compose run --rm \
    -e ROLE=both \
    -e VUS="$vus" \
    -e MSG_COUNT="$msg_count" \
    -e PAYLOAD_SIZE="$payload_size" \
    -e PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
    -e BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
    -e RAMP_UP_SECONDS="$BASE_RAMP_UP_SECONDS" \
    -e PUBLISH_START_SECONDS="$BASE_PUBLISH_START_SECONDS" \
    -e PUBLISH_MAX_DURATION_SECONDS="$BASE_PUBLISH_MAX_DURATION_SECONDS" \
    -e DRAIN_SECONDS="$BASE_DRAIN_SECONDS" \
    -e METRICS_URL=http://pogo:2019/metrics \
    -e METRICS_FILE="$metrics_file" \
    -e RESULT_FILE="$result_file" \
    go-receiver-pogo 2>&1 | tee "$RESULTS_DIR/run-$STAMP-${scenario}.log"

  write_go_row "$scenario" "$RESULTS_DIR/$(basename "$result_file")" "$compression"
}

run_go_sharded() {
  SHARD_VUS="$BASE_SHARD_VUS" \
  VUS="$BASE_VUS" \
  MSG_COUNT="$BASE_MSG_COUNT" \
  PAYLOAD_SIZE="$BASE_PAYLOAD_SIZE" \
  PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
  BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
  RAMP_UP_SECONDS="$BASE_RAMP_UP_SECONDS" \
  PUBLISH_START_SECONDS="$BASE_PUBLISH_START_SECONDS" \
  PUBLISH_MAX_DURATION_SECONDS="$BASE_PUBLISH_MAX_DURATION_SECONDS" \
  DRAIN_SECONDS="$BASE_DRAIN_SECONDS" \
    compose up --force-recreate \
      go-receiver-pogo-listener-1 \
      go-receiver-pogo-listener-2 \
      go-receiver-pogo-listener-3 \
      go-receiver-pogo-listener-4 \
      go-receiver-pogo-listener-5 \
      go-receiver-pogo-publisher 2>&1 | tee "$RESULTS_DIR/run-$STAMP-go-sharded-5x${BASE_SHARD_VUS}.log"

  write_go_sharded_row "go-sharded-5x${BASE_SHARD_VUS}"
}

run_deep() {
  write_meta
  write_audit_header
  render_runtime_caddyfile "$BASE_COMPRESSION"

  stop_all
  build_images

  run_k6_pogo
  stop_all

  run_k6_reverb
  stop_all

  start_pogo
  run_go_receiver "go-baseline" "$BASE_VUS" "$BASE_MSG_COUNT" "$BASE_PAYLOAD_SIZE" "$BASE_COMPRESSION"
  stop_all

  render_runtime_caddyfile "$BASE_COMPRESSION"
  start_pogo
  run_go_sharded
  stop_all

  render_runtime_caddyfile "$BASE_COMPRESSION"
  start_pogo
  for payload_size in 16 1024 4096; do
    run_go_receiver "payload-${payload_size}" "$BASE_VUS" "$BASE_MSG_COUNT" "$payload_size" "$BASE_COMPRESSION"
  done
  for msg_count in 10 100 250; do
    run_go_receiver "batch-${msg_count}" "$BASE_VUS" "$msg_count" "$BASE_PAYLOAD_SIZE" "$BASE_COMPRESSION"
  done
  stop_all

  for compression in false true; do
    render_runtime_caddyfile "$compression"
    start_pogo
    run_go_receiver "compression-${compression}-payload-4096" "$BASE_VUS" "$BASE_MSG_COUNT" "4096" "$compression"
    stop_all
  done
}

run_deep

{
  printf 'BENCHMARK SUMMARY\n'
  printf 'audit_file=%s\n' "$AUDIT_FILE"
  printf 'results_dir=%s\n' "$RESULTS_DIR"
  printf 'scenarios=%s\n' "$(( $(wc -l < "$AUDIT_FILE") - 1 ))"
} | tee "$SUMMARY_FILE"

printf 'Wrote benchmark audit to %s\n' "$AUDIT_FILE"
