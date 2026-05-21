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
BASE_PUBLISH_MESSAGE_INTERVAL_MS="${PUBLISH_MESSAGE_INTERVAL_MS:-0}"
BASE_RAMP_UP_SECONDS="${RAMP_UP_SECONDS:-10}"
BASE_PUBLISH_START_SECONDS="${PUBLISH_START_SECONDS:-12}"
BASE_PUBLISH_MAX_DURATION_SECONDS="${PUBLISH_MAX_DURATION_SECONDS:-}"
BASE_DRAIN_SECONDS="${DRAIN_SECONDS:-10}"
case "$(printf '%s' "${POGO_WS_ENABLE_COMPRESSION:-false}" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on)
    BASE_COMPRESSION=true
    ;;
  *)
    BASE_COMPRESSION=false
    ;;
esac
POGO_WS_MODULE_REF="${POGO_WS_MODULE_REF:-3a48a17503f71f5b1cfb2ec9f3c7115b64046736}"
export POGO_WS_MODULE_REF
RUNTIME_CADDYFILE="$RESULTS_DIR/run-$STAMP-Caddyfile"
COMPOSE_OVERRIDE_FILE="$RESULTS_DIR/run-$STAMP-compose.override.yaml"

compose() {
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_OVERRIDE_FILE" "$@"
}

result_path() {
  printf '/results/run-%s-%s' "$STAMP" "$1"
}

host_result_path() {
  printf '%s/%s' "$RESULTS_DIR" "$(basename "$1")"
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
    env | sort | grep -E '^(VUS|SHARD_VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|PUBLISH_MESSAGE_INTERVAL_MS|RAMP_UP_SECONDS|HOLD_SECONDS|RAMP_DOWN_SECONDS|PUBLISH_START_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|LATENCY_P95_THRESHOLD_MS|NO_CACHE|POGO_WS_.*)=' || true
    printf '\nhost=\n'
    uname -a || true
    printf '\ndocker=\n'
    docker version --format 'client={{.Client.Version}} server={{.Server.Version}}' 2>/dev/null || true
    docker compose version 2>/dev/null || true
  } > "$RESULTS_DIR/run-$STAMP-meta.txt"
}

write_audit_header() {
  printf 'scenario\tprobe\tdriver\trole\tvus\tmsg_count\tpayload_size\tpublish_batches\tcompression\tclient_compression\tnegotiated_compression\tdelivery_completeness\tobserved_messages\texpected_messages\treceive_p50_ms\treceive_p95_ms\treceive_p99_ms\tpublish_p95_ms\tconnect_errors\tconnect_retry_failures\tparse_errors\tread_errors\tpublish_errors\tdata_write_failures\tclient_dropped_messages\tbroker_dropped_messages\tconfig_match\n' > "$AUDIT_FILE"
}

write_k6_row() {
  local scenario="$1"
  local driver="$2"
  local file="$3"
  local compression="$4"
  local completeness
  local connect_errors
  local parse_errors
  local publish_errors
  local data_write_failures
  local client_dropped_messages
  local broker_dropped_messages
  local config_match=true

  completeness="$(json_number "$file" completeness)"
  connect_errors="$(json_number "$file" connectErrors)"
  parse_errors="$(json_number "$file" parseErrors)"
  publish_errors="$(json_number "$file" publishErrors)"
  data_write_failures="$(json_number "$file" dataWriteFailuresTotal)"
  client_dropped_messages="$(json_number "$file" clientDroppedMessagesTotal)"
  broker_dropped_messages="$(json_number "$file" brokerDroppedMessagesTotal)"

  assert_number_equals "$scenario delivery completeness" "$completeness" "1"
  assert_zero "$scenario connect errors" "$connect_errors"
  assert_zero "$scenario parse errors" "$parse_errors"
  assert_zero "$scenario publish errors" "$publish_errors"
  if [ "$driver" = "pogo" ]; then
    assert_zero "$scenario data write failures" "$data_write_failures"
  fi

  printf '%s\tk6\t%s\tboth\t%s\t%s\t%s\t%s\t%s\tn/a\tn/a\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t%s\t0\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$driver" \
    "$(json_number "$file" vus)" \
    "$(json_number "$file" msgCount)" \
    "$(json_number "$file" payloadSize)" \
    "$(json_number "$file" publishBatches)" \
    "$compression" \
    "$completeness" \
    "$(json_number "$file" observed)" \
    "$(json_number "$file" totalExpectedMessages)" \
    "$(json_number "$file" eventSentToReceivedP50Ms)" \
    "$(json_number "$file" eventSentToReceivedP95Ms)" \
    "$(json_number "$file" eventSentToReceivedP99Ms)" \
    "$(json_number "$file" publishP95Ms)" \
    "$connect_errors" \
    "$parse_errors" \
    "$publish_errors" \
    "$data_write_failures" \
    "$client_dropped_messages" \
    "$broker_dropped_messages" \
    "$config_match" >> "$AUDIT_FILE"
}

write_go_row() {
  local scenario="$1"
  local file="$2"
  local driver="$3"
  local compression="$4"
  local role="${5:-both}"
  local config_match=true
  local effective_compression
  local client_compression
  local negotiated_compression
  local data_write_failures
  local client_dropped_messages
  local broker_dropped_messages
  local vus

  vus="$(json_number "$file" vus)"
  client_compression="$(json_number "$file" clientCompression)"
  negotiated_compression="$(json_number "$file" negotiatedCompressionConnections)"
  data_write_failures="$(json_number "$file" dataWriteFailuresTotal)"
  client_dropped_messages="$(json_number "$file" clientDroppedMessagesTotal)"
  broker_dropped_messages="$(json_number "$file" brokerDroppedMessagesTotal)"

  if [ "$driver" = "pogo" ] && [ "$compression" = "true" ]; then
    effective_compression="$(json_number "$file" enableCompression)"
    if [ "$effective_compression" != "1" ] || [ "$client_compression" != "true" ] || [ "$negotiated_compression" != "$vus" ]; then
      config_match=false
    fi
  elif [ "$driver" = "pogo" ] && [ "$compression" = "false" ]; then
    effective_compression="$(json_number "$file" enableCompression)"
    if [ "$effective_compression" != "0" ] || [ "$negotiated_compression" != "0" ]; then
      config_match=false
    fi
  fi

  assert_number_equals "$scenario delivery completeness" "$(json_number "$file" deliveryCompleteness)" "1"
  assert_zero "$scenario connect errors" "$(json_number "$file" connectErrors)"
  assert_zero "$scenario parse errors" "$(json_number "$file" parseErrors)"
  assert_zero "$scenario read errors" "$(json_number "$file" readErrors)"
  assert_zero "$scenario publish errors" "$(json_number "$file" publishErrors)"

  if [ "$config_match" != "true" ]; then
    printf 'FAILED: %s effective compression did not match %s\n' "$scenario" "$compression" >&2
    exit 1
  fi
  if [ "$driver" = "pogo" ] && [ "$data_write_failures" != "null" ]; then
    assert_zero "$scenario data write failures" "$data_write_failures"
  fi

  printf '%s\tgo-receiver\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$driver" \
    "$role" \
    "$vus" \
    "$(json_number "$file" msgCount)" \
    "$(json_number "$file" payloadSize)" \
    "$(json_number "$file" publishBatches)" \
    "$compression" \
    "$client_compression" \
    "$negotiated_compression" \
    "$(json_number "$file" deliveryCompleteness)" \
    "$(json_number "$file" observedMessages)" \
    "$(json_number "$file" expectedMessages)" \
    "$(json_number "$file" sentToReadP50Ms)" \
    "$(json_number "$file" sentToReadP95Ms)" \
    "$(json_number "$file" sentToReadP99Ms)" \
    "$(json_number "$file" publishP95Ms)" \
    "$(json_number "$file" connectErrors)" \
    "$(json_number "$file" connectRetryFailures)" \
    "$(json_number "$file" parseErrors)" \
    "$(json_number "$file" readErrors)" \
    "$(json_number "$file" publishErrors)" \
    "$data_write_failures" \
    "$client_dropped_messages" \
    "$broker_dropped_messages" \
    "$config_match" >> "$AUDIT_FILE"
}

write_go_sharded_row() {
  local scenario="$1"
  local aggregate="$2"
  local publisher="$3"
  local shards=(
    "$4"
    "$5"
    "$6"
    "$7"
    "$8"
  )
  local config_match=true
  local effective_compression

  assert_number_equals "$scenario delivery completeness" "$(min_json_number deliveryCompleteness "${shards[@]}")" "1"
  assert_number_equals "$scenario aggregate delivery completeness" "$(json_number "$aggregate" deliveryCompleteness)" "1"
  assert_zero "$scenario connect errors" "$(max_json_number connectErrors "${shards[@]}")"
  assert_zero "$scenario parse errors" "$(max_json_number parseErrors "${shards[@]}")"
  assert_zero "$scenario read errors" "$(max_json_number readErrors "${shards[@]}")"
  assert_zero "$scenario publish errors" "$(json_number "$publisher" publishErrors)"
  assert_present "$scenario data write failures" "$(json_number "$publisher" dataWriteFailuresTotal)"
  assert_zero "$scenario data write failures" "$(json_number "$publisher" dataWriteFailuresTotal)"

  effective_compression="$(json_number "$publisher" enableCompression)"
  if [ "$BASE_COMPRESSION" = "true" ]; then
    if [ "$effective_compression" != "1" ] || [ "$(json_number "$aggregate" negotiatedCompressionConnections)" != "$((BASE_SHARD_VUS * 5))" ]; then
      config_match=false
    fi
  elif [ "$effective_compression" != "0" ] || [ "$(json_number "$aggregate" negotiatedCompressionConnections)" != "0" ]; then
    config_match=false
  fi

  if [ "$config_match" != "true" ]; then
    printf 'FAILED: %s effective compression did not match %s\n' "$scenario" "$BASE_COMPRESSION" >&2
    exit 1
  fi

  printf '%s\tgo-receiver\tpogo\tsharded-aggregate\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$(json_number "$aggregate" vus)" \
    "$(json_number "$aggregate" msgCount)" \
    "$(json_number "$aggregate" payloadSize)" \
    "$(json_number "$aggregate" publishBatches)" \
    "$BASE_COMPRESSION" \
    "$(json_number "$aggregate" clientCompression)" \
    "$(json_number "$aggregate" negotiatedCompressionConnections)" \
    "$(json_number "$aggregate" deliveryCompleteness)" \
    "$(json_number "$aggregate" observedMessages)" \
    "$(json_number "$aggregate" expectedMessages)" \
    "$(json_number "$aggregate" sentToReadP50Ms)" \
    "$(json_number "$aggregate" sentToReadP95Ms)" \
    "$(json_number "$aggregate" sentToReadP99Ms)" \
    "$(json_number "$publisher" publishP95Ms)" \
    "$(json_number "$aggregate" connectErrors)" \
    "$(json_number "$aggregate" connectRetryFailures)" \
    "$(json_number "$aggregate" parseErrors)" \
    "$(json_number "$aggregate" readErrors)" \
    "$(json_number "$publisher" publishErrors)" \
    "$(json_number "$publisher" dataWriteFailuresTotal)" \
    "$(json_number "$publisher" clientDroppedMessagesTotal)" \
    "$(json_number "$publisher" brokerDroppedMessagesTotal)" \
    "$config_match" >> "$AUDIT_FILE"
}

build_images() {
  local build_args=(build)

  if [ "${NO_CACHE:-0}" = "1" ]; then
    build_args+=(--no-cache)
  fi

  build_args+=(
    pogo
    reverb-app
    reverb-ws
    go-receiver-pogo
    go-receiver-pogo-listener-1
    go-receiver-pogo-listener-2
    go-receiver-pogo-listener-3
    go-receiver-pogo-listener-4
    go-receiver-pogo-listener-5
    go-receiver-pogo-publisher
    go-receiver-reverb
  )
  compose "${build_args[@]}"
  compose images > "$RESULTS_DIR/run-$STAMP-images.txt"
}

start_pogo() {
  compose up -d pogo
}

start_reverb() {
  compose up -d reverb-app reverb-ws
}

stop_all() {
  compose down -v --remove-orphans
}

run_k6_pogo() {
  local result_file
  local metrics_file

  result_file="$(result_path "pogo-summary.json")"
  metrics_file="$(result_path "pogo-metrics.prom")"
  export K6_POGO_RESULT_FILE="$result_file"
  export K6_POGO_METRICS_FILE="$metrics_file"

  compose up --force-recreate --abort-on-container-exit --exit-code-from k6-pogo k6-pogo 2>&1 \
    | tee "$RESULTS_DIR/run-$STAMP-pogo-k6.log"
  write_k6_row "pogo-k6-baseline" "pogo" "$(host_result_path "$result_file")" "$BASE_COMPRESSION"

  unset K6_POGO_RESULT_FILE K6_POGO_METRICS_FILE
}

run_k6_reverb() {
  local result_file

  result_file="$(result_path "reverb-summary.json")"
  export K6_REVERB_RESULT_FILE="$result_file"

  compose up --force-recreate --abort-on-container-exit --exit-code-from k6-reverb k6-reverb 2>&1 \
    | tee "$RESULTS_DIR/run-$STAMP-reverb-k6.log"
  write_k6_row "reverb-k6-baseline" "reverb" "$(host_result_path "$result_file")" "n/a"

  unset K6_REVERB_RESULT_FILE
}

run_go_receiver() {
  local scenario="$1"
  local driver="$2"
  local service="$3"
  local vus="$4"
  local msg_count="$5"
  local payload_size="$6"
  local compression="$7"
  local metrics_url="${8:-}"
  local result_file
  local metrics_file

  result_file="$(result_path "go-receiver-${scenario}-summary.json")"
  metrics_file="$(result_path "go-receiver-${scenario}-metrics.prom")"

  compose run --rm \
    -e DRIVER="$driver" \
    -e ROLE=both \
    -e VUS="$vus" \
    -e MSG_COUNT="$msg_count" \
    -e PAYLOAD_SIZE="$payload_size" \
    -e PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
    -e BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
    -e PUBLISH_MESSAGE_INTERVAL_MS="$BASE_PUBLISH_MESSAGE_INTERVAL_MS" \
    -e RAMP_UP_SECONDS="$BASE_RAMP_UP_SECONDS" \
    -e PUBLISH_START_SECONDS="$BASE_PUBLISH_START_SECONDS" \
    -e PUBLISH_MAX_DURATION_SECONDS="$BASE_PUBLISH_MAX_DURATION_SECONDS" \
    -e DRAIN_SECONDS="$BASE_DRAIN_SECONDS" \
    -e WS_ENABLE_COMPRESSION="$compression" \
    -e METRICS_URL="$metrics_url" \
    -e METRICS_FILE="$metrics_file" \
    -e RESULT_FILE="$result_file" \
    "$service" 2>&1 | tee "$RESULTS_DIR/run-$STAMP-${scenario}.log"

  write_go_row "$scenario" "$(host_result_path "$result_file")" "$driver" "$compression"
}

run_go_sharded() {
  local shard_1
  local shard_2
  local shard_3
  local shard_4
  local shard_5
  local publisher
  local metrics_file
  local aggregate

  shard_1="$(result_path "go-receiver-shard-1-summary.json")"
  shard_2="$(result_path "go-receiver-shard-2-summary.json")"
  shard_3="$(result_path "go-receiver-shard-3-summary.json")"
  shard_4="$(result_path "go-receiver-shard-4-summary.json")"
  shard_5="$(result_path "go-receiver-shard-5-summary.json")"
  publisher="$(result_path "go-receiver-sharded-publisher-summary.json")"
  metrics_file="$(result_path "go-receiver-sharded-metrics.prom")"
  aggregate="$(result_path "go-receiver-sharded-aggregate-summary.json")"

  export GO_RECEIVER_WS_ENABLE_COMPRESSION="$BASE_COMPRESSION"
  export GO_RECEIVER_SHARD_1_RESULT_FILE="$shard_1"
  export GO_RECEIVER_SHARD_2_RESULT_FILE="$shard_2"
  export GO_RECEIVER_SHARD_3_RESULT_FILE="$shard_3"
  export GO_RECEIVER_SHARD_4_RESULT_FILE="$shard_4"
  export GO_RECEIVER_SHARD_5_RESULT_FILE="$shard_5"
  export GO_RECEIVER_SHARDED_PUBLISHER_RESULT_FILE="$publisher"
  export GO_RECEIVER_SHARDED_METRICS_FILE="$metrics_file"

  SHARD_VUS="$BASE_SHARD_VUS" \
  VUS="$BASE_VUS" \
  MSG_COUNT="$BASE_MSG_COUNT" \
  PAYLOAD_SIZE="$BASE_PAYLOAD_SIZE" \
  PUBLISH_BATCHES="$BASE_PUBLISH_BATCHES" \
  BATCH_INTERVAL_SECONDS="$BASE_BATCH_INTERVAL_SECONDS" \
  PUBLISH_MESSAGE_INTERVAL_MS="$BASE_PUBLISH_MESSAGE_INTERVAL_MS" \
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

  compose run --rm \
    -e DRIVER=pogo \
    -e ROLE=aggregate \
    -e RESULT_FILE="$aggregate" \
    -e AGGREGATE_FILES="$shard_1,$shard_2,$shard_3,$shard_4,$shard_5" \
    go-receiver-pogo 2>&1 | tee -a "$RESULTS_DIR/run-$STAMP-go-sharded-5x${BASE_SHARD_VUS}.log"

  write_go_sharded_row \
    "go-sharded-5x${BASE_SHARD_VUS}" \
    "$(host_result_path "$aggregate")" \
    "$(host_result_path "$publisher")" \
    "$(host_result_path "$shard_1")" \
    "$(host_result_path "$shard_2")" \
    "$(host_result_path "$shard_3")" \
    "$(host_result_path "$shard_4")" \
    "$(host_result_path "$shard_5")"

  unset GO_RECEIVER_WS_ENABLE_COMPRESSION \
    GO_RECEIVER_SHARD_1_RESULT_FILE \
    GO_RECEIVER_SHARD_2_RESULT_FILE \
    GO_RECEIVER_SHARD_3_RESULT_FILE \
    GO_RECEIVER_SHARD_4_RESULT_FILE \
    GO_RECEIVER_SHARD_5_RESULT_FILE \
    GO_RECEIVER_SHARDED_PUBLISHER_RESULT_FILE \
    GO_RECEIVER_SHARDED_METRICS_FILE
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
  run_go_receiver "pogo-go-baseline" "pogo" "go-receiver-pogo" "$BASE_VUS" "$BASE_MSG_COUNT" "$BASE_PAYLOAD_SIZE" "$BASE_COMPRESSION" "http://pogo:2019/metrics"
  stop_all

  start_reverb
  run_go_receiver "reverb-go-baseline" "reverb" "go-receiver-reverb" "$BASE_VUS" "$BASE_MSG_COUNT" "$BASE_PAYLOAD_SIZE" "n/a" ""
  stop_all

  render_runtime_caddyfile "$BASE_COMPRESSION"
  start_pogo
  run_go_sharded
  stop_all

  render_runtime_caddyfile "$BASE_COMPRESSION"
  start_pogo
  for payload_size in 16 1024 4096; do
    run_go_receiver "payload-${payload_size}" "pogo" "go-receiver-pogo" "$BASE_VUS" "$BASE_MSG_COUNT" "$payload_size" "$BASE_COMPRESSION" "http://pogo:2019/metrics"
  done
  for msg_count in 10 100 250; do
    run_go_receiver "batch-${msg_count}" "pogo" "go-receiver-pogo" "$BASE_VUS" "$msg_count" "$BASE_PAYLOAD_SIZE" "$BASE_COMPRESSION" "http://pogo:2019/metrics"
  done
  stop_all

  for compression in false true; do
    render_runtime_caddyfile "$compression"
    start_pogo
    run_go_receiver "compression-${compression}-payload-4096" "pogo" "go-receiver-pogo" "$BASE_VUS" "$BASE_MSG_COUNT" "4096" "$compression" "http://pogo:2019/metrics"
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
