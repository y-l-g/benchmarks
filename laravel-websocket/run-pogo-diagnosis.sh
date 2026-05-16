#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/laravel-broadcast"
COMPOSE_FILE="$BENCH_DIR/compose.yaml"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AUDIT_FILE="$RESULTS_DIR/run-$STAMP-pogo-diagnosis-audit.tsv"

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
BASE_PUBLISH_START_SECONDS="${PUBLISH_START_SECONDS:-12}"
BASE_PUBLISH_MAX_DURATION_SECONDS="${PUBLISH_MAX_DURATION_SECONDS:-}"
BASE_DRAIN_SECONDS="${DRAIN_SECONDS:-10}"
BASE_SHARD_VUS="${SHARD_VUS:-100}"

{
  printf 'timestamp=%s\n' "$STAMP"
  printf 'git_sha=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  printf 'git_status=\n'
  git -C "$ROOT_DIR" status --short || true
  printf '\nbenchmark_env=\n'
  env | sort | grep -E '^(VUS|SHARD_VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|RAMP_UP_SECONDS|PUBLISH_START_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|POGO_WS_HOT_PATH_METRICS)=' || true
} > "$RESULTS_DIR/run-$STAMP-pogo-diagnosis-meta.txt"

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

write_single_audit_row() {
  local scenario="$1"
  local vus="$2"
  local msg_count="$3"
  local payload_size="$4"
  local file="$5"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$vus" \
    "$msg_count" \
    "$payload_size" \
    "$(json_number "$file" writeCompleteFromSentP95Ms)" \
    "$(json_number "$file" sentToReadP95Ms)" \
    "$(json_number "$file" sentToReadP99Ms)" \
    "$(json_number "$file" deliveryCompleteness)" \
    "$(json_number "$file" connectErrors)" \
    "$(json_number "$file" parseErrors)" \
    "$(json_number "$file" readErrors)" \
    "$(json_number "$file" publishErrors)"
}

write_sharded_audit_row() {
  local scenario="$1"
  local vus="$2"
  local msg_count="$3"
  local payload_size="$4"
  local publisher="$RESULTS_DIR/go-receiver-sharded-publisher-summary.json"
  local shards=(
    "$RESULTS_DIR/go-receiver-shard-1-summary.json"
    "$RESULTS_DIR/go-receiver-shard-2-summary.json"
    "$RESULTS_DIR/go-receiver-shard-3-summary.json"
    "$RESULTS_DIR/go-receiver-shard-4-summary.json"
    "$RESULTS_DIR/go-receiver-shard-5-summary.json"
  )

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$scenario" \
    "$vus" \
    "$msg_count" \
    "$payload_size" \
    "$(json_number "$publisher" writeCompleteFromSentP95Ms)" \
    "$(max_json_number sentToReadP95Ms "${shards[@]}")" \
    "$(max_json_number sentToReadP99Ms "${shards[@]}")" \
    "$(min_json_number deliveryCompleteness "${shards[@]}")" \
    "$(max_json_number connectErrors "${shards[@]}")" \
    "$(max_json_number parseErrors "${shards[@]}")" \
    "$(max_json_number readErrors "${shards[@]}")" \
    "$(json_number "$publisher" publishErrors)"
}

run_single_go_receiver() {
  local scenario="$1"
  local vus="$2"
  local msg_count="$3"
  local payload_size="$4"
  local result_file="/results/go-receiver-${scenario}-summary.json"
  local metrics_file="/results/go-receiver-${scenario}-metrics.prom"

  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
  docker compose -f "$COMPOSE_FILE" up -d pogo
  docker compose -f "$COMPOSE_FILE" run --rm \
    -e ROLE=both \
    -e VUS="$vus" \
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
    go-receiver-pogo 2>&1 | tee "$RESULTS_DIR/run-$STAMP-${scenario}.log"
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

  write_single_audit_row "$scenario" "$vus" "$msg_count" "$payload_size" "$RESULTS_DIR/$(basename "$result_file")" >> "$AUDIT_FILE"
}

run_sharded_go_receiver() {
  local scenario="go-sharded-5x${BASE_SHARD_VUS}"

  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
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
    docker compose -f "$COMPOSE_FILE" up --force-recreate \
      go-receiver-pogo-listener-1 \
      go-receiver-pogo-listener-2 \
      go-receiver-pogo-listener-3 \
      go-receiver-pogo-listener-4 \
      go-receiver-pogo-listener-5 \
      go-receiver-pogo-publisher 2>&1 | tee "$RESULTS_DIR/run-$STAMP-${scenario}.log"
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

  write_sharded_audit_row "$scenario" "$BASE_VUS" "$BASE_MSG_COUNT" "$BASE_PAYLOAD_SIZE" >> "$AUDIT_FILE"
}

printf 'scenario\tvus\tmsg_count\tpayload_size\twrite_complete_from_sent_p95_ms\tgo_receiver_p95_ms\tgo_receiver_p99_ms\tdelivery_completeness\tconnect_errors\tparse_errors\tread_errors\tpublish_errors\n' > "$AUDIT_FILE"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
docker compose -f "$COMPOSE_FILE" build --no-cache pogo go-receiver-pogo

run_single_go_receiver "go-vus-${BASE_VUS}" "$BASE_VUS" "$BASE_MSG_COUNT" "$BASE_PAYLOAD_SIZE"
run_sharded_go_receiver

for payload_size in 16 256 1024 4096; do
  run_single_go_receiver "payload-${payload_size}" "$BASE_VUS" "$BASE_MSG_COUNT" "$payload_size"
done

for msg_count in 10 50 100 250; do
  run_single_go_receiver "batch-${msg_count}" "$BASE_VUS" "$msg_count" "$BASE_PAYLOAD_SIZE"
done

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

printf 'Wrote Pogo diagnosis audit to %s\n' "$AUDIT_FILE"
