#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCH_DIR="$ROOT_DIR/benchmarks/laravel-broadcast"
COMPOSE_FILE="$BENCH_DIR/compose.yaml"
RESULTS_DIR="$BENCH_DIR/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "$RESULTS_DIR"
chmod 0777 "$RESULTS_DIR"
export K6_UID="${K6_UID:-$(id -u)}"
export K6_GID="${K6_GID:-$(id -g)}"

{
  printf 'timestamp=%s\n' "$STAMP"
  printf 'git_sha=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || printf 'unknown')"
  printf 'git_status=\n'
  git -C "$ROOT_DIR" status --short || true
  printf '\nbenchmark_env=\n'
  env | sort | grep -E '^(VUS|SHARD_VUS|MSG_COUNT|PAYLOAD_SIZE|PUBLISH_BATCHES|BATCH_INTERVAL_SECONDS|RAMP_UP_SECONDS|HOLD_SECONDS|RAMP_DOWN_SECONDS|PUBLISH_START_SECONDS|PUBLISH_MAX_DURATION_SECONDS|DRAIN_SECONDS|LATENCY_P95_THRESHOLD_MS|POGO_WS_HOT_PATH_METRICS)=' || true
} > "$RESULTS_DIR/run-$STAMP-meta.txt"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
docker compose -f "$COMPOSE_FILE" build --no-cache pogo reverb-app reverb-ws go-receiver-pogo
docker compose -f "$COMPOSE_FILE" images > "$RESULTS_DIR/run-$STAMP-images.txt"

docker compose -f "$COMPOSE_FILE" up --force-recreate --abort-on-container-exit --exit-code-from k6-pogo k6-pogo 2>&1 \
  | tee "$RESULTS_DIR/run-$STAMP-pogo.log"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

docker compose -f "$COMPOSE_FILE" up --force-recreate --abort-on-container-exit --exit-code-from k6-reverb k6-reverb 2>&1 \
  | tee "$RESULTS_DIR/run-$STAMP-reverb.log"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

docker compose -f "$COMPOSE_FILE" up --force-recreate \
  k6-pogo-listener-1 \
  k6-pogo-listener-2 \
  k6-pogo-listener-3 \
  k6-pogo-listener-4 \
  k6-pogo-listener-5 \
  k6-pogo-publisher 2>&1 \
  | tee "$RESULTS_DIR/run-$STAMP-pogo-sharded-k6.log"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

docker compose -f "$COMPOSE_FILE" up --force-recreate --abort-on-container-exit --exit-code-from go-receiver-pogo go-receiver-pogo 2>&1 \
  | tee "$RESULTS_DIR/run-$STAMP-go-receiver-pogo.log"

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

{
  printf '\nPROOF AUDIT\n'
  printf 'pogo_write_complete_from_sent_p95_ms=%s\n' "$(json_number "$RESULTS_DIR/pogo-summary.json" writeCompleteFromSentP95Ms)"
  printf 'k6_receive_p95_ms=%s\n' "$(json_number "$RESULTS_DIR/pogo-summary.json" eventSentToReceivedP95Ms)"
  printf 'sharded_k6_receive_p95_ms=%s\n' "$(
    max_json_number eventSentToReceivedP95Ms \
      "$RESULTS_DIR/pogo-shard-1-summary.json" \
      "$RESULTS_DIR/pogo-shard-2-summary.json" \
      "$RESULTS_DIR/pogo-shard-3-summary.json" \
      "$RESULTS_DIR/pogo-shard-4-summary.json" \
      "$RESULTS_DIR/pogo-shard-5-summary.json"
  )"
  printf 'go_receiver_sent_to_read_p95_ms=%s\n' "$(json_number "$RESULTS_DIR/go-receiver-pogo-summary.json" sentToReadP95Ms)"
  printf 'go_receiver_delivery_completeness=%s\n' "$(json_number "$RESULTS_DIR/go-receiver-pogo-summary.json" deliveryCompleteness)"
  printf 'sharded_k6_p95_is_max_shard_p95=true\n'
} | tee "$RESULTS_DIR/run-$STAMP-proof-audit.txt"

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

printf 'Wrote benchmark results to %s\n' "$RESULTS_DIR"
