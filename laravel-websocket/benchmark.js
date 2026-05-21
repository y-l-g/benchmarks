import ws from "k6/ws";
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const DRIVER = __ENV.DRIVER || "reverb";
const ROLE = __ENV.ROLE || "both";
const HOST = __ENV.HOST || "localhost";
const HTTP_HOST = __ENV.HTTP_HOST || HOST;
const WS_HOST = __ENV.WS_HOST || HOST;
const HTTP_PORT = __ENV.HTTP_PORT || "8000";
const WS_PORT = __ENV.WS_PORT || (DRIVER === "reverb" ? "8080" : "80");
const APP_KEY = __ENV.APP_KEY || (DRIVER === "reverb" ? "reverb-key" : "pogo-app");
const RESULT_FILE = __ENV.RESULT_FILE || `/results/${DRIVER}-summary.json`;
const METRICS_URL = __ENV.METRICS_URL || "";
const METRICS_FILE = __ENV.METRICS_FILE || `/results/${DRIVER}-metrics.prom`;

const VUS = parseInt(__ENV.VUS || "500", 10);
const MSG_COUNT = parseInt(__ENV.MSG_COUNT || "100", 10);
const PAYLOAD_SIZE = parseInt(__ENV.PAYLOAD_SIZE || "1024", 10);
const PUBLISH_BATCHES = parseInt(__ENV.PUBLISH_BATCHES || "20", 10);
const BATCH_INTERVAL_SECONDS = parseFloat(__ENV.BATCH_INTERVAL_SECONDS || "2");
const PUBLISH_MESSAGE_INTERVAL_MS = parseFloat(__ENV.PUBLISH_MESSAGE_INTERVAL_MS || "0");
const RAMP_UP_SECONDS = parseInt(__ENV.RAMP_UP_SECONDS || "10", 10);
const RAMP_DOWN_SECONDS = parseInt(__ENV.RAMP_DOWN_SECONDS || "5", 10);
const PUBLISH_START_SECONDS = parseInt(__ENV.PUBLISH_START_SECONDS || "12", 10);
const DRAIN_SECONDS = parseInt(__ENV.DRAIN_SECONDS || "10", 10);
const LATENCY_P95_THRESHOLD_MS = parseFloat(__ENV.LATENCY_P95_THRESHOLD_MS || "0");
const PUBLISH_WINDOW_SECONDS = Math.ceil(PUBLISH_BATCHES * BATCH_INTERVAL_SECONDS);
const PUBLISH_MAX_DURATION_SECONDS = parseInt(
  __ENV.PUBLISH_MAX_DURATION_SECONDS ||
    String(PUBLISH_WINDOW_SECONDS + 60),
  10
);
const REQUIRED_LISTENER_RAMP_DOWN_START_SECONDS =
  PUBLISH_START_SECONDS + PUBLISH_MAX_DURATION_SECONDS + DRAIN_SECONDS;
const MIN_HOLD_SECONDS = Math.max(
  1,
  Math.ceil(REQUIRED_LISTENER_RAMP_DOWN_START_SECONDS - RAMP_UP_SECONDS)
);
const HOLD_SECONDS = parseInt(__ENV.HOLD_SECONDS || String(MIN_HOLD_SECONDS), 10);
const PUBLISHER_COMPLETION_DEADLINE_SECONDS =
  PUBLISH_START_SECONDS + PUBLISH_MAX_DURATION_SECONDS;
const LISTENER_RAMP_DOWN_START_SECONDS = RAMP_UP_SECONDS + HOLD_SECONDS;
const SCHEDULE_IS_VALID =
  ROLE === "publisher" ||
  LISTENER_RAMP_DOWN_START_SECONDS >= REQUIRED_LISTENER_RAMP_DOWN_START_SECONDS;

if (!SCHEDULE_IS_VALID) {
  throw new Error(
    `Invalid benchmark schedule: listeners start ramp-down at ${LISTENER_RAMP_DOWN_START_SECONDS}s, ` +
      `but publisher maxDuration plus drain requires ${REQUIRED_LISTENER_RAMP_DOWN_START_SECONDS}s. ` +
      `Increase HOLD_SECONDS or reduce PUBLISH_MAX_DURATION_SECONDS.`
  );
}

if (!["both", "listeners", "publisher"].includes(ROLE)) {
  throw new Error(`Invalid ROLE=${ROLE}; expected both, listeners, or publisher.`);
}

const wsUrl = `ws://${WS_HOST}:${WS_PORT}/app/${APP_KEY}?protocol=7&client=js&version=8.4.0&flash=false`;

const msgLatencyMs = new Trend("msg_latency_ms");
const eventCreatedToReceivedMs = new Trend("event_created_to_received_ms");
const eventSentToReceivedMs = new Trend("event_sent_to_received_ms");
const publishDurationMs = new Trend("publish_duration_ms");
const msgsReceived = new Counter("msgs_received");
const subscriptionsSucceeded = new Counter("subscriptions_succeeded");
const connErrors = new Counter("conn_errors");
const parseErrors = new Counter("parse_errors");
const publishSuccess = new Rate("publish_success");

function parseLabels(raw) {
  const labels = {};
  if (!raw) {
    return labels;
  }

  raw.replace(/([^=,\s]+)="((?:\\.|[^"\\])*)"/g, (_, key, value) => {
    labels[key] = value.replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    return "";
  });

  return labels;
}

function parsePrometheusText(text) {
  const samples = [];

  for (const line of text.split("\n")) {
    if (!line || line[0] === "#") {
      continue;
    }

    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?|NaN|[+-]?Inf)/);
    if (!match) {
      continue;
    }

    const value = Number(match[3]);
    if (!Number.isFinite(value)) {
      continue;
    }

    samples.push({
      name: match[1],
      labels: parseLabels(match[2]),
      value,
    });
  }

  return samples;
}

function labelMatch(sampleLabels, wantedLabels) {
  for (const [key, value] of Object.entries(wantedLabels || {})) {
    if (sampleLabels[key] !== value) {
      return false;
    }
  }

  return true;
}

function counterValue(samples, name, labels) {
  return samples
    .filter((sample) => sample.name === name && labelMatch(sample.labels, labels))
    .reduce((sum, sample) => sum + sample.value, 0);
}

function histogramSummary(samples, baseName, labels) {
  const buckets = samples
    .filter((sample) => sample.name === `${baseName}_bucket` && labelMatch(sample.labels, labels))
    .map((sample) => ({
      le: sample.labels.le === "+Inf" ? Infinity : Number(sample.labels.le),
      value: sample.value,
    }))
    .filter((bucket) => Number.isFinite(bucket.le) || bucket.le === Infinity)
    .sort((a, b) => a.le - b.le);

  const count = counterValue(samples, `${baseName}_count`, labels);
  const sum = counterValue(samples, `${baseName}_sum`, labels);
  if (buckets.length === 0 || count === 0) {
    return null;
  }

  const quantile = (q) => {
    const target = count * q;
    let previousLe = 0;
    let previousCount = 0;

    for (const bucket of buckets) {
      if (bucket.value >= target) {
        if (bucket.le === Infinity) {
          return previousLe;
        }

        const bucketCount = bucket.value - previousCount;
        if (bucketCount <= 0) {
          return bucket.le;
        }

        const position = (target - previousCount) / bucketCount;
        return previousLe + (bucket.le - previousLe) * position;
      }

      previousLe = bucket.le;
      previousCount = bucket.value;
    }

    return null;
  };

  return {
    count,
    avg: sum / count,
    p50: quantile(0.5),
    p90: quantile(0.9),
    p95: quantile(0.95),
    p99: quantile(0.99),
  };
}

function scrapePrometheusMetrics() {
  if (!METRICS_URL) {
    return {
      scrape: { enabled: false },
      text: "",
      derived: null,
    };
  }

  const res = http.get(METRICS_URL, { timeout: "5s" });
  const scrape = {
    enabled: true,
    url: METRICS_URL,
    status: res.status,
    ok: res.status === 200,
    bodyBytes: res.body ? res.body.length : 0,
  };

  if (res.status !== 200 || !res.body) {
    return {
      scrape,
      text: res.body || "",
      derived: null,
    };
  }

  const samples = parsePrometheusText(res.body);
  scrape.samples = samples.length;
  return {
    scrape,
    text: res.body,
    derived: {
      fanoutSubscribers: histogramSummary(samples, "pogo_websocket_fanout_subscribers"),
      publishValidateSeconds: histogramSummary(samples, "pogo_websocket_publish_duration_seconds", { phase: "validate" }),
      publishBrokerSeconds: histogramSummary(samples, "pogo_websocket_publish_duration_seconds", { phase: "broker" }),
      publishTotalSeconds: histogramSummary(samples, "pogo_websocket_publish_duration_seconds", { phase: "total" }),
      brokerToHubDelaySeconds: histogramSummary(samples, "pogo_websocket_broker_to_hub_delay_seconds"),
      hubToShardDelaySeconds: histogramSummary(samples, "pogo_websocket_hub_to_shard_delay_seconds"),
      clientQueueDepth: histogramSummary(samples, "pogo_websocket_client_queue_depth"),
      clientQueueResidenceSeconds: histogramSummary(samples, "pogo_websocket_client_queue_residence_seconds"),
      writeDurationPreparedSeconds: histogramSummary(samples, "pogo_websocket_write_duration_seconds", { kind: "prepared" }),
      writeDurationBytesSeconds: histogramSummary(samples, "pogo_websocket_write_duration_seconds", { kind: "bytes" }),
      writeTotalDurationPreparedSeconds: histogramSummary(samples, "pogo_websocket_write_total_duration_seconds", { kind: "prepared" }),
      writeTotalDurationPreparedWithDeadlineSeconds: histogramSummary(samples, "pogo_websocket_write_total_duration_seconds", { kind: "prepared_with_deadline" }),
      writeTotalDurationBytesSeconds: histogramSummary(samples, "pogo_websocket_write_total_duration_seconds", { kind: "bytes" }),
      writeTotalDurationBytesWithDeadlineSeconds: histogramSummary(samples, "pogo_websocket_write_total_duration_seconds", { kind: "bytes_with_deadline" }),
      clientDroppedMessagesTotal: counterValue(samples, "pogo_websocket_client_dropped_messages_total"),
      brokerDroppedMessagesTotal: counterValue(samples, "pogo_websocket_broker_dropped_messages_total"),
      writeFailuresTotal: counterValue(samples, "pogo_websocket_write_failures_total"),
      writeFailuresPreparedTotal: counterValue(samples, "pogo_websocket_write_failures_total", { kind: "prepared" }),
      writeFailuresBytesTotal: counterValue(samples, "pogo_websocket_write_failures_total", { kind: "bytes" }),
      dataWriteFailuresTotal:
        counterValue(samples, "pogo_websocket_write_failures_total", { kind: "prepared" }) +
        counterValue(samples, "pogo_websocket_write_failures_total", { kind: "bytes" }),
      deliveryConfig: {
        outboundQueueSize: counterValue(samples, "pogo_websocket_delivery_config", { key: "outbound_queue_size" }),
        writeBurstSize: counterValue(samples, "pogo_websocket_delivery_config", { key: "write_burst_size" }),
        enableCompression: counterValue(samples, "pogo_websocket_delivery_config", { key: "enable_compression" }),
      },
    },
  };
}

function buildThresholds() {
  const thresholds = {
    conn_errors: ["count==0"],
    parse_errors: ["count==0"],
  };

  if (ROLE === "both" || ROLE === "listeners") {
    thresholds.ws_sessions = [`count==${VUS}`];
    thresholds.subscriptions_succeeded = [`count==${VUS}`];
  }
  if (ROLE === "both" || ROLE === "publisher") {
    thresholds.http_req_failed = ["rate==0"];
    thresholds.publish_success = ["rate==1"];
    thresholds.http_reqs = [`count==${PUBLISH_BATCHES}`];
  }
  if (LATENCY_P95_THRESHOLD_MS > 0 && (ROLE === "both" || ROLE === "listeners")) {
    thresholds.event_sent_to_received_ms = [`p(95)<${LATENCY_P95_THRESHOLD_MS}`];
  }

  return thresholds;
}

function buildScenarios() {
  const scenarios = {};

  if (ROLE === "both" || ROLE === "listeners") {
    scenarios.listeners = {
      executor: "ramping-vus",
      stages: [
        { duration: `${RAMP_UP_SECONDS}s`, target: VUS },
        { duration: `${HOLD_SECONDS}s`, target: VUS },
        { duration: `${RAMP_DOWN_SECONDS}s`, target: 0 },
      ],
      gracefulStop: "5s",
      exec: "listener",
    };
  }

  if (ROLE === "both" || ROLE === "publisher") {
    scenarios.publisher = {
      executor: "shared-iterations",
      vus: 1,
      iterations: PUBLISH_BATCHES,
      startTime: `${PUBLISH_START_SECONDS}s`,
      maxDuration: `${PUBLISH_MAX_DURATION_SECONDS}s`,
      exec: "publisher",
    };
  }

  return scenarios;
}

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  thresholds: buildThresholds(),
  scenarios: buildScenarios(),
};

export function listener() {
  const res = ws.connect(wsUrl, {}, (socket) => {
    socket.on("open", () => {
      socket.send(JSON.stringify({
        event: "pusher:subscribe",
        data: { channel: "bench-channel" },
      }));
    });

    socket.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data);
      } catch (_) {
        parseErrors.add(1);
        return;
      }

      if (msg.event === "pusher_internal:subscription_succeeded") {
        subscriptionsSucceeded.add(1);
        return;
      }

      if (msg.event !== "bench.event") {
        return;
      }

      try {
        const payload = JSON.parse(msg.data);
        const receivedAt = Date.now();
        if (Number.isFinite(payload.createdAt)) {
          eventCreatedToReceivedMs.add(Math.max(0, receivedAt - payload.createdAt));
        }
        if (Number.isFinite(payload.sentAt)) {
          const sentToReceived = Math.max(0, receivedAt - payload.sentAt);
          eventSentToReceivedMs.add(sentToReceived);
          msgLatencyMs.add(sentToReceived);
        }
        msgsReceived.add(1);
      } catch (_) {
        parseErrors.add(1);
      }
    });

    socket.on("error", () => connErrors.add(1));
  });

  check(res, { "websocket upgraded": (r) => r && r.status === 101 });
}

export function publisher() {
  const publishRequestSeconds = Math.ceil((MSG_COUNT * PUBLISH_MESSAGE_INTERVAL_MS) / 1000) + 5;
  const url = `http://${HTTP_HOST}:${HTTP_PORT}/fire?count=${MSG_COUNT}&size=${PAYLOAD_SIZE}&interval_ms=${PUBLISH_MESSAGE_INTERVAL_MS}`;
  const res = http.get(url, { timeout: `${Math.max(5, publishRequestSeconds)}s` });
  const ok = check(res, {
    published: (r) => r.status === 200,
    "published requested count": (r) => {
      try {
        return r.status === 200 && JSON.parse(r.body).count === MSG_COUNT;
      } catch (_) {
        return false;
      }
    },
  });

  publishSuccess.add(ok);
  publishDurationMs.add(res.timings.duration);
  sleep(BATCH_INTERVAL_SECONDS);
}

export function handleSummary(data) {
  const prometheus = scrapePrometheusMetrics();
  const count = (name) => data.metrics[name]?.values.count || 0;
  const percentile = (name, p) => data.metrics[name]?.values[p] ?? null;
  const subscribed = count("subscriptions_succeeded");
  const completedPublishBatches = count("http_reqs");
  const published = Math.round(completedPublishBatches * (data.metrics.publish_success?.values.rate || 0));
  const observed = count("msgs_received");
  const connectErrors = count("conn_errors");
  const parseErrorCount = count("parse_errors");
  const publishErrors = Math.max(0, PUBLISH_BATCHES - published);
  const expectedBatches = ROLE === "listeners" ? PUBLISH_BATCHES : published;
  const expectedPerSuccessfulBatch = subscribed * MSG_COUNT;
  const totalExpectedMessages = expectedPerSuccessfulBatch * expectedBatches;
  const missing = Math.max(0, totalExpectedMessages - observed);
  const diagnostics = prometheus.derived
    ? {
        publishTotalP95Ms: prometheus.derived.publishTotalSeconds?.p95 == null
          ? null
          : prometheus.derived.publishTotalSeconds.p95 * 1000,
        publishBrokerP95Ms: prometheus.derived.publishBrokerSeconds?.p95 == null
          ? null
          : prometheus.derived.publishBrokerSeconds.p95 * 1000,
        brokerToHubDelayP95Ms: prometheus.derived.brokerToHubDelaySeconds?.p95 == null
          ? null
          : prometheus.derived.brokerToHubDelaySeconds.p95 * 1000,
        hubToShardDelayP95Ms: prometheus.derived.hubToShardDelaySeconds?.p95 == null
          ? null
          : prometheus.derived.hubToShardDelaySeconds.p95 * 1000,
        clientQueueDepthP95: prometheus.derived.clientQueueDepth?.p95 ?? null,
        clientQueueDepthP99: prometheus.derived.clientQueueDepth?.p99 ?? null,
        clientQueueResidenceP95Ms: prometheus.derived.clientQueueResidenceSeconds?.p95 == null
          ? null
          : prometheus.derived.clientQueueResidenceSeconds.p95 * 1000,
        clientQueueResidenceP99Ms: prometheus.derived.clientQueueResidenceSeconds?.p99 == null
          ? null
          : prometheus.derived.clientQueueResidenceSeconds.p99 * 1000,
        writeDurationPreparedP95Ms: prometheus.derived.writeDurationPreparedSeconds?.p95 == null
          ? null
          : prometheus.derived.writeDurationPreparedSeconds.p95 * 1000,
        writeTotalDurationPreparedP95Ms: prometheus.derived.writeTotalDurationPreparedSeconds?.p95 == null
          ? null
          : prometheus.derived.writeTotalDurationPreparedSeconds.p95 * 1000,
        writeTotalDurationPreparedWithDeadlineP95Ms: prometheus.derived.writeTotalDurationPreparedWithDeadlineSeconds?.p95 == null
          ? null
          : prometheus.derived.writeTotalDurationPreparedWithDeadlineSeconds.p95 * 1000,
        clientDroppedMessagesTotal: prometheus.derived.clientDroppedMessagesTotal,
        brokerDroppedMessagesTotal: prometheus.derived.brokerDroppedMessagesTotal,
        writeFailuresTotal: prometheus.derived.writeFailuresTotal,
        dataWriteFailuresTotal: prometheus.derived.dataWriteFailuresTotal,
        outboundQueueSize: prometheus.derived.deliveryConfig.outboundQueueSize,
        writeBurstSize: prometheus.derived.deliveryConfig.writeBurstSize,
        enableCompression: prometheus.derived.deliveryConfig.enableCompression,
      }
    : null;

  const summary = {
    driver: DRIVER,
    role: ROLE,
    generatedAt: new Date().toISOString(),
    config: {
      role: ROLE,
      vus: VUS,
      msgCount: MSG_COUNT,
      payloadSize: PAYLOAD_SIZE,
      publishBatches: PUBLISH_BATCHES,
      batchIntervalSeconds: BATCH_INTERVAL_SECONDS,
      publishMessageIntervalMs: PUBLISH_MESSAGE_INTERVAL_MS,
      rampUpSeconds: RAMP_UP_SECONDS,
      holdSeconds: HOLD_SECONDS,
      rampDownSeconds: RAMP_DOWN_SECONDS,
      publishStartSeconds: PUBLISH_START_SECONDS,
      publishWindowSeconds: PUBLISH_WINDOW_SECONDS,
      publishMaxDurationSeconds: PUBLISH_MAX_DURATION_SECONDS,
      drainSeconds: DRAIN_SECONDS,
      latencyP95ThresholdMs: LATENCY_P95_THRESHOLD_MS,
    },
    validity: {
      scheduleIsValid: SCHEDULE_IS_VALID,
      listenerRampDownStartSeconds: LISTENER_RAMP_DOWN_START_SECONDS,
      publisherCompletionDeadlineSeconds: PUBLISHER_COMPLETION_DEADLINE_SECONDS,
      requiredListenerRampDownStartSeconds: REQUIRED_LISTENER_RAMP_DOWN_START_SECONDS,
      allPublishBatchesCompleted: completedPublishBatches === PUBLISH_BATCHES,
      allListenersSubscribed: ROLE === "publisher" ? null : subscribed === VUS,
    },
    delivery: {
      subscribed,
      completedPublishBatches,
      published,
      expectedBatches,
      expectedPerSuccessfulBatch,
      totalExpectedMessages,
      observed,
      missing,
      completeness: totalExpectedMessages === 0 ? 0 : observed / totalExpectedMessages,
    },
    errors: {
      connectErrors,
      connectRetryFailures: 0,
      parseErrors: parseErrorCount,
      readErrors: 0,
      publishErrors,
    },
    latency: {
      messageP50Ms: percentile("msg_latency_ms", "med"),
      messageP95Ms: percentile("msg_latency_ms", "p(95)"),
      messageP99Ms: percentile("msg_latency_ms", "p(99)"),
      eventCreatedToReceivedP50Ms: percentile("event_created_to_received_ms", "med"),
      eventCreatedToReceivedP95Ms: percentile("event_created_to_received_ms", "p(95)"),
      eventCreatedToReceivedP99Ms: percentile("event_created_to_received_ms", "p(99)"),
      eventSentToReceivedP50Ms: percentile("event_sent_to_received_ms", "med"),
      eventSentToReceivedP95Ms: percentile("event_sent_to_received_ms", "p(95)"),
      eventSentToReceivedP99Ms: percentile("event_sent_to_received_ms", "p(99)"),
      publishP95Ms: percentile("publish_duration_ms", "p(95)"),
      publishP99Ms: percentile("publish_duration_ms", "p(99)"),
    },
    prometheus: {
      scrape: prometheus.scrape,
      derived: prometheus.derived,
    },
    diagnostics,
    metrics: data.metrics,
  };

  const diagnosticLines = [];
  if (diagnostics) {
    if (diagnostics.clientQueueDepthP95 != null) {
      diagnosticLines.push(`client_queue_depth_p95=${diagnostics.clientQueueDepthP95}`);
    }
    if (diagnostics.clientQueueDepthP99 != null) {
      diagnosticLines.push(`client_queue_depth_p99=${diagnostics.clientQueueDepthP99}`);
    }
    if (diagnostics.publishTotalP95Ms != null) {
      diagnosticLines.push(`publish_total_p95_ms=${diagnostics.publishTotalP95Ms}`);
    }
    if (diagnostics.publishBrokerP95Ms != null) {
      diagnosticLines.push(`publish_broker_p95_ms=${diagnostics.publishBrokerP95Ms}`);
    }
    if (diagnostics.brokerToHubDelayP95Ms != null) {
      diagnosticLines.push(`broker_to_hub_p95_ms=${diagnostics.brokerToHubDelayP95Ms}`);
    }
    if (diagnostics.hubToShardDelayP95Ms != null) {
      diagnosticLines.push(`hub_to_shard_p95_ms=${diagnostics.hubToShardDelayP95Ms}`);
    }
    if (diagnostics.clientQueueResidenceP95Ms != null) {
      diagnosticLines.push(`client_queue_residence_p95_ms=${diagnostics.clientQueueResidenceP95Ms}`);
    }
    if (diagnostics.clientQueueResidenceP99Ms != null) {
      diagnosticLines.push(`client_queue_residence_p99_ms=${diagnostics.clientQueueResidenceP99Ms}`);
    }
    if (diagnostics.writeDurationPreparedP95Ms != null) {
      diagnosticLines.push(`prepared_write_duration_p95_ms=${diagnostics.writeDurationPreparedP95Ms}`);
    }
    if (diagnostics.writeTotalDurationPreparedP95Ms != null) {
      diagnosticLines.push(`prepared_write_total_duration_p95_ms=${diagnostics.writeTotalDurationPreparedP95Ms}`);
    }
    if (diagnostics.writeTotalDurationPreparedWithDeadlineP95Ms != null) {
      diagnosticLines.push(`prepared_write_total_duration_with_deadline_p95_ms=${diagnostics.writeTotalDurationPreparedWithDeadlineP95Ms}`);
    }
    diagnosticLines.push(`delivery_outbound_queue_size=${diagnostics.outboundQueueSize}`);
    diagnosticLines.push(`delivery_write_burst_size=${diagnostics.writeBurstSize}`);
    diagnosticLines.push(`delivery_enable_compression=${diagnostics.enableCompression}`);
    diagnosticLines.push(`client_dropped_messages=${diagnostics.clientDroppedMessagesTotal}`);
    diagnosticLines.push(`broker_dropped_messages=${diagnostics.brokerDroppedMessagesTotal}`);
    diagnosticLines.push(`data_write_failures=${diagnostics.dataWriteFailuresTotal}`);
  }

  const outputs = {
    stdout: [
      "",
      "BENCHMARK SUMMARY",
      `driver=${summary.driver}`,
      `role=${summary.role}`,
      `subscribers=${subscribed}`,
      `successful_batches=${published}`,
      `completed_publish_batches=${completedPublishBatches}`,
      `expected_messages=${totalExpectedMessages}`,
      `observed_messages=${observed}`,
      `missing_messages=${missing}`,
      `delivery_completeness=${summary.delivery.completeness}`,
      `connect_errors=${connectErrors}`,
      `parse_errors=${parseErrorCount}`,
      `publish_errors=${publishErrors}`,
      `schedule_is_valid=${SCHEDULE_IS_VALID}`,
      `msg_latency_p95_ms=${summary.latency.messageP95Ms}`,
      `event_created_to_received_p95_ms=${summary.latency.eventCreatedToReceivedP95Ms}`,
      `event_sent_to_received_p95_ms=${summary.latency.eventSentToReceivedP95Ms}`,
      `publish_duration_p95_ms=${summary.latency.publishP95Ms}`,
      `prometheus_metrics_ok=${prometheus.scrape.ok || false}`,
      ...diagnosticLines,
      `summary_file=${RESULT_FILE}`,
      "",
    ].join("\n"),
    [RESULT_FILE]: JSON.stringify(summary, null, 2),
  };

  if (METRICS_URL) {
    outputs[METRICS_FILE] = prometheus.text || "";
  }

  return outputs;
}
