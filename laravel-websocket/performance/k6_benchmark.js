import ws from "k6/ws";
import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter } from "k6/metrics";

// Metrics
const latencyTrend = new Trend("broadcast_latency_ms");
const msgCounter = new Counter("messages_received");
const connectionErrors = new Counter("connection_errors");

// Configuration
const HOST = __ENV.TARGET_HOST || "localhost:9090"; // Matches e2e test port default
const APP_ID = __ENV.APP_ID || "test-app";
const CHANNEL = "public-benchmark";
const EVENT = "benchmark-event";

export const options = {
  scenarios: {
    // 1. Consumers: Connect and Subscribe
    consumers: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "10s", target: 50 }, // Warm up
        { duration: "20s", target: 200 }, // Load
        { duration: "10s", target: 200 }, // Sustain
        { duration: "10s", target: 0 }, // Cooldown
      ],
      exec: "consumer",
    },
    // 2. Publisher: Trigger broadcasts via HTTP
    publisher: {
      executor: "constant-arrival-rate",
      rate: 5, // 5 messages per second
      timeUnit: "1s",
      duration: "50s",
      preAllocatedVUs: 1,
      startTime: "10s", // Wait for consumers
      exec: "publisher",
    },
  },
};

export function consumer() {
  const url = `ws://${HOST}/app/${APP_ID}`;

  const res = ws.connect(url, {}, function (socket) {
    socket.on("open", () => {
      // Subscribe
      socket.send(
        JSON.stringify({
          event: "pusher:subscribe",
          data: { channel: CHANNEL },
        })
      );
    });

    socket.on("message", (data) => {
      const msg = JSON.parse(data);

      // Calculate Latency
      if (msg.event === EVENT) {
        msgCounter.add(1);
        // Payload is expected to be a JSON string inside msg.data
        try {
          const payload = JSON.parse(msg.data);
          if (payload.ts) {
            const now = Date.now();
            latencyTrend.add(now - payload.ts);
          }
        } catch (e) {
          // ignore parsing errors
        }
      }
    });

    socket.on("close", () => {});

    socket.on("error", (e) => {
      connectionErrors.add(1);
    });

    // Pogo/Pusher Keep-Alive
    // Server expects pong, but client usually sends ping?
    // Pusher protocol: Client sends pusher:ping, Server responds pusher:pong
    socket.setInterval(() => {
      socket.send(JSON.stringify({ event: "pusher:ping" }));
    }, 30000);
  });

  check(res, { "status is 101": (r) => r && r.status === 101 });
}

export function publisher() {
  // We need a way to publish.
  // We assume the test environment exposes the publish.php fixture.
  // URL: http://localhost:9090/publish/publish.php

  const payload = JSON.stringify({ ts: Date.now(), bench: true });

  const params = {
    app_id: APP_ID,
    channel: CHANNEL,
    event: EVENT,
    data: payload,
  };

  const qs = Object.keys(params)
    .map((key) => `${key}=${encodeURIComponent(params[key])}`)
    .join("&");

  const pubUrl = `http://${HOST}/publish/publish.php?${qs}`;

  const res = http.get(pubUrl);

  check(res, {
    "publish success": (r) => r.status === 200,
  });
}
