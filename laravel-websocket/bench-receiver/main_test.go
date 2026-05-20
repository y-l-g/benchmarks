package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseBenchSentAtFromPusherStringData(t *testing.T) {
	raw := json.RawMessage(`"{\"id\":1,\"sentAt\":1234.5,\"payload\":\"xxx\"}"`)

	got, err := parseBenchSentAt(raw)
	if err != nil {
		t.Fatalf("parseBenchSentAt returned error: %v", err)
	}
	if got != 1234.5 {
		t.Fatalf("sentAt = %v, want 1234.5", got)
	}
}

func TestParseBenchSentAtFromObjectData(t *testing.T) {
	raw := json.RawMessage(`{"id":1,"sentAt":9876,"payload":"xxx"}`)

	got, err := parseBenchSentAt(raw)
	if err != nil {
		t.Fatalf("parseBenchSentAt returned error: %v", err)
	}
	if got != 9876 {
		t.Fatalf("sentAt = %v, want 9876", got)
	}
}

func TestParseBenchSentAtRejectsMissingSentAt(t *testing.T) {
	if _, err := parseBenchSentAt(json.RawMessage(`{"id":1}`)); err == nil {
		t.Fatal("parseBenchSentAt returned nil error for missing sentAt")
	}
}

func TestPercentile(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50}

	tests := map[string]struct {
		q    float64
		want float64
	}{
		"p50": {q: 0.50, want: 30},
		"p90": {q: 0.90, want: 46},
		"p95": {q: 0.95, want: 48},
		"p99": {q: 0.99, want: 49.6},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := percentile(values, tt.q)
			if got != tt.want {
				t.Fatalf("percentile(%v) = %v, want %v", tt.q, got, tt.want)
			}
		})
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := percentile(nil, 0.95); got != 0 {
		t.Fatalf("percentile(nil) = %v, want 0", got)
	}
}

func TestLatencyHistogramQuantile(t *testing.T) {
	histogram := latencyHistogram([]float64{0.1, 1.2, 1.8, 7.1})

	if got := percentileFromHistogram(histogram, 0.50); got != 2 {
		t.Fatalf("histogram p50 = %v, want 2", got)
	}
	if got := percentileFromHistogram(histogram, 0.95); got != 8 {
		t.Fatalf("histogram p95 = %v, want 8", got)
	}
}

func TestMergeHistograms(t *testing.T) {
	merged := mergeHistograms([]summary{
		{Latency: latencySummary{Histogram: []latencyBucket{{UpperBoundMs: 1, Count: 2}}}},
		{Latency: latencySummary{Histogram: []latencyBucket{{UpperBoundMs: 1, Count: 3}, {UpperBoundMs: 5, Count: 1}}}},
	})

	if len(merged) != 2 {
		t.Fatalf("merged bucket count = %d, want 2", len(merged))
	}
	if merged[0].UpperBoundMs != 1 || merged[0].Count != 5 {
		t.Fatalf("first merged bucket = %#v, want upperBound=1 count=5", merged[0])
	}
	if merged[1].UpperBoundMs != 5 || merged[1].Count != 1 {
		t.Fatalf("second merged bucket = %#v, want upperBound=5 count=1", merged[1])
	}
}

func TestRunAggregateRejectsObservedMessagesWithoutHistogram(t *testing.T) {
	dir := t.TempDir()
	inputFile := filepath.Join(dir, "shard.json")
	outputFile := filepath.Join(dir, "aggregate.json")
	input := summary{
		Driver: "pogo",
		Config: config{
			VUs:            1,
			MsgCount:       1,
			PayloadSize:    16,
			PublishBatches: 1,
		},
		Delivery: delivery{
			ObservedMessages: 1,
		},
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input summary: %v", err)
	}
	if err := os.WriteFile(inputFile, encoded, 0o644); err != nil {
		t.Fatalf("write input summary: %v", err)
	}

	err = runAggregate(config{
		Driver:         "pogo",
		ResultFile:     outputFile,
		AggregateFiles: inputFile,
	})
	if err == nil {
		t.Fatal("runAggregate returned nil error")
	}
	if !strings.Contains(err.Error(), "no latency histogram") {
		t.Fatalf("runAggregate error = %q, want missing histogram error", err)
	}
}

func TestDecorateDialErrorIncludesStatusAndBody(t *testing.T) {
	err := decorateDialError(io.ErrUnexpectedEOF, &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Body:       io.NopCloser(strings.NewReader("Too Many Requests\n")),
	})

	got := err.Error()
	if !strings.Contains(got, "status=429") {
		t.Fatalf("decorated error %q does not include status", got)
	}
	if !strings.Contains(got, "Too Many Requests") {
		t.Fatalf("decorated error %q does not include body", got)
	}
}

func TestPrometheusHistogramQuantile(t *testing.T) {
	text := `
# HELP pogo_websocket_client_queue_residence_seconds test
pogo_websocket_client_queue_residence_seconds_bucket{le="0.01"} 10
pogo_websocket_client_queue_residence_seconds_bucket{le="0.05"} 95
pogo_websocket_client_queue_residence_seconds_bucket{le="0.1"} 100
pogo_websocket_client_queue_residence_seconds_bucket{le="+Inf"} 100
pogo_websocket_client_queue_residence_seconds_sum 2.0
pogo_websocket_client_queue_residence_seconds_count 100
`

	got, ok := prometheusHistogramQuantile(text, "pogo_websocket_client_queue_residence_seconds", 0.95)
	if !ok {
		t.Fatal("prometheusHistogramQuantile returned ok=false")
	}
	if got != 0.05 {
		t.Fatalf("p95 = %v, want 0.05", got)
	}
}

func TestPrometheusGaugeValue(t *testing.T) {
	text := `
pogo_websocket_delivery_config{key="write_burst_size"} 8
pogo_websocket_delivery_config{key="enable_compression"} 1
`

	if got := prometheusGaugeValue(text, "pogo_websocket_delivery_config", "write_burst_size"); got != 8 {
		t.Fatalf("write_burst_size = %v, want 8", got)
	}
	if got := prometheusGaugeValue(text, "pogo_websocket_delivery_config", "enable_compression"); got != 1 {
		t.Fatalf("enable_compression = %v, want 1", got)
	}
}

func TestPrometheusCounterValue(t *testing.T) {
	text := `
pogo_websocket_write_failures_total{kind="close"} 500
pogo_websocket_write_failures_total{kind="prepared"} 2
pogo_websocket_write_failures_total{kind="bytes"} 3
`

	if got := prometheusCounterValue(text, "pogo_websocket_write_failures_total", nil); got != 505 {
		t.Fatalf("all write failures = %v, want 505", got)
	}
	if got := prometheusCounterValue(text, "pogo_websocket_write_failures_total", map[string]string{"kind": "prepared"}); got != 2 {
		t.Fatalf("prepared write failures = %v, want 2", got)
	}
}
