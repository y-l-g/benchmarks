package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const benchChannel = "bench-channel"

type config struct {
	Driver              string  `json:"driver"`
	Role                string  `json:"role"`
	VUs                 int     `json:"vus"`
	MsgCount            int     `json:"msgCount"`
	PayloadSize         int     `json:"payloadSize"`
	PublishBatches      int     `json:"publishBatches"`
	BatchIntervalSecs   float64 `json:"batchIntervalSeconds"`
	RampUpSeconds       int     `json:"rampUpSeconds"`
	PublishStartSeconds int     `json:"publishStartSeconds"`
	PublishMaxDuration  int     `json:"publishMaxDurationSeconds"`
	DrainSeconds        int     `json:"drainSeconds"`
	HTTPHost            string  `json:"httpHost"`
	WSHost              string  `json:"wsHost"`
	HTTPPort            string  `json:"httpPort"`
	WSPort              string  `json:"wsPort"`
	AppKey              string  `json:"appKey"`
	ResultFile          string  `json:"resultFile"`
	MetricsURL          string  `json:"metricsUrl"`
	MetricsFile         string  `json:"metricsFile"`
	SubscriptionTimeout int     `json:"subscriptionTimeoutSeconds"`
	ClientCompression   bool    `json:"clientCompression"`
	AggregateFiles      string  `json:"aggregateFiles,omitempty"`
}

type summary struct {
	Driver      string         `json:"driver"`
	Probe       string         `json:"probe"`
	GeneratedAt string         `json:"generatedAt"`
	Config      config         `json:"config"`
	Delivery    delivery       `json:"delivery"`
	Latency     latencySummary `json:"latency"`
	WebSocket   websocketStats `json:"websocket"`
	Diagnostics *diagnostics   `json:"diagnostics"`
	Errors      errorSummary   `json:"errors"`
}

type delivery struct {
	Subscribed             int     `json:"subscribed"`
	CompletedBatches       int     `json:"completedPublishBatches"`
	ExpectedMessages       int     `json:"expectedMessages"`
	ObservedMessages       int     `json:"observedMessages"`
	MissingMessages        int     `json:"missingMessages"`
	DeliveryCompleteness   float64 `json:"deliveryCompleteness"`
	AllListenersSubscribed bool    `json:"allListenersSubscribed"`
}

type latencySummary struct {
	Samples         int             `json:"samples"`
	SentToReadMinMs float64         `json:"sentToReadMinMs"`
	SentToReadAvgMs float64         `json:"sentToReadAvgMs"`
	SentToReadP50Ms float64         `json:"sentToReadP50Ms"`
	SentToReadP90Ms float64         `json:"sentToReadP90Ms"`
	SentToReadP95Ms float64         `json:"sentToReadP95Ms"`
	SentToReadP99Ms float64         `json:"sentToReadP99Ms"`
	SentToReadMaxMs float64         `json:"sentToReadMaxMs"`
	PublishP95Ms    float64         `json:"publishP95Ms"`
	PublishP99Ms    float64         `json:"publishP99Ms"`
	Histogram       []latencyBucket `json:"histogram,omitempty"`
}

type diagnostics struct {
	FanoutDurationP95Ms        float64 `json:"fanoutDurationP95Ms"`
	FanoutSubscribersP95       float64 `json:"fanoutSubscribersP95"`
	ClientQueueDepthP95        float64 `json:"clientQueueDepthP95"`
	ClientQueueDepthP99        float64 `json:"clientQueueDepthP99"`
	ClientQueueResidenceP95Ms  float64 `json:"clientQueueResidenceP95Ms"`
	ClientQueueResidenceP99Ms  float64 `json:"clientQueueResidenceP99Ms"`
	OutboundQueueSize          float64 `json:"outboundQueueSize"`
	WriteBurstSize             float64 `json:"writeBurstSize"`
	EnableCompression          float64 `json:"enableCompression"`
	ClientDroppedMessagesTotal float64 `json:"clientDroppedMessagesTotal"`
	BrokerDroppedMessagesTotal float64 `json:"brokerDroppedMessagesTotal"`
	WriteFailuresTotal         float64 `json:"writeFailuresTotal"`
	DataWriteFailuresTotal     float64 `json:"dataWriteFailuresTotal"`
}

type errorSummary struct {
	ConnectErrors        int64  `json:"connectErrors"`
	ConnectRetryFailures int64  `json:"connectRetryFailures"`
	LastConnectError     string `json:"lastConnectError,omitempty"`
	ReadErrors           int64  `json:"readErrors"`
	ParseErrors          int64  `json:"parseErrors"`
	PublishErrors        int64  `json:"publishErrors"`
}

type receiver struct {
	conn *websocket.Conn
}

type websocketStats struct {
	NegotiatedCompressionConnections int `json:"negotiatedCompressionConnections"`
}

type latencyBucket struct {
	UpperBoundMs int   `json:"upperBoundMs"`
	Count        int64 `json:"count"`
}

type pusherMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type benchPayload struct {
	SentAt float64 `json:"sentAt"`
}

func main() {
	if err := run(context.Background(), loadConfig()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, cfg config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	if cfg.Role == "aggregate" {
		return runAggregate(cfg)
	}

	if cfg.Role == "publisher" {
		return runPublisherOnly(cfg)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	startedAt := time.Now()

	latencies := make(chan float64, cfg.VUs*cfg.MsgCount*max(1, cfg.PublishBatches))
	subscribed := make(chan struct{}, cfg.VUs)
	errs := &errorSummary{}
	receivers := make([]receiver, 0, cfg.VUs)
	wsStats := websocketStats{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	connectDeadline := time.Now().Add(time.Duration(max(cfg.RampUpSeconds, cfg.SubscriptionTimeout)) * time.Second)
	connectPace := time.Duration(0)
	if cfg.RampUpSeconds > 0 && cfg.VUs > 0 {
		connectPace = time.Duration(cfg.RampUpSeconds) * time.Second / time.Duration(cfg.VUs)
	}

	for i := 0; i < cfg.VUs; i++ {
		conn, extensions, err := dialReceiver(cfg, connectDeadline, errs)
		if err != nil {
			atomic.AddInt64(&errs.ConnectErrors, 1)
			errs.LastConnectError = fmt.Sprintf("connect receiver %d: %v", i, err)
			break
		}
		if strings.Contains(extensions, "permessage-deflate") {
			wsStats.NegotiatedCompressionConnections++
		}
		mu.Lock()
		receivers = append(receivers, receiver{conn: conn})
		mu.Unlock()

		wg.Add(1)
		go func(c *websocket.Conn) {
			defer wg.Done()
			readLoop(ctx, c, subscribed, latencies, errs)
		}(conn)

		if err := conn.WriteJSON(map[string]any{
			"event": "pusher:subscribe",
			"data":  map[string]string{"channel": benchChannel},
		}); err != nil {
			atomic.AddInt64(&errs.ConnectErrors, 1)
			errs.LastConnectError = fmt.Sprintf("subscribe receiver %d: %v", i, err)
			break
		}

		if connectPace > 0 {
			time.Sleep(connectPace)
		}
	}

	subscribedCount := waitForSubscriptions(subscribed, cfg.VUs, time.Duration(cfg.SubscriptionTimeout)*time.Second)
	if subscribedCount != cfg.VUs {
		cancel()
		closeReceivers(receivers)
		wg.Wait()
		return writeSummary(cfg, subscribedCount, 0, nil, nil, *errs, nil, wsStats)
	}

	completedBatches := 0
	var publishDurations []float64
	if cfg.Role == "both" {
		sleepUntil(startedAt.Add(time.Duration(cfg.PublishStartSeconds) * time.Second))
		completedBatches, publishDurations = publishBatches(cfg, errs)
	} else {
		completedBatches = cfg.PublishBatches
	}

	expectedMessages := subscribedCount * cfg.MsgCount * completedBatches
	values := collectLatencies(latencies, expectedMessages, receiveTimeout(cfg))

	cancel()
	closeReceivers(receivers)
	wg.Wait()

	var diag *diagnostics
	if cfg.Role == "both" {
		var err error
		diag, err = scrapeDiagnostics(cfg)
		if err != nil {
			errs.LastConnectError = err.Error()
		}
	}
	return writeSummary(cfg, subscribedCount, completedBatches, values, publishDurations, *errs, diag, wsStats)
}

func runPublisherOnly(cfg config) error {
	errs := &errorSummary{}
	if cfg.PublishStartSeconds > 0 {
		time.Sleep(time.Duration(cfg.PublishStartSeconds) * time.Second)
	}
	completedBatches, publishDurations := publishBatches(cfg, errs)
	diag, err := scrapeDiagnostics(cfg)
	if err != nil {
		errs.LastConnectError = err.Error()
	}
	return writeSummary(cfg, 0, completedBatches, nil, publishDurations, *errs, diag, websocketStats{})
}

func runAggregate(cfg config) error {
	files := splitList(cfg.AggregateFiles)
	if len(files) == 0 {
		return errors.New("AGGREGATE_FILES must list at least one summary file")
	}

	inputs := make([]summary, 0, len(files))
	for _, file := range files {
		encoded, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read aggregate summary %s: %w", file, err)
		}
		var decoded summary
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			return fmt.Errorf("decode aggregate summary %s: %w", file, err)
		}
		if decoded.Delivery.ObservedMessages > 0 && len(decoded.Latency.Histogram) == 0 {
			return fmt.Errorf("aggregate summary %s has %d observed messages but no latency histogram; rebuild the go receiver listener images", file, decoded.Delivery.ObservedMessages)
		}
		inputs = append(inputs, decoded)
	}

	aggregateCfg := cfg
	aggregateCfg.Driver = firstNonEmpty(cfg.Driver, inputs[0].Driver)
	aggregateCfg.Role = "aggregate"
	aggregateCfg.VUs = 0
	aggregateCfg.MsgCount = inputs[0].Config.MsgCount
	aggregateCfg.PayloadSize = inputs[0].Config.PayloadSize
	aggregateCfg.PublishBatches = inputs[0].Config.PublishBatches
	aggregateCfg.BatchIntervalSecs = inputs[0].Config.BatchIntervalSecs
	aggregateCfg.RampUpSeconds = inputs[0].Config.RampUpSeconds
	aggregateCfg.PublishStartSeconds = inputs[0].Config.PublishStartSeconds
	aggregateCfg.PublishMaxDuration = inputs[0].Config.PublishMaxDuration
	aggregateCfg.DrainSeconds = inputs[0].Config.DrainSeconds
	aggregateCfg.ClientCompression = inputs[0].Config.ClientCompression

	mergedBuckets := mergeHistograms(inputs)
	out := summary{
		Driver:      aggregateCfg.Driver,
		Probe:       "go-receiver-aggregate",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Config:      aggregateCfg,
		Latency: latencySummary{
			Histogram:       mergedBuckets,
			SentToReadP50Ms: percentileFromHistogram(mergedBuckets, 0.50),
			SentToReadP90Ms: percentileFromHistogram(mergedBuckets, 0.90),
			SentToReadP95Ms: percentileFromHistogram(mergedBuckets, 0.95),
			SentToReadP99Ms: percentileFromHistogram(mergedBuckets, 0.99),
		},
	}

	allListenersSubscribed := true
	minSet := false
	weightedAvgSum := 0.0
	for _, in := range inputs {
		aggregateCfg.VUs += in.Config.VUs
		out.Delivery.Subscribed += in.Delivery.Subscribed
		out.Delivery.ExpectedMessages += in.Delivery.ExpectedMessages
		out.Delivery.ObservedMessages += in.Delivery.ObservedMessages
		out.Delivery.MissingMessages += in.Delivery.MissingMessages
		out.WebSocket.NegotiatedCompressionConnections += in.WebSocket.NegotiatedCompressionConnections
		out.Errors.ConnectErrors += in.Errors.ConnectErrors
		out.Errors.ConnectRetryFailures += in.Errors.ConnectRetryFailures
		out.Errors.ReadErrors += in.Errors.ReadErrors
		out.Errors.ParseErrors += in.Errors.ParseErrors
		out.Errors.PublishErrors += in.Errors.PublishErrors
		if in.Errors.LastConnectError != "" {
			out.Errors.LastConnectError = in.Errors.LastConnectError
		}
		if !in.Delivery.AllListenersSubscribed {
			allListenersSubscribed = false
		}
		if in.Delivery.CompletedBatches > out.Delivery.CompletedBatches {
			out.Delivery.CompletedBatches = in.Delivery.CompletedBatches
		}
		if in.Latency.Samples > 0 {
			if !minSet || in.Latency.SentToReadMinMs < out.Latency.SentToReadMinMs {
				out.Latency.SentToReadMinMs = in.Latency.SentToReadMinMs
				minSet = true
			}
			if in.Latency.SentToReadMaxMs > out.Latency.SentToReadMaxMs {
				out.Latency.SentToReadMaxMs = in.Latency.SentToReadMaxMs
			}
			out.Latency.Samples += in.Latency.Samples
			weightedAvgSum += in.Latency.SentToReadAvgMs * float64(in.Latency.Samples)
		}
	}
	out.Config = aggregateCfg
	out.Delivery.AllListenersSubscribed = allListenersSubscribed
	if out.Delivery.ExpectedMessages > 0 {
		out.Delivery.DeliveryCompleteness = float64(out.Delivery.ObservedMessages) / float64(out.Delivery.ExpectedMessages)
	}
	if out.Latency.Samples > 0 {
		out.Latency.SentToReadAvgMs = weightedAvgSum / float64(out.Latency.Samples)
	}

	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.ResultFile, append(encoded, '\n'), 0o644); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("GO RECEIVER AGGREGATE SUMMARY")
	fmt.Printf("subscribers=%d\n", out.Delivery.Subscribed)
	fmt.Printf("completed_publish_batches=%d\n", out.Delivery.CompletedBatches)
	fmt.Printf("expected_messages=%d\n", out.Delivery.ExpectedMessages)
	fmt.Printf("observed_messages=%d\n", out.Delivery.ObservedMessages)
	fmt.Printf("missing_messages=%d\n", out.Delivery.MissingMessages)
	fmt.Printf("delivery_completeness=%g\n", out.Delivery.DeliveryCompleteness)
	fmt.Printf("sent_to_read_p95_ms=%g\n", out.Latency.SentToReadP95Ms)
	fmt.Printf("connect_errors=%d\n", out.Errors.ConnectErrors)
	fmt.Printf("connect_retry_failures=%d\n", out.Errors.ConnectRetryFailures)
	fmt.Printf("parse_errors=%d\n", out.Errors.ParseErrors)
	fmt.Printf("read_errors=%d\n", out.Errors.ReadErrors)
	fmt.Printf("summary_file=%s\n", cfg.ResultFile)
	fmt.Println()
	return nil
}

func publishBatches(cfg config, errs *errorSummary) (int, []float64) {
	completedBatches := 0
	durations := make([]float64, 0, cfg.PublishBatches)
	for i := 0; i < cfg.PublishBatches; i++ {
		startedAt := time.Now()
		if err := publishBatch(cfg); err != nil {
			atomic.AddInt64(&errs.PublishErrors, 1)
			errs.LastConnectError = err.Error()
			break
		}
		durations = append(durations, float64(time.Since(startedAt))/float64(time.Millisecond))
		completedBatches++
		time.Sleep(durationFromSeconds(cfg.BatchIntervalSecs))
	}
	return completedBatches, durations
}

func dialReceiver(cfg config, deadline time.Time, errs *errorSummary) (*websocket.Conn, string, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout:  10 * time.Second,
		EnableCompression: cfg.ClientCompression,
	}
	var lastErr error

	for {
		conn, res, err := dialer.Dial(wsURL(cfg), nil)
		if err == nil {
			extensions := ""
			if res != nil {
				extensions = res.Header.Get("Sec-WebSocket-Extensions")
			}
			return conn, extensions, nil
		}

		lastErr = decorateDialError(err, res)
		atomic.AddInt64(&errs.ConnectRetryFailures, 1)
		errs.LastConnectError = lastErr.Error()

		if time.Now().Add(250 * time.Millisecond).After(deadline) {
			return nil, "", lastErr
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func decorateDialError(err error, res *http.Response) error {
	if res == nil {
		return err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
	if len(body) == 0 {
		return fmt.Errorf("%w: status=%d", err, res.StatusCode)
	}
	return fmt.Errorf("%w: status=%d body=%q", err, res.StatusCode, string(body))
}

func readLoop(ctx context.Context, conn *websocket.Conn, subscribed chan<- struct{}, latencies chan<- float64, errs *errorSummary) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				atomic.AddInt64(&errs.ReadErrors, 1)
			}
			return
		}

		msg, ok, err := parsePusherMessage(data)
		if err != nil {
			atomic.AddInt64(&errs.ParseErrors, 1)
			continue
		}
		if !ok {
			continue
		}

		switch msg.Event {
		case "pusher_internal:subscription_succeeded":
			select {
			case subscribed <- struct{}{}:
			default:
			}
		case "bench.event":
			sentAt, err := parseBenchSentAt(msg.Data)
			if err != nil {
				atomic.AddInt64(&errs.ParseErrors, 1)
				continue
			}
			latency := float64(time.Now().UnixNano())/float64(time.Millisecond) - sentAt
			if latency < 0 {
				latency = 0
			}
			select {
			case latencies <- latency:
			case <-ctx.Done():
				return
			}
		}
	}
}

func parsePusherMessage(data []byte) (pusherMessage, bool, error) {
	var msg pusherMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return msg, false, err
	}
	if msg.Event == "" {
		return msg, false, nil
	}
	return msg, true, nil
}

func parseBenchSentAt(raw json.RawMessage) (float64, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		raw = json.RawMessage(asString)
	}

	var payload benchPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	if !isFinite(payload.SentAt) || payload.SentAt <= 0 {
		return 0, errors.New("missing sentAt")
	}
	return payload.SentAt, nil
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	pos := q * float64(len(sorted)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	weight := pos - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func latencyStats(values []float64, publishDurations []float64) latencySummary {
	sort.Float64s(values)
	out := latencySummary{
		Samples:         len(values),
		SentToReadP50Ms: percentile(values, 0.50),
		SentToReadP90Ms: percentile(values, 0.90),
		SentToReadP95Ms: percentile(values, 0.95),
		SentToReadP99Ms: percentile(values, 0.99),
		PublishP95Ms:    percentile(sortedCopy(publishDurations), 0.95),
		PublishP99Ms:    percentile(sortedCopy(publishDurations), 0.99),
		Histogram:       latencyHistogram(values),
	}
	if len(values) == 0 {
		return out
	}

	out.SentToReadMinMs = values[0]
	out.SentToReadMaxMs = values[len(values)-1]
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	out.SentToReadAvgMs = sum / float64(len(values))
	return out
}

func sortedCopy(values []float64) []float64 {
	if len(values) == 0 {
		return nil
	}
	copied := append([]float64(nil), values...)
	sort.Float64s(copied)
	return copied
}

func latencyHistogram(values []float64) []latencyBucket {
	if len(values) == 0 {
		return nil
	}

	counts := make(map[int]int64)
	for _, value := range values {
		if value < 0 {
			value = 0
		}
		upperBound := int(math.Ceil(value))
		counts[upperBound]++
	}

	bounds := make([]int, 0, len(counts))
	for bound := range counts {
		bounds = append(bounds, bound)
	}
	sort.Ints(bounds)

	buckets := make([]latencyBucket, 0, len(bounds))
	for _, bound := range bounds {
		buckets = append(buckets, latencyBucket{
			UpperBoundMs: bound,
			Count:        counts[bound],
		})
	}
	return buckets
}

func percentileFromHistogram(buckets []latencyBucket, q float64) float64 {
	total := int64(0)
	for _, bucket := range buckets {
		total += bucket.Count
	}
	if total == 0 {
		return 0
	}

	target := int64(math.Ceil(q * float64(total)))
	if target < 1 {
		target = 1
	}
	seen := int64(0)
	for _, bucket := range buckets {
		seen += bucket.Count
		if seen >= target {
			return float64(bucket.UpperBoundMs)
		}
	}
	return float64(buckets[len(buckets)-1].UpperBoundMs)
}

func mergeHistograms(summaries []summary) []latencyBucket {
	counts := make(map[int]int64)
	for _, in := range summaries {
		for _, bucket := range in.Latency.Histogram {
			counts[bucket.UpperBoundMs] += bucket.Count
		}
	}

	bounds := make([]int, 0, len(counts))
	for bound := range counts {
		bounds = append(bounds, bound)
	}
	sort.Ints(bounds)

	out := make([]latencyBucket, 0, len(bounds))
	for _, bound := range bounds {
		out = append(out, latencyBucket{
			UpperBoundMs: bound,
			Count:        counts[bound],
		})
	}
	return out
}

func collectLatencies(latencies <-chan float64, expected int, timeout time.Duration) []float64 {
	values := make([]float64, 0, expected)
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for len(values) < expected {
		select {
		case latency := <-latencies:
			values = append(values, latency)
		case <-timer.C:
			return values
		}
	}

	return values
}

func waitForSubscriptions(subscribed <-chan struct{}, expected int, timeout time.Duration) int {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	count := 0

	for count < expected {
		select {
		case <-subscribed:
			count++
		case <-timer.C:
			return count
		}
	}

	return count
}

func publishBatch(cfg config) error {
	u := url.URL{
		Scheme: "http",
		Host:   cfg.HTTPHost + ":" + cfg.HTTPPort,
		Path:   "/fire",
	}
	query := u.Query()
	query.Set("count", strconv.Itoa(cfg.MsgCount))
	query.Set("size", strconv.Itoa(cfg.PayloadSize))
	u.RawQuery = query.Encode()

	client := http.Client{Timeout: time.Duration(max(5, int(math.Ceil(cfg.BatchIntervalSecs)))) * time.Second}
	res, err := client.Get(u.String())
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("publish status %d: %s", res.StatusCode, string(body))
	}

	var decoded struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return err
	}
	if decoded.Count != cfg.MsgCount {
		return fmt.Errorf("published count %d, want %d", decoded.Count, cfg.MsgCount)
	}
	return nil
}

func scrapeDiagnostics(cfg config) (*diagnostics, error) {
	if cfg.MetricsURL == "" {
		return nil, nil
	}

	client := http.Client{Timeout: 5 * time.Second}
	res, err := client.Get(cfg.MetricsURL)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if cfg.MetricsFile != "" {
		if err := os.WriteFile(cfg.MetricsFile, body, 0o644); err != nil {
			return nil, err
		}
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics status %d", res.StatusCode)
	}

	text := string(body)
	fanoutDurationP95, _ := prometheusHistogramQuantile(text, "pogo_websocket_fanout_duration_seconds", 0.95)
	fanoutSubscribersP95, _ := prometheusHistogramQuantile(text, "pogo_websocket_fanout_subscribers", 0.95)
	clientQueueDepthP95, _ := prometheusHistogramQuantile(text, "pogo_websocket_client_queue_depth", 0.95)
	clientQueueDepthP99, _ := prometheusHistogramQuantile(text, "pogo_websocket_client_queue_depth", 0.99)
	clientQueueResidenceP95, _ := prometheusHistogramQuantile(text, "pogo_websocket_client_queue_residence_seconds", 0.95)
	clientQueueResidenceP99, _ := prometheusHistogramQuantile(text, "pogo_websocket_client_queue_residence_seconds", 0.99)
	writeFailuresPrepared := prometheusCounterValue(text, "pogo_websocket_write_failures_total", map[string]string{"kind": "prepared"})
	writeFailuresBytes := prometheusCounterValue(text, "pogo_websocket_write_failures_total", map[string]string{"kind": "bytes"})
	return &diagnostics{
		FanoutDurationP95Ms:        fanoutDurationP95 * 1000,
		FanoutSubscribersP95:       fanoutSubscribersP95,
		ClientQueueDepthP95:        clientQueueDepthP95,
		ClientQueueDepthP99:        clientQueueDepthP99,
		ClientQueueResidenceP95Ms:  clientQueueResidenceP95 * 1000,
		ClientQueueResidenceP99Ms:  clientQueueResidenceP99 * 1000,
		OutboundQueueSize:          prometheusGaugeValue(text, "pogo_websocket_delivery_config", "outbound_queue_size"),
		WriteBurstSize:             prometheusGaugeValue(text, "pogo_websocket_delivery_config", "write_burst_size"),
		EnableCompression:          prometheusGaugeValue(text, "pogo_websocket_delivery_config", "enable_compression"),
		ClientDroppedMessagesTotal: prometheusCounterValue(text, "pogo_websocket_client_dropped_messages_total", nil),
		BrokerDroppedMessagesTotal: prometheusCounterValue(text, "pogo_websocket_broker_dropped_messages_total", nil),
		WriteFailuresTotal:         prometheusCounterValue(text, "pogo_websocket_write_failures_total", nil),
		DataWriteFailuresTotal:     writeFailuresPrepared + writeFailuresBytes,
	}, nil
}

func prometheusCounterValue(text string, name string, labels map[string]string) float64 {
	total := 0.0
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		metricName, metricLabels := prometheusNameAndLabels(fields[0])
		if metricName != name || !labelsMatch(metricLabels, labels) {
			continue
		}

		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || !isFinite(value) {
			continue
		}
		total += value
	}
	return total
}

func prometheusNameAndLabels(metric string) (string, map[string]string) {
	start := strings.Index(metric, "{")
	if start < 0 {
		return metric, nil
	}
	end := strings.LastIndex(metric, "}")
	if end < start {
		return metric, nil
	}
	return metric[:start], parsePrometheusLabels(metric[start+1 : end])
}

func parsePrometheusLabels(raw string) map[string]string {
	labels := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		labels[key] = strings.Trim(value, `"`)
	}
	return labels
}

func labelsMatch(sample map[string]string, wanted map[string]string) bool {
	for key, value := range wanted {
		if sample[key] != value {
			return false
		}
	}
	return true
}

func prometheusGaugeValue(text, name, key string) float64 {
	needle := name + `{key="` + key + `"}`
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, needle+" ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || !isFinite(value) {
			return 0
		}
		return value
	}
	return 0
}

func prometheusHistogramQuantile(text, baseName string, q float64) (float64, bool) {
	type bucket struct {
		le    float64
		value float64
	}

	var buckets []bucket
	count := 0.0
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || !isFinite(value) {
			continue
		}

		metric := fields[0]
		if strings.HasPrefix(metric, baseName+"_bucket{") {
			le, ok := prometheusLE(metric)
			if ok {
				buckets = append(buckets, bucket{le: le, value: value})
			}
		}
		if metric == baseName+"_count" || strings.HasPrefix(metric, baseName+"_count{") {
			count += value
		}
	}
	if len(buckets) == 0 || count == 0 {
		return 0, false
	}

	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })
	target := count * q
	previousLe := 0.0
	previousCount := 0.0
	for _, bucket := range buckets {
		if bucket.value >= target {
			if math.IsInf(bucket.le, 1) {
				return previousLe, true
			}
			bucketCount := bucket.value - previousCount
			if bucketCount <= 0 {
				return bucket.le, true
			}
			position := (target - previousCount) / bucketCount
			return previousLe + (bucket.le-previousLe)*position, true
		}
		previousLe = bucket.le
		previousCount = bucket.value
	}
	return 0, false
}

func prometheusLE(metric string) (float64, bool) {
	const label = `le="`
	start := strings.Index(metric, label)
	if start < 0 {
		return 0, false
	}
	start += len(label)
	end := strings.Index(metric[start:], `"`)
	if end < 0 {
		return 0, false
	}
	raw := metric[start : start+end]
	if raw == "+Inf" {
		return math.Inf(1), true
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func writeSummary(cfg config, subscribedCount int, completedBatches int, values []float64, publishDurations []float64, errs errorSummary, diag *diagnostics, wsStats websocketStats) error {
	latency := latencyStats(values, publishDurations)
	expected := subscribedCount * cfg.MsgCount * completedBatches
	missing := max(0, expected-len(values))
	completeness := 0.0
	if expected > 0 {
		completeness = float64(len(values)) / float64(expected)
	}

	out := summary{
		Driver:      cfg.Driver,
		Probe:       "go-receiver",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Config:      cfg,
		Delivery: delivery{
			Subscribed:             subscribedCount,
			CompletedBatches:       completedBatches,
			ExpectedMessages:       expected,
			ObservedMessages:       len(values),
			MissingMessages:        missing,
			DeliveryCompleteness:   completeness,
			AllListenersSubscribed: subscribedCount == cfg.VUs,
		},
		Latency:     latency,
		WebSocket:   wsStats,
		Diagnostics: diag,
		Errors:      errs,
	}

	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.ResultFile, append(encoded, '\n'), 0o644); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("GO RECEIVER SUMMARY")
	fmt.Printf("subscribers=%d\n", out.Delivery.Subscribed)
	fmt.Printf("completed_publish_batches=%d\n", out.Delivery.CompletedBatches)
	fmt.Printf("expected_messages=%d\n", out.Delivery.ExpectedMessages)
	fmt.Printf("observed_messages=%d\n", out.Delivery.ObservedMessages)
	fmt.Printf("missing_messages=%d\n", out.Delivery.MissingMessages)
	fmt.Printf("delivery_completeness=%g\n", out.Delivery.DeliveryCompleteness)
	fmt.Printf("sent_to_read_p95_ms=%g\n", out.Latency.SentToReadP95Ms)
	fmt.Printf("connect_errors=%d\n", out.Errors.ConnectErrors)
	fmt.Printf("connect_retry_failures=%d\n", out.Errors.ConnectRetryFailures)
	fmt.Printf("parse_errors=%d\n", out.Errors.ParseErrors)
	fmt.Printf("read_errors=%d\n", out.Errors.ReadErrors)
	fmt.Printf("summary_file=%s\n", cfg.ResultFile)
	fmt.Println()
	return nil
}

func closeReceivers(receivers []receiver) {
	for _, r := range receivers {
		_ = r.conn.Close()
	}
}

func wsURL(cfg config) string {
	return fmt.Sprintf("ws://%s:%s/app/%s?protocol=7&client=go&version=1.0.0&flash=false", cfg.WSHost, cfg.WSPort, cfg.AppKey)
}

func receiveTimeout(cfg config) time.Duration {
	return time.Duration(cfg.PublishMaxDuration+cfg.DrainSeconds) * time.Second
}

func durationFromSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func loadConfig() config {
	return config{
		Driver:              envString("DRIVER", "pogo"),
		Role:                envString("ROLE", "both"),
		VUs:                 envInt("VUS", 500),
		MsgCount:            envInt("MSG_COUNT", 100),
		PayloadSize:         envInt("PAYLOAD_SIZE", 1024),
		PublishBatches:      envInt("PUBLISH_BATCHES", 20),
		BatchIntervalSecs:   envFloat("BATCH_INTERVAL_SECONDS", 2),
		RampUpSeconds:       envInt("RAMP_UP_SECONDS", 10),
		PublishStartSeconds: envInt("PUBLISH_START_SECONDS", 12),
		PublishMaxDuration:  envInt("PUBLISH_MAX_DURATION_SECONDS", envInt("PUBLISH_BATCHES", 20)*int(math.Ceil(envFloat("BATCH_INTERVAL_SECONDS", 2)))+60),
		DrainSeconds:        envInt("DRAIN_SECONDS", 10),
		HTTPHost:            envString("HTTP_HOST", "pogo"),
		WSHost:              envString("WS_HOST", "pogo"),
		HTTPPort:            envString("HTTP_PORT", "8000"),
		WSPort:              envString("WS_PORT", "8000"),
		AppKey:              envString("APP_KEY", "pogo-app"),
		ResultFile:          envString("RESULT_FILE", "/results/go-receiver-pogo-summary.json"),
		MetricsURL:          envString("METRICS_URL", ""),
		MetricsFile:         envString("METRICS_FILE", ""),
		SubscriptionTimeout: envInt("SUBSCRIPTION_TIMEOUT_SECONDS", 30),
		ClientCompression:   envBool("WS_ENABLE_COMPRESSION", false),
		AggregateFiles:      envString("AGGREGATE_FILES", ""),
	}
}

func validateConfig(cfg config) error {
	if cfg.Role != "both" && cfg.Role != "listeners" && cfg.Role != "publisher" && cfg.Role != "aggregate" {
		return errors.New("ROLE must be both, listeners, publisher, or aggregate")
	}
	if cfg.Role != "publisher" && cfg.Role != "aggregate" && cfg.VUs <= 0 {
		return errors.New("VUS must be greater than 0")
	}
	if cfg.MsgCount < 0 {
		return errors.New("MSG_COUNT must not be negative")
	}
	if cfg.PublishBatches < 0 {
		return errors.New("PUBLISH_BATCHES must not be negative")
	}
	if cfg.BatchIntervalSecs < 0 {
		return errors.New("BATCH_INTERVAL_SECONDS must not be negative")
	}
	return nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func sleepUntil(deadline time.Time) {
	if remaining := time.Until(deadline); remaining > 0 {
		time.Sleep(remaining)
	}
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
