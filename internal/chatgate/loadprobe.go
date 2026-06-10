package chatgate

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// LoadProbe polls a vLLM /metrics endpoint and exposes whether the
// backend is "loaded" (queue deeper than threshold). Callers consult
// LoadProbe.Loaded() to decide whether to skip optional LLM work
// (rerank, HyDE, paraphrase) for the current request — graceful
// degradation under spike rather than queue-then-time-out.
//
// vLLM exposes Prometheus-format metrics including:
//
//	vllm:num_requests_running{...} N
//	vllm:num_requests_waiting{...} N
//
// We sum waiting across engines/models; running is informational only.
// When the inferred URL doesn't return parseable metrics (e.g. running
// against OpenAI), Probe is a no-op and Loaded() always returns false.
type LoadProbe struct {
	url       string
	threshold int64
	interval  time.Duration
	http      *http.Client

	waiting atomic.Int64
	running atomic.Int64
	loaded  atomic.Bool
	calls   atomic.Int64
	errors  atomic.Int64
}

// NewLoadProbe builds a probe targeting metricsURL (e.g.
// http://vllm:8000/metrics). When threshold is reached, Loaded()
// returns true and stays true until the next poll observes the queue
// drained below threshold. Zero threshold disables the probe.
func NewLoadProbe(metricsURL string, threshold int, interval time.Duration) *LoadProbe {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &LoadProbe{
		url:       metricsURL,
		threshold: int64(threshold),
		interval:  interval,
		http: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				ResponseHeaderTimeout: 1 * time.Second,
			},
		},
	}
}

// VLLMMetricsURLFrom takes an OpenAI-compatible chat URL (e.g.
// http://host:8000/v1/chat/completions) and returns the vLLM
// metrics endpoint at the same origin (http://host:8000/metrics).
// Returns "" when the input can't be parsed — caller should treat
// that as "no probe."
func VLLMMetricsURLFrom(chatURL string) string {
	if chatURL == "" {
		return ""
	}
	u, err := url.Parse(chatURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host + "/metrics"
}

// Run starts the polling loop. Cancel via ctx. Safe to call once per
// probe instance; subsequent calls return immediately.
func (p *LoadProbe) Run(ctx context.Context) {
	if p == nil || p.url == "" || p.threshold <= 0 {
		return
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	p.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *LoadProbe) pollOnce(ctx context.Context) {
	p.calls.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, http.NoBody)
	if err != nil {
		p.errors.Add(1)
		return
	}
	resp, err := p.http.Do(req)
	if err != nil {
		p.errors.Add(1)
		// Endpoint unreachable — assume healthy. Tripping degraded mode
		// on a probe outage would do exactly the wrong thing under a
		// real spike, where the metrics endpoint itself can be slow.
		p.loaded.Store(false)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		p.errors.Add(1)
		p.loaded.Store(false)
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if err != nil {
		p.errors.Add(1)
		return
	}
	waiting, running := parseVLLMMetrics(string(body))
	p.waiting.Store(waiting)
	p.running.Store(running)
	p.loaded.Store(waiting >= p.threshold)
}

// parseVLLMMetrics extracts and sums num_requests_waiting + _running
// across all model/engine label combos. Prometheus text format: lines
// like `vllm:num_requests_waiting{engine="0",model_name="foo"} 3.0`.
func parseVLLMMetrics(body string) (waiting, running int64) {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		var prefix string
		switch {
		case strings.HasPrefix(line, "vllm:num_requests_waiting"):
			prefix = "vllm:num_requests_waiting"
		case strings.HasPrefix(line, "vllm:num_requests_running"):
			prefix = "vllm:num_requests_running"
		default:
			continue
		}
		// Skip variants like vllm:num_requests_waiting_by_reason — only
		// take the bare counter.
		rest := line[len(prefix):]
		if rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		// Find the numeric value at end of line.
		sp := strings.LastIndexByte(line, ' ')
		if sp < 0 || sp == len(line)-1 {
			continue
		}
		fv, err := strconv.ParseFloat(strings.TrimSpace(line[sp+1:]), 64)
		if err != nil {
			continue
		}
		n := int64(fv)
		if prefix == "vllm:num_requests_waiting" {
			waiting += n
		} else {
			running += n
		}
	}
	return waiting, running
}

// Loaded reports whether the backend is queue-deep enough to warrant
// shedding optional LLM work. Lock-free, safe for hot-path use.
func (p *LoadProbe) Loaded() bool {
	if p == nil {
		return false
	}
	return p.loaded.Load()
}

// ProbeStats is a snapshot for /stats publishing.
type ProbeStats struct {
	Loaded     bool   `json:"loaded"`
	Waiting    int64  `json:"waiting"`
	Running    int64  `json:"running"`
	Threshold  int64  `json:"threshold"`
	Polls      int64  `json:"polls"`
	Errors     int64  `json:"errors"`
	IntervalMs int64  `json:"interval_ms"`
	URL        string `json:"url"`
}

func (p *LoadProbe) Stats() ProbeStats {
	if p == nil {
		return ProbeStats{}
	}
	return ProbeStats{
		Loaded:     p.loaded.Load(),
		Waiting:    p.waiting.Load(),
		Running:    p.running.Load(),
		Threshold:  p.threshold,
		Polls:      p.calls.Load(),
		Errors:     p.errors.Load(),
		IntervalMs: p.interval.Milliseconds(),
		URL:        p.url,
	}
}
