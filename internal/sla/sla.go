// Package sla tracks per-endpoint latency + error rate against
// operator-configured SLA targets and persists a violation log to
// disk so out-of-band tooling (and the operator) can see when the
// service stopped meeting its promises.
//
// Design points:
//
//   - Lock-free hot path. Observations are atomic-only into a fixed-
//     size circular sample buffer. Producers don't block readers.
//
//   - Per-endpoint isolation. /search and /answer have wildly
//     different latency budgets; a single histogram would lose that.
//
//   - Violation log persists to disk (append-only JSONL). On restart
//     the file is rotated so we don't keep a single growing log.
//
//   - Evaluator runs in its own goroutine, walks the per-endpoint
//     samples each tick, compares p95/p99/error-rate to targets, and
//     records a Violation when out of bounds.
//
//   - No external dependencies (Prometheus, OTLP). cosift's deploy
//     story is single-binary; the SLA monitor stays in-process and
//     surfaces via /sla.
package sla

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Target is the per-endpoint SLA goal.
type Target struct {
	Endpoint     string        `json:"endpoint"`
	P95          time.Duration `json:"p95"`
	P99          time.Duration `json:"p99"`
	MaxErrorRate float64       `json:"max_error_rate"` // e.g. 0.01 = 1%
}

// Sample is one observed call.
type Sample struct {
	Latency time.Duration
	OK      bool
	At      time.Time
}

// Violation is what the evaluator records when a target is breached.
type Violation struct {
	Endpoint   string        `json:"endpoint"`
	At         time.Time     `json:"at"`
	Metric     string        `json:"metric"`   // "p95", "p99", "error_rate"
	Observed   float64       `json:"observed"` // ms for latency, ratio for error_rate
	Target     float64       `json:"target"`
	WindowSec  int           `json:"window_sec"`
	SampleSize int           `json:"sample_size"`
	Severity   string        `json:"severity"` // "warn" / "critical"
	Latency    time.Duration `json:"-"`
}

// Monitor is the in-process SLA tracker.
type Monitor struct {
	targets map[string]Target
	window  time.Duration

	mu      sync.RWMutex
	buckets map[string]*ringBuffer

	logPath string
	logMu   sync.Mutex

	totalObserved   atomic.Int64
	totalViolations atomic.Int64

	// Last-evaluated snapshot per endpoint, surfaced on /sla.
	lastMu     sync.RWMutex
	lastStatus map[string]EndpointStatus
	lastEvalAt time.Time

	// Tracks the most recently logged severity for
	// each (endpoint, metric) combo. A violation is only persisted to
	// disk when the severity changes ("" → warn → critical, or back).
	// Without this, a single bad sample stuck in the rolling window
	// generates a duplicate jsonl entry every evaluation tick.
	lastSeverity map[string]string // key = endpoint+"\x00"+metric → "warn" / "critical" / "" (recovered)
}

// EndpointStatus is the most recent evaluation result for an endpoint.
type EndpointStatus struct {
	Endpoint   string        `json:"endpoint"`
	Samples    int           `json:"samples"`
	P50        time.Duration `json:"p50"`
	P95        time.Duration `json:"p95"`
	P99        time.Duration `json:"p99"`
	ErrorRate  float64       `json:"error_rate"`
	OKMeetsSLA bool          `json:"ok_meets_sla"`
	Violations []Violation   `json:"violations,omitempty"`
}

// New builds a Monitor with the given targets, rolling sample window,
// and on-disk violation log path. Empty logPath = no persistence (still
// kept in memory for /sla).
func New(targets []Target, window time.Duration, logPath string) (*Monitor, error) {
	if window <= 0 {
		window = 5 * time.Minute
	}
	m := &Monitor{
		targets:      make(map[string]Target, len(targets)),
		window:       window,
		buckets:      make(map[string]*ringBuffer),
		logPath:      logPath,
		lastStatus:   make(map[string]EndpointStatus),
		lastSeverity: make(map[string]string),
	}
	for _, t := range targets {
		if t.Endpoint == "" {
			return nil, errors.New("sla: target missing endpoint")
		}
		m.targets[t.Endpoint] = t
		m.buckets[t.Endpoint] = newRingBuffer(1024)
	}
	if logPath != "" {
		if err := m.rotateLog(); err != nil {
			return nil, fmt.Errorf("sla: rotate log: %w", err)
		}
	}
	return m, nil
}

// Observe records one call.
func (m *Monitor) Observe(endpoint string, latency time.Duration, ok bool) {
	if m == nil {
		return
	}
	m.totalObserved.Add(1)
	m.mu.RLock()
	rb, exists := m.buckets[endpoint]
	m.mu.RUnlock()
	if !exists {
		// Unknown endpoint — lazy-add bucket so observers can see it
		// even before a target is configured.
		m.mu.Lock()
		if rb = m.buckets[endpoint]; rb == nil {
			rb = newRingBuffer(1024)
			m.buckets[endpoint] = rb
		}
		m.mu.Unlock()
	}
	rb.add(Sample{Latency: latency, OK: ok, At: time.Now()})
}

// Run starts the evaluator loop. Each tick:
//   - drop samples older than window
//   - compute p50/p95/p99 + error rate per endpoint
//   - compare to target; record + persist violations
//
// Cancel via ctx.
func (m *Monitor) Run(stop <-chan struct{}, interval time.Duration) {
	if m == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			m.evaluate()
		}
	}
}

func (m *Monitor) evaluate() {
	m.mu.RLock()
	endpoints := make([]string, 0, len(m.buckets))
	for k := range m.buckets {
		endpoints = append(endpoints, k)
	}
	m.mu.RUnlock()

	now := time.Now()
	cutoff := now.Add(-m.window)
	statusMap := make(map[string]EndpointStatus, len(endpoints))

	for _, ep := range endpoints {
		m.mu.RLock()
		rb := m.buckets[ep]
		m.mu.RUnlock()
		samples := rb.snapshot(cutoff)
		if len(samples) == 0 {
			statusMap[ep] = EndpointStatus{Endpoint: ep, OKMeetsSLA: true}
			continue
		}
		st := computeStatus(ep, samples)
		t, hasTarget := m.targets[ep]
		if hasTarget {
			var vios []Violation
			vios = m.checkMetric(vios, ep, now, "p95", float64(st.P95.Milliseconds()), float64(t.P95.Milliseconds()), len(samples), t.P95 > 0 && st.P95 > t.P95)
			vios = m.checkMetric(vios, ep, now, "p99", float64(st.P99.Milliseconds()), float64(t.P99.Milliseconds()), len(samples), t.P99 > 0 && st.P99 > t.P99)
			vios = m.checkMetric(vios, ep, now, "error_rate", st.ErrorRate, t.MaxErrorRate, len(samples), t.MaxErrorRate > 0 && st.ErrorRate > t.MaxErrorRate)
			st.Violations = vios
			st.OKMeetsSLA = !violatingAny(t, st)
		} else {
			st.OKMeetsSLA = true // no target → can't violate
		}
		statusMap[ep] = st
	}

	m.lastMu.Lock()
	m.lastStatus = statusMap
	m.lastEvalAt = now
	m.lastMu.Unlock()
}

// checkMetric handles one (endpoint, metric) pair per evaluation tick.
// Persists a violation to disk only when the severity transitions (e.g.
// recovered → warn, warn → critical, critical → recovered). Without this
// gating, a single bad sample stuck in the rolling window generated a
// duplicate jsonl entry every 30s tick — observed on GH200 as 60+ near-
// identical lines for a single 31s /stats outlier.
func (m *Monitor) checkMetric(out []Violation, endpoint string, now time.Time, metric string, observed, target float64, n int, violating bool) []Violation {
	key := endpoint + "\x00" + metric
	severity := ""
	if violating {
		severity = "warn"
		if target > 0 && observed > target*1.5 {
			severity = "critical"
		}
	}
	prev := m.lastSeverity[key]
	if severity == prev {
		// No transition — suppress.
		if violating {
			// Still want to surface in the in-memory snapshot, just don't
			// re-log to disk. Build the Violation without persisting.
			out = append(out, Violation{
				Endpoint: endpoint, At: now, Metric: metric,
				Observed: observed, Target: target,
				WindowSec: int(m.window / time.Second), SampleSize: n,
				Severity: severity,
			})
		}
		return out
	}
	m.lastSeverity[key] = severity
	if severity == "" {
		// Transitioned BACK to meeting SLA — log a recovery marker.
		m.persist(Violation{
			Endpoint: endpoint, At: now, Metric: metric,
			Observed: observed, Target: target,
			WindowSec: int(m.window / time.Second), SampleSize: n,
			Severity: "recovered",
		})
		return out
	}
	v := Violation{
		Endpoint: endpoint, At: now, Metric: metric,
		Observed: observed, Target: target,
		WindowSec: int(m.window / time.Second), SampleSize: n,
		Severity: severity,
	}
	m.totalViolations.Add(1)
	m.persist(v)
	return append(out, v)
}

// violatingAny returns whether any tracked metric is breaching the
// target. Used to set OKMeetsSLA without re-running the comparisons.
func violatingAny(t Target, st EndpointStatus) bool {
	if t.P95 > 0 && st.P95 > t.P95 {
		return true
	}
	if t.P99 > 0 && st.P99 > t.P99 {
		return true
	}
	if t.MaxErrorRate > 0 && st.ErrorRate > t.MaxErrorRate {
		return true
	}
	return false
}

func (m *Monitor) persist(v Violation) {
	if m.logPath == "" {
		return
	}
	m.logMu.Lock()
	defer m.logMu.Unlock()
	f, err := os.OpenFile(m.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	_, _ = f.Write(b)
	_, _ = f.Write([]byte("\n"))
}

// rotateLog moves any pre-existing log aside with a timestamp suffix
// so the live log file stays bounded.
func (m *Monitor) rotateLog() error {
	if m.logPath == "" {
		return nil
	}
	if _, err := os.Stat(m.logPath); err == nil {
		ts := time.Now().Format("20060102T150405")
		_ = os.Rename(m.logPath, m.logPath+"."+ts)
	}
	return nil
}

// Snapshot returns the latest evaluation state.
type Snapshot struct {
	EvaluatedAt     string                    `json:"evaluated_at"`
	WindowSec       int                       `json:"window_sec"`
	Endpoints       map[string]EndpointStatus `json:"endpoints"`
	TotalObserved   int64                     `json:"total_observed"`
	TotalViolations int64                     `json:"total_violations"`
	LogPath         string                    `json:"log_path,omitempty"`
}

func (m *Monitor) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}
	m.lastMu.RLock()
	defer m.lastMu.RUnlock()
	endpoints := make(map[string]EndpointStatus, len(m.lastStatus))
	for k, v := range m.lastStatus {
		endpoints[k] = v
	}
	return Snapshot{
		EvaluatedAt:     m.lastEvalAt.UTC().Format(time.RFC3339),
		WindowSec:       int(m.window / time.Second),
		Endpoints:       endpoints,
		TotalObserved:   m.totalObserved.Load(),
		TotalViolations: m.totalViolations.Load(),
		LogPath:         m.logPath,
	}
}

// computeStatus walks the samples to derive percentiles + error rate.
func computeStatus(endpoint string, samples []Sample) EndpointStatus {
	durs := make([]time.Duration, 0, len(samples))
	fails := 0
	for _, s := range samples {
		durs = append(durs, s.Latency)
		if !s.OK {
			fails++
		}
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	errRate := float64(fails) / float64(len(samples))
	return EndpointStatus{
		Endpoint:  endpoint,
		Samples:   len(samples),
		P50:       pct(durs, 0.50),
		P95:       pct(durs, 0.95),
		P99:       pct(durs, 0.99),
		ErrorRate: errRate,
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
