package sla

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestObserveAndEvaluate(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "sla.jsonl")
	m, err := New([]Target{
		{Endpoint: "/search", P95: 500 * time.Millisecond, P99: time.Second, MaxErrorRate: 0.05},
		{Endpoint: "/answer", P95: 8 * time.Second, P99: 20 * time.Second, MaxErrorRate: 0.05},
	}, time.Minute, logPath)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// All fast, none failed → no violations.
	for i := 0; i < 50; i++ {
		m.Observe("/search", 100*time.Millisecond, true)
	}
	m.evaluate()
	snap := m.Snapshot()
	if !snap.Endpoints["/search"].OKMeetsSLA {
		t.Errorf("expected /search to meet SLA: %+v", snap.Endpoints["/search"])
	}
	if got := snap.TotalViolations; got != 0 {
		t.Errorf("violations: got %d want 0", got)
	}

	// Slow /search → p95 breach.
	for i := 0; i < 100; i++ {
		m.Observe("/search", 2*time.Second, true)
	}
	m.evaluate()
	snap = m.Snapshot()
	if snap.Endpoints["/search"].OKMeetsSLA {
		t.Errorf("expected /search to fail SLA")
	}
	if snap.TotalViolations == 0 {
		t.Errorf("expected at least 1 violation")
	}

	// Log file should have JSONL entries.
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if len(b) == 0 {
		t.Errorf("log file empty after violations")
	}
	var v Violation
	if err := json.Unmarshal(splitFirstLine(b), &v); err != nil {
		t.Errorf("decode log line: %v", err)
	}
	if v.Endpoint != "/search" {
		t.Errorf("logged endpoint: got %q", v.Endpoint)
	}
}

func TestErrorRateViolation(t *testing.T) {
	m, _ := New([]Target{
		{Endpoint: "/answer", P95: 30 * time.Second, MaxErrorRate: 0.05},
	}, time.Minute, "")
	for i := 0; i < 100; i++ {
		m.Observe("/answer", time.Second, i%5 != 0) // 20% failure
	}
	m.evaluate()
	st := m.Snapshot().Endpoints["/answer"]
	if st.OKMeetsSLA {
		t.Errorf("expected error-rate violation; status=%+v", st)
	}
	if st.ErrorRate < 0.15 {
		t.Errorf("error_rate: got %v", st.ErrorRate)
	}
}

func TestRingBufferOverwrite(t *testing.T) {
	rb := newRingBuffer(4)
	now := time.Now()
	for i := 0; i < 10; i++ {
		rb.add(Sample{Latency: time.Duration(i) * time.Millisecond, OK: true, At: now})
	}
	out := rb.snapshot(now.Add(-time.Hour))
	if len(out) != 4 {
		t.Errorf("snapshot len: got %d want 4 (capacity)", len(out))
	}
}

func TestNilMonitorSafe(t *testing.T) {
	var m *Monitor
	m.Observe("/x", time.Second, true) // must not panic
	snap := m.Snapshot()
	if snap.TotalObserved != 0 || snap.TotalViolations != 0 || snap.WindowSec != 0 {
		t.Errorf("nil snapshot not zero: %+v", snap)
	}
}

func splitFirstLine(b []byte) []byte {
	for i, c := range b {
		if c == '\n' {
			return b[:i]
		}
	}
	return b
}
