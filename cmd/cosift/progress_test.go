package main

import (
	"bytes"
	"context"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/eval"
)

// captureLog redirects the package-default logger to a buffer for the duration
// of f and returns what was logged. Used to assert on progressReporter output.
func captureLog(t *testing.T, f func()) string {
	t.Helper()
	prev := log.Writer()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	f()
	return buf.String()
}

func TestProgressReporterFiresOnceAfterInterval(t *testing.T) {
	p := newProgressReporter("test", 100, 10*time.Millisecond)
	// First call immediately after construction → no log (last set to now).
	out := captureLog(t, func() {
		p.maybeLog(1)
	})
	if strings.Contains(out, "test:") {
		t.Errorf("first call shouldn't log; got %q", out)
	}
	// Wait past the interval, call again → exactly one log line.
	time.Sleep(20 * time.Millisecond)
	out = captureLog(t, func() {
		p.maybeLog(50)
	})
	if !strings.Contains(out, "test: 50/100 (50.0%)") {
		t.Errorf("expected progress line at 50%%; got %q", out)
	}
	// Immediately after the log → rate-limited, no second log.
	out = captureLog(t, func() {
		p.maybeLog(60)
	})
	if strings.Contains(out, "test:") {
		t.Errorf("second call within interval shouldn't log; got %q", out)
	}
}

func TestProgressReporterRateLimitedTightLoop(t *testing.T) {
	// Tight loop with 1000 calls should produce at most ~total_time/interval lines.
	p := newProgressReporter("loop", 1000, 50*time.Millisecond)
	out := captureLog(t, func() {
		for i := 1; i <= 1000; i++ {
			p.maybeLog(i)
		}
	})
	// Without rate limiting this would emit 1000 lines; with 50ms interval and a
	// loop that finishes in microseconds, we expect zero or one lines.
	lines := strings.Count(out, "\n")
	if lines > 2 {
		t.Errorf("tight loop should emit at most ~2 lines (got %d): %q", lines, out)
	}
}

func TestProgressReporterDisabledZeroInterval(t *testing.T) {
	p := newProgressReporter("disabled", 100, 0)
	out := captureLog(t, func() {
		time.Sleep(2 * time.Millisecond)
		// Manually pre-stale the `last` field to defeat the time check (we want to
		// verify the interval == 0 check fires FIRST).
		p.last = time.Now().Add(-1 * time.Hour)
		p.maybeLog(50)
	})
	if strings.Contains(out, "disabled") {
		t.Errorf("interval=0 should disable logging entirely: %q", out)
	}
}

func TestProgressReporterZeroTotalNoCrash(t *testing.T) {
	// total=0 must not divide-by-zero.
	p := newProgressReporter("empty", 0, 1*time.Millisecond)
	p.last = time.Now().Add(-1 * time.Hour) // force interval-elapsed
	out := captureLog(t, func() {
		p.maybeLog(0)
	})
	// We silently skip when total=0 (nothing meaningful to report).
	if strings.Contains(out, "empty") {
		t.Errorf("total=0 should suppress logging: %q", out)
	}
}

func TestProgressReporterFormat(t *testing.T) {
	// Verify the exact format: `<label>: <current>/<total> (<pct>%)`.
	p := newProgressReporter("docs", 1000, 1*time.Millisecond)
	p.last = time.Now().Add(-1 * time.Hour) // force fire
	out := captureLog(t, func() {
		p.maybeLog(250)
	})
	if !strings.Contains(out, "docs: 250/1000 (25.0%)") {
		t.Errorf("format mismatch: %q", out)
	}
}

func TestRunIngestEmitsProgressForLargeCorpus(t *testing.T) {
	// Use a tiny -progress interval and enough docs that the per-doc work
	// crosses the interval threshold. Verify at least one progress line lands.
	dir := filepath.Join(t.TempDir(), "data")
	cfg := config.Default()
	cfg.DataDir = dir

	// 30 docs is enough that the inner BM25 index work crosses ~5ms in CI.
	docs := make([]eval.CorpusDoc, 30)
	for i := range docs {
		docs[i] = eval.CorpusDoc{
			URL:   "https://x/doc" + string(rune('a'+i%26)),
			Title: "Doc",
			Text:  strings.Repeat("body content ", 50),
		}
	}
	jsonlPath := filepath.Join(t.TempDir(), "corpus.jsonl")
	writeJSONL(t, jsonlPath, docs)

	out := captureLog(t, func() {
		if err := runIngest(context.Background(), cfg,
			[]string{"-corpus", jsonlPath, "-format", "jsonl", "-progress", "1ms"}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	})
	// At least one progress line should have fired ("ingest docs: N/30").
	if !strings.Contains(out, "ingest docs:") {
		t.Errorf("expected at least one `ingest docs:` progress line; got %q", out)
	}
}

func TestRunIngestProgressDisabled(t *testing.T) {
	// -progress 0 should suppress per-loop progress lines entirely.
	dir := filepath.Join(t.TempDir(), "data")
	cfg := config.Default()
	cfg.DataDir = dir

	docs := make([]eval.CorpusDoc, 20)
	for i := range docs {
		docs[i] = eval.CorpusDoc{
			URL:   "https://y/doc" + string(rune('a'+i%26)),
			Title: "Y",
			Text:  "y body",
		}
	}
	jsonlPath := filepath.Join(t.TempDir(), "corpus.jsonl")
	writeJSONL(t, jsonlPath, docs)

	out := captureLog(t, func() {
		if err := runIngest(context.Background(), cfg,
			[]string{"-corpus", jsonlPath, "-format", "jsonl", "-progress", "0"}); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	})
	if strings.Contains(out, "ingest docs:") {
		t.Errorf("-progress 0 should suppress progress lines; got %q", out)
	}
	// The summary line at the end of ingest should still be present.
	if !strings.Contains(out, "ingest:") {
		t.Errorf("expected final summary line: %q", out)
	}
}
