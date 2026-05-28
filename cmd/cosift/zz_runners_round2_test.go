package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/store"
)

// seedTinyStore opens a SQLite store and inserts two documents so Stats /
// Outcomes / GC have something to work with.
func seedTinyStore(t *testing.T) *config.Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, u := range []string{"https://x.example/a", "https://x.example/b"} {
		_, err := s.UpsertDocument(context.Background(), &store.Document{
			URL: u, Title: "t", Text: "body", Source: "test",
			FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %q: %v", u, err)
		}
	}
	s.Close()
	cfg := config.Default()
	cfg.DataDir = dir
	return cfg
}

func TestRunStatsSQLiteHappy(t *testing.T) {
	cfg := seedTinyStore(t)
	stdout := captureStdoutCosift(t, func() {
		if err := runStats(context.Background(), cfg, nil); err != nil {
			t.Errorf("runStats: %v", err)
		}
	})
	if !strings.Contains(stdout, "documents:") {
		t.Errorf("missing 'documents:': %s", stdout)
	}
	if !strings.Contains(stdout, "backend: sqlite") {
		t.Errorf("missing backend label: %s", stdout)
	}
}

func TestRunStatsUnknownBackend(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runStats(context.Background(), cfg, []string{"-backend", "redis"})
	if err == nil {
		t.Error("expected error for unknown backend")
	}
	if !strings.Contains(err.Error(), "unknown -backend") {
		t.Errorf("got %v", err)
	}
}

// runStats(-backend=pebble) on a fresh dir creates a new Pebble store
// and returns zero counts. Just confirms the pebble branch executes
// without erroring.
func TestRunStatsPebbleEmpty(t *testing.T) {
	cfg := seedTinyStore(t)
	stdout := captureStdoutCosift(t, func() {
		if err := runStats(context.Background(), cfg, []string{"-backend", "pebble"}); err != nil {
			t.Errorf("runStats pebble: %v", err)
		}
	})
	if !strings.Contains(stdout, "backend: pebble") {
		t.Errorf("missing backend label: %s", stdout)
	}
}

func TestRunGCHappy(t *testing.T) {
	cfg := seedTinyStore(t)
	// Vacuum=false so we don't touch SQLite VACUUM machinery in tests.
	err := runGC(context.Background(), cfg, []string{"-vacuum=false", "-min-attempts", "100"})
	if err != nil {
		t.Errorf("runGC: %v", err)
	}
}

func TestRunGCWithParaphraseTTL(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runGC(context.Background(), cfg, []string{
		"-vacuum=false", "-paraphrase-ttl", "1h",
	})
	if err != nil {
		t.Errorf("runGC: %v", err)
	}
}

func TestRunOutcomesEmptyJSON(t *testing.T) {
	cfg := seedTinyStore(t)
	dst := filepath.Join(t.TempDir(), "out.json")
	if err := runOutcomes(context.Background(), cfg, []string{
		"-output", dst, "-format", "json",
	}); err != nil {
		t.Fatalf("runOutcomes: %v", err)
	}
	body, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	// Empty store → JSON null OR []; both are valid for the writer's
	// encoder. Parse and accept either.
	trimmed := strings.TrimSpace(string(body))
	if trimmed != "null" && trimmed != "[]" {
		var arr []interface{}
		if err := json.Unmarshal(body, &arr); err != nil {
			t.Errorf("output not JSON: %s", body)
		}
	}
}

func TestRunOutcomesEmptyCSV(t *testing.T) {
	cfg := seedTinyStore(t)
	dst := filepath.Join(t.TempDir(), "out.csv")
	if err := runOutcomes(context.Background(), cfg, []string{
		"-output", dst, "-format", "csv",
	}); err != nil {
		t.Fatalf("runOutcomes: %v", err)
	}
	body, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read out: %v", err)
	}
	rdr := csv.NewReader(strings.NewReader(string(body)))
	rows, err := rdr.ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	// Header row at minimum.
	if len(rows) < 1 {
		t.Errorf("expected at least header row, got %d", len(rows))
	}
	if len(rows[0]) < 7 || rows[0][0] != "id" {
		t.Errorf("unexpected header: %v", rows[0])
	}
}

func TestRunOutcomesBadFormat(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runOutcomes(context.Background(), cfg, []string{
		"-output", "x", "-format", "xml",
	})
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}

func TestRunStatusFileTextWithoutTarget(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	body := `{
		"frontier_queued": 1, "frontier_in_flight": 1, "frontier_done": 1, "frontier_errored": 0,
		"started_at": "2026-01-01T00:00:00Z", "written_at": "2026-01-01T00:00:01Z"
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	stdout := captureStdoutCosift(t, func() {
		if err := runStatusFile(context.Background(), cfg, nil); err != nil {
			t.Errorf("runStatusFile: %v", err)
		}
	})
	for _, want := range []string{"queued:     1", "processed:"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("missing %q: %s", want, stdout)
		}
	}
}

func TestRunStatusFileStaleWarning(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crawl-status.json")
	// written_at far in the past → "stale" warning + age > 30s.
	body := `{
		"frontier_queued": 0, "frontier_in_flight": 0, "frontier_done": 0, "frontier_errored": 0,
		"started_at": "2020-01-01T00:00:00Z", "written_at": "2020-01-01T00:00:00Z"
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := minimalConfig(tmp)
	stdout := captureStdoutCosift(t, func() {
		if err := runStatusFile(context.Background(), cfg, nil); err != nil {
			t.Errorf("runStatusFile: %v", err)
		}
	})
	if !strings.Contains(stdout, "WARNING") {
		t.Errorf("missing stale WARNING: %s", stdout)
	}
}
