package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

// seedQueryTestStore creates a temp store + index with three docs that share
// the word "concurrent" so any query for it should return all three.
func seedQueryTestStore(t *testing.T) *config.Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	s, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/go", "Go concurrent programming", "Go has goroutines for concurrent programming"},
		{"https://x/rust", "Rust concurrent ownership", "Rust handles concurrent access via ownership"},
		{"https://x/python", "Python concurrent futures", "Python concurrent futures module"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}
	cfg := config.Default()
	cfg.DataDir = dir
	return cfg
}

// captureStdout redirects stdout for the duration of f and returns what was written.
// Used to assert on `cosift query`'s printf output.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan []byte)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.Bytes()
	}()
	f()
	_ = w.Close()
	os.Stdout = orig
	return string(<-done)
}

func TestRunQueryJSONOutput(t *testing.T) {
	cfg := seedQueryTestStore(t)
	out := captureStdout(t, func() {
		if err := runQuery(context.Background(), cfg, "concurrent", []string{"-json"}); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
	})
	// Parse as JSON array.
	var hits []struct {
		URL   string  `json:"url"`
		Title string  `json:"title"`
		Score float64 `json:"score"`
	}
	if err := json.Unmarshal([]byte(out), &hits); err != nil {
		t.Fatalf("parse json output: %v\noutput was: %q", err, out)
	}
	if len(hits) != 3 {
		t.Errorf("want 3 hits, got %d (%+v)", len(hits), hits)
	}
	// Top hit should have URL, Title, and a non-zero Score.
	if hits[0].URL == "" || hits[0].Title == "" || hits[0].Score == 0 {
		t.Errorf("top hit missing fields: %+v", hits[0])
	}
}

func TestRunQueryKLimit(t *testing.T) {
	cfg := seedQueryTestStore(t)
	out := captureStdout(t, func() {
		if err := runQuery(context.Background(), cfg, "concurrent", []string{"-k", "2", "-json"}); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
	})
	var hits []map[string]any
	if err := json.Unmarshal([]byte(out), &hits); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(hits) != 2 {
		t.Errorf("k=2 should return 2 hits, got %d", len(hits))
	}
}

func TestRunQueryKValidation(t *testing.T) {
	cfg := seedQueryTestStore(t)
	if err := runQuery(context.Background(), cfg, "x", []string{"-k", "0"}); err == nil {
		t.Errorf("k=0 should error")
	}
	if err := runQuery(context.Background(), cfg, "x", []string{"-k", "1000"}); err == nil {
		t.Errorf("k=1000 (>100) should error")
	}
}

func TestRunQueryDefaultPreserved(t *testing.T) {
	// No flags → human-readable output, k=10 default.
	cfg := seedQueryTestStore(t)
	out := captureStdout(t, func() {
		if err := runQuery(context.Background(), cfg, "concurrent", nil); err != nil {
			t.Fatalf("runQuery: %v", err)
		}
	})
	// Should NOT be JSON (no leading bracket).
	if len(out) > 0 && out[0] == '[' {
		t.Errorf("default output should be human-readable, got JSON: %s", out)
	}
	// Should contain at least one of our test URLs.
	if !containsAny(out, "https://x/go", "https://x/rust", "https://x/python") {
		t.Errorf("default output missing expected URLs: %s", out)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if bytesContains([]byte(s), []byte(sub)) {
			return true
		}
	}
	return false
}

func bytesContains(haystack, needle []byte) bool {
	return bytes.Contains(haystack, needle)
}
