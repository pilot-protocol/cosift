package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

// TestPebbleServeEndToEnd — populate a Pebble store, launch the
// pebble-serve handler in-process against a free port, hit /healthz +
// /stats + /search + /contents, assert the responses are coherent.
// Iter 205. The /search assertion is the load-bearing one: it proves
// the Pebble backend serves real BM25 results through an HTTP layer.
func TestPebbleServeEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	idx := index.NewPebbleBM25(ps)
	ctx := context.Background()

	corpus := []struct{ url, title, text string }{
		{"https://x/raft", "Raft consensus", "Raft is a distributed consensus algorithm."},
		{"https://x/paxos", "Paxos algorithm", "Paxos is the classical distributed consensus algorithm."},
		{"https://x/cooking", "Cooking pasta", "Boil water, salt, drop pasta, drain when al dente."},
	}
	for _, d := range corpus {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	ps.Close()

	// Launch pebble-serve on a free port in a goroutine.
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		cfg := &config.Config{
			Server: config.Server{Addr: addr},
		}
		done <- runPebbleServe(serveCtx, cfg, []string{"-dir", dir, "-addr", addr})
	}()

	if !waitForPort(addr, 4*time.Second) {
		t.Fatalf("server didn't come up on %s within 4s", addr)
	}
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Logf("server shutdown took >3s")
		}
	}()

	base := "http://" + addr

	// /healthz
	if resp := mustGet(t, base+"/healthz"); resp["status"] != "ok" {
		t.Errorf("healthz: %v", resp)
	}

	// /stats
	stats := mustGet(t, base+"/stats")
	if int(stats["documents"].(float64)) != len(corpus) {
		t.Errorf("stats documents: want %d, got %v", len(corpus), stats["documents"])
	}
	if stats["backend"] != "pebble" {
		t.Errorf("stats backend: want pebble, got %v", stats["backend"])
	}

	// /search — raft query
	got := mustGet(t, base+"/search?q=raft+consensus&k=3")
	hits, ok := got["hits"].([]any)
	if !ok || len(hits) == 0 {
		t.Fatalf("search /search?q=raft: no hits in %+v", got)
	}
	topURL := hits[0].(map[string]any)["url"].(string)
	if topURL != "https://x/raft" {
		t.Errorf("top hit for raft query: want https://x/raft, got %s", topURL)
	}

	// /contents
	contents := mustGet(t, base+"/contents?url="+url.QueryEscape("https://x/raft"))
	if contents["title"] != "Raft consensus" {
		t.Errorf("contents title: %v", contents["title"])
	}
	if !strings.Contains(contents["text"].(string), "Raft") {
		t.Errorf("contents text: %v", contents["text"])
	}
}

// mustGet GETs the URL and JSON-decodes the response. Fails the test on
// any HTTP or decode error.
func mustGet(t *testing.T, urlStr string) map[string]any {
	t.Helper()
	resp, err := http.Get(urlStr)
	if err != nil {
		t.Fatalf("GET %s: %v", urlStr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", urlStr, err)
	}
	if resp.StatusCode >= 400 {
		t.Fatalf("GET %s: HTTP %d: %s", urlStr, resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s: %v (body=%s)", urlStr, err, body)
	}
	return out
}

func waitForPort(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
