package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// TestCrawlerAgainstPebbleBackend — iter 212. NewWithBackend builds a
// crawler bound to a PebbleStore + PebbleBM25; the same end-to-end flow
// (seed → run → query → assert text indexed) that TestCrawlEmbedsAndPersistsPassage
// covers for SQLite must succeed against the Pebble path.
//
// No embedder is wired — Pebble vector indexing during crawl is a separate
// follow-up (HNSW write bridge). This iter proves BM25-only crawl
// end-to-end against the new backend.
func TestCrawlerAgainstPebbleBackend(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := NewWithBackend(cfg, ps, index.NewPebbleBM25(ps))

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Doc landed in the Pebble store.
	doc, err := ps.GetDocByURL(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("GetDocByURL: %v", err)
	}
	if doc.Title == "" {
		t.Errorf("doc title should be populated, got empty (full doc: %+v)", doc)
	}

	// BM25 returns the doc on a query for one of its terms.
	pbm := index.NewPebbleBM25(ps)
	hits, err := pbm.Search(context.Background(), "concurrent programming", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("post-crawl PebbleBM25 search returned 0 hits")
	}
	if hits[0].URL != srv.URL {
		t.Errorf("top hit: want %s, got %s", srv.URL, hits[0].URL)
	}
}
