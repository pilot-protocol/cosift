package index

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

func newPebbleBM25(t *testing.T) (*store.PebbleStore, *PebbleBM25) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble")
	p, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, NewPebbleBM25(p)
}

// TestPebbleBM25BasicSearch — same shape as TestBM25IndexAndSearch but
// over PebbleStore. Establishes that the new backend can do a full
// index → query round-trip with sensible ranking.
func TestPebbleBM25BasicSearch(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()

	docs := []struct {
		url, title, text string
	}{
		{"https://example.com/a", "Go programming language", "Go is a statically typed compiled language designed at Google. Concurrency primitives, garbage collection, simple syntax."},
		{"https://example.com/b", "Rust programming language", "Rust is a systems language focused on safety and concurrency. Ownership model prevents data races at compile time."},
		{"https://example.com/c", "Cooking pasta", "Boil water, salt it, add pasta. Drain when al dente. Toss with sauce."},
	}
	for _, d := range docs {
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

	hits, err := idx.Search(ctx, "go concurrency primitives", 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("no hits")
	}
	if hits[0].URL != "https://example.com/a" {
		t.Errorf("top hit URL: got %s, want example.com/a", hits[0].URL)
	}

	hits, _ = idx.Search(ctx, "pasta sauce", 3)
	if len(hits) == 0 || hits[0].URL != "https://example.com/c" {
		t.Errorf("cooking query top hit: %+v", hits)
	}

	hits, _ = idx.Search(ctx, "the a of", 5)
	if len(hits) != 0 {
		t.Errorf("stopword-only query returned hits: %+v", hits)
	}
}

// TestPebbleBM25IDFStopwordFilter locks in the Phase-1 latency fix: query
// terms with IDF below the threshold are pruned before postings scan
// (where the static stopword list misses corpus-specific high-DF words
// like "what" / "how"). Lossless on the meaningful terms; the fallback
// guarantees at least one term keeps firing so single-stopword queries
// still return something.
func TestPebbleBM25IDFStopwordFilter(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()

	// Corpus shape: "filler" appears in 9/10 docs (very low IDF), "needle"
	// appears in 1/10 (very high IDF). Query "filler needle" must still
	// return the doc with "needle" at the top, regardless of pruning.
	docs := []struct {
		url, title, text string
	}{
		{"https://x/0", "doc 0", "filler text content"},
		{"https://x/1", "doc 1", "filler text content"},
		{"https://x/2", "doc 2", "filler text content"},
		{"https://x/3", "doc 3", "filler text content"},
		{"https://x/4", "doc 4", "filler text content"},
		{"https://x/5", "doc 5", "filler text content"},
		{"https://x/6", "doc 6", "filler text content"},
		{"https://x/7", "doc 7", "filler text content"},
		{"https://x/8", "doc 8", "filler text content"},
		{"https://x/9", "needle doc", "needle in haystack, no filler here"},
	}
	for _, d := range docs {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", d.url, err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index %s: %v", d.url, err)
		}
	}

	// Default (filter active): "filler needle" must return needle doc on top.
	hits, err := idx.Search(ctx, "filler needle", 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 || hits[0].URL != "https://x/9" {
		t.Errorf("default filter: top hit should be needle doc, got %+v", hits)
	}

	// Stopword-only fallback: query with only the high-DF term still
	// returns something instead of zero hits.
	hits, _ = idx.Search(ctx, "filler", 3)
	if len(hits) == 0 {
		t.Errorf("filler-only query: fallback should keep one term active, got 0 hits")
	}

	// COSIFT_BM25_MIN_IDF=0 disables pruning entirely (regression switch).
	t.Setenv("COSIFT_BM25_MIN_IDF", "0")
	hits, _ = idx.Search(ctx, "filler needle", 3)
	if len(hits) == 0 || hits[0].URL != "https://x/9" {
		t.Errorf("with filter disabled, needle doc must still rank top: %+v", hits)
	}
}

// TestPebbleBM25MatchesSQLite — the centerpiece behavioral parity check.
// Index the same corpus into both SQLite-backed BM25 and PebbleBM25; query
// each with several queries; assert the top-3 hits agree by URL set. The
// exact score values are NOT required to be identical (Pebble computes
// avgDocLen by scanning 'l', SQLite by SQL AVG — both should agree to
// epsilon but tiny differences are tolerable); we compare URL sets.
func TestPebbleBM25MatchesSQLite(t *testing.T) {
	ctx := context.Background()

	// Set up two stores indexing the same docs.
	sqliteStore, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer sqliteStore.Close()
	sqliteIdx := NewBM25(sqliteStore)

	pebbleStore, pebbleIdx := newPebbleBM25(t)

	docs := []struct {
		url, title, text string
	}{
		{"https://x/raft", "Raft consensus protocol",
			"Raft is a distributed consensus algorithm. Leader election. Log replication. Safety guarantees."},
		{"https://x/paxos", "Paxos algorithm",
			"Paxos is the classical distributed consensus algorithm. Proposers, acceptors, learners."},
		{"https://x/distributed", "Distributed systems overview",
			"Distributed systems cover consensus, replication, partition tolerance. Includes raft and paxos."},
		{"https://x/cooking", "How to boil pasta",
			"Boil water with salt. Drop pasta. Stir occasionally. Drain when al dente."},
		{"https://x/networking", "Networking 101",
			"Networks transport packets between hosts. TCP, UDP, IP. Routing and switching."},
	}
	for _, d := range docs {
		// SQLite side
		idA, err := sqliteStore.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("sqlite upsert: %v", err)
		}
		if err := sqliteIdx.IndexDocument(ctx, idA, d.title, d.text); err != nil {
			t.Fatalf("sqlite index: %v", err)
		}
		// Pebble side
		idB, err := pebbleStore.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("pebble upsert: %v", err)
		}
		if err := pebbleIdx.IndexDocument(ctx, idB, d.title, d.text); err != nil {
			t.Fatalf("pebble index: %v", err)
		}
	}

	queries := []string{
		"raft consensus",
		"paxos algorithm",
		"distributed systems",
		"pasta water",
		`"leader election"`,
	}
	for _, q := range queries {
		sqliteHits, err := sqliteIdx.Search(ctx, q, 3)
		if err != nil {
			t.Fatalf("sqlite search %q: %v", q, err)
		}
		pebbleHits, err := pebbleIdx.Search(ctx, q, 3)
		if err != nil {
			t.Fatalf("pebble search %q: %v", q, err)
		}
		// Compare URL sets — order can vary at score ties.
		sqliteURLs := urlSet(sqliteHits)
		pebbleURLs := urlSet(pebbleHits)
		if !sameURLSet(sqliteURLs, pebbleURLs) {
			t.Errorf("query %q: backends disagree\n  sqlite hits: %v\n  pebble hits: %v",
				q, sqliteURLs, pebbleURLs)
		}
		// Stronger check on the top hit: should be identical for non-tied scores.
		if len(sqliteHits) > 0 && len(pebbleHits) > 0 {
			if sqliteHits[0].URL != pebbleHits[0].URL {
				t.Logf("query %q top hit differs (may be a tie): sqlite=%s pebble=%s",
					q, sqliteHits[0].URL, pebbleHits[0].URL)
			}
		}
	}
}

func urlSet(hits []Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.URL
	}
	sort.Strings(out)
	return out
}

func sameURLSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPebbleBM25PhraseFilter — phrase queries through the Pebble path.
func TestPebbleBM25PhraseFilter(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()

	docs := []struct{ url, title, text string }{
		{"https://x/exact", "Greetings", "I say hello world to everyone."},
		{"https://x/split", "Greetings 2", "I say hello there world to everyone."},
		{"https://x/reverse", "Greetings 3", "I say world hello to everyone."},
	}
	for _, d := range docs {
		id, _ := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
	hits, err := idx.Search(ctx, `"hello world"`, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 {
		t.Errorf("want exactly 1 hit for verbatim phrase; got %d: %v", len(hits), urlSet(hits))
	} else if hits[0].URL != "https://x/exact" {
		t.Errorf("want exact-match doc, got %s", hits[0].URL)
	}
}
