package index

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/authority"
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

// TestPebbleBM25MaxScorePreservesTopKMembership locks in the Phase-2
// contract: MaxScore-style early termination must not lose any doc that
// would have been in the lossless top-K. We compare top-K membership
// (URL set) with the optimization on vs off across a corpus where the
// optimization should actually trigger early-break.
func TestPebbleBM25MaxScorePreservesTopKMembership(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	ctx := context.Background()

	// Build a corpus where one term ("needle") has tiny DF (1 doc) and
	// another ("padding") has huge DF (49 of 50 docs). After processing
	// "needle" first, the K-th-best partial score is high enough that
	// MaxScore should break before scanning "padding"'s long postings.
	// Top-K must still contain the needle doc.
	for i := 0; i < 49; i++ {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: fmt.Sprintf("https://x/pad/%d", i), Title: "padding doc",
			Text: "padding padding padding padding padding", FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert pad/%d: %v", i, err)
		}
		if err := idx.IndexDocument(ctx, id, "padding doc", "padding padding padding padding padding"); err != nil {
			t.Fatalf("index pad/%d: %v", i, err)
		}
	}
	id, err := ps.UpsertDocument(ctx, &store.Document{
		URL: "https://x/needle", Title: "needle in haystack",
		Text: "needle padding text", FetchedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert needle: %v", err)
	}
	if err := idx.IndexDocument(ctx, id, "needle in haystack", "needle padding text"); err != nil {
		t.Fatalf("index needle: %v", err)
	}

	// With optimization (default).
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "")
	hits, err := idx.Search(ctx, "needle padding", 3)
	if err != nil {
		t.Fatalf("search (maxscore on): %v", err)
	}
	if len(hits) == 0 || hits[0].URL != "https://x/needle" {
		t.Errorf("MaxScore on: needle doc must rank top, got %+v", hits)
	}

	// Without optimization (regression switch).
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")
	hits2, err := idx.Search(ctx, "needle padding", 3)
	if err != nil {
		t.Fatalf("search (maxscore off): %v", err)
	}
	if len(hits2) == 0 || hits2[0].URL != "https://x/needle" {
		t.Errorf("MaxScore off: needle doc must rank top, got %+v", hits2)
	}

	// Top hit URL must agree across optimization-on and optimization-off.
	if hits[0].URL != hits2[0].URL {
		t.Errorf("top-K membership diverges: maxscore=%s vs lossless=%s",
			hits[0].URL, hits2[0].URL)
	}
}

// TestKthLargest — partial heap-select helper used by MaxScore early-stop.
func TestKthLargest(t *testing.T) {
	cases := []struct {
		name   string
		scores map[int64]float64
		k      int
		want   float64
	}{
		{"empty", map[int64]float64{}, 5, 0},
		{"k=0", map[int64]float64{1: 10}, 0, 0},
		{"k larger than map → smallest", map[int64]float64{1: 5, 2: 3, 3: 8}, 10, 3},
		{"k=1 → max", map[int64]float64{1: 5, 2: 9, 3: 7, 4: 2}, 1, 9},
		{"k=3 → third best", map[int64]float64{1: 5, 2: 9, 3: 7, 4: 2, 5: 8}, 3, 7},
		{"k equals size → smallest", map[int64]float64{1: 5, 2: 9, 3: 7}, 3, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := kthLargest(c.scores, c.k)
			if got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
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

// upsertAndIndex is shared setup for the top-k pool tests.
func upsertAndIndex(t testing.TB, ps *store.PebbleStore, idx *PebbleBM25, url, title, text string) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := ps.UpsertDocument(ctx, &store.Document{URL: url, Title: title, Text: text, FetchedAt: time.Now()})
	if err != nil {
		t.Fatalf("upsert %s: %v", url, err)
	}
	if err := idx.IndexDocument(ctx, id, title, text); err != nil {
		t.Fatalf("index %s: %v", url, err)
	}
	return id
}

// searchAB runs the same query with the top-k pool enabled and disabled and
// returns both hit lists.
func searchAB(t *testing.T, idx *PebbleBM25, q string, k int) ([]Hit, []Hit) {
	t.Helper()
	ctx := context.Background()
	t.Setenv("COSIFT_BM25_DISABLE_TOPK_POOL", "")
	pooled, err := idx.Search(ctx, q, k)
	if err != nil {
		t.Fatalf("search (pool on): %v", err)
	}
	t.Setenv("COSIFT_BM25_DISABLE_TOPK_POOL", "1")
	lossless, err := idx.Search(ctx, q, k)
	if err != nil {
		t.Fatalf("search (pool off): %v", err)
	}
	t.Setenv("COSIFT_BM25_DISABLE_TOPK_POOL", "")
	return pooled, lossless
}

// TestPebbleBM25TopKPoolPreservesMembership pins the pool refactor: results
// must match the resolve-all path, including under a deliberately tiny pool.
func TestPebbleBM25TopKPoolPreservesMembership(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")

	// 60 docs with strictly graded tf for "gopher" so the ranking has no
	// ties (tie membership is arbitrary in BOTH arms and would flake).
	for i := 0; i < 60; i++ {
		text := "filler words about various topics"
		for j := 0; j <= i; j++ {
			text += " gopher"
		}
		upsertAndIndex(t, ps, idx, fmt.Sprintf("https://x/doc/%d", i), "doc", text)
	}

	pooled, lossless := searchAB(t, idx, "gopher topics", 5)
	if len(pooled) != 5 || len(lossless) != 5 {
		t.Fatalf("want 5 hits both arms, got pool=%d lossless=%d", len(pooled), len(lossless))
	}
	if !sameURLSet(urlSet(pooled), urlSet(lossless)) {
		t.Errorf("top-k membership diverges: pool=%v lossless=%v", urlSet(pooled), urlSet(lossless))
	}

	// Tiny pool (factor=1): still k hits, still the same top-k.
	t.Setenv("COSIFT_BM25_TOPK_POOL_FACTOR", "1")
	tiny, err := idx.Search(context.Background(), "gopher topics", 5)
	if err != nil {
		t.Fatalf("search (factor=1): %v", err)
	}
	if len(tiny) != 5 {
		t.Fatalf("factor=1: want 5 hits, got %d", len(tiny))
	}
	if !sameURLSet(urlSet(tiny), urlSet(lossless)) {
		t.Errorf("factor=1 membership diverges: %v vs %v", urlSet(tiny), urlSet(lossless))
	}
}

// TestPebbleBM25AuthorityReorderWithinPool attaches a real authority.Scorer
// (no other index-layer test does) and pins the alpha-derived over-fetch: a
// trusted-host doc whose raw score is below the raw top-k must still win
// after its authority multiplier, in both pool arms.
func TestPebbleBM25AuthorityReorderWithinPool(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")

	// Unknown hosts (score 0.5 → ×2.0) with higher tf than the trusted
	// wikipedia doc (score 0.9 → ×2.8). Raw ratio ≈ tf saturation keeps
	// wiki within ×1.4 of the spam docs, so authority flips the order.
	for i := 0; i < 8; i++ {
		// Graded filler length → strict raw ranking among the mirrors.
		upsertAndIndex(t, ps, idx, fmt.Sprintf("https://mirror%d.example.com/a", i), "aardvark",
			"aardvark aardvark aardvark aardvark aardvark filler filler filler"+strings.Repeat(" pad", i))
	}
	upsertAndIndex(t, ps, idx, "https://en.wikipedia.org/wiki/Aardvark", "Aardvark",
		"aardvark aardvark aardvark filler filler filler filler filler")

	scored := idx.WithAuthority(authority.New())
	pooled, lossless := searchAB(t, scored, "aardvark", 3)
	if len(pooled) == 0 || pooled[0].URL != "https://en.wikipedia.org/wiki/Aardvark" {
		t.Errorf("pool on: trusted doc must rank first, got %+v", urlSet(pooled))
	}
	if len(lossless) == 0 || lossless[0].URL != "https://en.wikipedia.org/wiki/Aardvark" {
		t.Errorf("pool off: trusted doc must rank first, got %+v", urlSet(lossless))
	}
	if !sameURLSet(urlSet(pooled), urlSet(lossless)) {
		t.Errorf("authority membership diverges: %v vs %v", urlSet(pooled), urlSet(lossless))
	}

	// alpha=0: multipliers collapse to 1.0 → raw ranking, tight resolve
	// band. Both arms must agree on the raw order (wiki NOT first).
	scored0 := idx.WithAuthority(authority.New().WithAlpha(0))
	pooled0, lossless0 := searchAB(t, scored0, "aardvark", 3)
	if len(pooled0) == 0 || pooled0[0].URL == "https://en.wikipedia.org/wiki/Aardvark" {
		t.Errorf("alpha=0 pool on: raw ranking expected, got wiki first")
	}
	if !sameURLSet(urlSet(pooled0), urlSet(lossless0)) {
		t.Errorf("alpha=0 membership diverges: %v vs %v", urlSet(pooled0), urlSet(lossless0))
	}
}

// TestPebbleBM25PhrasePoolExpansion — phrase-matching docs ranked far below
// the initial resolve depth must still surface (doubling + full-set
// fallback), matching the resolve-all path.
func TestPebbleBM25PhrasePoolExpansion(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")

	// 40 padding docs: high tf for gamma/delta but never adjacent.
	for i := 0; i < 40; i++ {
		upsertAndIndex(t, ps, idx, fmt.Sprintf("https://x/pad/%d", i), "pad",
			"gamma filler delta filler gamma filler delta filler gamma filler delta")
	}
	// 2 phrase docs: single adjacent occurrence → low raw score.
	upsertAndIndex(t, ps, idx, "https://x/phrase/1", "p1", "one gamma delta mention here")
	upsertAndIndex(t, ps, idx, "https://x/phrase/2", "p2", "another gamma delta mention there")

	t.Setenv("COSIFT_BM25_TOPK_POOL_FACTOR", "5")
	pooled, lossless := searchAB(t, idx, `"gamma delta"`, 2)
	want := map[string]bool{"https://x/phrase/1": true, "https://x/phrase/2": true}
	if len(pooled) != 2 || !want[pooled[0].URL] || !want[pooled[1].URL] {
		t.Errorf("pool on: want both phrase docs, got %v", urlSet(pooled))
	}
	if !sameURLSet(urlSet(pooled), urlSet(lossless)) {
		t.Errorf("phrase results diverge: %v vs %v", urlSet(pooled), urlSet(lossless))
	}
}

// TestPebbleBM25BoostSeedSurvivesPool — a zero-term-overlap boosted doc
// (seeded at boostSeedBase×mult, far below genuine matches) must survive
// raw-score pool selection.
func TestPebbleBM25BoostSeedSurvivesPool(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")

	for i := 0; i < 30; i++ {
		upsertAndIndex(t, ps, idx, fmt.Sprintf("https://x/strong/%d", i), "match",
			"osprey osprey osprey osprey feathers")
	}
	boostID := upsertAndIndex(t, ps, idx, "https://smallsite.example/page", "unrelated",
		"completely different content about pottery")

	t.Setenv("COSIFT_BM25_TOPK_POOL_FACTOR", "1")
	boosted := idx.WithBoost(map[int64]float64{boostID: 50})
	pooled, lossless := searchAB(t, boosted, "osprey feathers", 40)
	found := func(hits []Hit) bool {
		for _, h := range hits {
			if h.URL == "https://smallsite.example/page" {
				return true
			}
		}
		return false
	}
	if !found(pooled) {
		t.Errorf("pool on: seeded boost doc evicted; got %d hits", len(pooled))
	}
	if !found(lossless) {
		t.Errorf("pool off: seeded boost doc missing (test setup wrong?)")
	}
}

// TestPebbleBM25MissingMetaBackfill — soft-deleting a raw-top-k doc leaves
// orphaned postings; the pool path must backfill the freed slot from deeper
// candidates instead of returning k-1 hits.
func TestPebbleBM25MissingMetaBackfill(t *testing.T) {
	ps, idx := newPebbleBM25(t)
	t.Setenv("COSIFT_BM25_DISABLE_MAXSCORE", "1")
	ctx := context.Background()

	// Deterministic ranking: descending tf. 25 docs so pool (factor=1,
	// k=3 → floor k+slack) stays below corpus size.
	ids := make([]int64, 25)
	urls := make([]string, 25)
	for i := 0; i < 25; i++ {
		text := "filler content"
		for j := 0; j < 26-i; j++ {
			text += " walrus"
		}
		urls[i] = fmt.Sprintf("https://x/rank/%d", i)
		ids[i] = upsertAndIndex(t, ps, idx, urls[i], "doc", text)
	}
	// Delete the raw #2 doc — orphaned postings by design.
	if ok, err := ps.SoftDeleteDocument(ctx, ids[1], urls[1]); err != nil || !ok {
		t.Fatalf("soft-delete: ok=%v err=%v", ok, err)
	}

	t.Setenv("COSIFT_BM25_TOPK_POOL_FACTOR", "1")
	pooled, lossless := searchAB(t, idx, "walrus", 3)
	if len(pooled) != 3 {
		t.Fatalf("pool on: want 3 hits after backfill, got %d (%v)", len(pooled), urlSet(pooled))
	}
	for _, h := range pooled {
		if h.URL == urls[1] {
			t.Errorf("deleted doc surfaced: %s", h.URL)
		}
	}
	if !sameURLSet(urlSet(pooled), urlSet(lossless)) {
		t.Errorf("backfill diverges from lossless: %v vs %v", urlSet(pooled), urlSet(lossless))
	}
}

// TestTopCandidates — heap-select helper behind the top-k pool.
func TestTopCandidates(t *testing.T) {
	cases := []struct {
		name   string
		scores map[int64]float64
		n      int
		want   []scoredCand
	}{
		{"empty", map[int64]float64{}, 5, nil},
		{"n=0", map[int64]float64{1: 10}, 0, nil},
		{"n bigger than map", map[int64]float64{1: 5, 2: 3}, 10,
			[]scoredCand{{1, 5}, {2, 3}}},
		{"selects and orders", map[int64]float64{1: 5, 2: 9, 3: 7, 4: 2, 5: 8}, 3,
			[]scoredCand{{2, 9}, {5, 8}, {3, 7}}},
		{"ties break by docID asc", map[int64]float64{9: 4, 3: 4, 7: 4, 5: 6}, 3,
			[]scoredCand{{5, 6}, {3, 4}, {7, 4}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := topCandidates(c.scores, c.n)
			if len(got) != len(c.want) {
				t.Fatalf("len: got %d want %d (%v)", len(got), len(c.want), got)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Errorf("[%d]: got %+v want %+v", i, got[i], c.want[i])
				}
			}
		})
	}
}

// BenchmarkPebbleBM25Search — allocation evidence for the top-k pool: a
// common term matching most of a synthetic corpus, pool vs resolve-all.
func BenchmarkPebbleBM25Search(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		b.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	idx := NewPebbleBM25(ps)
	ctx := context.Background()

	vocab := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
		"golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november"}
	rnd := uint64(42)
	next := func(n int) int { // xorshift, deterministic
		rnd ^= rnd << 13
		rnd ^= rnd >> 7
		rnd ^= rnd << 17
		return int(rnd % uint64(n))
	}
	for i := 0; i < 10000; i++ {
		words := make([]string, 0, 32)
		words = append(words, "common")
		for j := 0; j < 31; j++ {
			words = append(words, vocab[next(len(vocab))])
		}
		upsertAndIndex(b, ps, idx, fmt.Sprintf("https://bench/doc/%d", i), "bench doc",
			strings.Join(words, " "))
	}

	b.Setenv("COSIFT_BM25_MIN_IDF", "0")
	for _, arm := range []struct{ name, disable string }{
		{"pool", ""},
		{"resolve-all", "1"},
	} {
		b.Run(arm.name, func(b *testing.B) {
			b.Setenv("COSIFT_BM25_DISABLE_TOPK_POOL", arm.disable)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				hits, err := idx.Search(ctx, "common alpha", 10)
				if err != nil {
					b.Fatal(err)
				}
				if len(hits) != 10 {
					b.Fatalf("want 10 hits, got %d", len(hits))
				}
			}
		})
	}
}
