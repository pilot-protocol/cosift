package main

// Iter 353: unit tests for the small pure helpers introduced during the
// path-2 rework. These functions are called from inside the HTTP handler
// and CLI consumer paths; the E2E test exercises them transitively, but
// behavior contracts are clearer when locked down at the function level.

import (
	"math"
	"testing"
	"time"

	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/server"
	"github.com/calinteodor/cosift/internal/store"
)

func TestNormalizeExpandMode(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", ""},
		{"true", "hyde"},
		{"hyde", "hyde"},
		{"paraphrase", "paraphrase"},
		{"HYDE", ""},     // case-sensitive
		{"false", ""},    // not a recognized strategy
		{"unknown", ""},  // typo
		{"hybrid", ""},   // SQLite-side retriever value, not an expansion
	}
	for _, c := range cases {
		got := normalizeExpandMode(c.in)
		if got != c.want {
			t.Errorf("normalizeExpandMode(%q): want %q, got %q", c.in, c.want, got)
		}
	}
}

func TestSourceIDOf(t *testing.T) {
	cases := []struct {
		name string
		i    int
		src  server.AnswerSource
		want int
	}{
		{"explicit id preserved", 0, server.AnswerSource{ID: 7}, 7},
		{"explicit id beats position", 4, server.AnswerSource{ID: 2}, 2},
		{"zero id falls back to position+1", 0, server.AnswerSource{}, 1},
		{"zero id at index 2 → 3", 2, server.AnswerSource{}, 3},
	}
	for _, c := range cases {
		got := sourceIDOf(c.i, c.src)
		if got != c.want {
			t.Errorf("sourceIDOf(%d, %+v): want %d, got %d", c.i, c.src, c.want, got)
		}
	}
}

func TestRrfFuse(t *testing.T) {
	// Iter 354: lock down rrfFuse contract (iter 272). RRF score per URL =
	// sum over lists of 1/(k+rank+1). Construct a fixture with no ties so
	// the ranking is deterministic — sort.Slice doesn't guarantee a stable
	// order for equal-keyed elements.
	listA := []index.Hit{
		{URL: "https://a", Title: "A"},
		{URL: "https://b", Title: "B"},
		{URL: "https://c", Title: "C"},
	}
	listB := []index.Hit{
		{URL: "https://a", Title: "A"},
		{URL: "https://b", Title: "B"},
	}
	got := rrfFuse([][]index.Hit{listA, listB}, 60)

	// a should rank first (rank 0 in both lists).
	if len(got) == 0 || got[0].URL != "https://a" {
		t.Fatalf("rrfFuse: expected https://a as top hit, got %+v", got)
	}
	// b should be second (rank 1 in both lists).
	if len(got) < 2 || got[1].URL != "https://b" {
		t.Errorf("rrfFuse: expected https://b as second, got %+v", got)
	}
	// c should be third (only appears in A at rank 2).
	if len(got) < 3 || got[2].URL != "https://c" {
		t.Errorf("rrfFuse: expected https://c as third, got %+v", got)
	}
	// Score is set to the fused RRF score, not the original Hit.Score.
	if got[0].Score <= 0 {
		t.Errorf("rrfFuse: top hit should have positive fused score, got %v", got[0].Score)
	}
	// Empty input → empty output.
	if out := rrfFuse(nil, 60); len(out) != 0 {
		t.Errorf("rrfFuse(nil): want empty, got %+v", out)
	}
	// fuseK<=0 falls back to default (60). Doesn't crash; same ranking.
	got0 := rrfFuse([][]index.Hit{listA, listB}, 0)
	if len(got0) == 0 || got0[0].URL != "https://a" {
		t.Errorf("rrfFuse(fuseK=0): expected https://a top, got %+v", got0)
	}

	// Iter 378: hybrid-fallback near-miss. /search?retriever=hybrid runs
	// BM25 and dense; if one returns an empty list (e.g. graph loaded but
	// query embeds to a vec with no neighbors), rrfFuse must preserve the
	// other list's ordering — not crash, not drop everything.
	emptyDense := [][]index.Hit{listA, {}}
	gotEmpty := rrfFuse(emptyDense, 60)
	if len(gotEmpty) != 3 || gotEmpty[0].URL != "https://a" || gotEmpty[1].URL != "https://b" || gotEmpty[2].URL != "https://c" {
		t.Errorf("rrfFuse with one empty list should preserve other's ranking, got %+v", gotEmpty)
	}
	// And the dual: empty BM25, dense intact (the URL-mode dense case).
	emptyBM := [][]index.Hit{{}, listB}
	gotEmpty2 := rrfFuse(emptyBM, 60)
	if len(gotEmpty2) != 2 || gotEmpty2[0].URL != "https://a" || gotEmpty2[1].URL != "https://b" {
		t.Errorf("rrfFuse with empty BM25 list should preserve dense ranking, got %+v", gotEmpty2)
	}

	// Single-list input: behaves as a pass-through of the input order. Same
	// invariant the iter-373 hybrid fallback relies on when only one
	// retriever fires.
	single := rrfFuse([][]index.Hit{listA}, 60)
	if len(single) != 3 || single[0].URL != "https://a" || single[2].URL != "https://c" {
		t.Errorf("rrfFuse single list should preserve order, got %+v", single)
	}
}

func TestParseSubQueries(t *testing.T) {
	// Iter 355: lock down the planner-output parser (iter 243). The chat
	// client returns a JSON array, sometimes wrapped in markdown fences,
	// sometimes prefixed by chatty prose. Falls back to [fallback] when
	// the array can't be located.
	cases := []struct {
		name, raw, fallback string
		want                []string
	}{
		{"bare array", `["a","b"]`, "fb", []string{"a", "b"}},
		{"fenced json", "```json\n[\"a\",\"b\"]\n```", "fb", []string{"a", "b"}},
		{"fenced plain", "```\n[\"a\"]\n```", "fb", []string{"a"}},
		{"chatty prefix", `Sure! Here is the plan: ["a","b","c"]`, "fb", []string{"a", "b", "c"}},
		{"trailing whitespace", `["a"]   `, "fb", []string{"a"}},
		{"empty raw → fallback", ``, "fb-empty", []string{"fb-empty"}},
		{"no array → fallback", `not a json array`, "fb-missing", []string{"fb-missing"}},
		{"malformed array → fallback", `[unclosed`, "fb-bad", []string{"fb-bad"}},
	}
	for _, c := range cases {
		got := parseSubQueries(c.raw, c.fallback)
		if len(got) != len(c.want) {
			t.Errorf("%s: want %d items, got %d (%v)", c.name, len(c.want), len(got), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: item %d: want %q, got %q", c.name, i, c.want[i], got[i])
			}
		}
	}
}

func TestPeekWarnings(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"no field", `{"hits":[]}`, nil},
		{"empty array", `{"warnings":[]}`, nil},
		{"single warning", `{"warnings":["expand=foo unknown"]}`, []string{"expand=foo unknown"}},
		{"two warnings", `{"warnings":["a","b"]}`, []string{"a", "b"}},
		{"malformed JSON tolerated", `not json`, nil},
	}
	for _, c := range cases {
		got := peekWarnings([]byte(c.body))
		if len(got) != len(c.want) {
			t.Errorf("%s: want %d warnings, got %d (%v)", c.name, len(c.want), len(got), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: warning %d: want %q, got %q", c.name, i, c.want[i], got[i])
			}
		}
	}
}

// TestRateLimiter — iter 394. Burst is consumed instantly, then the bucket
// refills at rpm/60 per second. Whitelisted IPs bypass entirely.
func TestRateLimiter(t *testing.T) {
	rl := &rateLimiter{
		rpm:       60, // 1 token/sec refill
		burst:     3,
		whitelist: map[string]bool{"127.0.0.1": true},
	}
	// 3 burst tokens → all allowed.
	for i := 0; i < 3; i++ {
		if !rl.allow("10.0.0.1") {
			t.Errorf("burst[%d]: expected allow", i)
		}
	}
	// 4th immediately → denied (bucket empty, no time elapsed).
	if rl.allow("10.0.0.1") {
		t.Errorf("4th request: expected deny")
	}
	// Whitelisted IP always allowed.
	for i := 0; i < 100; i++ {
		if !rl.allow("127.0.0.1") {
			t.Errorf("whitelist[%d]: expected allow", i)
		}
	}
	// Different IPs get independent buckets.
	if !rl.allow("10.0.0.2") {
		t.Errorf("second IP first request: expected allow")
	}
	// Nil limiter is a no-op (limiting disabled).
	var disabled *rateLimiter
	for i := 0; i < 1000; i++ {
		if !disabled.allow("any") {
			t.Errorf("nil rl[%d]: expected allow", i)
		}
	}
}

// TestParseDecayHalfLife — iter 389. Valid positive floats pass; everything
// else short-circuits time-decay.
func TestParseDecayHalfLife(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"", 0, false},
		{"30", 30, true},
		{"0.5", 0.5, true},
		{"0", 0, false},
		{"-1", 0, false},
		{"36500", 36500, true},
		{"36501", 0, false},
		{"nan", 0, false},
		{"hello", 0, false},
	}
	for _, c := range cases {
		got, ok := parseDecayHalfLife(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseDecayHalfLife(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// TestApplyTimeDecay — iter 389. Hits with publish dates get multiplied by
// exp(-ln2 · age / halfLife) and resort. Half-life=30 days: 30d-old hit
// drops to 0.5x; 60d-old to 0.25x. Hits without PublishedAt are unchanged.
func TestApplyTimeDecay(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d30 := now.AddDate(0, 0, -30)
	d60 := now.AddDate(0, 0, -60)
	hits := []searchHit{
		{URL: "https://fresh", Score: 1.0, PublishedAt: &now},
		{URL: "https://30d", Score: 1.0, PublishedAt: &d30},
		{URL: "https://60d", Score: 1.0, PublishedAt: &d60},
		{URL: "https://nodate", Score: 0.9}, // no PublishedAt — kept as-is
	}
	applyTimeDecay(hits, 30, now)

	// Fresh: ~1.0; 30d: ~0.5; 60d: ~0.25; nodate: 0.9 unchanged.
	// After decay+resort: fresh (1.0) > nodate (0.9) > 30d (0.5) > 60d (0.25).
	wantURLs := []string{"https://fresh", "https://nodate", "https://30d", "https://60d"}
	for i, u := range wantURLs {
		if hits[i].URL != u {
			t.Errorf("hits[%d]: want URL %q, got %q (score=%v)", i, u, hits[i].URL, hits[i].Score)
		}
	}
	if math.Abs(float64(hits[2].Score-0.5)) > 0.01 {
		t.Errorf("30d-old hit score: want ~0.5, got %v", hits[2].Score)
	}
	if math.Abs(float64(hits[3].Score-0.25)) > 0.01 {
		t.Errorf("60d-old hit score: want ~0.25, got %v", hits[3].Score)
	}
	if hits[1].Score != 0.9 {
		t.Errorf("nodate hit score: want unchanged 0.9, got %v", hits[1].Score)
	}

	// Empty / non-positive halfLife is a no-op.
	hits2 := []searchHit{{URL: "https://a", Score: 1.0, PublishedAt: &d60}}
	applyTimeDecay(hits2, 0, now)
	if hits2[0].Score != 1.0 {
		t.Errorf("halfLife=0: want no-op, got score %v", hits2[0].Score)
	}
}

// TestParseMMRLambda — iter 384. Valid floats in [0,1] pass; everything else
// short-circuits MMR.
func TestParseMMRLambda(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"", 0, false},
		{"0", 0, true},
		{"0.5", 0.5, true},
		{"1", 1, true},
		{"-0.1", 0, false},
		{"1.5", 0, false},
		{"not a number", 0, false},
	}
	for _, c := range cases {
		got, ok := parseMMRLambda(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseMMRLambda(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// TestMMRSelect — iter 384. With 3 candidates: a (highly relevant), b
// (near-duplicate of a, also highly relevant), c (moderately relevant but
// diverse). At λ=0.4 (diversity-leaning) MMR picks a, then c (diverse from
// a beats the near-dup b), then b. λ=1.0 reverts to pure relevance.
func TestMMRSelect(t *testing.T) {
	q := []float32{1, 0, 0}
	hits := []searchHit{
		{URL: "https://a", Title: "highly relevant"},
		{URL: "https://b", Title: "near-duplicate of a"},
		{URL: "https://c", Title: "diverse from a"},
	}
	// a near q (rel=1.0); b near q AND near a (rel=0.999, sim(b,a)=0.999);
	// c moderate q + orthogonal-ish to a (rel=0.3, sim(c,a)=0.3).
	vecs := [][]float32{
		{1, 0, 0},
		{0.999, 0.045, 0},
		{0.3, 0.954, 0},
	}
	got := mmrSelect(q, hits, vecs, 0.4)
	if len(got) != 3 {
		t.Fatalf("mmrSelect: want 3 hits returned, got %d", len(got))
	}
	if got[0].URL != "https://a" {
		t.Errorf("mmrSelect[0]: want https://a (highest relevance), got %s", got[0].URL)
	}
	if got[1].URL != "https://c" {
		t.Errorf("mmrSelect[1]: want https://c (diverse from a beats near-dup b at λ=0.4), got %s", got[1].URL)
	}
	if got[2].URL != "https://b" {
		t.Errorf("mmrSelect[2]: want https://b (penalized as near-dup of a), got %s", got[2].URL)
	}

	// λ=1 → identity (pure relevance, no diversification). Returns hits unchanged.
	identity := mmrSelect(q, hits, vecs, 1.0)
	if identity[0].URL != "https://a" || identity[1].URL != "https://b" || identity[2].URL != "https://c" {
		t.Errorf("mmrSelect(λ=1): want input order [a b c], got %+v", identity)
	}

	// Empty / single-hit short-circuits.
	if out := mmrSelect(q, nil, nil, 0.4); len(out) != 0 {
		t.Errorf("mmrSelect(empty): want empty, got %+v", out)
	}
	single := mmrSelect(q, hits[:1], vecs[:1], 0.4)
	if len(single) != 1 || single[0].URL != "https://a" {
		t.Errorf("mmrSelect(single): want pass-through, got %+v", single)
	}
}

// TestPebbleInfoJSON locks down the iter-380/381 jq-friendly shape so future
// changes to the offline pebble-info path can't silently drop a field or
// regress the retrievers list. /stats reads the same shape — broken parity
// would silently break dashboards that consume both.
func TestPebbleInfoJSON(t *testing.T) {
	// Empty store: hnswOK=false, no doc length math.
	empty := pebbleInfoJSON("/tmp/empty", store.Stats{Documents: 0}, 0, 0, index.HNSWMeta{}, false)
	if got, _ := empty["hnsw_loaded"].(bool); got {
		t.Errorf("empty store hnsw_loaded: want false, got true")
	}
	retr, _ := empty["retrievers"].([]string)
	if len(retr) != 2 || retr[0] != "bm25" || retr[1] != "bm25-mlt" {
		t.Errorf("empty store retrievers: want [bm25 bm25-mlt], got %v", retr)
	}
	if _, ok := empty["avg_doc_len"]; ok {
		t.Errorf("empty store: avg_doc_len should be absent (zero docs), got %v", empty["avg_doc_len"])
	}
	if _, ok := empty["vector_nodes"]; ok {
		t.Errorf("empty store: vector_nodes should be absent without graph")
	}

	// Loaded store + HNSW: full shape including avg, vector_nodes, dense+hybrid.
	full := pebbleInfoJSON("/tmp/full", store.Stats{Documents: 100}, 50000, 100, index.HNSWMeta{NodeCount: 200, Dim: 768}, true)
	if got, _ := full["hnsw_loaded"].(bool); !got {
		t.Errorf("full store hnsw_loaded: want true")
	}
	if got, _ := full["vector_nodes"].(int); got != 200 {
		t.Errorf("vector_nodes: want 200, got %v", full["vector_nodes"])
	}
	if got, _ := full["vector_dim"].(int); got != 768 {
		t.Errorf("vector_dim: want 768, got %v", full["vector_dim"])
	}
	if got, _ := full["avg_doc_len"].(float64); got != 500.0 {
		t.Errorf("avg_doc_len: want 500.0, got %v", full["avg_doc_len"])
	}
	frArr, _ := full["retrievers"].([]string)
	wantR := map[string]bool{"bm25": true, "bm25-mlt": true, "dense": true, "hybrid": true}
	for _, r := range frArr {
		delete(wantR, r)
	}
	if len(wantR) != 0 {
		t.Errorf("full store retrievers: missing %v, got %v", wantR, frArr)
	}
}
