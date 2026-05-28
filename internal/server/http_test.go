package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/store"
)

func newTestSrv(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		httpSrv.Close()
		s.Close()
	})
	return httpSrv, s
}

func seed(t *testing.T, s *store.Store) {
	t.Helper()
	ctx := context.Background()
	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "Go programming", "Go is a statically typed compiled language with concurrency primitives."},
		{"https://x/b", "Rust programming", "Rust is memory safe without a garbage collector."},
	}
	for _, d := range docs {
		id, err := s.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("index: %v", err)
		}
	}
}

func TestHealthz(t *testing.T) {
	srv, _ := newTestSrv(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
}

func TestSearchHappyPath(t *testing.T) {
	srv, s := newTestSrv(t)
	seed(t, s)

	resp, err := http.Get(srv.URL + "/search?q=go+concurrency&k=5")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}

	var sr SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sr.Query != "go concurrency" {
		t.Errorf("query echo: got %q", sr.Query)
	}
	if len(sr.Hits) == 0 {
		t.Fatal("expected hits, got none")
	}
	if sr.Hits[0].URL != "https://x/a" {
		t.Errorf("top hit: got %q want https://x/a", sr.Hits[0].URL)
	}
	if sr.Hits[0].Source != "bm25" {
		t.Errorf("source: got %q want bm25", sr.Hits[0].Source)
	}
}

func TestSearchBadParams(t *testing.T) {
	srv, _ := newTestSrv(t)

	resp, _ := http.Get(srv.URL + "/search")
	if resp.StatusCode != 400 {
		t.Errorf("missing q: got %d want 400", resp.StatusCode)
	}

	resp, _ = http.Get(srv.URL + "/search?q=hi&k=999")
	if resp.StatusCode != 400 {
		t.Errorf("k out of range: got %d want 400", resp.StatusCode)
	}

	resp, _ = http.Get(srv.URL + "/search?q=hi&k=notanint")
	if resp.StatusCode != 400 {
		t.Errorf("non-int k: got %d want 400", resp.StatusCode)
	}
}

// stubEmbedder returns a fixed-dim vector that pulls toward whichever URL's
// embedding was registered for the same query text. Just enough to verify
// that the hybrid path actually fuses with the BM25 hits.
type stubEmbedder struct {
	dim    int
	byText map[string][]float32
}

// Compile-time interface assertion.
var _ embed.Embedder = (*stubEmbedder)(nil)

func (s *stubEmbedder) Model() string { return "stub" }
func (s *stubEmbedder) Dim() int      { return s.dim }
func (s *stubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if v, ok := s.byText[t]; ok {
			cp := make([]float32, len(v))
			copy(cp, v)
			out[i] = cp
			continue
		}
		v := make([]float32, s.dim)
		v[0] = 1 // arbitrary non-zero default
		out[i] = v
	}
	return out, nil
}

func TestSearchDenseAndHybrid(t *testing.T) {
	srv, s := newTestSrv(t)
	seed(t, s)

	// Manually build a small VectorIndex tied to the seeded docs.
	vi := index.NewVectorIndex(4)
	vi.Add("https://x/a", "Go programming", []float32{1, 0, 0, 0})
	vi.Add("https://x/b", "Rust programming", []float32{0, 1, 0, 0})
	emb := &stubEmbedder{dim: 4, byText: map[string][]float32{
		"rust": {0, 1, 0, 0}, // make rust query land on doc B
		"go":   {1, 0, 0, 0},
	}}

	// Cast and wire.
	(srv.Client()) // touch to avoid unused
	// We can't re-wrap the running httptest server. Spin up a fresh one with
	// vector enabled, mirroring newTestSrv but adding WithVector.
	srv.Close()

	s2, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s2.Close() })
	// Seed.
	idx := index.NewBM25(s2)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "Go programming", "Go is a statically typed compiled language with concurrency primitives."},
		{"https://x/b", "Rust programming", "Rust is memory safe without a garbage collector."},
	}
	for _, d := range docs {
		id, _ := s2.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}
	srv2 := New(s2).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv2.Handler())
	defer httpSrv.Close()

	// Dense: the stub embedder maps "rust" → vector that hits doc B at cosine=1.0.
	resp, err := http.Get(httpSrv.URL + "/search?q=rust&retriever=dense&k=2")
	if err != nil {
		t.Fatalf("dense: %v", err)
	}
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 || sr.Hits[0].URL != "https://x/b" {
		t.Errorf("dense top hit: %+v", sr.Hits)
	}
	if sr.Hits[0].Source != "dense" {
		t.Errorf("dense source field: got %q", sr.Hits[0].Source)
	}

	// Hybrid: returns fused; should include both.
	resp, _ = http.Get(httpSrv.URL + "/search?q=rust&retriever=hybrid&k=2")
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 {
		t.Errorf("hybrid: no hits")
	}
	if sr.Hits[0].Source != "hybrid" {
		t.Errorf("hybrid source field: got %q", sr.Hits[0].Source)
	}
}

// per-request ?hybrid_dense_weight= override beats server defaults.
// Same corpus + retrievers as's deterministic-orderings test; verify
// the query param wins over WithDefaults's stale value.
func TestSearchHybridDenseWeightPerRequestOverride(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/A", "doc A", "alpha alpha alpha beta"},
		{"https://x/B", "doc B", "alpha beta beta beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/A", "doc A", []float32{1, 0})
	vi.Add("https://x/B", "doc B", []float32{0, 1})
	emb := &stubEmbedder{dim: 2, byText: map[string][]float32{"alpha": {0, 1}}}

	// Server default = 0 (equal-weight). Per-request ?hybrid_dense_weight=5 should
	// flip B (dense-favored) above A.
	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&retriever=hybrid&k=2&hybrid_dense_weight=5")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) < 2 || sr.Hits[0].URL != "https://x/B" {
		t.Errorf("?hybrid_dense_weight=5 must flip ordering; got %+v", sr.Hits)
	}
}

// /admin/stats surfaces Paraphrases + HyDECache counts from the
// store. Inserts 2 paraphrase rows + 3 HyDE rows directly via SaveParaphrases
// / SaveHyDE, asserts the response carries the right counts.
func TestAdminStatsLLMCacheCounts(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	_ = s.SaveParaphrases(ctx, "stub-model", "query-A", []string{"para1", "para2"})
	_ = s.SaveParaphrases(ctx, "stub-model", "query-B", []string{"para3"})
	_ = s.SaveHyDE(ctx, "stub-model", "query-A", "hypothetical passage A")
	_ = s.SaveHyDE(ctx, "stub-model", "query-B", "hypothetical passage B")
	_ = s.SaveHyDE(ctx, "stub-model", "query-C", "hypothetical passage C")

	srv := New(s).WithAdminToken("tok")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/stats", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var as AdminStatsResponse
	_ = json.NewDecoder(resp.Body).Decode(&as)
	if as.Paraphrases != 2 {
		t.Errorf("Paraphrases: got %d want 2", as.Paraphrases)
	}
	if as.HyDECache != 3 {
		t.Errorf("HyDECache: got %d want 3", as.HyDECache)
	}
}

// MMR wiring sweep — unit tests for mmrFromQuery (the parsing
// helper extracted at N=5 sites) + smoke tests for /answer and /research.
//
// Why smoke tests, not behavioral assertions: MMR's effect through hybrid
// retrieval's RRF fusion is dampened on small corpora — fusion rewards
// multi-list appearance, which masks diversity picks. The MMR algorithm
// itself is tested in vector_test.go. For /answer + /research the wiring
// contract is: ?mmr=true must NOT 500 AND must reach mmrFromQuery, which
// is verified separately as a unit test.

func TestMMRFromQueryParsing(t *testing.T) {
	cases := []struct {
		name string
		path string
		want *mmrParams
	}{
		{"absent → no ctx value", "/?", nil},
		{"empty value → no ctx value", "/?mmr=", nil},
		{"false → no ctx value", "/?mmr=false", nil},
		{"true → enabled, lambda default 0.7", "/?mmr=true", &mmrParams{enabled: true, lambda: 0.7}},
		{"1 → enabled, lambda default 0.7", "/?mmr=1", &mmrParams{enabled: true, lambda: 0.7}},
		{"true + lambda override", "/?mmr=true&mmr_lambda=0.3", &mmrParams{enabled: true, lambda: 0.3}},
		{"true + lambda bad parse → fall back to 0.7", "/?mmr=true&mmr_lambda=abc", &mmrParams{enabled: true, lambda: 0.7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "http://localhost"+tc.path, nil)
			ctx := mmrFromQuery(context.Background(), req)
			got, ok := ctx.Value(mmrParamsKey{}).(mmrParams)
			if tc.want == nil {
				if ok {
					t.Errorf("want no ctx value, got %+v", got)
				}
				return
			}
			if !ok {
				t.Fatalf("want %+v, got no ctx value", *tc.want)
			}
			if got.enabled != tc.want.enabled || got.lambda != tc.want.lambda {
				t.Errorf("got %+v want %+v", got, *tc.want)
			}
		})
	}
}

// Smoke test: /answer?mmr=true and /research?mmr=true must reach the
// handlers without error. The algorithm runs inside runDense; effects are
// algorithm-tested in Iter already.
func TestMMRWiringOnAnswerAndResearchSmoke(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/doc", Title: "Doc", Text: "alpha content",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Doc", "alpha content")

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/doc", "Doc", []float32{1, 0})
	emb := &stubEmbedder{dim: 2, byText: map[string][]float32{"alpha": {1, 0}}}
	chat := &polyChat{
		planReply:  `["alpha facet"]`,
		synthReply: "answer with [1]",
		hydePrefix: "HYPO::",
	}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	cases := []string{
		"/answer?q=alpha&k=2&mmr=true",
		"/answer?q=alpha&k=2&stream=true&mmr=true",
		"/research?q=alpha&strategy=planner&mmr=true",
		"/research?q=alpha&strategy=planner&stream=true&mmr=true",
	}
	for _, path := range cases {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(httpSrv.URL + path)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("status %d: %s", resp.StatusCode, body)
			}
			// Drain body so SSE handlers complete.
			_, _ = io.ReadAll(resp.Body)
		})
	}
}

// /find_similar?mmr=true re-ranks the kNN result for diversity.
// Same corpus shape as's TestSearchMMRPromotesDiversity, plus a
// "seed" doc. Pure kNN returns near-duplicates first; MMR surfaces the
// distinct vector earlier.
func TestFindSimilarWithMMR(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// Seed doc + 3 near-duplicates + 1 orthogonal distinct doc.
	docs := []struct {
		url  string
		text string
		vec  []float32
	}{
		{"https://x/seed", "alpha seed query", []float32{1, 0, 0}},
		{"https://x/dup1", "Dup1", []float32{0.99, 0.1, 0}},
		{"https://x/dup2", "Dup2", []float32{0.98, 0.2, 0}},
		{"https://x/dup3", "Dup3", []float32{0.97, 0.25, 0}},
		{"https://x/distinct", "Distinct", []float32{0, 0, 1}},
	}
	for _, d := range docs {
		_, _ = s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.url, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
	}

	vi := index.NewVectorIndex(3)
	for _, d := range docs {
		vi.Add(d.url, d.url, d.vec)
	}
	// stubEmbedder returns the seed vector for the seed doc's text.
	emb := &stubEmbedder{dim: 3, byText: map[string][]float32{
		"https://x/seed\n\nalpha seed query": {1, 0, 0},
	}}

	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	urls := func(path string) ([]string, string) {
		resp, _ := http.Get(httpSrv.URL + path)
		var fr FindSimilarResponse
		_ = json.NewDecoder(resp.Body).Decode(&fr)
		resp.Body.Close()
		out := make([]string, len(fr.Hits))
		for i, h := range fr.Hits {
			out[i] = h.URL
		}
		src := ""
		if len(fr.Hits) > 0 {
			src = fr.Hits[0].Source
		}
		return out, src
	}

	// Baseline kNN: 3 dups dominate top-3; distinct lands last.
	base, baseSrc := urls("/find_similar?url=https://x/seed&k=4")
	if len(base) != 4 {
		t.Fatalf("baseline /find_similar: expected 4 hits, got %d", len(base))
	}
	if base[3] != "https://x/distinct" {
		t.Errorf("baseline: distinct should be last by pure relevance, got %+v", base)
	}
	if strings.Contains(baseSrc, "+mmr") {
		t.Errorf("baseline source should NOT include +mmr tag, got %q", baseSrc)
	}

	// MMR with lambda=0.5: distinct moves to rank 2 (right after top-relevance).
	mmr, mmrSrc := urls("/find_similar?url=https://x/seed&k=3&mmr=true&mmr_lambda=0.5")
	if len(mmr) != 3 {
		t.Fatalf("MMR k=3: expected 3 hits, got %d", len(mmr))
	}
	// Either dup1 wins rank-1 (top relevance) AND distinct appears in top-3.
	foundDistinct := false
	for _, u := range mmr {
		if u == "https://x/distinct" {
			foundDistinct = true
			break
		}
	}
	if !foundDistinct {
		t.Errorf("MMR with lambda=0.5 should promote distinct into top-3, got %+v", mmr)
	}
	if !strings.Contains(mmrSrc, "+mmr(lambda=0.50)") {
		t.Errorf("MMR source should carry +mmr(lambda=0.50) tag, got %q", mmrSrc)
	}
}

// AnswerSource.Score populated from retrieval hits + ?calibrate=true
// normalizes to within-response fractions. Mirrors's /search behavior.
func TestAnswerSourceScoreAndCalibrate(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, text string }{
		{"https://x/strong", "alpha alpha alpha"},
		{"https://x/weak", "alpha sparse"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.url, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.url, d.text)
	}
	chat := &polyChat{synthReply: "answer [1] [2]"}

	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// (1) Default /answer: Score populated, ScoreCalibrated NOT.
	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&k=2")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var ar AnswerResponse
	_ = json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()
	if len(ar.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(ar.Sources))
	}
	if ar.Sources[0].Score <= 0 {
		t.Errorf("default: Score[0] should be > 0, got %v", ar.Sources[0].Score)
	}
	for _, src := range ar.Sources {
		if src.ScoreCalibrated != 0 {
			t.Errorf("default: ScoreCalibrated should be 0/omitted, got %v on %s", src.ScoreCalibrated, src.URL)
		}
	}

	// (2) ?calibrate=true: ScoreCalibrated populated, top = 1.0, all in (0, 1].
	resp, _ = http.Get(httpSrv.URL + "/answer?q=alpha&k=2&calibrate=true")
	_ = json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()
	if ar.Sources[0].ScoreCalibrated != 1.0 {
		t.Errorf("calibrate: top ScoreCalibrated should be 1.0, got %v", ar.Sources[0].ScoreCalibrated)
	}
	for i, src := range ar.Sources {
		if src.ScoreCalibrated <= 0 || src.ScoreCalibrated > 1 {
			t.Errorf("calibrate hit[%d]: ScoreCalibrated out of (0,1]: %v", i, src.ScoreCalibrated)
		}
	}
}

// calibrateSources unit tests, mirroring's calibrateHits.
func TestCalibrateSourcesBasic(t *testing.T) {
	sources := []AnswerSource{
		{ID: 1, URL: "a", Score: 8.0},
		{ID: 2, URL: "b", Score: 4.0},
		{ID: 3, URL: "c", Score: 2.0},
	}
	calibrateSources(sources)
	if sources[0].ScoreCalibrated != 1.0 {
		t.Errorf("top: got %v want 1.0", sources[0].ScoreCalibrated)
	}
	if sources[1].ScoreCalibrated != 0.5 {
		t.Errorf("mid: got %v want 0.5", sources[1].ScoreCalibrated)
	}
	if sources[2].ScoreCalibrated != 0.25 {
		t.Errorf("bot: got %v want 0.25", sources[2].ScoreCalibrated)
	}
}

func TestCalibrateSourcesEdgeCases(t *testing.T) {
	calibrateSources(nil)              // nil-safe
	calibrateSources([]AnswerSource{}) // empty-safe
	all0 := []AnswerSource{{Score: 0}, {Score: 0}}
	calibrateSources(all0)
	for _, s := range all0 {
		if s.ScoreCalibrated != 0 {
			t.Errorf("zero-max should leave ScoreCalibrated at 0, got %v", s.ScoreCalibrated)
		}
	}
}

// triple composability on /answer — ?mmr=true + ?hyde=true + ?prf=true.
// Pair tests
// don't cover all 3-way interactions. This test locks the realistic operator
// power-user request shape: enable all three IR-quality features at once.
//
// Test design (matches's constraints since MMR participates):
//   - 8 docs needed (>6 for MMR to actually rerank, not early-return)
//   - dups + 1 orthogonal distinct
//   - Some docs contain "alpha", some don't, so PRF has something to mine
//
// Load-bearing assertions:
//  1. HTTP 200 — none of the three features 500s on their own or combined
//  2. trackingEmbedder saw "HYPO::alpha" (HyDE wired); never saw raw "alpha"
//  3. chat.synthCalls > 0 (synth fired with whatever sources resulted)
//
// The "+prf" effect is not behaviorally asserted here — /answer's response
// shape doesn't surface per-hit source tags. The wiring contract for PRF is
// covered by's TestPRFWiringOnAnswerSmoke; this test confirms PRF
// running alongside HyDE+MMR doesn't crash or skip the other features.
func TestAnswerTripleCompose(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// 8 docs: 5 alpha-containing dups (for PRF to mine terms from + BM25
	// to hit) + 2 noise (alpha-containing too so PRF stays meaningful) +
	// 1 distinct without alpha but orthogonal vector (MMR's diversity pick).
	docs := []struct {
		url, text string
		vec       []float32
	}{
		{"https://x/dup1", "alpha primary content", []float32{1, 0, 0}},
		{"https://x/dup2", "alpha secondary content", []float32{0.99, 0.14, 0}},
		{"https://x/dup3", "alpha tertiary content", []float32{0.97, 0.24, 0}},
		{"https://x/dup4", "alpha quaternary content", []float32{0.95, 0.31, 0}},
		{"https://x/dup5", "alpha quinary content", []float32{0.92, 0.39, 0}},
		{"https://x/dup6", "alpha senary content", []float32{0.88, 0.47, 0}},
		{"https://x/dup7", "alpha septenary content", []float32{0.83, 0.56, 0}},
		{"https://x/distinct", "other text no match", []float32{0, 0, 1}},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.url, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.url, d.text)
	}

	vi := index.NewVectorIndex(3)
	for _, d := range docs {
		vi.Add(d.url, d.url, d.vec)
	}
	emb := &trackingEmbedder{dim: 3, byText: map[string][]float32{
		"alpha":       {1, 0, 0},
		"HYPO::alpha": {0, 1, 0}, // distinguishable from raw query embedding
	}}
	chat := &polyParaHydeChat{
		hydePrefix: "HYPO::",
		synthReply: "synthesized answer with [1]",
	}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&k=3&hyde=true&mmr=true&mmr_lambda=0.7&prf=true&prf_terms=3&prf_docs=3")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var ar AnswerResponse
	_ = json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	if ar.Answer == "" {
		t.Errorf("expected non-empty synth answer")
	}
	if len(ar.Sources) == 0 {
		t.Errorf("expected ≥1 source in answer")
	}

	// HyDE wiring assertion: embedder saw HYPO::alpha but never raw "alpha".
	emb.mu.Lock()
	embedded := append([]string(nil), emb.embedded...)
	emb.mu.Unlock()
	for _, t2 := range embedded {
		if t2 == "alpha" {
			t.Errorf("embedder saw raw query %q — HyDE wiring lost under triple compose", t2)
		}
	}
	sawHypo := false
	for _, t2 := range embedded {
		if t2 == "HYPO::alpha" {
			sawHypo = true
			break
		}
	}
	if !sawHypo {
		t.Errorf("embedder never saw HYPO::alpha; embedded=%+v", embedded)
	}

	// Synth fired.
	chat.mu.Lock()
	if chat.hydeCalls < 1 {
		t.Errorf("expected ≥1 HyDE call (main query), got %d", chat.hydeCalls)
	}
	chat.mu.Unlock()
}

// MMR + ?hybrid_dense_weight= composability on /search?retriever=hybrid.
//
// Both features touch the hybrid retrieval path:
//   - MMR (mmrParamsKey) → runDense swaps Search to SearchMMR
//   - hybrid_dense_weight (hybridDenseWeightKey) → runSearch's hybrid branch
//     applies the weight to the RRF fusion
//
// Code inspection suggests independent — MMR modifies the dense LIST that
// goes into RRF; weight modifies HOW RRF fuses. lesson: prove it
// via differential outcome.
//
// Test design constraints surfaced while building this:
//  1. SearchMMR's early-return fires when len(candidates) ≤ requested-k.
//     Hybrid passes k*2 to runDense; with request k=3, runDense passes
//     k=6 to SearchMMR. Need >6 candidates in the vector index to make
//     MMR actually rerank.
//  2. For the differential to be observable in top-k of the fused result,
//     noise vectors must be SPREAD so MMR's pure-diversity picks them
//     out (otherwise MMR clusters dup-likes near dup1).
//  3. The "distinct" doc must lack BM25 hits (no alpha token) so its
//     RRF score depends solely on dense rank — making dense rank shifts
//     observable end-to-end.
//
// 8-doc corpus: 2 alpha-containing dups + 5 spread noise docs (no alpha,
// no orthogonal) + 1 distinct (orthogonal, no alpha). With MMR lambda=0.0
// (pure diversity) + hybrid_dense_weight=2.0, MMR puts distinct at dense
// rank 2 and noise vectors fill ranks 3+; baseline (no MMR) pure-cosine
// dense pushes distinct to rank 8 and noise dominates top of dense.
func TestSearchMMRAndHybridDenseWeightCompose(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// dup1, dup2 contain "alpha" — BM25 returns these.
	// noise1..5 + distinct have no "alpha"; BM25 score 0.
	// noise vectors spread along (cos to dup1) axis so MMR's diversity
	// picks them (least-similar-to-dup1) AFTER distinct.
	docs := []struct {
		url, text string
		vec       []float32
	}{
		{"https://x/dup1", "alpha primary", []float32{1, 0, 0}},
		{"https://x/dup2", "alpha secondary", []float32{0.99, 0.14, 0}},
		{"https://x/noise1", "irrelevant a", []float32{0.97, 0.24, 0}},
		{"https://x/noise2", "irrelevant b", []float32{0.95, 0.31, 0}},
		{"https://x/noise3", "irrelevant c", []float32{0.92, 0.39, 0}},
		{"https://x/noise4", "irrelevant d", []float32{0.88, 0.47, 0}},
		{"https://x/noise5", "irrelevant e", []float32{0.83, 0.56, 0}},
		{"https://x/distinct", "other content", []float32{0, 0, 1}},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.url, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.url, d.text)
	}

	vi := index.NewVectorIndex(3)
	for _, d := range docs {
		vi.Add(d.url, d.url, d.vec)
	}
	// Query "alpha" → embeds to (1,0,0) aligned with dup1.
	emb := &stubEmbedder{dim: 3, byText: map[string][]float32{
		"alpha": {1, 0, 0},
	}}

	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	topURLs := func(path string) []string {
		resp, _ := http.Get(httpSrv.URL + path)
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		return out
	}

	// Baseline: hybrid_dense_weight=2.0 but NO MMR. Dense pure-cosine returns
	// [dup1, dup2, dup3, distinct]; distinct at rank 4 gets cut from top-3.
	base := topURLs("/search?q=alpha&retriever=hybrid&k=3&hybrid_dense_weight=2.0")
	if len(base) != 3 {
		t.Fatalf("baseline: expected 3 hits, got %d", len(base))
	}
	for _, u := range base {
		if u == "https://x/distinct" {
			t.Errorf("baseline (no MMR): distinct should NOT be in top-3 by pure cosine; got %+v", base)
		}
	}

	// Composed: same hybrid_dense_weight + MMR lambda=0.0 (pure diversity).
	// Dense MMR returns [dup1, distinct, dup2, dup3]; distinct now at dense
	// rank 2; RRF with weight 2.0 brings it into top-3.
	composed := topURLs("/search?q=alpha&retriever=hybrid&k=3&hybrid_dense_weight=2.0&mmr=true&mmr_lambda=0.0")
	if len(composed) != 3 {
		t.Fatalf("composed: expected 3 hits, got %d", len(composed))
	}
	foundDistinct := false
	for _, u := range composed {
		if u == "https://x/distinct" {
			foundDistinct = true
			break
		}
	}
	if !foundDistinct {
		t.Errorf("composed (MMR + weight=2.0): distinct should appear in top-3; got %+v", composed)
	}

	// Sanity: dup1 (top relevance + present in both BM25 and dense) is
	// always rank 1 regardless of MMR/weight settings.
	if base[0] != "https://x/dup1" {
		t.Errorf("baseline rank-1: expected dup1, got %s", base[0])
	}
	if composed[0] != "https://x/dup1" {
		t.Errorf("composed rank-1: expected dup1, got %s", composed[0])
	}
}

// HyDE + MMR composability on /search?retriever=dense.
//
// Both features touch runDense:
//   - HyDE swaps the embedding source (raw q → hypothetical passage)
//   - MMR swaps the algorithm (vidx.Search → vidx.SearchMMR)
//
// By inspection they operate on independent state. Per's bug-class
// lesson, "assume broken until proven via trackingEmbedder." This test is
// the proof — locks in the contract so any future refactor that breaks the
// independence fails immediately.
//
// Load-bearing assertions:
//  1. Embedder sees `HYPO::alpha` (HyDE passage), NOT `alpha` (raw query).
//     Catches the case where MMR's branch accidentally bypasses HyDE.
//  2. With lambda=0.0 (pure diversity), distinct doc appears in top-3.
//     Catches the case where HyDE's branch bypasses MMR (uses Search not SearchMMR).
//  3. Source tag carries both `+hyde` and `+mmr(...)` markers.
func TestSearchHyDEAndMMRCompose(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct {
		url, text string
		vec       []float32
	}{
		// HyDE passage embeds to (0, 1, 0) per stubEmbedder. Engineer the
		// vector corpus so:
		// - 3 near-dups cluster around (0, 1, 0) — top-relevance for HyDE
		// - 1 distinct doc at (0, 0, 1) — orthogonal to HyDE passage
		// - The raw query "alpha" embeds to (1, 0, 0) — orthogonal to ALL docs.
		// If HyDE is bypassed, dense returns zero-affinity hits and the
		// test fails on assertion 2.
		{"https://x/dup1", "alpha content 1", []float32{0, 1, 0}},
		{"https://x/dup2", "alpha content 2", []float32{0.1, 0.99, 0}},
		{"https://x/dup3", "alpha content 3", []float32{0.2, 0.98, 0}},
		{"https://x/distinct", "alpha content 4", []float32{0, 0, 1}},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.url, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.url, d.text)
	}

	vi := index.NewVectorIndex(3)
	for _, d := range docs {
		vi.Add(d.url, d.url, d.vec)
	}
	emb := &trackingEmbedder{dim: 3, byText: map[string][]float32{
		"alpha":       {1, 0, 0}, // raw query — orthogonal to everything
		"HYPO::alpha": {0, 1, 0}, // HyDE passage — aligned with dup1/2/3
	}}
	chat := &fakeHyDEChat{reply: "HYPO::alpha"}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&retriever=dense&k=3&hyde=true&mmr=true&mmr_lambda=0.0")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if len(sr.Hits) != 3 {
		t.Fatalf("expected 3 hits, got %d (%+v)", len(sr.Hits), sr.Hits)
	}

	// Assertion 1: embedder saw the HyDE passage, NOT the raw query.
	emb.mu.Lock()
	embedded := append([]string(nil), emb.embedded...)
	emb.mu.Unlock()
	for _, t2 := range embedded {
		if t2 == "alpha" {
			t.Errorf("embedder saw raw query %q — HyDE wiring lost when composed with MMR", t2)
		}
	}
	sawHypo := false
	for _, t2 := range embedded {
		if t2 == "HYPO::alpha" {
			sawHypo = true
			break
		}
	}
	if !sawHypo {
		t.Errorf("embedder never saw HYPO::alpha; embedded=%+v", embedded)
	}

	// Assertion 2: MMR's diversity pick is present. dup1 = top relevance;
	// pure-diversity MMR (lambda=0.0) should put the orthogonal distinct
	// doc into the top-3. If MMR is bypassed when HyDE is on, dense returns
	// [dup1, dup2, dup3] by relevance and distinct is at rank 4 (cut by k=3).
	foundDistinct := false
	for _, h := range sr.Hits {
		if h.URL == "https://x/distinct" {
			foundDistinct = true
			break
		}
	}
	if !foundDistinct {
		t.Errorf("MMR's diversity pick missing — composition with HyDE broke MMR; got %+v",
			func() []string {
				out := make([]string, len(sr.Hits))
				for i, h := range sr.Hits {
					out[i] = h.URL
				}
				return out
			}())
	}

	// Assertion 3: source tag self-documents both features.
	src := sr.Hits[0].Source
	if !strings.Contains(src, "+hyde") {
		t.Errorf("source missing +hyde marker: %q", src)
	}
	if !strings.Contains(src, "+mmr(") {
		t.Errorf("source missing +mmr( marker: %q", src)
	}
}

// HyDE + ?expand=true compose correctly on /answer. The bug
// caught here: pre, all paraphrase retrievals reused HyDE-of-q on
// the dense leg, silently breaking expand's diversification. The fix is
// per-paraphrase HyDE generation, mirroring's /research pattern.
//
// Load-bearing assertion: trackingEmbedder records each text passed to
// Embed(). Expect HYPO::q (main) + HYPO::pq1 + HYPO::pq2 — three distinct
// hypothetical passages. NOT raw paraphrase text. NOT the same HYPO::q
// repeated three times.
func TestAnswerHyDEAndExpandCompose(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/doc", Title: "Doc", Text: "alpha content",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Doc", "alpha content")

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/doc", "Doc", []float32{1, 0})
	emb := &trackingEmbedder{dim: 2, byText: map[string][]float32{}}

	// polyChat handles both paraphraser + HyDE call sites:
	// - "Generate paraphrases" prompt → JSON array
	// - "Write a brief, factual passage" prompt → HYPO::<query>
	// - synth prompt → canned answer
	chat := &polyParaHydeChat{
		paraReply:  `["beta variant", "gamma variant"]`,
		hydePrefix: "HYPO::",
		synthReply: "answered with [1]",
	}

	srv := New(s).WithVector(vi, emb).WithChat(chat).WithParaphraser(chat, 2)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&k=2&hyde=true&expand=true")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Expect 3 HyDE invocations: main "alpha" + 2 paraphrases "beta variant",
	// "gamma variant". The cache may de-dup repeats; with 3 distinct
	// queries we should see exactly 3 hyde calls.
	chat.mu.Lock()
	if chat.hydeCalls != 3 {
		t.Errorf("expected 3 HyDE LLM calls (main + 2 paraphrases), got %d (queries: %+v)", chat.hydeCalls, chat.hydeQueries)
	}
	wantHydeQ := map[string]bool{
		"alpha": false, "beta variant": false, "gamma variant": false,
	}
	for _, q := range chat.hydeQueries {
		if _, ok := wantHydeQ[q]; ok {
			wantHydeQ[q] = true
		}
	}
	for q, saw := range wantHydeQ {
		if !saw {
			t.Errorf("HyDE never called for %q; queries=%+v", q, chat.hydeQueries)
		}
	}
	chat.mu.Unlock()

	// Embedder must have seen each variant's HYPOTHETICAL passage (HYPO::<q>),
	// NOT the raw variant text. Three distinct embedding inputs.
	emb.mu.Lock()
	embedded := append([]string(nil), emb.embedded...)
	emb.mu.Unlock()
	for _, t2 := range embedded {
		switch t2 {
		case "alpha", "beta variant", "gamma variant":
			t.Errorf("embedder saw raw query/paraphrase %q — HyDE+expand wiring broken; should see hypothetical passage", t2)
		}
	}
	wantHypos := map[string]bool{
		"HYPO::alpha": false, "HYPO::beta variant": false, "HYPO::gamma variant": false,
	}
	for _, t2 := range embedded {
		if _, ok := wantHypos[t2]; ok {
			wantHypos[t2] = true
		}
	}
	for hypo, saw := range wantHypos {
		if !saw {
			t.Errorf("embedder never saw hypothetical passage %q; embedded=%+v", hypo, embedded)
		}
	}
}

// polyParaHydeChat extends polyChat by also handling the
// paraphraser system prompt — returns a JSON array on that purpose.
type polyParaHydeChat struct {
	paraReply   string // for "Generate paraphrases" prompts
	hydePrefix  string
	synthReply  string
	mu          sync.Mutex
	hydeCalls   int
	hydeQueries []string
}

func (p *polyParaHydeChat) Model() string { return "poly-para-hyde-chat" }
func (p *polyParaHydeChat) Chat(_ context.Context, msgs []embed.ChatMsg) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var system, user string
	for _, m := range msgs {
		switch m.Role {
		case "system":
			system = m.Content
		case "user":
			user = m.Content
		}
	}
	switch {
	case strings.Contains(system, "paraphrases of a search query"):
		return p.paraReply, nil
	case strings.Contains(system, "Write a brief, factual passage"):
		p.hydeCalls++
		p.hydeQueries = append(p.hydeQueries, user)
		return p.hydePrefix + user, nil
	default:
		return p.synthReply, nil
	}
}

// /answer?hyde=true must actually wire HyDE — shipped
// HyDE for /search and I (loosely) claimed "/answer inherits via runSearch"
// in NOTES. That was WRONG — /answer's handler never parsed the
// param. fixes the bug AND captures the regression class via this
// test: assert the trackingEmbedder sees the hypothetical passage, NOT the
// raw query. Same load-bearing shape as hybrid-PRF +
// research-HyDE tests.
//
// Also covers /answer?stream=true&hyde=true.
func TestAnswerWithHyDE(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/doc", Title: "Doc", Text: "alpha content",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Doc", "alpha content")

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/doc", "Doc", []float32{1, 0})
	emb := &trackingEmbedder{dim: 2, byText: map[string][]float32{}}

	chat := &polyChat{
		hydePrefix: "HYPO::",
		synthReply: "answer with [1]",
	}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// (1) Non-streaming /answer.
	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&hyde=true&k=1")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("non-streaming /answer?hyde=true status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// HyDE LLM was called once for "alpha".
	chat.mu.Lock()
	if chat.hydeCalls != 1 {
		t.Errorf("non-streaming: expected 1 HyDE call, got %d", chat.hydeCalls)
	}
	if len(chat.hydeQueries) != 1 || chat.hydeQueries[0] != "alpha" {
		t.Errorf("non-streaming: HyDE should see raw query 'alpha'; got %+v", chat.hydeQueries)
	}
	chat.mu.Unlock()

	// Embedder saw HYPO::alpha, not "alpha".
	emb.mu.Lock()
	for _, t2 := range emb.embedded {
		if t2 == "alpha" {
			t.Errorf("non-streaming: embedder saw raw query %q — HyDE wiring on /answer is broken", t2)
		}
	}
	sawHypo := false
	for _, t2 := range emb.embedded {
		if t2 == "HYPO::alpha" {
			sawHypo = true
			break
		}
	}
	if !sawHypo {
		t.Errorf("non-streaming: embedder never saw HYPO::alpha; embedded=%+v", emb.embedded)
	}
	emb.mu.Unlock()

	// (2) Streaming /answer — same checks. Reset trackers.
	chat.mu.Lock()
	chat.calls = 0
	chat.hydeCalls = 0
	chat.hydeQueries = chat.hydeQueries[:0]
	chat.mu.Unlock()
	emb.mu.Lock()
	emb.embedded = emb.embedded[:0]
	emb.mu.Unlock()

	resp, _ = http.Get(httpSrv.URL + "/answer?q=alpha&hyde=true&k=1&stream=true")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("streaming /answer?hyde=true status %d: %s", resp.StatusCode, body)
	}
	// Consume the SSE body so the handler completes.
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	chat.mu.Lock()
	// Streaming path may use cached HyDE (same "alpha" query as run 1); allow 0 OR 1.
	if chat.hydeCalls > 1 {
		t.Errorf("streaming: expected ≤1 HyDE call (may hit L1 cache from run 1), got %d", chat.hydeCalls)
	}
	chat.mu.Unlock()

	emb.mu.Lock()
	for _, t2 := range emb.embedded {
		if t2 == "alpha" {
			t.Errorf("streaming: embedder saw raw query %q — HyDE wiring on streaming /answer is broken", t2)
		}
	}
	sawHypoStream := false
	for _, t2 := range emb.embedded {
		if t2 == "HYPO::alpha" {
			sawHypoStream = true
			break
		}
	}
	if !sawHypoStream {
		t.Errorf("streaming: embedder never saw HYPO::alpha; embedded=%+v", emb.embedded)
	}
	emb.mu.Unlock()
}

// /research?hyde=true generates a HyDE passage per variant
// (planner sub-query or paraphrase), and retrieves with each variant's
// hypothetical passage rather than the variant text itself. Load-bearing
// assertion: the embedder records each VARIANT'S passage as the embed input,
// proving per-variant HyDE wiring through gatherResearchPassages.
func TestResearchWithHyDE(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "Doc A", "alpha topic content"},
		{"https://x/b", "Doc B", "beta topic content"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/a", "Doc A", []float32{1, 0})
	vi.Add("https://x/b", "Doc B", []float32{0, 1})
	emb := &trackingEmbedder{dim: 2, byText: map[string][]float32{}}

	chat := &polyChat{
		planReply:  `["alpha facet","beta facet"]`,
		hydePrefix: "HYPO::",
		synthReply: "synthesized answer with [1]",
	}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=topic&hyde=true&strategy=planner")
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// HyDE was called once per variant (2 sub-queries from the planner stub).
	chat.mu.Lock()
	if chat.hydeCalls != 2 {
		t.Errorf("expected 2 HyDE LLM calls (one per variant), got %d", chat.hydeCalls)
	}
	hydeQs := append([]string(nil), chat.hydeQueries...)
	chat.mu.Unlock()
	if len(hydeQs) != 2 || hydeQs[0] != "alpha facet" || hydeQs[1] != "beta facet" {
		t.Errorf("HyDE should be called with each variant verbatim; got %+v", hydeQs)
	}

	// Embedder must have seen each variant's HYPOTHETICAL passage, NOT the
	// variant text itself. If regresses and passes the variant
	// straight to runDense, "alpha facet"/"beta facet" appear in emb.embedded.
	emb.mu.Lock()
	embedded := append([]string(nil), emb.embedded...)
	emb.mu.Unlock()
	wantHypos := map[string]bool{"HYPO::alpha facet": false, "HYPO::beta facet": false}
	for _, t2 := range embedded {
		if t2 == "alpha facet" || t2 == "beta facet" {
			t.Errorf("embedder saw raw variant %q — HyDE wiring is broken; should see hypothetical passage instead", t2)
		}
		if _, ok := wantHypos[t2]; ok {
			wantHypos[t2] = true
		}
	}
	for hypo, saw := range wantHypos {
		if !saw {
			t.Errorf("embedder never saw hypothetical passage %q", hypo)
		}
	}
}

// calibrateHits unit tests.

func TestCalibrateHitsBasic(t *testing.T) {
	hits := []SearchHit{
		{URL: "a", Score: 8.0},
		{URL: "b", Score: 4.0},
		{URL: "c", Score: 2.0},
	}
	calibrateHits(hits)
	if hits[0].ScoreCalibrated != 1.0 {
		t.Errorf("top hit: got %v want 1.0", hits[0].ScoreCalibrated)
	}
	if hits[1].ScoreCalibrated != 0.5 {
		t.Errorf("middle hit: got %v want 0.5", hits[1].ScoreCalibrated)
	}
	if hits[2].ScoreCalibrated != 0.25 {
		t.Errorf("bottom hit: got %v want 0.25", hits[2].ScoreCalibrated)
	}
}

func TestCalibrateHitsEmpty(t *testing.T) {
	// Empty slice must not panic.
	calibrateHits(nil)
	calibrateHits([]SearchHit{})
}

func TestCalibrateHitsZeroMax(t *testing.T) {
	// All scores zero → no normalization (avoid divide-by-zero).
	hits := []SearchHit{{URL: "a", Score: 0}, {URL: "b", Score: 0}}
	calibrateHits(hits)
	for _, h := range hits {
		if h.ScoreCalibrated != 0 {
			t.Errorf("zero-max should leave ScoreCalibrated at 0, got %v for %s", h.ScoreCalibrated, h.URL)
		}
	}
}

func TestCalibrateHitsSingleHit(t *testing.T) {
	hits := []SearchHit{{URL: "a", Score: 7.5}}
	calibrateHits(hits)
	if hits[0].ScoreCalibrated != 1.0 {
		t.Errorf("single hit must be 1.0, got %v", hits[0].ScoreCalibrated)
	}
}

// integration: ?calibrate=true populates score_calibrated; default
// (no query param) leaves it omitted.
func TestSearchCalibrateRoundtrip(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/strong", "Strong match", "alpha alpha alpha alpha alpha"},
		{"https://x/weak", "Weak match", "alpha sparse content here"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Default: no score_calibrated emitted.
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=2")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) < 2 {
		t.Fatalf("expected 2 hits, got %d", len(sr.Hits))
	}
	for _, h := range sr.Hits {
		if h.ScoreCalibrated != 0 {
			t.Errorf("default: score_calibrated should be 0/omitted, got %v on %s", h.ScoreCalibrated, h.URL)
		}
	}

	// ?calibrate=true: top hit must be 1.0; others must be in (0, 1].
	resp, _ = http.Get(httpSrv.URL + "/search?q=alpha&k=2&calibrate=true")
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if sr.Hits[0].ScoreCalibrated != 1.0 {
		t.Errorf("calibrate top hit: got %v want 1.0", sr.Hits[0].ScoreCalibrated)
	}
	for i, h := range sr.Hits {
		if h.ScoreCalibrated <= 0 || h.ScoreCalibrated > 1 {
			t.Errorf("hit[%d] score_calibrated out of (0,1]: got %v", i, h.ScoreCalibrated)
		}
	}
	if sr.Hits[1].ScoreCalibrated >= sr.Hits[0].ScoreCalibrated {
		t.Errorf("rank-2 calibrated score should be < rank-1; got %v vs %v", sr.Hits[1].ScoreCalibrated, sr.Hits[0].ScoreCalibrated)
	}
}

// trackingEmbedder records every text passed to Embed(). Used by
// to assert that the dense sub-call sees ONLY the original query, never the
// PRF-expanded one.
type trackingEmbedder struct {
	dim      int
	byText   map[string][]float32
	mu       sync.Mutex
	embedded []string
}

func (e *trackingEmbedder) Model() string { return "tracking" }
func (e *trackingEmbedder) Dim() int      { return e.dim }
func (e *trackingEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.embedded = append(e.embedded, texts...)
	e.mu.Unlock()
	out := make([][]float32, len(texts))
	for i, t := range texts {
		if v, ok := e.byText[t]; ok {
			cp := make([]float32, len(v))
			copy(cp, v)
			out[i] = cp
			continue
		}
		v := make([]float32, e.dim)
		v[0] = 1
		out[i] = v
	}
	return out, nil
}

// hybrid PRF expands the BM25 sub-query while dense stays on the
// original query. Asserts the load-bearing wiring contract: the embedder
// must be asked to embed ONLY "concurrency" (the original query), NEVER the
// PRF-expanded variant. If ever regresses to wire the expanded
// query into runDense, this test fails immediately.
//
// Also asserts source carries +prf(N).
func TestSearchHybridPRFExpandsBM25Only(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/dup1", "Dup1", "concurrency primitive routines"},
		{"https://x/dup2", "Dup2", "concurrency model routines"},
		{"https://x/dup3", "Dup3", "concurrency code routines"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(3)
	vi.Add("https://x/dup1", "Dup1", []float32{1, 0, 0})
	vi.Add("https://x/dup2", "Dup2", []float32{0.99, 0.1, 0})
	vi.Add("https://x/dup3", "Dup3", []float32{0.98, 0.2, 0})
	emb := &trackingEmbedder{dim: 3, byText: map[string][]float32{
		"concurrency": {1, 0, 0},
	}}

	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=concurrency&retriever=hybrid&k=3&prf=true&prf_terms=3&prf_docs=3")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if len(sr.Hits) == 0 {
		t.Fatalf("hybrid PRF returned no hits")
	}
	if !strings.Contains(sr.Hits[0].Source, "+prf(") {
		t.Errorf("hybrid PRF source should carry +prf(N) tag, got %q", sr.Hits[0].Source)
	}

	// Load-bearing assertion: the embedder must NEVER have been asked to
	// embed the expanded query. The expansion picks "routines" (doc-freq=3);
	// expanded q = "concurrency routines". If regressed and wired
	// the expanded query into runDense, the tracker would have it.
	emb.mu.Lock()
	embedded := append([]string(nil), emb.embedded...)
	emb.mu.Unlock()
	if len(embedded) == 0 {
		t.Fatalf("dense sub-call never fired — hybrid path is broken")
	}
	for _, t2 := range embedded {
		if t2 != "concurrency" {
			t.Errorf("dense embedder was asked to embed %q — only %q (original query) is allowed; PRF expansion must stay BM25-only", t2, "concurrency")
		}
	}
}

// HyDE swaps the dense query embedding for the embedding of an
// LLM-generated hypothetical answer. We can't run a real LLM in tests, so
// the stubChat returns a hand-crafted passage and the stubEmbedder maps
// the passage text to a vector that hits a specific doc.
//
// Corpus has 2 docs:
//   - dup1 vec aligned with the raw-query embedding (keyword "goroutines")
//   - distinct vec aligned with the hypothetical-answer embedding
//
// Baseline ?retriever=dense: dup1 wins (raw keyword embedding).
// With ?hyde=true: distinct wins (the hypothetical-answer embedding is used).
// Verifies the ctx-threaded query swap is end-to-end functional.
func TestSearchHyDEViaHandler(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/keyword-match", "Keyword", "goroutines goroutines goroutines"},
		{"https://x/answer-match", "Answer", "lightweight threads in the runtime"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/keyword-match", "Keyword", []float32{1, 0})
	vi.Add("https://x/answer-match", "Answer", []float32{0, 1})

	// stubEmbedder maps "goroutines" (raw query) → keyword-match vector;
	// "lightweight threads in the runtime" (hypothetical answer) → answer-match vector.
	emb := &stubEmbedder{dim: 2, byText: map[string][]float32{
		"goroutines":                         {1, 0},
		"lightweight threads in the runtime": {0, 1},
	}}

	// fakeHyDEChat returns the canned hypothetical passage.
	chat := &fakeHyDEChat{reply: "lightweight threads in the runtime"}

	srv := New(s).WithVector(vi, emb).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	topURL := func(path string) (string, string) {
		resp, _ := http.Get(httpSrv.URL + path)
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		if len(sr.Hits) == 0 {
			return "", ""
		}
		return sr.Hits[0].URL, sr.Hits[0].Source
	}

	// Baseline dense — raw "goroutines" embedding → keyword-match wins.
	base, baseSrc := topURL("/search?q=goroutines&retriever=dense&k=1")
	if base != "https://x/keyword-match" {
		t.Errorf("baseline dense: expected keyword-match top, got %q", base)
	}
	if strings.Contains(baseSrc, "+hyde") {
		t.Errorf("baseline source should NOT include +hyde tag, got %q", baseSrc)
	}

	// With ?hyde=true — hypothetical-answer embedding → answer-match wins.
	hyde, hydeSrc := topURL("/search?q=goroutines&retriever=dense&k=1&hyde=true")
	if hyde != "https://x/answer-match" {
		t.Errorf("hyde dense: expected answer-match top, got %q", hyde)
	}
	if !strings.Contains(hydeSrc, "+hyde") {
		t.Errorf("hyde source should carry +hyde tag, got %q", hydeSrc)
	}
}

// ?hyde=true returns 400 when no chat client is configured.
func TestSearchHyDERequiresChat(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// No WithChat, no WithVector — minimum-config server.
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=anything&hyde=true")
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("hyde without chat: got %d want 400", resp.StatusCode)
	}
}

// ?hyde=true returns 400 when no embedder is configured (chat
// alone isn't enough — HyDE only helps DENSE/HYBRID retrieval).
func TestSearchHyDERequiresEmbedder(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat := &fakeHyDEChat{reply: "passage"}
	srv := New(s).WithChat(chat) // no WithVector
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=anything&hyde=true")
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("hyde without embedder: got %d want 400", resp.StatusCode)
	}
}

// PRF query expansion. Query "concurrency" alone hits docs that
// mention "concurrency" but misses a related doc that uses "goroutines"
// instead. After running initial BM25, PRF mines the top hits for
// distinctive terms ("goroutines" appears in 2+ of them) and re-searches
// with the expanded query — the goroutines-only doc now surfaces. Source
// tag gains "+prf(N)".
func TestSearchPRFExpandsRecall(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// 3 docs that explicitly mention "concurrency" + "goroutines" — these
	// are the seed hits the original query "concurrency" finds.
	// 1 doc that uses "goroutines" but NOT "concurrency" — this is the
	// recall miss. PRF should surface it after term expansion.
	docs := []struct{ url, title, text string }{
		{"https://x/go-intro", "Go intro", "Go has built-in concurrency via goroutines and channels"},
		{"https://x/go-deep", "Go deeper", "concurrency in Go is achieved through goroutines and select"},
		{"https://x/go-perf", "Go perf", "tuning concurrency-heavy code that spawns many goroutines"},
		{"https://x/goroutines", "All about routines", "goroutines are lightweight threads managed by the runtime"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	urls := func(path string) ([]string, string) {
		resp, _ := http.Get(httpSrv.URL + path)
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		src := ""
		if len(sr.Hits) > 0 {
			src = sr.Hits[0].Source
		}
		return out, src
	}

	// Baseline: "concurrency" misses the goroutines-only doc.
	base, _ := urls("/search?q=concurrency&k=10")
	for _, u := range base {
		if u == "https://x/goroutines" {
			t.Fatalf("baseline already includes goroutines doc — corpus design is broken; got %+v", base)
		}
	}

	// PRF: query expansion should surface "goroutines" from the top hits
	// and bring the goroutines-only doc into the result set.
	prf, prfSrc := urls("/search?q=concurrency&k=10&prf=true&prf_terms=3&prf_docs=3")
	found := false
	for _, u := range prf {
		if u == "https://x/goroutines" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("PRF should surface goroutines doc after expanding query with terms from top hits; got %+v", prf)
	}
	if !strings.Contains(prfSrc, "+prf(") {
		t.Errorf("PRF source should carry +prf(N) tag, got %q", prfSrc)
	}
}

// `/search?mmr=true&retriever=dense` re-ranks top candidates for
// diversity. Same corpus shape as the unit test in vector_test.go: 3 near-dup
// vectors + 1 distinct. Verify (a) baseline dense puts a near-dup at rank 1
// (which it always does — it's the top-relevance pick), then a near-dup at
// rank 2; (b) MMR keeps rank 1 the same but flips rank 2 to the distinct doc.
// Also asserts source tag `+mmr(lambda=0.50)` appears.
func TestSearchMMRViaHandler(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// Seed BM25 too so the docs exist for /search; text bodies all contain
	// "alpha" so any retriever ranks them. The dense path is what we're testing.
	docs := []struct{ url, title, text string }{
		{"https://x/dup1", "Dup1", "alpha content one"},
		{"https://x/dup2", "Dup2", "alpha content two"},
		{"https://x/dup3", "Dup3", "alpha content three"},
		{"https://x/distinct", "Distinct", "alpha content four different"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(3)
	vi.Add("https://x/dup1", "Dup1", []float32{1, 0, 0})
	vi.Add("https://x/dup2", "Dup2", []float32{0.99, 0.1, 0})
	vi.Add("https://x/dup3", "Dup3", []float32{0.98, 0.2, 0})
	vi.Add("https://x/distinct", "Distinct", []float32{0, 0, 1})
	emb := &stubEmbedder{dim: 3, byText: map[string][]float32{
		"alpha": {1, 0, 0},
	}}

	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	urls := func(path string) ([]string, string) {
		resp, _ := http.Get(httpSrv.URL + path)
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		src := ""
		if len(sr.Hits) > 0 {
			src = sr.Hits[0].Source
		}
		return out, src
	}

	// Baseline dense: k=3 (request 3 of 4 docs). 3 dups dominate the top —
	// distinct loses out at this k.
	base, baseSrc := urls("/search?q=alpha&retriever=dense&k=3")
	if len(base) != 3 {
		t.Fatalf("baseline dense: expected 3 hits, got %d", len(base))
	}
	// Pure relevance: distinct's score is 0 vs dups' 0.98-1.0, so distinct
	// is not in the top-3 (it's the only one excluded).
	for _, u := range base {
		if u == "https://x/distinct" {
			t.Errorf("baseline dense k=3: distinct should NOT be in top-3 by pure relevance, got %+v", base)
		}
	}
	if strings.Contains(baseSrc, "+mmr") {
		t.Errorf("baseline source should NOT include +mmr tag, got %q", baseSrc)
	}

	// MMR with lambda=0.5: distinct should appear in top-3 (diversity bonus
	// promotes it over near-duplicate dup2 or dup3).
	mmr, mmrSrc := urls("/search?q=alpha&retriever=dense&k=3&mmr=true&mmr_lambda=0.5")
	if len(mmr) != 3 {
		t.Fatalf("MMR: expected 3 hits, got %d", len(mmr))
	}
	if mmr[0] != "https://x/dup1" {
		t.Errorf("MMR rank 1 should be top-relevance dup1, got %s", mmr[0])
	}
	foundDistinct := false
	for _, u := range mmr {
		if u == "https://x/distinct" {
			foundDistinct = true
			break
		}
	}
	if !foundDistinct {
		t.Errorf("MMR with lambda=0.5 should promote distinct into top-3, got %+v", mmr)
	}
	if !strings.Contains(mmrSrc, "+mmr(lambda=0.50)") {
		t.Errorf("MMR source should carry +mmr(lambda=0.50) tag, got %q", mmrSrc)
	}
}

// Exercises 4 hit filters + sort in a single
// request and verifies the order of operations is stable:
//  1. retrieval (BM25 here)
//  2. date filter
//  3. domain filter
//  4. sort
//  5. enrichment + GetDocMetas fetch
//  6. author filter
//
// Corpus engineered so each filter drops at least one doc, leaving exactly 2
// surviving hits whose order is sort-determined. If any filter is silently
// short-circuited or applied in the wrong order, this test fails.
func TestSearchComposability(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	ctx := context.Background()

	idx := index.NewBM25(s)
	t2024 := func(month, day int) time.Time {
		return time.Date(2024, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	}
	docs := []store.Document{
		// Survives every filter; mid-window (August). Expected rank 2 in date_desc.
		{URL: "https://example.com/aug", Domain: "example.com", Title: "Aug piece", Text: "alpha analysis", Author: "Jane Doe", PublishedAt: t2024(8, 15), Source: "test", FetchedAt: time.Now()},
		// Survives every filter; latest in window (September). Expected rank 1 in date_desc.
		{URL: "https://example.com/sep", Domain: "example.com", Title: "Sep piece", Text: "alpha report", Author: "Jane Doe", PublishedAt: t2024(9, 10), Source: "test", FetchedAt: time.Now()},
		// Drops on date filter (March is before since=2024-04-01).
		{URL: "https://example.com/mar", Domain: "example.com", Title: "Mar piece", Text: "alpha story", Author: "Jane Doe", PublishedAt: t2024(3, 15), Source: "test", FetchedAt: time.Now()},
		// Drops on author filter (John Smith).
		{URL: "https://example.com/jun", Domain: "example.com", Title: "Jun piece", Text: "alpha analysis", Author: "John Smith", PublishedAt: t2024(6, 20), Source: "test", FetchedAt: time.Now()},
		// Drops on domain filter (blog.example.com matches example.com via suffix BUT we'll use exclude_domains to drop it).
		{URL: "https://blog.example.com/jul", Domain: "blog.example.com", Title: "Blog Jul", Text: "alpha briefing", Author: "Jane Doe", PublishedAt: t2024(7, 15), Source: "test", FetchedAt: time.Now()},
		// Drops on undated (date filter active → skip docs with zero PublishedAt).
		{URL: "https://example.com/anon", Domain: "example.com", Title: "Anon piece", Text: "alpha note", Author: "Jane Doe", Source: "test", FetchedAt: time.Now()},
		// Drops on date filter (December is after until=2024-10-01).
		{URL: "https://example.com/dec", Domain: "example.com", Title: "Dec piece", Text: "alpha brief", Author: "Jane Doe", PublishedAt: t2024(12, 1), Source: "test", FetchedAt: time.Now()},
	}
	for i := range docs {
		id, _ := s.UpsertDocument(ctx, &docs[i])
		_ = idx.IndexDocument(ctx, id, docs[i].Title, docs[i].Text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// All four filters + date_desc sort, applied together.
	q := "/search?q=alpha&k=10" +
		"&include_domains=example.com&exclude_domains=blog.example.com" +
		"&since=2024-04-01&until=2024-10-01" +
		"&author=jane" +
		"&sort=date_desc"
	resp, _ := http.Get(httpSrv.URL + q)
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if len(sr.Hits) != 2 {
		t.Fatalf("expected exactly 2 surviving hits, got %d: %+v", len(sr.Hits), sr.Hits)
	}
	// Sort must be date_desc → Sep first, Aug second.
	want := []string{"https://example.com/sep", "https://example.com/aug"}
	for i, w := range want {
		if sr.Hits[i].URL != w {
			t.Errorf("hit[%d]: got %q want %q", i, sr.Hits[i].URL, w)
		}
	}
	// The retained hits must carry the enrichment fields (locks the
	// "enrichment fires AFTER filtering" contract — if it ran before,
	// metas would be sized for the pre-filter set, wasted SQL).
	if sr.Hits[0].Author == "" || sr.Hits[0].Domain == "" || sr.Hits[0].PublishedAt == nil {
		t.Errorf("hit[0] missing enrichment: author=%q domain=%q published=%v",
			sr.Hits[0].Author, sr.Hits[0].Domain, sr.Hits[0].PublishedAt)
	}
	// Source tag should record every filter that fired. Order is the
	// pipeline order; if it's wrong, this fails too.
	src := sr.Hits[0].Source
	for _, tag := range []string{"date-filter", "domain-filter", "sort=date_desc", "author-filter"} {
		if !strings.Contains(src, tag) {
			t.Errorf("source missing %q tag: %q", tag, src)
		}
	}
}

// SearchHit.favicon surfaces documents.favicon end-to-end.
func TestSearchFaviconField(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/with-icon", Title: "Iconed", Text: "alpha story",
		Favicon: "https://x/favicon.ico", Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Iconed", "alpha story")

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=1")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 {
		t.Fatal("expected 1 hit")
	}
	if sr.Hits[0].Favicon != "https://x/favicon.ico" {
		t.Errorf("favicon: got %q want %q", sr.Hits[0].Favicon, "https://x/favicon.ico")
	}
}

// SearchHit.image surfaces documents.image. Exercises the
// store → DocMeta → SearchHit JSON path (parser → Document is locked in
// by the parse_meta tests).
func TestSearchImageField(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/withimg", Title: "Imaged", Text: "alpha story",
		Image: "https://cdn.x/cover.jpg", Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Imaged", "alpha story")

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=1")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 {
		t.Fatal("expected 1 hit")
	}
	if sr.Hits[0].Image != "https://cdn.x/cover.jpg" {
		t.Errorf("image: got %q want %q", sr.Hits[0].Image, "https://cdn.x/cover.jpg")
	}
}

// /answer synth prompt sees Author when present, omits the line
// when Author is empty (so the LLM doesn't get a blank "Author: " that invites
// hallucinated attribution). Test exercises BOTH branches with one corpus.
func TestAnswerSynthPromptIncludesAuthor(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text, author string }{
		{"https://x/bylined", "Bylined article", "alpha programming insight", "Jane Doe"},
		{"https://x/anon", "Anon piece", "alpha programming overview", ""},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Author: d.author,
			Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &stubChat{}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&k=2")
	resp.Body.Close()

	user := chat.lastUser
	if !strings.Contains(user, "Author: Jane Doe") {
		t.Errorf("synth prompt should include Author line for bylined doc; got:\n%s", user)
	}
	// The anon doc's entry must NOT have an empty "Author: " line — that would
	// invite the LLM to hallucinate attribution from a blank.
	if strings.Contains(user, "Author: \n") || strings.Contains(user, "Author:\n") {
		t.Errorf("synth prompt should omit empty Author line for unauthored doc; got:\n%s", user)
	}
}

// ?author= and ?exclude_author= filters. Three-doc corpus with
// distinct authors; verify include narrows, exclude widens, both compose,
// and an undated/unauthored doc is dropped only when include is non-empty.
func TestSearchAuthorFilter(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text, author string }{
		{"https://x/jane", "By Jane", "alpha story", "Jane Doe"},
		{"https://x/john", "By John", "alpha analysis", "John Smith"},
		{"https://x/anon", "Anon piece", "alpha report", ""},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Author: d.author,
			Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	urls := func(path string) []string {
		resp, _ := http.Get(httpSrv.URL + path)
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		resp.Body.Close()
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		return out
	}

	// (1) No filter: all 3 docs.
	all := urls("/search?q=alpha&k=5")
	if len(all) != 3 {
		t.Fatalf("baseline: expected 3 hits, got %d (%+v)", len(all), all)
	}

	// (2) ?author=jane: only the Jane doc.
	got := urls("/search?q=alpha&k=5&author=jane")
	if len(got) != 1 || got[0] != "https://x/jane" {
		t.Errorf("author=jane: expected [https://x/jane], got %+v", got)
	}

	// (3) ?exclude_author=jane: Jane dropped, others kept (including anon, since
	//     the anon doc has no author to match against the exclude pattern).
	got = urls("/search?q=alpha&k=5&exclude_author=jane")
	for _, u := range got {
		if u == "https://x/jane" {
			t.Errorf("exclude_author=jane should drop Jane, got %+v", got)
		}
	}
	if len(got) != 2 {
		t.Errorf("exclude_author=jane: expected 2 hits (john + anon), got %d %+v", len(got), got)
	}

	// (4) Multi-pattern include: "jane,john" keeps both bylined, drops anon.
	got = urls("/search?q=alpha&k=5&author=jane,john")
	if len(got) != 2 {
		t.Errorf("author=jane,john: expected 2 hits, got %d %+v", len(got), got)
	}
	for _, u := range got {
		if u == "https://x/anon" {
			t.Errorf("anon doc should be dropped when include is non-empty, got %+v", got)
		}
	}

	// (5) Case insensitive: ?author=JANE matches "Jane Doe".
	got = urls("/search?q=alpha&k=5&author=JANE")
	if len(got) != 1 || got[0] != "https://x/jane" {
		t.Errorf("case-insensitive author=JANE: expected [https://x/jane], got %+v", got)
	}
}

// SearchHit.author populated from documents.author column,
// driven by JSON-LD author.name extraction in the crawler. This test
// exercises the store layer directly (we don't run a crawl); the crawler→
// parser→store path is covered by the existing JSON-LD parse tests.
func TestSearchAuthorField(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/byline", Title: "Bylined article", Text: "alpha content",
		Author: "Jane Doe", Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Bylined article", "alpha content")

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=1")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 {
		t.Fatal("expected 1 hit")
	}
	if sr.Hits[0].Author != "Jane Doe" {
		t.Errorf("author: got %q want %q", sr.Hits[0].Author, "Jane Doe")
	}
}

// ?include_text=true populates SearchHit.Text inline.
// ?max_text=N caps each hit's text. Default cap is 5000 chars.
func TestSearchIncludeText(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	longBody := strings.Repeat("alpha keyword content ", 500) // ~10000 chars
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/long", Title: "Long doc", Text: longBody,
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Long doc", longBody)

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// (1) Default: no Text field.
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=1")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) == 0 {
		t.Fatal("expected 1 hit")
	}
	if sr.Hits[0].Text != "" {
		t.Errorf("default: Text should be empty, got %d chars", len(sr.Hits[0].Text))
	}

	// (2) include_text=true: Text populated, capped at default 5000.
	resp, _ = http.Get(httpSrv.URL + "/search?q=alpha&k=1&include_text=true")
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits[0].Text) == 0 {
		t.Errorf("include_text=true should populate Text")
	}
	if len(sr.Hits[0].Text) > 5000 {
		t.Errorf("default max_text=5000 should cap text, got %d chars", len(sr.Hits[0].Text))
	}
	if len(sr.Hits[0].Text) != 5000 {
		t.Errorf("on a >5000-char doc, Text should be exactly 5000 chars (substr cap), got %d", len(sr.Hits[0].Text))
	}

	// (3) Custom max_text=100.
	resp, _ = http.Get(httpSrv.URL + "/search?q=alpha&k=1&include_text=true&max_text=100")
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits[0].Text) != 100 {
		t.Errorf("max_text=100 should cap text at 100 chars, got %d", len(sr.Hits[0].Text))
	}
}

// per-request ?expand_main_weight= override beats server defaults.
// Mirrors's ?hybrid_dense_weight= test for the expand path.
func TestSearchExpandMainWeightPerRequestOverride(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/A", "doc A", "alpha alpha alpha beta"},
		{"https://x/B", "doc B", "alpha beta beta beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &paraphraseChat{reply: `["beta"]`}
	// Server default = 0 (equal-weight). ?expand_main_weight=5 should win.
	srv := New(s).WithParaphraser(chat, 1)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&expand=true&k=2&expand_main_weight=5")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) < 2 || sr.Hits[0].URL != "https://x/A" {
		t.Errorf("?expand_main_weight=5 must flip ordering; got %+v", sr.Hits)
	}
}

// /admin/config surfaces the Iter weight knobs so operators
// can verify their config without restarting the server.
func TestAdminConfigSurfacesWeightKnobs(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s).WithAdminToken("k").WithDefaults(Defaults{
		ExpandMainWeight:  2.5,
		HybridDenseWeight: 3.0,
	})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/config", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	bs := string(body)
	// JSON tags converted Defaults fields to snake_case for /admin/config.
	if !strings.Contains(bs, `"expand_main_weight":2.5`) {
		t.Errorf("expand_main_weight not surfaced in /admin/config response: %s", bs)
	}
	if !strings.Contains(bs, `"hybrid_dense_weight":3`) {
		t.Errorf("hybrid_dense_weight not surfaced in /admin/config response: %s", bs)
	}
}

// HybridDenseWeight knob shifts hybrid retrieval toward dense.
// Corpus engineered so BM25 ranks A first (high "alpha" tf) and dense ranks B
// first (stub embedder maps query "alpha" → unit vector aligned with B).
// With HybridDenseWeight=5.0, the dense list dominates → B wins.
func TestSearchHybridDenseWeightShiftsOrdering(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/A", "doc A", "alpha alpha alpha beta"},
		{"https://x/B", "doc B", "alpha beta beta beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(2)
	vi.Add("https://x/A", "doc A", []float32{1, 0})
	vi.Add("https://x/B", "doc B", []float32{0, 1})
	emb := &stubEmbedder{dim: 2, byText: map[string][]float32{
		"alpha": {0, 1},
	}}

	srv := New(s).WithVector(vi, emb).WithDefaults(Defaults{HybridDenseWeight: 5.0})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&retriever=hybrid&k=2")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if len(sr.Hits) < 2 {
		t.Fatalf("expected 2 hits, got %d (%+v)", len(sr.Hits), sr.Hits)
	}
	if sr.Hits[0].URL != "https://x/B" {
		t.Errorf("with HybridDenseWeight=5.0, B (dense-favored) should outrank A (BM25-favored); got %+v", sr.Hits)
	}
}

// stubChat returns the captured user prompt back as the answer — lets the test
// assert the prompt format without doing real LLM inference.
type stubChat struct {
	lastSystem string
	lastUser   string
}

var _ embed.ChatClient = (*stubChat)(nil)

func (s *stubChat) Model() string { return "stub-chat" }
func (s *stubChat) Chat(_ context.Context, msgs []embed.ChatMsg) (string, error) {
	for _, m := range msgs {
		if m.Role == "system" {
			s.lastSystem = m.Content
		}
		if m.Role == "user" {
			s.lastUser = m.Content
		}
	}
	return "Go is statically typed [1]. Rust is memory safe [2].", nil
}

func TestAnswerHappyPath(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "Go programming", "Go is a statically typed compiled language with concurrency primitives."},
		{"https://x/b", "Rust programming", "Rust is memory safe without a garbage collector."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &stubChat{}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/answer?q=go+programming&k=2")
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d", resp.StatusCode)
	}
	var ar AnswerResponse
	_ = json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	if ar.Calibrated {
		t.Errorf("calibrated must be false in v0")
	}
	if len(ar.Sources) == 0 {
		t.Fatal("no sources returned")
	}
	if ar.Sources[0].ID != 1 {
		t.Errorf("first source id: got %d want 1", ar.Sources[0].ID)
	}
	// Sources block should contain the source ids the model is meant to cite.
	if !strings.Contains(chat.lastUser, "[1]") || !strings.Contains(chat.lastUser, "Title: ") {
		t.Errorf("user prompt missing sources block: %s", chat.lastUser)
	}
	if !strings.Contains(chat.lastSystem, "Cite sources") {
		t.Errorf("system prompt missing citation instruction: %s", chat.lastSystem)
	}
}

func TestAnswerWithExpand(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/1", "Doc one", "alpha primary keyword unique"},
		{"https://x/2", "Doc two", "second beta different keyword"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	// Two chat clients: paraphrase chat (for ?expand=true) and answer chat.
	pchat := &paraphraseChat{reply: `["second beta"]`}
	achat := &scriptedChat{replies: []string{"Combined answer using [1] and [2]."}}

	srv := New(s).WithChat(achat).WithParaphraser(pchat, 1)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// expand=true: /answer should fan out paraphrases, fuse, and pass BOTH docs to the LLM.
	resp, _ := http.Get(httpSrv.URL + "/answer?q=alpha&expand=true&k=5")
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var ar AnswerResponse
	_ = json.NewDecoder(resp.Body).Decode(&ar)
	resp.Body.Close()

	if pchat.calls != 1 {
		t.Errorf("paraphrase chat: got %d calls want 1", pchat.calls)
	}
	if achat.calls != 1 {
		t.Errorf("answer chat: got %d calls want 1", achat.calls)
	}
	// Both x/1 (main) and x/2 (paraphrase) should appear in sources.
	urls := map[string]bool{}
	for _, src := range ar.Sources {
		urls[src.URL] = true
	}
	if !urls["https://x/1"] || !urls["https://x/2"] {
		t.Errorf("expand should bring both docs into sources, got %+v", ar.Sources)
	}
}

func TestAnswerExpandUnconfigured(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	achat := &scriptedChat{replies: []string{"ok"}}
	srv := New(s).WithChat(achat) // no paraphraser
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()
	resp, _ := http.Get(httpSrv.URL + "/answer?q=hi&expand=true")
	if resp.StatusCode != 400 {
		t.Errorf("expand without paraphraser: got %d want 400", resp.StatusCode)
	}
}

func TestAnswerWithoutChat(t *testing.T) {
	srv, _ := newTestSrv(t)
	resp, _ := http.Get(srv.URL + "/answer?q=hi")
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 without WithChat: got %d", resp.StatusCode)
	}
}

func TestFindSimilar(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "Go programming", "Go statically typed concurrency."},
		{"https://x/b", "Rust programming", "Rust memory safe concurrency."},
		{"https://x/c", "Cooking pasta", "boil water al dente."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	vi := index.NewVectorIndex(4)
	vi.Add("https://x/a", "Go programming", []float32{1, 0, 0, 0})
	vi.Add("https://x/b", "Rust programming", []float32{0.95, 0.05, 0, 0}) // close to A
	vi.Add("https://x/c", "Cooking pasta", []float32{0, 1, 0, 0})          // far

	emb := &stubEmbedder{dim: 4, byText: map[string][]float32{
		// /find_similar embeds (title + "\n\n" + text). Make the seed doc's text
		// hash to A's vec so we can predict the answer.
		"Go programming\n\nGo statically typed concurrency.": {1, 0, 0, 0},
	}}
	srv := New(s).WithVector(vi, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/find_similar?url=https://x/a&k=2")
	if err != nil {
		t.Fatalf("find_similar: %v", err)
	}
	var sr FindSimilarResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if sr.URL != "https://x/a" {
		t.Errorf("seed echo: %q", sr.URL)
	}
	if len(sr.Hits) == 0 {
		t.Fatal("no similar hits")
	}
	for _, h := range sr.Hits {
		if h.URL == "https://x/a" {
			t.Errorf("seed URL must be filtered: %+v", sr.Hits)
		}
	}
	if sr.Hits[0].URL != "https://x/b" {
		t.Errorf("expected B (close to A) as top similar, got %q (hits: %+v)", sr.Hits[0].URL, sr.Hits)
	}
}

// stubReranker reorders candidates by their ID. Lets the test pre-compute the
// expected output and verify the rerank path actually shuffles the results.
type stubReranker struct {
	order []string
	calls int
}

var _ rerank.Reranker = (*stubReranker)(nil)

func (s *stubReranker) Name() string { return "stub" }
func (s *stubReranker) Rerank(_ context.Context, _ string, _ []rerank.Candidate) ([]string, error) {
	s.calls++
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out, nil
}

func TestSearchWithRerankerReordersResults(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/a", "A", "alpha programming"},
		{"https://x/b", "B", "beta programming"},
		{"https://x/c", "C", "gamma programming"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	rr := &stubReranker{order: []string{"https://x/c", "https://x/a", "https://x/b"}}
	srv := New(s).WithReranker(rr, 20)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Without ?rerank=false the reranker is the default (configured).
	resp, _ := http.Get(httpSrv.URL + "/search?q=programming&k=3")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if rr.calls != 1 {
		t.Errorf("reranker calls: got %d want 1", rr.calls)
	}
	if len(sr.Hits) != 3 {
		t.Fatalf("hits: got %d want 3", len(sr.Hits))
	}
	if sr.Hits[0].URL != "https://x/c" || sr.Hits[1].URL != "https://x/a" || sr.Hits[2].URL != "https://x/b" {
		t.Errorf("rerank order not applied: %+v", sr.Hits)
	}
	if !strings.Contains(sr.Hits[0].Source, "rerank") {
		t.Errorf("source field should mark rerank: got %q", sr.Hits[0].Source)
	}
}

func TestSearchRerankOptOut(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/a", Title: "A", Text: "alpha", Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "A", "alpha")

	rr := &stubReranker{order: []string{"https://x/a"}}
	srv := New(s).WithReranker(rr, 20)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&rerank=false")
	resp.Body.Close()
	if rr.calls != 0 {
		t.Errorf("?rerank=false should skip reranker: got %d calls", rr.calls)
	}
}

func TestAdminRecrawlAuthMatrix(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	body := `{"urls": ["https://example.com/a", "https://example.com/b"]}`

	// 1) No admin_token configured → always 403.
	srv1 := New(s)
	h1 := httptest.NewServer(srv1.Handler())
	defer h1.Close()
	resp, _ := http.Post(h1.URL+"/admin/recrawl", "application/json", strings.NewReader(body))
	if resp.StatusCode != 403 {
		t.Errorf("no token configured: got %d want 403", resp.StatusCode)
	}

	// 2) Token configured, no Authorization header → 401.
	srv2 := New(s).WithAdminToken("secret-abc")
	h2 := httptest.NewServer(srv2.Handler())
	defer h2.Close()
	resp, _ = http.Post(h2.URL+"/admin/recrawl", "application/json", strings.NewReader(body))
	if resp.StatusCode != 401 {
		t.Errorf("no header: got %d want 401", resp.StatusCode)
	}

	// 3) Token configured, wrong bearer → 401.
	req, _ := http.NewRequest("POST", h2.URL+"/admin/recrawl", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer wrong")
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Errorf("wrong token: got %d want 401", resp.StatusCode)
	}

	// 4) Correct bearer → 200 + URLs queued.
	req, _ = http.NewRequest("POST", h2.URL+"/admin/recrawl", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-abc")
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 200 {
		t.Errorf("good auth: got %d want 200", resp.StatusCode)
	}
	var rr AdminRecrawlResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()
	if len(rr.Queued) != 2 {
		t.Errorf("queued: got %d want 2 (resp: %+v)", len(rr.Queued), rr)
	}
	stats, _ := s.GetFrontierStats(context.Background())
	if stats.Queued != 2 {
		t.Errorf("frontier should have 2 queued URLs after admin call, got %d", stats.Queued)
	}
}

func TestAdminStatsShape(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// Seed two docs so the response is non-empty.
	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://example.com/a", "A", "alpha text"},
		{"https://example.com/b", "B", "beta text"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Domain: "example.com", Title: d.title, Text: d.text,
			Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Unauthorized first.
	resp, _ := http.Get(httpSrv.URL + "/admin/stats")
	if resp.StatusCode != 401 {
		t.Errorf("unauth: got %d want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/stats", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d", resp.StatusCode)
	}

	var as AdminStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&as); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if as.Documents != 2 {
		t.Errorf("documents: got %d want 2", as.Documents)
	}
	if as.TopDomains["example.com"] != 2 {
		t.Errorf("domain count: got %+v want example.com=2", as.TopDomains)
	}
	// Capability flags should reflect a BM25-only server.
	if as.DenseEnabled {
		t.Errorf("dense_enabled should be false (no WithVector)")
	}
	if as.AnswerEnabled {
		t.Errorf("answer_enabled should be false (no WithChat)")
	}
	if as.FetcherEnabled {
		t.Errorf("fetcher_enabled should be false (no WithFetcher)")
	}
}

// TestAdminConfigShape verifies the /admin/config endpoint returns the
// resolved Defaults block + capability flags, gated behind bearer auth.
// Mirrors TestAdminStatsShape's auth pattern.
func TestAdminConfigShape(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat := &scriptedChat{replies: []string{"unused"}}
	srv := New(s).
		WithChat(chat).
		WithParaphraser(chat, 2).
		WithAdminToken("secret").
		WithDefaults(Defaults{
			Retriever:        "hybrid",
			Expand:           true,
			ResearchStrategy: "paraphrase",
		})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Unauth → 401.
	resp, _ := http.Get(httpSrv.URL + "/admin/config")
	if resp.StatusCode != 401 {
		t.Errorf("unauth: got %d want 401", resp.StatusCode)
	}
	resp.Body.Close()

	// Auth → 200 with the configured defaults + capabilities reflected.
	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/config", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp2.StatusCode)
	}

	var cr AdminConfigResponse
	if err := json.NewDecoder(resp2.Body).Decode(&cr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cr.Defaults.Retriever != "hybrid" {
		t.Errorf("defaults.retriever: got %q want %q", cr.Defaults.Retriever, "hybrid")
	}
	if !cr.Defaults.Expand {
		t.Errorf("defaults.expand: got %v want true", cr.Defaults.Expand)
	}
	if cr.Defaults.ResearchStrategy != "paraphrase" {
		t.Errorf("defaults.research_strategy: got %q want %q", cr.Defaults.ResearchStrategy, "paraphrase")
	}
	// Capabilities reflect the configured server.
	if !cr.Capabilities.ChatEnabled {
		t.Errorf("chat_enabled should be true")
	}
	if !cr.Capabilities.ParaphraserEnabled {
		t.Errorf("paraphraser_enabled should be true")
	}
	if cr.Capabilities.DenseEnabled {
		t.Errorf("dense_enabled should be false (no WithVector)")
	}
	if cr.Capabilities.RerankEnabled {
		t.Errorf("rerank_enabled should be false (no WithReranker)")
	}
	if !cr.Capabilities.AdminEnabled {
		t.Errorf("admin_enabled should be true (WithAdminToken was called)")
	}
	if cr.Capabilities.ChatModel != "scripted" {
		t.Errorf("chat_model: got %q want %q", cr.Capabilities.ChatModel, "scripted")
	}
	if cr.Version == "" {
		t.Errorf("version must be set (defaults to %q)", "dev")
	}
}

// TestAdminConfigResearchSynthK verifies the ResearchSynthK field
// round-trips through /admin/config, locking the JSON shape against regression.
func TestAdminConfigResearchSynthK(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	srv := New(s).
		WithAdminToken("k").
		WithDefaults(Defaults{ResearchSynthK: 3})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/config", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	var cr AdminConfigResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.Defaults.ResearchSynthK != 3 {
		t.Errorf("ResearchSynthK round-trip: got %d want 3 — full defaults %+v", cr.Defaults.ResearchSynthK, cr.Defaults)
	}
}

// TestAdminConfigZeroDefaults verifies that a server constructed without
// WithDefaults reports zero-valued defaults — empty strings, false booleans.
// The endpoint must work whether or not the operator set defaults.
func TestAdminConfigZeroDefaults(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/config", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var cr AdminConfigResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.Defaults.Retriever != "" || cr.Defaults.Expand || cr.Defaults.ResearchStrategy != "" {
		t.Errorf("zero defaults expected, got %+v", cr.Defaults)
	}
	if cr.Capabilities.ChatEnabled || cr.Capabilities.DenseEnabled {
		t.Errorf("bm25-only server should report no chat/dense capability")
	}
}

func TestFeedbackRecordsOutcome(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := `{"query":"go programming","url":"https://x/a","score":0.87,"useful":true,"source":"thumbs"}`
	resp, _ := http.Post(httpSrv.URL+"/feedback", "application/json", strings.NewReader(body))
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	total, useful, _ := s.CountOutcomes(context.Background())
	if total != 1 || useful != 1 {
		t.Errorf("counts: total=%d useful=%d want 1/1", total, useful)
	}

	// Missing required fields → 400.
	resp, _ = http.Post(httpSrv.URL+"/feedback", "application/json", strings.NewReader(`{"query":"only q"}`))
	if resp.StatusCode != 400 {
		t.Errorf("missing url: got %d want 400", resp.StatusCode)
	}
}

func TestAdminRecrawlBadInput(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	send := func(body string) int {
		req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/recrawl", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer k")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := send(`not json`); got != 400 {
		t.Errorf("bad json: got %d want 400", got)
	}
	if got := send(`{"urls": []}`); got != 400 {
		t.Errorf("empty urls: got %d want 400", got)
	}
}

// paraphraseChat returns a fixed paraphrase JSON array — lets the expand path
// be exercised without real LLM calls.
type paraphraseChat struct {
	reply string
	calls int
}

var _ embed.ChatClient = (*paraphraseChat)(nil)

func (p *paraphraseChat) Model() string { return "paraphrase-stub" }
func (p *paraphraseChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	p.calls++
	return p.reply, nil
}

func TestSearchExpandFusesParaphraseResults(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// Three docs. The main query "alpha" only finds doc1. The paraphrase
	// "second" finds doc2. RRF fusion should return both with doc1 first
	// (it appears in the main-query result, which has higher fused weight
	// because it's the only list containing it).
	docs := []struct{ url, title, text string }{
		{"https://x/1", "Doc one", "alpha primary keyword unique"},
		{"https://x/2", "Doc two", "second beta keyword different"},
		{"https://x/3", "Doc three", "gamma unrelated content"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &paraphraseChat{reply: `["second beta", "another paraphrase that hits nothing"]`}
	srv := New(s).WithParaphraser(chat, 2)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// expand=true: should fuse main results with paraphrase results.
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&expand=true&k=5")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if chat.calls != 1 {
		t.Errorf("paraphrase calls: got %d want 1", chat.calls)
	}
	if !strings.Contains(sr.Hits[0].Source, "expand") {
		t.Errorf("source should mark expand: %q", sr.Hits[0].Source)
	}
	// Both docs should appear (doc1 from main, doc2 from paraphrase).
	urls := map[string]bool{}
	for _, h := range sr.Hits {
		urls[h.URL] = true
	}
	if !urls["https://x/1"] || !urls["https://x/2"] {
		t.Errorf("expected both x/1 (main) and x/2 (paraphrase) in fused result: %+v", sr.Hits)
	}

	// expand=false (default): only main results.
	resp, _ = http.Get(httpSrv.URL + "/search?q=alpha")
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if chat.calls != 1 {
		t.Errorf("default-off: paraphrase should not fire; got %d calls total", chat.calls)
	}
	if len(sr.Hits) == 0 || sr.Hits[0].URL != "https://x/1" {
		t.Errorf("default-off: expected x/1, got %+v", sr.Hits)
	}
}

// ExpandMainWeight knob shifts fused ordering toward the main query.
// Corpus engineered so the main query (q="alpha") prefers doc A and the
// paraphrase (q="beta") prefers doc B; both return the same two docs in
// opposite orders so they tie under equal-weight RRF. With
// ExpandMainWeight=5.0, the main retriever's ranking dominates → A wins.
func TestSearchExpandMainWeightShiftsOrdering(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// doc A: "alpha" tf=3, "beta" tf=1 → main "alpha" picks A first, paraphrase "beta" picks A second.
	// doc B: "alpha" tf=1, "beta" tf=3 → mirror; "beta" picks B first.
	docs := []struct{ url, title, text string }{
		{"https://x/A", "doc A", "alpha alpha alpha beta"},
		{"https://x/B", "doc B", "alpha beta beta beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &paraphraseChat{reply: `["beta"]`}
	srv := New(s).WithParaphraser(chat, 1).WithDefaults(Defaults{ExpandMainWeight: 5.0})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&expand=true&k=2")
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()

	if len(sr.Hits) < 2 {
		t.Fatalf("expected 2 hits, got %d (%+v)", len(sr.Hits), sr.Hits)
	}
	if sr.Hits[0].URL != "https://x/A" {
		t.Errorf("with ExpandMainWeight=5.0, A should outrank B in fused result; got %+v", sr.Hits)
	}
}

func TestParaphraseCacheMetrics(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/1", Title: "doc", Text: "alpha text",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "doc", "alpha text")

	chat := &paraphraseChat{reply: `["alpha synonym"]`}
	srv := New(s).WithParaphraser(chat, 1)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Three calls with the same query → 1 miss (cold), 2 L1 hits.
	for i := 0; i < 3; i++ {
		resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&expand=true")
		resp.Body.Close()
	}
	if chat.calls != 1 {
		t.Errorf("LLM called %d times, want 1 (rest from L1)", chat.calls)
	}

	resp, _ := http.Get(httpSrv.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(body)
	for _, want := range []string{
		`cosift_paraphrase_cache_total{result="l1_hit"} 2`,
		`cosift_paraphrase_cache_total{result="miss"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing metric line: %q\n%s", want, out)
		}
	}

	// New server, same store → L2 hit on the first call.
	chat2 := &paraphraseChat{reply: `["should not be called"]`}
	srv2 := New(s).WithParaphraser(chat2, 1)
	httpSrv2 := httptest.NewServer(srv2.Handler())
	defer httpSrv2.Close()
	resp, _ = http.Get(httpSrv2.URL + "/search?q=alpha&expand=true")
	resp.Body.Close()

	resp, _ = http.Get(httpSrv2.URL + "/metrics")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), `cosift_paraphrase_cache_total{result="l2_hit"} 1`) {
		t.Errorf("L2 hit counter not recorded on second server:\n%s", body)
	}
}

func TestParaphraseCacheCrossInstance(t *testing.T) {
	// First server populates the SQLite paraphrase cache via an LLM call.
	// Second server (sharing the same store) should serve from cache without
	// calling the LLM — closes the "cold processes pay" gap.
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/1", Title: "doc", Text: "alpha keyword",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "doc", "alpha keyword")

	chat1 := &paraphraseChat{reply: `["alpha synonym"]`}
	srv1 := New(s).WithParaphraser(chat1, 1)
	httpSrv1 := httptest.NewServer(srv1.Handler())
	resp, _ := http.Get(httpSrv1.URL + "/search?q=alpha&expand=true")
	resp.Body.Close()
	httpSrv1.Close()
	if chat1.calls != 1 {
		t.Errorf("first instance: expected 1 LLM call, got %d", chat1.calls)
	}

	// Verify L2 was populated.
	cached, _ := s.GetParaphrases(context.Background(), "paraphrase-stub", "alpha")
	if len(cached) != 1 || cached[0] != "alpha synonym" {
		t.Errorf("L2 cache not populated: %+v", cached)
	}

	// Second server, same store, different chat client. Should hit L2 — zero LLM calls.
	chat2 := &paraphraseChat{reply: `["should not be called"]`}
	srv2 := New(s).WithParaphraser(chat2, 1)
	httpSrv2 := httptest.NewServer(srv2.Handler())
	defer httpSrv2.Close()
	resp, _ = http.Get(httpSrv2.URL + "/search?q=alpha&expand=true")
	resp.Body.Close()
	if chat2.calls != 0 {
		t.Errorf("second instance: should hit L2 cache, got %d LLM calls", chat2.calls)
	}
}

func TestSearchExpandUnconfigured(t *testing.T) {
	srv, _ := newTestSrv(t)
	resp, _ := http.Get(srv.URL + "/search?q=hi&expand=true")
	if resp.StatusCode != 400 {
		t.Errorf("expand=true without WithParaphraser: got %d want 400", resp.StatusCode)
	}
}

func TestSearchDenseUnconfigured(t *testing.T) {
	srv, _ := newTestSrv(t)
	resp, _ := http.Get(srv.URL + "/search?q=hi&retriever=dense")
	if resp.StatusCode != 400 {
		t.Errorf("dense without WithVector: got %d want 400", resp.StatusCode)
	}
}

func TestContentsFromStore(t *testing.T) {
	srv, s := newTestSrv(t)
	seed(t, s)

	resp, err := http.Get(srv.URL + "/contents?url=https://x/a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var cr ContentsResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.URL != "https://x/a" || !cr.Cached || cr.Text == "" {
		t.Errorf("contents body: %+v", cr)
	}
}

func TestContentsNotFound(t *testing.T) {
	srv, _ := newTestSrv(t)
	resp, _ := http.Get(srv.URL + "/contents?url=https://nope/")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 for unknown URL, got %d", resp.StatusCode)
	}
}

func TestContentsOnDemandFetch(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	called := 0
	srv := New(s).WithFetcher(func(_ context.Context, u string) (string, string, string, error) {
		called++
		return "Stub Title", "stub body text from on-demand fetch", "en", nil
	})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// URL not in store → falls back to fetcher.
	resp, _ := http.Get(httpSrv.URL + "/contents?url=https://example.com/fresh")
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var cr ContentsResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	resp.Body.Close()
	if cr.Cached {
		t.Errorf("on-demand fetch result must report cached=false")
	}
	if cr.Title != "Stub Title" {
		t.Errorf("title from fetcher: got %q", cr.Title)
	}
	if called != 1 {
		t.Errorf("fetcher called %d times, want 1", called)
	}

	// Repeat call — we deliberately do NOT persist on-demand fetches, so it
	// should hit the fetcher again.
	_, _ = http.Get(httpSrv.URL + "/contents?url=https://example.com/fresh")
	if called != 2 {
		t.Errorf("on-demand fetches should not be persisted; expected fetcher called twice, got %d", called)
	}
}

// scriptedChat returns different responses on consecutive calls — lets the test
// exercise the multi-step /research pipeline (plan → synth).
type scriptedChat struct {
	replies []string
	calls   int

	// When set, ChatStream emits each rune as its own chunk for the second
	// reply (the synth reply) — lets streaming-research tests assert chunked output.
	streamSynth bool
}

var _ embed.ChatClient = (*scriptedChat)(nil)
var _ embed.StreamingChatClient = (*scriptedChat)(nil)

func (s *scriptedChat) Model() string { return "scripted" }
func (s *scriptedChat) Chat(_ context.Context, _ []embed.ChatMsg) (string, error) {
	if s.calls >= len(s.replies) {
		return "", nil
	}
	out := s.replies[s.calls]
	s.calls++
	return out, nil
}
func (s *scriptedChat) ChatStream(_ context.Context, _ []embed.ChatMsg, onChunk func(string)) (string, error) {
	if s.calls >= len(s.replies) {
		return "", nil
	}
	out := s.replies[s.calls]
	s.calls++
	if s.streamSynth && onChunk != nil {
		// Emit one chunk per word so tests can verify chunking arrived.
		for _, w := range strings.Fields(out) {
			onChunk(w + " ")
		}
	}
	return out, nil
}

func TestResearchHappyPath(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/go", "Go programming language", "Go is statically typed, with garbage collection and goroutines."},
		{"https://x/rust", "Rust programming language", "Rust enforces memory safety via ownership and borrowing."},
		{"https://x/concurrency", "Concurrency models", "Goroutines vs threads. Channel-based concurrency vs shared state."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &scriptedChat{replies: []string{
		`["go concurrency", "rust memory safety"]`,
		"Go uses goroutines [1,3]. Rust enforces memory safety via ownership [2].",
	}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/research?q=compare+go+and+rust")
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()

	if len(rr.Plan) != 2 {
		t.Errorf("plan: got %d sub-queries want 2 — got %+v", len(rr.Plan), rr.Plan)
	}
	if len(rr.Sources) == 0 {
		t.Errorf("no sources collected")
	}
	if chat.calls != 2 {
		t.Errorf("expected 2 chat calls (plan + synth), got %d", chat.calls)
	}
	if rr.Answer == "" {
		t.Errorf("empty answer")
	}
	if rr.Calibrated {
		t.Errorf("calibrated must be false")
	}
}

func TestResearchStreaming(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/go", "Go programming", "Go has goroutines."},
		{"https://x/rust", "Rust programming", "Rust enforces memory safety."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &scriptedChat{replies: []string{
		`["go concurrency", "rust safety"]`,
		"Go uses goroutines [1]. Rust enforces memory safety [2].",
	}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/research?q=compare+go+and+rust&stream=true")
	if err != nil {
		t.Fatalf("research stream: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("content-type: got %q want SSE", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// Verify event sequence: plan → retrieved (>=1) → synthesizing → done.
	for _, ev := range []string{"event: plan", "event: retrieved", "event: synthesizing", "event: done"} {
		if !strings.Contains(out, ev) {
			t.Errorf("missing %q in stream: %s", ev, out)
		}
	}
	// "plan" event must come before "done".
	if strings.Index(out, "event: plan") > strings.Index(out, "event: done") {
		t.Errorf("event order wrong")
	}
	// Done event should carry the full ResearchResponse JSON.
	if !strings.Contains(out, `"answer":`) || !strings.Contains(out, `"calibrated":false`) {
		t.Errorf("done payload missing fields: %s", out)
	}
}

func TestResearchStreamingEmitsAnswerChunks(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/p", Title: "doc", Text: "content about widgets",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "doc", "content about widgets")

	chat := &scriptedChat{
		replies:     []string{`["widgets"]`, "Widgets are useful [1]"},
		streamSynth: true,
	}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=widgets&stream=true")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// Synth reply was "Widgets are useful [1]" — 4 words → 4 answer_chunk events.
	chunks := strings.Count(out, "event: answer_chunk")
	if chunks < 3 {
		t.Errorf("expected >=3 answer_chunk events, got %d. stream:\n%s", chunks, out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("missing done event")
	}
}

func TestResearchPlanFallback(t *testing.T) {
	// Planner returns garbage — endpoint should fall back to using the
	// original query as the only sub-query, not crash.
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/p", Title: "doc", Text: "some content about widgets", Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "doc", "some content about widgets")

	chat := &scriptedChat{replies: []string{"not json at all, sorry", "synthesized answer [1]"}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=widgets")
	if resp.StatusCode != 200 {
		t.Errorf("expected 200 on plan fallback, got %d", resp.StatusCode)
	}
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()
	if len(rr.Plan) != 1 || rr.Plan[0] != "widgets" {
		t.Errorf("expected fallback plan = [%q], got %+v", "widgets", rr.Plan)
	}
}

func TestStats(t *testing.T) {
	srv, s := newTestSrv(t)
	seed(t, s)

	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d", resp.StatusCode)
	}
	var st StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if st.Documents != 2 {
		t.Errorf("doc count: got %d want 2", st.Documents)
	}
}

// TestResearchStrategyParaphrase verifies that /research?strategy=paraphrase
// uses the paraphraser instead of the planner. The scripted chat returns NO
// sub-queries (would fail planner mode); the scripted paraphraser returns one
// rewording that hits the second doc. The synth chat call's user message must
// mention "paraphrases used" instead of "sub-queries used".
func TestResearchStrategyParaphrase(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/alpha", "Alpha topic", "alpha discusses alpha"},
		{"https://x/beta", "Beta topic", "beta covers second beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	// Two separate chat clients so we can verify which one is called:
	// pchat (paraphraser) is called by ?strategy=paraphrase, NOT the synth chat.
	pchat := &scriptedChat{replies: []string{`["second beta"]`}}
	synthChat := &scriptedChat{replies: []string{"Alpha [1]. Beta [2]."}}
	srv := New(s).WithChat(synthChat).WithParaphraser(pchat, 1)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/research?q=alpha&strategy=paraphrase")
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()

	if rr.Strategy != "paraphrase" {
		t.Errorf("strategy: got %q want %q", rr.Strategy, "paraphrase")
	}
	if len(rr.Plan) != 1 || rr.Plan[0] != "second beta" {
		t.Errorf("plan: want [\"second beta\"], got %+v", rr.Plan)
	}
	if pchat.calls != 1 {
		t.Errorf("paraphraser chat calls: got %d want 1", pchat.calls)
	}
	// synthChat called once for the final synthesis only (no plan call).
	if synthChat.calls != 1 {
		t.Errorf("synth chat calls: got %d want 1 (no planner LLM call in paraphrase strategy)", synthChat.calls)
	}
	urls := make(map[string]bool, len(rr.Sources))
	for _, src := range rr.Sources {
		urls[src.URL] = true
	}
	if !urls["https://x/alpha"] || !urls["https://x/beta"] {
		t.Errorf("paraphrase RRF should have surfaced both docs; got sources %+v", rr.Sources)
	}
}

// TestResearchStrategyParaphraseUnconfigured verifies the 400 when the user
// asks for paraphrase strategy but the server doesn't have a paraphraser.
func TestResearchStrategyParaphraseUnconfigured(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat := &scriptedChat{replies: []string{"unreachable"}}
	srv := New(s).WithChat(chat) // chat configured for synth, but NO paraphraser
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/research?q=anything&strategy=paraphrase")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", resp.StatusCode)
	}
	if chat.calls != 0 {
		t.Errorf("unconfigured paraphrase must not burn LLM calls; got %d calls", chat.calls)
	}
}

// TestResearchStrategyUnknown verifies the 400 on an unknown strategy.
func TestResearchStrategyUnknown(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	chat := &scriptedChat{replies: []string{"unreachable"}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=anything&strategy=bogus")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d want 400 on unknown strategy", resp.StatusCode)
	}
}

// TestDefaultsResearchStrategy verifies that WithDefaults({ResearchStrategy:"paraphrase"})
// makes /research with no ?strategy= use the paraphrase path, while an explicit
// ?strategy=planner still overrides.
func TestDefaultsResearchStrategy(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/alpha", "Alpha topic", "alpha discusses alpha"},
		{"https://x/beta", "Beta topic", "beta covers second beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	// Default → paraphrase. Param absent → paraphrase. Param=planner → planner.
	pchat := &scriptedChat{replies: []string{`["second beta"]`, `["alpha sub", "beta sub"]`}}
	synthChat := &scriptedChat{replies: []string{"a [1]", "b [1]"}}
	srv := New(s).
		WithChat(synthChat).
		WithParaphraser(pchat, 1).
		WithDefaults(Defaults{ResearchStrategy: "paraphrase"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// No ?strategy= → should pick up paraphrase default.
	resp, _ := http.Get(httpSrv.URL + "/research?q=alpha")
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()
	if rr.Strategy != "paraphrase" {
		t.Errorf("default strategy: got %q want paraphrase", rr.Strategy)
	}
	if pchat.calls != 1 {
		t.Errorf("paraphraser must be called for default=paraphrase; got %d calls", pchat.calls)
	}

	// Explicit ?strategy=planner overrides the default.
	resp2, _ := http.Get(httpSrv.URL + "/research?q=alpha&strategy=planner")
	var rr2 ResearchResponse
	_ = json.NewDecoder(resp2.Body).Decode(&rr2)
	resp2.Body.Close()
	if rr2.Strategy != "planner" {
		t.Errorf("override strategy: got %q want planner", rr2.Strategy)
	}
	// Now synth has been called twice (one per request).
	if synthChat.calls != 2 {
		t.Errorf("synth calls: got %d want 2", synthChat.calls)
	}
}

// TestDefaultsExpand verifies that WithDefaults({Expand:true}) makes /search
// expand by default, and that ?expand=false overrides off.
func TestDefaultsExpand(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/alpha", "alpha", "alpha discusses alpha"},
		{"https://x/beta", "beta", "beta covers second beta"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &scriptedChat{replies: []string{`["second beta"]`, `["second beta"]`}}
	srv := New(s).
		WithParaphraser(chat, 1).
		WithDefaults(Defaults{Expand: true})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// No ?expand= → default kicks in. The paraphraser is invoked.
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha")
	resp.Body.Close()
	if chat.calls != 1 {
		t.Errorf("default expand=true: paraphraser should be called once, got %d", chat.calls)
	}

	// Explicit ?expand=false overrides off — no paraphraser call.
	resp2, _ := http.Get(httpSrv.URL + "/search?q=alpha&expand=false")
	resp2.Body.Close()
	if chat.calls != 1 {
		t.Errorf("expand=false override: paraphraser should NOT be called, total calls now %d (want still 1)", chat.calls)
	}
}

// TestDefaultsRetriever verifies that WithDefaults({Retriever:"bm25"}) is the
// no-op default. Real change to "hybrid"/"dense" requires WithVector — covered
// implicitly by the existing hybrid tests when dense is configured.
func TestDefaultsRetriever(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/a", Title: "alpha", Text: "alpha content",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "alpha", "alpha content")

	srv := New(s).WithDefaults(Defaults{Retriever: "bm25"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}

	// Asking for hybrid without WithVector should still 400 — the default
	// doesn't bypass capability checks.
	srv2 := New(s).WithDefaults(Defaults{Retriever: "hybrid"})
	httpSrv2 := httptest.NewServer(srv2.Handler())
	defer httpSrv2.Close()
	resp2, _ := http.Get(httpSrv2.URL + "/search?q=alpha")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Errorf("hybrid default w/o WithVector: got %d want 400", resp2.StatusCode)
	}
}

// TestResearchPlannerStrategyExplicit verifies that strategy=planner is a no-op
// (matches the default behavior). Same fixture as TestResearchHappyPath but the
// strategy is explicit. Locks the default in.
func TestResearchPlannerStrategyExplicit(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title, text string }{
		{"https://x/go", "Go programming language", "Go is statically typed with goroutines."},
		{"https://x/rust", "Rust programming language", "Rust enforces memory safety via ownership."},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}
	chat := &scriptedChat{replies: []string{
		`["go concurrency", "rust memory safety"]`,
		"Go [1]. Rust [2].",
	}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=go+vs+rust&strategy=planner")
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	resp.Body.Close()

	if rr.Strategy != "planner" {
		t.Errorf("strategy: got %q want %q", rr.Strategy, "planner")
	}
	if len(rr.Plan) != 2 {
		t.Errorf("plan: got %d sub-queries want 2", len(rr.Plan))
	}
	if chat.calls != 2 {
		t.Errorf("planner = plan call + synth call; got %d", chat.calls)
	}
}

// TestSearchDateFilter verifies date-aware search: ?since= and ?until=
// filter results by document PublishedAt; docs with unknown publication date
// (PublishedAt zero) are excluded when any filter is active.
func TestSearchDateFilter(t *testing.T) {
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	// Three docs with varied publication dates.
	docs := []struct {
		url, title, text string
		published        time.Time
	}{
		{"https://x/2023", "Old article", "concurrency goroutines", time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2024", "Mid article", "concurrency goroutines", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2025", "New article", "concurrency goroutines", time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/undated", "Undated article", "concurrency goroutines", time.Time{}},
	}
	for _, d := range docs {
		id, err := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text,
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: d.published,
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", d.url, err)
		}
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Helper: extract URLs from a search response.
	getURLs := func(query string) []string {
		resp, err := http.Get(httpSrv.URL + "/search?" + query)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status %d for %q", resp.StatusCode, query)
		}
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		return out
	}

	// No filter: all 4 docs returned (including undated).
	urls := getURLs("q=concurrency+goroutines&k=10")
	if len(urls) != 4 {
		t.Errorf("no filter: want 4 hits got %d (%v)", len(urls), urls)
	}

	// since=2024-01-01: drops 2023 + undated.
	urls = getURLs("q=concurrency+goroutines&k=10&since=2024-01-01")
	if !containsURL(urls, "https://x/2024") || !containsURL(urls, "https://x/2025") {
		t.Errorf("since=2024-01-01 should include 2024+2025: %v", urls)
	}
	if containsURL(urls, "https://x/2023") || containsURL(urls, "https://x/undated") {
		t.Errorf("since=2024-01-01 should exclude 2023+undated: %v", urls)
	}

	// until=2024-12-31: drops 2025 + undated.
	urls = getURLs("q=concurrency+goroutines&k=10&until=2024-12-31")
	if !containsURL(urls, "https://x/2023") || !containsURL(urls, "https://x/2024") {
		t.Errorf("until=2024-12-31 should include 2023+2024: %v", urls)
	}
	if containsURL(urls, "https://x/2025") || containsURL(urls, "https://x/undated") {
		t.Errorf("until=2024-12-31 should exclude 2025+undated: %v", urls)
	}

	// Range: since AND until — only 2024.
	urls = getURLs("q=concurrency+goroutines&k=10&since=2024-01-01&until=2024-12-31")
	if len(urls) != 1 || urls[0] != "https://x/2024" {
		t.Errorf("range filter should leave only 2024: %v", urls)
	}

	// Bad date format → 400.
	resp, _ := http.Get(httpSrv.URL + "/search?q=concurrency&since=last+tuesday")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad date format should 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func containsURL(urls []string, u string) bool {
	for _, x := range urls {
		if x == u {
			return true
		}
	}
	return false
}

// TestSearchDateFilterRFC3339 verifies the alternate date format works too.
func TestSearchDateFilterRFC3339(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/2024", Title: "Article", Text: "alpha",
		Source: "test", FetchedAt: time.Now(),
		PublishedAt: time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
	})
	_ = idx.IndexDocument(context.Background(), id, "Article", "alpha")

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// RFC3339 format for since.
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&since=2024-01-01T00:00:00Z")
	if resp.StatusCode != 200 {
		t.Fatalf("RFC3339 since should accept: status %d", resp.StatusCode)
	}
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	resp.Body.Close()
	if len(sr.Hits) != 1 {
		t.Errorf("expected 1 hit, got %d", len(sr.Hits))
	}
}

// TestSearchSortByDate verifies ?sort=date_desc/date_asc. Mixed dated +
// undated docs are sorted with undated last regardless of direction.
func TestSearchSortByDate(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct {
		url, title string
		published  time.Time
	}{
		{"https://x/2023", "tessellation old", time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2025", "tessellation new", time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2024", "tessellation mid", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/undated", "tessellation undated", time.Time{}},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: "tessellation pattern",
			Source: "test", FetchedAt: time.Now(), PublishedAt: d.published,
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, "tessellation pattern")
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	getURLs := func(query string) []string {
		resp, _ := http.Get(httpSrv.URL + "/search?" + query)
		defer resp.Body.Close()
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		return out
	}

	// date_desc: newest first, undated last
	urls := getURLs("q=tessellation&k=10&sort=date_desc")
	if len(urls) != 4 {
		t.Fatalf("date_desc: want 4 hits, got %d (%v)", len(urls), urls)
	}
	wantDesc := []string{"https://x/2025", "https://x/2024", "https://x/2023", "https://x/undated"}
	for i, w := range wantDesc {
		if urls[i] != w {
			t.Errorf("date_desc[%d]: got %q want %q (full: %v)", i, urls[i], w, urls)
		}
	}

	// date_asc: oldest first, undated still last
	urls = getURLs("q=tessellation&k=10&sort=date_asc")
	if len(urls) != 4 {
		t.Fatalf("date_asc: want 4 hits, got %d", len(urls))
	}
	wantAsc := []string{"https://x/2023", "https://x/2024", "https://x/2025", "https://x/undated"}
	for i, w := range wantAsc {
		if urls[i] != w {
			t.Errorf("date_asc[%d]: got %q want %q (full: %v)", i, urls[i], w, urls)
		}
	}

	// Default (no sort) = relevance, undated docs not excluded
	urls = getURLs("q=tessellation&k=10")
	if len(urls) != 4 {
		t.Errorf("default sort: want 4 hits, got %d", len(urls))
	}

	// sort=relevance explicit is same as no sort
	urls = getURLs("q=tessellation&k=10&sort=relevance")
	if len(urls) != 4 {
		t.Errorf("sort=relevance explicit: want 4 hits, got %d", len(urls))
	}

	// Bad sort value → 400
	resp, _ := http.Get(httpSrv.URL + "/search?q=tessellation&sort=oldest_first")
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad sort value should 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestSearchSortByDateWithFilter verifies sort + filter compose correctly.
// Filter runs first (excludes undated), then sort orders the remainder.
func TestSearchSortByDateWithFilter(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct {
		url       string
		published time.Time
	}{
		{"https://x/2023", time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2024", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/2025", time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)},
		{"https://x/undated", time.Time{}},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: "T", Text: "alpha",
			Source: "test", FetchedAt: time.Now(), PublishedAt: d.published,
		})
		_ = idx.IndexDocument(context.Background(), id, "T", "alpha")
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// since=2024-01-01 + sort=date_asc → only 2024 + 2025, oldest first
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=10&since=2024-01-01&sort=date_asc")
	defer resp.Body.Close()
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	if len(sr.Hits) != 2 {
		t.Fatalf("filter+sort: want 2 hits, got %d (%+v)", len(sr.Hits), sr.Hits)
	}
	if sr.Hits[0].URL != "https://x/2024" || sr.Hits[1].URL != "https://x/2025" {
		t.Errorf("filter+sort order wrong: %+v", sr.Hits)
	}
}

// TestSearchDomainFilter verifies ?include_domains= and ?exclude_domains=.
// Suffix matching means "example.com" matches "blog.example.com" too.
func TestSearchDomainFilter(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	docs := []struct{ url, title string }{
		{"https://docs.example.com/api", "ex docs api"},
		{"https://blog.example.com/post", "ex blog post"},
		{"https://other.org/page", "other page"},
		{"https://evilexample.com/page", "evil look-alike"}, // NOT a subdomain of example.com
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: "alpha content",
			Source: "test", FetchedAt: time.Now(),
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, "alpha content")
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	getURLs := func(query string) []string {
		resp, _ := http.Get(httpSrv.URL + "/search?" + query)
		defer resp.Body.Close()
		var sr SearchResponse
		_ = json.NewDecoder(resp.Body).Decode(&sr)
		out := make([]string, len(sr.Hits))
		for i, h := range sr.Hits {
			out[i] = h.URL
		}
		return out
	}

	// No filter: all 4 hits.
	urls := getURLs("q=alpha&k=10")
	if len(urls) != 4 {
		t.Errorf("no filter: want 4, got %d (%v)", len(urls), urls)
	}

	// Include example.com: matches docs + blog (subdomains), NOT evilexample.com (strict suffix boundary).
	urls = getURLs("q=alpha&k=10&include_domains=example.com")
	if !containsURL(urls, "https://docs.example.com/api") || !containsURL(urls, "https://blog.example.com/post") {
		t.Errorf("include_domains=example.com should match docs+blog subdomains: %v", urls)
	}
	if containsURL(urls, "https://other.org/page") || containsURL(urls, "https://evilexample.com/page") {
		t.Errorf("include_domains=example.com should NOT match other.org or evilexample.com: %v", urls)
	}

	// Exclude evilexample.com: keeps everything else.
	urls = getURLs("q=alpha&k=10&exclude_domains=evilexample.com")
	if containsURL(urls, "https://evilexample.com/page") {
		t.Errorf("exclude_domains should drop evilexample.com: %v", urls)
	}
	if !containsURL(urls, "https://docs.example.com/api") {
		t.Errorf("exclude_domains shouldn't drop unrelated hosts: %v", urls)
	}

	// Comma-separated: include both example.com and other.org.
	urls = getURLs("q=alpha&k=10&include_domains=example.com,other.org")
	for _, want := range []string{"https://docs.example.com/api", "https://blog.example.com/post", "https://other.org/page"} {
		if !containsURL(urls, want) {
			t.Errorf("include_domains CSV missing %q: %v", want, urls)
		}
	}
	if containsURL(urls, "https://evilexample.com/page") {
		t.Errorf("include_domains CSV included evilexample: %v", urls)
	}

	// Include + exclude combo: include example.com but exclude blog.example.com → only docs.
	urls = getURLs("q=alpha&k=10&include_domains=example.com&exclude_domains=blog.example.com")
	if len(urls) != 1 || urls[0] != "https://docs.example.com/api" {
		t.Errorf("include+exclude composition wrong: %v", urls)
	}
}

// TestSearchHitEnrichmentFields verifies — /search response carries
// PublishedAt + Domain per hit.: callers shouldn't have to round-trip
// /contents to get publication date or domain.
func TestSearchHitEnrichmentFields(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	published := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	for _, d := range []struct {
		url, title, text, domain string
		pub                      time.Time
	}{
		{"https://docs.example.com/a", "T1", "alpha", "docs.example.com", published},
		{"https://other.org/b", "T2", "alpha", "other.org", time.Time{}}, // undated
	} {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Domain: d.domain,
			Source: "test", FetchedAt: time.Now(), PublishedAt: d.pub,
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&k=10")
	defer resp.Body.Close()
	var sr SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sr.Hits) != 2 {
		t.Fatalf("hits: got %d want 2", len(sr.Hits))
	}
	// Find each hit by URL (order may vary by score).
	var dated, undated *SearchHit
	for i := range sr.Hits {
		if sr.Hits[i].URL == "https://docs.example.com/a" {
			dated = &sr.Hits[i]
		}
		if sr.Hits[i].URL == "https://other.org/b" {
			undated = &sr.Hits[i]
		}
	}
	if dated == nil || undated == nil {
		t.Fatalf("missing one of the expected hits: %+v", sr.Hits)
	}
	if dated.Domain != "docs.example.com" {
		t.Errorf("dated.Domain: got %q want %q", dated.Domain, "docs.example.com")
	}
	if dated.PublishedAt == nil || !dated.PublishedAt.Equal(published) {
		t.Errorf("dated.PublishedAt: got %v want %v", dated.PublishedAt, published)
	}
	if undated.Domain != "other.org" {
		t.Errorf("undated.Domain: got %q want %q", undated.Domain, "other.org")
	}
	// Undated must omit PublishedAt entirely (nil pointer + omitempty → not in JSON).
	if undated.PublishedAt != nil {
		t.Errorf("undated.PublishedAt should be nil, got %v", undated.PublishedAt)
	}
}

// TestSearchHitExcerptFallback verifies — BM25-only hits get an Excerpt
// (first 500 chars of body) since they have no Highlight. Confirms the
// fallback semantics: Excerpt is populated when Highlight is nil.
func TestSearchHitExcerptFallback(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	bodyText := "The Go programming language is well-suited for concurrent programming. " +
		"Goroutines are lightweight threads managed by the Go runtime. " +
		"Channels provide a way to communicate between goroutines. " +
		"This makes Go a powerful choice for building concurrent systems."
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/go", Title: "Go", Text: bodyText, Domain: "x",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "Go", bodyText)

	srv := New(s) // BM25-only (no WithVector → no dense → no highlight)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/search?q=goroutines&k=1")
	defer resp.Body.Close()
	var sr SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(sr.Hits) != 1 {
		t.Fatalf("hits: got %d want 1", len(sr.Hits))
	}
	hit := sr.Hits[0]
	if hit.Highlight != nil {
		t.Errorf("BM25-only hit should have no Highlight, got %+v", hit.Highlight)
	}
	if hit.Excerpt == "" {
		t.Errorf("BM25-only hit should have Excerpt populated; full hit: %+v", hit)
	}
	// Excerpt should contain at least the beginning of the body.
	if !strings.Contains(hit.Excerpt, "Go programming language") {
		t.Errorf("Excerpt should contain start of body, got: %q", hit.Excerpt)
	}
}

// TestResearchSourcesEnriched verifies — /research sources carry
// Domain + PublishedAt + Excerpt to match /search response shape.
func TestResearchSourcesEnriched(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	published := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	docs := []struct{ url, title, text, domain string }{
		{"https://docs.example.com/a", "Go programming language", "Go is statically typed, with garbage collection and goroutines.", "docs.example.com"},
		{"https://docs.example.com/b", "Rust programming language", "Rust enforces memory safety via ownership and borrowing.", "docs.example.com"},
	}
	for _, d := range docs {
		id, _ := s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Domain: d.domain,
			Source: "test", FetchedAt: time.Now(), PublishedAt: published,
		})
		_ = idx.IndexDocument(context.Background(), id, d.title, d.text)
	}

	chat := &scriptedChat{replies: []string{
		`["go concurrency", "rust memory safety"]`,
		"Go uses goroutines [1]. Rust enforces memory safety via ownership [2].",
	}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/research?q=compare+go+and+rust")
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	defer resp.Body.Close()
	var rr ResearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&rr)
	if len(rr.Sources) == 0 {
		t.Fatal("no sources")
	}
	for i, src := range rr.Sources {
		if src.Domain != "docs.example.com" {
			t.Errorf("source[%d].Domain: got %q want docs.example.com", i, src.Domain)
		}
		if src.PublishedAt == nil || !src.PublishedAt.Equal(published) {
			t.Errorf("source[%d].PublishedAt: got %v want %v", i, src.PublishedAt, published)
		}
		if src.Excerpt == "" {
			t.Errorf("source[%d].Excerpt should be populated, got empty", i)
		}
	}
}

// TestResearchSourcesEnrichedJSONShape locks the wire format — undated docs
// emit no published_at, empty domains emit no domain.
func TestResearchSourcesEnrichedJSONShape(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/undated", Title: "Undated", Text: "alpha undated content",
		Source: "test", FetchedAt: time.Now(), // PublishedAt intentionally zero; Domain empty
	})
	_ = idx.IndexDocument(context.Background(), id, "Undated", "alpha undated content")

	chat := &scriptedChat{replies: []string{
		`["alpha"]`,
		"undated says alpha [1].",
	}}
	srv := New(s).WithChat(chat)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/research?q=alpha")
	defer resp.Body.Close()
	// Read raw JSON body so we can grep for absent fields.
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)
	// published_at must NOT appear (undated docs)
	if strings.Contains(body, "\"published_at\"") {
		t.Errorf("undated doc should omit published_at: %s", body)
	}
	// domain must NOT appear (empty domain)
	if strings.Contains(body, "\"domain\":\"\"") {
		t.Errorf("empty domain should be omitted, not emitted as empty string: %s", body)
	}
	// Excerpt should appear (body is non-empty)
	if !strings.Contains(body, "\"excerpt\"") {
		t.Errorf("excerpt should be present: %s", body)
	}
}

// TestSearchHitExcerptOmittedWhenEmpty verifies the JSON wire format —
// excerpt is omitempty so empty-bodied docs don't emit `"excerpt": ""`.
func TestSearchHitExcerptOmittedWhenEmpty(t *testing.T) {
	bare := SearchHit{URL: "https://x", Title: "T", Score: 1.0, Source: "bm25"}
	b, _ := json.Marshal(bare)
	s := string(b)
	if strings.Contains(s, "\"excerpt\"") {
		t.Errorf("bare hit should omit excerpt: %s", s)
	}
	withExcerpt := SearchHit{URL: "https://x", Title: "T", Score: 1.0, Source: "bm25", Excerpt: "hello"}
	b2, _ := json.Marshal(withExcerpt)
	if !strings.Contains(string(b2), "\"excerpt\":\"hello\"") {
		t.Errorf("populated excerpt should appear in JSON: %s", b2)
	}
}

// TestSearchHitEnrichmentJSONShape locks the wire format — PublishedAt and
// Domain must be omitempty so undated/no-domain hits emit clean JSON.
func TestSearchHitEnrichmentJSONShape(t *testing.T) {
	// Marshal a SearchHit with no enrichment fields → should not produce
	// "domain":"" or "published_at":null in the JSON.
	bare := SearchHit{URL: "https://x", Title: "T", Score: 1.0, Source: "bm25"}
	b, _ := json.Marshal(bare)
	s := string(b)
	if strings.Contains(s, "\"domain\"") {
		t.Errorf("bare hit should omit domain: %s", s)
	}
	if strings.Contains(s, "\"published_at\"") {
		t.Errorf("bare hit should omit published_at: %s", s)
	}
	// With fields set, both should appear.
	pub := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	full := SearchHit{URL: "https://x", Title: "T", Score: 1.0, Source: "bm25", Domain: "x.com", PublishedAt: &pub}
	b2, _ := json.Marshal(full)
	s2 := string(b2)
	if !strings.Contains(s2, "\"domain\":\"x.com\"") {
		t.Errorf("enriched hit missing domain: %s", s2)
	}
	if !strings.Contains(s2, "\"published_at\"") {
		t.Errorf("enriched hit missing published_at: %s", s2)
	}
}

// TestContentsBatchHappyPath verifies — POST /contents accepts a list
// of URLs, returns per-URL outcomes including found-false for misses.
func TestContentsBatchHappyPath(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// Two indexed docs + one URL we won't index.
	for _, d := range []struct{ url, title, text string }{
		{"https://x/a", "A", "alpha text"},
		{"https://x/b", "B", "beta text"},
	} {
		_, _ = s.UpsertDocument(context.Background(), &store.Document{
			URL: d.url, Title: d.title, Text: d.text, Source: "test", FetchedAt: time.Now(),
		})
	}
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	body := `{"urls": ["https://x/a", "https://x/b", "https://x/missing"]}`
	resp, err := http.Post(httpSrv.URL+"/contents", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	var br ContentsBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&br); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(br.Results) != 3 {
		t.Fatalf("results: got %d want 3", len(br.Results))
	}
	// Order preserved.
	if br.Results[0].URL != "https://x/a" || !br.Results[0].Found {
		t.Errorf("results[0]: %+v", br.Results[0])
	}
	if br.Results[0].Title != "A" || br.Results[0].Text != "alpha text" {
		t.Errorf("results[0] content: %+v", br.Results[0])
	}
	if br.Results[1].URL != "https://x/b" || !br.Results[1].Found {
		t.Errorf("results[1]: %+v", br.Results[1])
	}
	if br.Results[2].URL != "https://x/missing" || br.Results[2].Found {
		t.Errorf("results[2] should be found=false: %+v", br.Results[2])
	}
	if br.Results[2].Error != "not in index" {
		t.Errorf("missing URL should report 'not in index', got: %q", br.Results[2].Error)
	}
}

// TestContentsBatchEmpty verifies that empty url array → 400.
func TestContentsBatchEmpty(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Post(httpSrv.URL+"/contents", "application/json", strings.NewReader(`{"urls":[]}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty urls: got %d want 400", resp.StatusCode)
	}
}

// TestContentsBatchMalformedJSON verifies invalid body → 400 with reason.
func TestContentsBatchMalformedJSON(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Post(httpSrv.URL+"/contents", "application/json", strings.NewReader(`{ this isn't json }`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed json: got %d want 400", resp.StatusCode)
	}
}

// TestContentsBatchOverLimit verifies the 100-URL cap is enforced.
func TestContentsBatchOverLimit(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	urls := make([]string, 101)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://x/%d", i)
	}
	body, _ := json.Marshal(map[string][]string{"urls": urls})
	resp, _ := http.Post(httpSrv.URL+"/contents", "application/json", strings.NewReader(string(body)))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("over-limit batch: got %d want 400", resp.StatusCode)
	}
}

// TestContentsBatchGETStillWorks verifies the single-URL GET path
// is unchanged when adds the POST variant.
func TestContentsBatchGETStillWorks(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	_, _ = s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/single", Title: "Single", Text: "single doc",
		Source: "test", FetchedAt: time.Now(),
	})
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/contents?url=https://x/single")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	var cr ContentsResponse
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	if cr.URL != "https://x/single" || cr.Title != "Single" {
		t.Errorf("GET /contents broken: %+v", cr)
	}
}

// TestRobotsTxt verifies — /robots.txt advertises /sitemap.xml as
// absolute URL, allows everything except /admin/*. Pairs with sitemap.
func TestRobotsTxt(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/robots.txt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type: got %q want text/plain", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	bs := string(body)
	for _, want := range []string{
		"User-agent: *",
		"Allow: /",
		"Disallow: /admin/",
		"Sitemap: ",
		"/sitemap.xml",
	} {
		if !strings.Contains(bs, want) {
			t.Errorf("missing %q in:\n%s", want, bs)
		}
	}
	// Sitemap URL must be absolute (scheme + host + path).
	if !strings.Contains(bs, httpSrv.URL+"/sitemap.xml") {
		t.Errorf("sitemap URL should be absolute matching the request host; got:\n%s\n(expected: %s/sitemap.xml)", bs, httpSrv.URL)
	}
}

// TestRobotsTxtSitemapResolvable verifies the sitemap URL advertised by
// robots.txt actually resolves (200 OK + sitemap content).
func TestRobotsTxtSitemapResolvable(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Get(httpSrv.URL + "/robots.txt")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	// Extract the sitemap URL from the robots.txt body.
	var sitemapURL string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "Sitemap: ") {
			sitemapURL = strings.TrimPrefix(line, "Sitemap: ")
			break
		}
	}
	if sitemapURL == "" {
		t.Fatal("no Sitemap: line found")
	}
	// Hit the advertised URL.
	resp2, err := http.Get(sitemapURL)
	if err != nil {
		t.Fatalf("fetch advertised sitemap: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("advertised sitemap status: got %d want 200", resp2.StatusCode)
	}
	if ct := resp2.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/xml") {
		t.Errorf("advertised sitemap content-type: got %q", ct)
	}
}

// TestSitemapEmptyCorpus verifies the /sitemap.xml endpoint emits
// well-formed XML even when the corpus is empty.
func TestSitemapEmptyCorpus(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/sitemap.xml")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/xml") {
		t.Errorf("content-type: got %q want application/xml...", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	bs := string(body)
	if !strings.Contains(bs, "<?xml") {
		t.Errorf("missing XML prolog: %s", bs)
	}
	if !strings.Contains(bs, "<urlset") {
		t.Errorf("missing <urlset>: %s", bs)
	}
	if !strings.Contains(bs, "</urlset>") {
		t.Errorf("missing </urlset>: %s", bs)
	}
}

// TestSitemapWithDocs verifies URLs + lastmod are emitted per doc.
func TestSitemapWithDocs(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// Two docs: one with a lastChangedAt, one without.
	changed := time.Date(2024, 6, 15, 14, 32, 0, 0, time.UTC)
	_, _ = s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://example.com/a", Title: "A", Text: "alpha",
		Source: "test", FetchedAt: time.Now(),
		LastChangedAt: changed.Unix(),
	})
	_, _ = s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://example.com/b", Title: "B", Text: "beta",
		Source: "test", FetchedAt: time.Now(),
		// LastChangedAt intentionally zero
	})

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()
	resp, _ := http.Get(httpSrv.URL + "/sitemap.xml")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	bs := string(body)

	for _, want := range []string{
		"<loc>https://example.com/a</loc>",
		"<loc>https://example.com/b</loc>",
		"<lastmod>2024-06-15T14:32:00Z</lastmod>",
	} {
		if !strings.Contains(bs, want) {
			t.Errorf("missing %q in:\n%s", want, bs)
		}
	}
	// Doc b should NOT have a lastmod (LastChangedAt was zero).
	if strings.Contains(bs, `<loc>https://example.com/b</loc><lastmod>`) {
		t.Errorf("doc b should omit lastmod (LastChangedAt zero): %s", bs)
	}
}

// TestSitemapXMLEscape verifies URLs containing XML metacharacters are
// escaped — important for query strings with `&` etc.
func TestSitemapXMLEscape(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://x.com/a?b=1&c=2", "https://x.com/a?b=1&amp;c=2"},
		{`https://x.com/"weird"`, "https://x.com/&quot;weird&quot;"},
		{"https://x.com/<script>", "https://x.com/&lt;script&gt;"},
		{"https://x.com/normal", "https://x.com/normal"},
		{"https://x.com/'", "https://x.com/&apos;"},
	}
	for _, c := range cases {
		got := escapeXMLText(c.in)
		if got != c.want {
			t.Errorf("escapeXMLText(%q) = %q want %q", c.in, got, c.want)
		}
	}
}

// TestDashboardHTML smoke-tests the dashboard endpoint. Verifies it
// serves static HTML + carries the "with publish date" card so the
// dashboard renders the stat correctly.
func TestDashboardHTML(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })
	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/dashboard")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s2 := string(body)
	// Static page is public chrome — should serve without auth.
	if !strings.Contains(s2, "cosift dashboard") {
		t.Errorf("missing title")
	}
	// addition: the dashboard JS must reference docs_with_published_at
	// AND render a "with publish date" card.
	if !strings.Contains(s2, "docs_with_published_at") {
		t.Errorf("dashboard should reference docs_with_published_at field from /admin/stats")
	}
	if !strings.Contains(s2, "with publish date") {
		t.Errorf("dashboard should render the 'with publish date' card")
	}
	// the dashboard JS must reference the LLM-cache fields
	// AND render the conditional "paraphrases" / "hyde cache" cards. Both
	// are zero-hidden so the bare HTML always contains the field name plus
	// the card title strings — they're rendered only when count > 0.
	for _, want := range []string{"paraphrases", "hyde_cache", "hyde cache"} {
		if !strings.Contains(s2, want) {
			t.Errorf("dashboard should reference %q", want)
		}
	}
}

// TestAdminStatsDocsWithPublishedAt verifies the corpus-shape stat.
// Mixed dated + undated docs; only the dated ones are counted in the field.
func TestAdminStatsDocsWithPublishedAt(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	now := time.Now()
	// Three dated, two undated.
	for i, pub := range []time.Time{
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 11, 20, 0, 0, 0, 0, time.UTC),
		{},
		{},
	} {
		_, err := s.UpsertDocument(context.Background(), &store.Document{
			URL:         fmt.Sprintf("https://x/%d", i),
			Title:       "T",
			Text:        "alpha",
			Source:      "test",
			FetchedAt:   now,
			PublishedAt: pub,
		})
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("GET", httpSrv.URL+"/admin/stats", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()

	var as AdminStatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&as); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if as.Documents != 5 {
		t.Errorf("documents: got %d want 5", as.Documents)
	}
	if as.DocsWithPublishedAt != 3 {
		t.Errorf("docs_with_published_at: got %d want 3 (3 dated of 5 docs)", as.DocsWithPublishedAt)
	}
}

func TestSplitCSV(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"foo.com", []string{"foo.com"}},
		{"foo.com,bar.com", []string{"foo.com", "bar.com"}},
		{" foo.com , BAR.com ", []string{"foo.com", "bar.com"}}, // trim + lowercase
		{",,,foo.com,,", []string{"foo.com"}},                   // drop empties
	}
	for _, c := range cases {
		got := splitCSV(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitCSV(%q): got %v want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitCSV(%q)[%d]: got %q want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

func TestMatchesAnyDomain(t *testing.T) {
	cases := []struct {
		host     string
		patterns []string
		want     bool
	}{
		{"example.com", []string{"example.com"}, true},              // exact
		{"blog.example.com", []string{"example.com"}, true},         // subdomain via suffix
		{"deep.nested.example.com", []string{"example.com"}, true},  // deeper subdomain
		{"evilexample.com", []string{"example.com"}, false},         // not a subdomain — strict boundary
		{"notexample.com", []string{"example.com"}, false},          // not a subdomain
		{"example.com", []string{"other.org", "example.com"}, true}, // matches second pattern
		{"example.com", []string{"other.org"}, false},               // no match
		{"example.com", nil, false},                                 // empty patterns
		{"EXAMPLE.COM", []string{"example.com"}, true},              // case-insensitive on host
	}
	for _, c := range cases {
		if got := matchesAnyDomain(c.host, c.patterns); got != c.want {
			t.Errorf("matchesAnyDomain(%q, %v) = %v want %v", c.host, c.patterns, got, c.want)
		}
	}
}

func TestSearchDomainFilterExactHostMatch(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	idx := index.NewBM25(s)
	id, _ := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://example.com/page", Title: "T", Text: "alpha",
		Source: "test", FetchedAt: time.Now(),
	})
	_ = idx.IndexDocument(context.Background(), id, "T", "alpha")

	srv := New(s)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Bare host match (exact equality, no subdomain).
	resp, _ := http.Get(httpSrv.URL + "/search?q=alpha&include_domains=example.com")
	defer resp.Body.Close()
	var sr SearchResponse
	_ = json.NewDecoder(resp.Body).Decode(&sr)
	if len(sr.Hits) != 1 {
		t.Errorf("exact host match: want 1 hit, got %d", len(sr.Hits))
	}
}

// TestStoreDocumentPublishedAtRoundTrip verifies the schema migration:
// PublishedAt persists through UpsertDocument → GetDocByURL.
func TestStoreDocumentPublishedAtRoundTrip(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	want := time.Date(2024, 3, 15, 14, 32, 0, 0, time.UTC)
	_, err := s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/a", Title: "T", Text: "x",
		Source: "test", FetchedAt: time.Now(),
		PublishedAt: want,
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := s.GetDocByURL(context.Background(), "https://x/a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.PublishedAt.Equal(want) {
		t.Errorf("PublishedAt round-trip: got %v want %v", got.PublishedAt, want)
	}

	// Zero PublishedAt round-trips as zero (not 1970).
	_, _ = s.UpsertDocument(context.Background(), &store.Document{
		URL: "https://x/b", Title: "T2", Text: "y",
		Source: "test", FetchedAt: time.Now(),
		// PublishedAt intentionally zero
	})
	got2, _ := s.GetDocByURL(context.Background(), "https://x/b")
	if !got2.PublishedAt.IsZero() {
		t.Errorf("zero PublishedAt should round-trip as zero, got %v", got2.PublishedAt)
	}
}
