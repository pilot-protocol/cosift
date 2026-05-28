package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// seedReembedStore inserts N docs so the reembed loop has something to process.
// Each doc has enough text that the chunker emits ≥1 chunk.
func seedReembedStore(t *testing.T, n int) *store.Store {
	t.Helper()
	// Iter 134: OpenMemory.
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	for i := 0; i < n; i++ {
		url := "https://x/doc" + string(rune('a'+i%26))
		_, err := s.UpsertDocument(context.Background(), &store.Document{
			URL:       url,
			Title:     "Doc",
			Text:      strings.Repeat("content sentence. ", 30),
			Source:    "test",
			FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	return s
}

// reembedStubEmbedder returns a fixed-length vector for each text. Not a
// real embedder; just enough to satisfy the Embedder interface for tests.
type reembedStubEmbedder struct{ model string }

func (e *reembedStubEmbedder) Model() string { return e.model }
func (e *reembedStubEmbedder) Dim() int      { return 4 }
func (e *reembedStubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0, 0, 0}
	}
	return out, nil
}

func TestAdminReembedRequiresEmbedder(t *testing.T) {
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	// No WithVector → no embedder configured.
	srv := New(s).WithAdminToken("k")
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("want 400 without embedder, got %d", resp.StatusCode)
	}
}

func TestAdminReembedRequiresAuth(t *testing.T) {
	s := seedReembedStore(t, 1)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp, _ := http.Post(httpSrv.URL+"/admin/reembed", "application/json", strings.NewReader("{}"))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Errorf("want 401/403 without bearer, got %d", resp.StatusCode)
	}
}

func TestAdminReembedEmptyCorpus(t *testing.T) {
	// Zero docs → started + done, no progress events.
	s, _ := store.OpenMemory()
	t.Cleanup(func() { s.Close() })

	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer k")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	if !strings.Contains(out, "event: started") {
		t.Errorf("missing started event: %q", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("missing done event: %q", out)
	}
	if !strings.Contains(out, `"total_docs":0`) {
		t.Errorf("started event should report 0 total_docs: %q", out)
	}
	if !strings.Contains(out, `"docs_processed":0`) {
		t.Errorf("done event should report 0 docs_processed: %q", out)
	}
}

func TestAdminReembedSmallCorpus(t *testing.T) {
	s := seedReembedStore(t, 3)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	if !strings.Contains(out, "event: started") {
		t.Errorf("missing started: %q", out)
	}
	if !strings.Contains(out, `"target_model":"stub-v1"`) {
		t.Errorf("started should name target model: %q", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("missing done: %q", out)
	}
	// done event must report all 3 docs processed.
	if !strings.Contains(out, `"docs_processed":3`) {
		t.Errorf("done should report 3 docs processed: %q", out)
	}
	// Passages were written (chunker produces ≥1 per doc on text this size).
	if strings.Contains(out, `"passages_written":0`) {
		t.Errorf("expected non-zero passages_written: %q", out)
	}
}

func TestAdminReembedDropOld(t *testing.T) {
	// Seed: 2 docs, write passages under "old-model" first.
	s := seedReembedStore(t, 2)
	docs, _ := s.ListDocuments(context.Background(), 0)
	for _, d := range docs {
		err := s.UpsertPassage(context.Background(), &store.Passage{
			DocID:     d.ID,
			Offset:    0,
			Length:    10,
			Model:     "old-model",
			Embedding: []float32{0, 1, 0, 0},
		})
		if err != nil {
			t.Fatalf("seed old passage: %v", err)
		}
	}

	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"drop_old":true}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	if !strings.Contains(out, "event: done") {
		t.Errorf("missing done: %q", out)
	}
	// drop_old=true → done event reports dropped_old > 0
	if strings.Contains(out, `"dropped_old":0`) {
		t.Errorf("drop_old=true should drop old passages: %q", out)
	}
}

func TestAdminReembedBadInput(t *testing.T) {
	s := seedReembedStore(t, 1)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// Malformed JSON in body → 400.
	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", strings.NewReader("not json"))
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Content-Type", "application/json")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("malformed body should 400; got %d", resp.StatusCode)
	}
}

// seedMixedDateReembedStore inserts docs with mixed published_at: some pre-2026,
// some post-2026, some undated. Lets iter-116 since-filter tests exercise the
// boundary semantics.
func seedMixedDateReembedStore(t *testing.T) *store.Store {
	t.Helper()
	// Iter 134: OpenMemory.
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	docs := []store.Document{
		{URL: "https://x/old", Title: "Old", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
		{URL: "https://x/recent", Title: "Recent", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now(),
			PublishedAt: time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)},
		{URL: "https://x/undated", Title: "Undated", Text: strings.Repeat("content ", 30),
			Source: "test", FetchedAt: time.Now(),
			// PublishedAt deliberately zero
		},
	}
	for i := range docs {
		if _, err := s.UpsertDocument(context.Background(), &docs[i]); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	return s
}

func TestAdminReembedSinceFiltersOlderDocs(t *testing.T) {
	// since=2026-01-01 → old (2024) dropped, recent (2026) kept, undated dropped.
	s := seedMixedDateReembedStore(t)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"since":"2026-01-01"}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// started should report total_docs=1 (only "recent" survives the filter).
	if !strings.Contains(out, `"total_docs":1`) {
		t.Errorf("expected total_docs=1 after since filter; got %q", out)
	}
	if !strings.Contains(out, `"docs_processed":1`) {
		t.Errorf("expected docs_processed=1 in done event; got %q", out)
	}
}

func TestAdminReembedSinceMatchesNothing(t *testing.T) {
	// since=2030-01-01 → no docs match → started total_docs=0, done docs_processed=0.
	s := seedMixedDateReembedStore(t)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"since":"2030-01-01"}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	if !strings.Contains(out, `"total_docs":0`) {
		t.Errorf("expected total_docs=0 for future since; got %q", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("expected done event (no-match should succeed, not error): %q", out)
	}
	if strings.Contains(out, "event: error") {
		t.Errorf("no-match should NOT emit error event: %q", out)
	}
}

// trackingStubEmbedder counts Embed calls so iter-125 tests can verify the
// dry-run path doesn't invoke the embedder. Builds on the iter-112 reembed
// stub (which doesn't track calls) without modifying that fixture.
type trackingStubEmbedder struct {
	model      string
	embedCalls int
}

func (e *trackingStubEmbedder) Model() string { return e.model }
func (e *trackingStubEmbedder) Dim() int      { return 4 }
func (e *trackingStubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.embedCalls++
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{1, 0, 0, 0}
	}
	return out, nil
}

func TestAdminReembedDryRunSkipsEmbedLoop(t *testing.T) {
	// Iter 125: dry_run=true → started event reports count, done event
	// reports zeros + dry_run:true. The embedder is NOT invoked.
	s := seedReembedStore(t, 5)
	emb := &trackingStubEmbedder{model: "stub-v1"}
	srv := New(s).WithAdminToken("k").WithVector(nil, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"dry_run":true}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	if !strings.Contains(out, `"total_docs":5`) {
		t.Errorf("started should still report 5 docs (filter pipeline runs): %q", out)
	}
	if !strings.Contains(out, `"dry_run":true`) {
		t.Errorf("done event missing dry_run echo: %q", out)
	}
	if !strings.Contains(out, `"docs_processed":0`) {
		t.Errorf("dry-run should report 0 docs_processed: %q", out)
	}
	if !strings.Contains(out, `"passages_written":0`) {
		t.Errorf("dry-run should report 0 passages_written: %q", out)
	}
	// Critical assertion: embedder was NOT invoked (would have been called
	// once per doc batch on the non-dry-run path).
	if emb.embedCalls != 0 {
		t.Errorf("dry-run should NOT call embedder; got %d Embed() calls", emb.embedCalls)
	}
}

func TestAdminReembedDryRunComposesWithSince(t *testing.T) {
	// Dry-run + since filter: total_docs reflects POST-filter count.
	// Operator previewing "how many 2026+ docs would be reembedded?" sees
	// the filter result without spending LLM credits.
	s := seedMixedDateReembedStore(t)
	emb := &trackingStubEmbedder{model: "stub-v1"}
	srv := New(s).WithAdminToken("k").WithVector(nil, emb)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"dry_run":true,"since":"2026-01-01"}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)

	// seedMixedDateReembedStore inserts: old (2024), recent (2026), undated.
	// since=2026-01-01 keeps only "recent" → total_docs=1.
	if !strings.Contains(out, `"total_docs":1`) {
		t.Errorf("dry-run + since should filter then count; got %q", out)
	}
	if emb.embedCalls != 0 {
		t.Errorf("dry-run should NOT call embedder even with since; got %d calls", emb.embedCalls)
	}
}

func TestAdminReembedSinceMalformed(t *testing.T) {
	// Malformed since → 400 with structured error, NOT an SSE error event
	// (caller using curl expects iter-77 /search?since= behavior).
	s := seedReembedStore(t, 1)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed",
		strings.NewReader(`{"since":"yesterday"}`))
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("malformed since should 400; got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "since") {
		t.Errorf("error should mention since field: %q", body)
	}
}

func TestAdminReembedEmptyBodyOK(t *testing.T) {
	// Empty body is valid (defaults to drop_old=false). Content-Length:0 → skip JSON decode.
	s := seedReembedStore(t, 1)
	srv := New(s).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", nil)
	req.Header.Set("Authorization", "Bearer k")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("empty body should be OK; got %d", resp.StatusCode)
	}
}

// Iter 145: WithChunker override shrinks chunk size → more passages written.
// Seeds a single ~200-word doc, then runs reembed twice: once at defaults, once
// with WithChunker(50, 10). The smaller setting must produce strictly more
// passages — proves the iter-145 wiring threads through to the reembed handler.
func TestAdminReembedHonorsWithChunker(t *testing.T) {
	countPassages := func(srv *Server) int {
		httpSrv := httptest.NewServer(srv.Handler())
		defer httpSrv.Close()
		req, _ := http.NewRequest("POST", httpSrv.URL+"/admin/reembed", strings.NewReader(`{"drop_old":true}`))
		req.Header.Set("Authorization", "Bearer k")
		resp, _ := http.DefaultClient.Do(req)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		// passages_written shows up in the done event JSON.
		bs := string(body)
		// Crude but adequate: find passages_written: and parse the integer.
		idx := strings.Index(bs, `"passages_written":`)
		if idx < 0 {
			t.Fatalf("no passages_written in response: %s", bs)
		}
		rest := bs[idx+len(`"passages_written":`):]
		end := strings.IndexAny(rest, ",}\n")
		var n int
		_, err := fmt.Sscanf(rest[:end], "%d", &n)
		if err != nil {
			t.Fatalf("parse passages_written: %v (raw=%s)", err, rest[:end])
		}
		return n
	}

	seed := func() *store.Store {
		s, _ := store.OpenMemory()
		t.Cleanup(func() { s.Close() })
		_, _ = s.UpsertDocument(context.Background(), &store.Document{
			URL:       "https://x/long",
			Title:     "Long Doc",
			Text:      strings.Repeat("content sentence with multiple words. ", 50), // ~250 words
			Source:    "test",
			FetchedAt: time.Now(),
		})
		return s
	}

	defaultS := seed()
	defaultSrv := New(defaultS).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"})
	defaultN := countPassages(defaultSrv)

	tightS := seed()
	tightSrv := New(tightS).WithAdminToken("k").WithVector(nil, &reembedStubEmbedder{model: "stub-v1"}).
		WithChunker(50, 10)
	tightN := countPassages(tightSrv)

	if tightN <= defaultN {
		t.Errorf("WithChunker(50,10) must produce more passages than default; got default=%d tight=%d", defaultN, tightN)
	}
	if tightN < 3 {
		t.Errorf("tight chunker on ~250 words should produce ≥3 passages, got %d", tightN)
	}
}
