// Round 3 coverage push for cmd/cosift.
//
// Adds: openai mock HTTP server, populatedPebbleStore fixture, and
// handler/runner tests that exercise paths the earlier rounds skipped
// because they needed either a real OpenAI endpoint or a non-empty
// Pebble + HNSW index.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// ---------------------------------------------------------------------------
// openaiTestServer — pluggable httptest server speaking the OpenAI wire shape.
// ---------------------------------------------------------------------------

// openaiMock is a configurable httptest fake speaking the OpenAI HTTP shape
// for /v1/chat/completions and /v1/embeddings.
type openaiMock struct {
	srv *httptest.Server

	mu               sync.Mutex
	chatCalls        int
	embedCalls       int
	embedCallTexts   [][]string
	chatRespOverride string
	embedDim         int
}

// openaiTestServer spins up a httptest.Server that responds with a canned
// "fake response" choice for chat completions and a deterministic vector
// for embeddings.
//
// Defaults:
//   - chat returns: {"choices":[{"message":{"role":"assistant","content":"fake response"}}]}
//   - embeddings return one vector of `embedDim` floats per input (a hash of
//     the text bytes scaled to [0,1])
//
// Override per-test via SetChatResponse / SetEmbedDim.
func openaiTestServer(t *testing.T) *openaiMock {
	t.Helper()
	m := &openaiMock{embedDim: 8}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		m.chatCalls++
		resp := m.chatRespOverride
		m.mu.Unlock()
		if resp == "" {
			resp = "fake response"
		}
		// Match the SSE-style shape only when the client asked for stream=true.
		var body struct {
			Stream bool `json:"stream"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", resp)
			fmt.Fprintf(w, "data: [DONE]\n\n")
			return
		}
		out := map[string]any{
			"choices": []map[string]any{
				{"message": map[string]any{"role": "assistant", "content": resp}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
			Model string   `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		m.mu.Lock()
		m.embedCalls++
		dim := m.embedDim
		m.embedCallTexts = append(m.embedCallTexts, req.Input)
		m.mu.Unlock()
		data := make([]map[string]any, len(req.Input))
		for i, t := range req.Input {
			vec := deterministicVec(t, dim)
			data[i] = map[string]any{"embedding": vec, "index": i}
		}
		out := map[string]any{"data": data, "model": req.Model}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	m.srv = httptest.NewServer(mux)
	t.Cleanup(func() { m.srv.Close() })
	return m
}

// SetChatResponse overrides the chat completion content for subsequent calls.
func (m *openaiMock) SetChatResponse(s string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chatRespOverride = s
}

// SetEmbedDim swaps the embedding vector dimensionality.
func (m *openaiMock) SetEmbedDim(d int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.embedDim = d
}

// ChatCalls returns the number of /v1/chat/completions requests served.
func (m *openaiMock) ChatCalls() int { m.mu.Lock(); defer m.mu.Unlock(); return m.chatCalls }

// EmbedCalls returns the number of /v1/embeddings requests served.
func (m *openaiMock) EmbedCalls() int { m.mu.Lock(); defer m.mu.Unlock(); return m.embedCalls }

// URL returns the mock server base URL (e.g. http://127.0.0.1:PORT).
func (m *openaiMock) URL() string { return m.srv.URL }

// deterministicVec returns a length-dim []float32 hashed from text — same
// input → same vector, so tests don't depend on RNG state.
func deterministicVec(text string, dim int) []float32 {
	out := make([]float32, dim)
	if dim == 0 {
		return out
	}
	var sum uint32
	for _, b := range []byte(text) {
		sum = sum*31 + uint32(b)
	}
	for i := 0; i < dim; i++ {
		// Scale each component into [-1,1] using a cheap pseudo-LCG seeded
		// by the text checksum + the index.
		seed := sum*uint32(i+1) + 0x9E3779B9
		v := float32(int32(seed%2000)-1000) / 1000.0
		out[i] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// populatedPebbleStore — a Pebble store with 6 docs + their BM25 postings,
// plus a built-up HNSW graph with deterministic vectors.
// ---------------------------------------------------------------------------

type populatedFixture struct {
	dir    string
	ps     *store.PebbleStore
	idx    *index.PebbleBM25
	hnsw   *index.HNSW
	docs   []string
	dim    int
	closed bool // idempotent close guard so tests can free the dir + cleanup is a no-op
}

// Close releases the Pebble handle. Safe to call more than once — needed
// because runStats / runHNSWRebuild / runBenchPQ all need exclusive access
// and tests close the fixture before invoking them.
func (f *populatedFixture) Close() {
	if f == nil || f.closed {
		return
	}
	f.closed = true
	_ = f.ps.Close()
}

// populatedPebbleStore creates a temp Pebble dir, indexes 6 small docs,
// builds an HNSW graph keyed off deterministic vectors, and returns the
// open handles. Cleanup is registered via t.Cleanup so the caller doesn't
// need to remember to Close.
func populatedPebbleStore(t *testing.T) *populatedFixture {
	t.Helper()
	dim := 8
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}

	idx := index.NewPebbleBM25(ps)
	hnsw := index.NewHNSW(dim)
	ctx := context.Background()

	corpus := []struct{ url, title, text string }{
		{"https://x.example/raft", "Raft consensus protocol", "Raft is a distributed consensus algorithm. Leader election. Log replication."},
		{"https://x.example/paxos", "Paxos algorithm", "Paxos is the classical distributed consensus algorithm. Proposers, acceptors, learners."},
		{"https://x.example/distributed", "Distributed systems overview", "Distributed systems cover consensus, replication, partition tolerance. Includes raft and paxos."},
		{"https://x.example/cooking", "How to boil pasta", "Boil water with salt. Drop pasta. Stir occasionally. Drain when al dente."},
		{"https://x.example/networking", "Networking 101", "Networks transport packets between hosts. TCP, UDP, IP. Routing and switching."},
		{"https://x.example/docs", "Reference docs", "Documentation index. APIs, schemas, RPC contracts."},
	}
	urls := make([]string, 0, len(corpus))
	for _, d := range corpus {
		id, err := ps.UpsertDocument(ctx, &store.Document{
			URL: d.url, Title: d.title, Text: d.text, FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("UpsertDocument: %v", err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatalf("IndexDocument: %v", err)
		}
		hnsw.Add(d.url, d.title, deterministicVec(d.title+" "+d.text, dim))
		urls = append(urls, d.url)
	}
	f := &populatedFixture{
		dir: dir, ps: ps, idx: idx, hnsw: hnsw, docs: urls, dim: dim,
	}
	t.Cleanup(f.Close)
	return f
}

// makeServer builds a pebbleHTTP wired to the fixture + an optional mock
// embedder/chat. Callers attach the mock OpenAI URL via cfg-style helpers
// elsewhere; for tests we just plug in the real client pointed at our mock.
func (f *populatedFixture) makeServer(mock *openaiMock) *pebbleHTTP {
	srv := &pebbleHTTP{
		store:        f.ps,
		idx:          f.idx,
		hnsw:         f.hnsw,
		hasVectors:   true,
		vectorDim:    f.dim,
		vectorNodes:  f.hnsw.Len(),
		hydeCache:    make(map[string]string),
		hydeCacheCap: 16,
		paraCache:    make(map[string][]string),
		paraCacheCap: 16,
		started:      time.Now(),
	}
	if mock != nil {
		srv.chat = embed.NewOpenAIChat("", mock.URL()+"/v1", "gpt-4o-mini-test")
		srv.embedder = embed.NewOpenAIClient("", mock.URL()+"/v1", "text-embedding-3-small-test", f.dim)
	}
	return srv
}

// ---------------------------------------------------------------------------
// openaiTestServer self-checks
// ---------------------------------------------------------------------------

func TestOpenAITestServerChat(t *testing.T) {
	m := openaiTestServer(t)
	client := embed.NewOpenAIChat("", m.URL()+"/v1", "gpt-test")
	got, err := client.Chat(context.Background(), []embed.ChatMsg{{Role: "user", Content: "hi"}})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if got != "fake response" {
		t.Errorf("chat got %q, want 'fake response'", got)
	}
	if m.ChatCalls() != 1 {
		t.Errorf("chat calls = %d, want 1", m.ChatCalls())
	}
}

func TestOpenAITestServerEmbed(t *testing.T) {
	m := openaiTestServer(t)
	client := embed.NewOpenAIClient("", m.URL()+"/v1", "embed-test", 8)
	vecs, err := client.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vecs) != 2 || len(vecs[0]) != 8 {
		t.Errorf("vecs shape = %dx%d, want 2x8", len(vecs), len(vecs[0]))
	}
	// Deterministic — same input must give same vector.
	again, _ := client.Embed(context.Background(), []string{"hello"})
	if vecs[0][0] != again[0][0] {
		t.Errorf("non-deterministic: %v vs %v", vecs[0][0], again[0][0])
	}
}

func TestOpenAITestServerSetChatResponse(t *testing.T) {
	m := openaiTestServer(t)
	m.SetChatResponse("custom answer")
	client := embed.NewOpenAIChat("", m.URL()+"/v1", "gpt-test")
	got, err := client.Chat(context.Background(), []embed.ChatMsg{{Role: "user", Content: "?"}})
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	if got != "custom answer" {
		t.Errorf("got %q, want 'custom answer'", got)
	}
}

// ---------------------------------------------------------------------------
// populatedPebbleStore self-check
// ---------------------------------------------------------------------------

func TestPopulatedPebbleStoreBuilds(t *testing.T) {
	f := populatedPebbleStore(t)
	if len(f.docs) != 6 {
		t.Errorf("doc count = %d, want 6", len(f.docs))
	}
	if f.hnsw.Len() != 6 {
		t.Errorf("hnsw len = %d, want 6", f.hnsw.Len())
	}
	// BM25 round-trip
	hits, err := f.idx.Search(context.Background(), "raft consensus", 3)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("no BM25 hits for 'raft consensus'")
	}
}

// ---------------------------------------------------------------------------
// runStats — pebble path with a populated dir (was 0% before).
// ---------------------------------------------------------------------------

func TestRunStatsPebblePopulated(t *testing.T) {
	f := populatedPebbleStore(t)
	// runStats opens <DataDir>/pebble, so DataDir = parent of fixture dir.
	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	// Close the fixture's handle so runStats can re-open the dir (Pebble
	// holds an exclusive lock).
	f.Close()

	out := captureStdoutCosift(t, func() {
		if err := runStats(context.Background(), cfg, []string{"-backend", "pebble"}); err != nil {
			t.Errorf("runStats pebble: %v", err)
		}
	})
	if !strings.Contains(out, "backend: pebble") {
		t.Errorf("missing backend label: %s", out)
	}
	if !strings.Contains(out, "documents:") {
		t.Errorf("missing documents line: %s", out)
	}
}

// ---------------------------------------------------------------------------
// handleSearch — bm25 retrieval against populated store.
// ---------------------------------------------------------------------------

func TestHandleSearchBM25Happy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)

	req := httptest.NewRequest(http.MethodGet, "/search?q=raft+consensus&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp searchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Hits) == 0 {
		t.Errorf("no hits returned: %s", rec.Body.String())
	}
}

func TestHandleSearchMissingQ(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/search", nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleSearchBadSince(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/search?q=raft&since=NOT-A-DATE", nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (bad since), body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSearchIncludeDomains(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	q := url.QueryEscape("x.example")
	req := httptest.NewRequest(http.MethodGet, "/search?q=raft&include_domains="+q, nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSearchDenseRetriever(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	req := httptest.NewRequest(http.MethodGet, "/search?q=consensus&retriever=dense&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleSearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if mock.EmbedCalls() == 0 {
		t.Errorf("dense retriever didn't hit the mock embedder")
	}
}

// ---------------------------------------------------------------------------
// handleFindSimilar — needs a populated store + known URL.
// ---------------------------------------------------------------------------

func TestHandleFindSimilarHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	target := url.QueryEscape(f.docs[0])
	req := httptest.NewRequest(http.MethodGet, "/find_similar?url="+target+"&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilar(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleFindSimilarMissingArgs(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/find_similar", nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilar(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleFindSimilarUnknownURL(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	u := url.QueryEscape("https://x.example/never-indexed")
	req := httptest.NewRequest(http.MethodGet, "/find_similar?url="+u, nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilar(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestHandleFindSimilarText(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet,
		"/find_similar?text=consensus+algorithm+raft&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleFindSimilar(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// handleFindSimilarPOST + toValues — JSON re-encoding paths.
// ---------------------------------------------------------------------------

func TestHandleFindSimilarPOSTBadJSON(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodPost, "/find_similar",
		bytes.NewReader([]byte("not-json")))
	rec := httptest.NewRecorder()
	srv.handleFindSimilarPOST(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleFindSimilarPOSTHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	body := map[string]any{
		"url":             f.docs[0],
		"k":               3,
		"include_text":    true,
		"include_domains": "x.example",
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/find_similar", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	srv.handleFindSimilarPOST(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestSynthRequestToValuesAllFields(t *testing.T) {
	req := synthRequest{
		Q: "x", K: 7, IncludeDomains: "a.com", ExcludeDomains: "b.com",
		Since: "2025-01-01", Until: "2025-12-31",
		IncludeText: true, Rerank: true, Expand: "hyde", Stream: true,
	}
	v := req.toValues()
	want := map[string]string{
		"q": "x", "k": "7", "include_domains": "a.com", "exclude_domains": "b.com",
		"since": "2025-01-01", "until": "2025-12-31",
		"include_text": "true", "rerank": "true", "expand": "hyde", "stream": "true",
	}
	for k, expect := range want {
		if got := v.Get(k); got != expect {
			t.Errorf("toValues[%s] = %q, want %q", k, got, expect)
		}
	}
}

func TestSynthRequestToValuesEmpty(t *testing.T) {
	v := synthRequest{}.toValues()
	if len(v) != 0 {
		t.Errorf("empty toValues = %v, want empty", v)
	}
}

// ---------------------------------------------------------------------------
// handleAnswer — populated store + mock chat client.
// ---------------------------------------------------------------------------

func TestHandleAnswerHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)

	req := httptest.NewRequest(http.MethodGet, "/answer?q=raft+consensus&k=2", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswer(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if mock.ChatCalls() == 0 {
		t.Errorf("handleAnswer didn't reach the chat mock")
	}
	if !strings.Contains(rec.Body.String(), "fake response") {
		t.Errorf("missing fake response in body: %s", rec.Body.String())
	}
}

func TestHandleAnswerNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil) // no chat configured
	req := httptest.NewRequest(http.MethodGet, "/answer?q=raft", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswer(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestHandleAnswerMissingQ(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/answer", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswer(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleAnswerBadSince(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/answer?q=x&since=garbage", nil)
	rec := httptest.NewRecorder()
	srv.handleAnswer(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// handleAnswerPOST / handleResearchPOST — JSON body parsing wrappers.
// ---------------------------------------------------------------------------

func TestHandleAnswerPOSTBadJSON(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodPost, "/answer",
		bytes.NewReader([]byte("{bad-json")))
	rec := httptest.NewRecorder()
	srv.handleAnswerPOST(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleAnswerPOSTHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	body := map[string]any{"q": "consensus", "k": 2}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/answer", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	srv.handleAnswerPOST(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleResearchPOSTBadJSON(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodPost, "/research",
		bytes.NewReader([]byte("{")))
	rec := httptest.NewRecorder()
	srv.handleResearchPOST(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// handleDomains / handleQueue — visibility endpoints over the store.
// ---------------------------------------------------------------------------

func TestHandleDomainsHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/domains?limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleDomains(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["domains"]; !ok {
		t.Errorf("missing domains key: %s", rec.Body.String())
	}
}

func TestHandleDomainsTopAlias(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/domains?top=5", nil)
	rec := httptest.NewRecorder()
	srv.handleDomains(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestHandleQueueHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/queue?top=10", nil)
	rec := httptest.NewRecorder()
	srv.handleQueue(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

// ---------------------------------------------------------------------------
// runRefreshDue — sqlite store, dry-run path (no embedder needed).
// ---------------------------------------------------------------------------

func TestRunRefreshDueDryRunEmpty(t *testing.T) {
	cfg := seedTinyStore(t)
	// Empty frontier → 0 due.
	err := runRefreshDue(context.Background(), cfg, []string{"-dry-run", "-limit", "5"})
	if err != nil {
		t.Errorf("runRefreshDue: %v", err)
	}
}

func TestRunRefreshDueDryRunCustomBounds(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runRefreshDue(context.Background(), cfg, []string{
		"-dry-run", "-min", "1s", "-max", "24h", "-limit", "10",
	})
	if err != nil {
		t.Errorf("runRefreshDue: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runHNSWRebuild — needs populated Pebble dir with an HNSW persisted.
// ---------------------------------------------------------------------------

func TestRunHNSWRebuildDryRunForce(t *testing.T) {
	f := populatedPebbleStore(t)
	// Persist HNSW to disk so the rebuild can load it.
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist hnsw: %v", err)
	}
	// Close so runHNSWRebuild can re-open exclusively.
	f.Close()

	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		err := runHNSWRebuild(context.Background(), cfg, []string{
			"-dir", f.dir, "-dry-run", "-force",
		})
		if err != nil {
			t.Errorf("runHNSWRebuild: %v", err)
		}
	})
	if !strings.Contains(out, "hnsw-rebuild") {
		t.Errorf("missing rebuild output: %s", out)
	}
}

func TestRunHNSWRebuildNoZombies(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist hnsw: %v", err)
	}
	f.Close()

	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		err := runHNSWRebuild(context.Background(), cfg, []string{
			"-dir", f.dir, // no -force, no zombies → early return
		})
		if err != nil {
			t.Errorf("runHNSWRebuild: %v", err)
		}
	})
	if !strings.Contains(out, "nothing to do") {
		t.Errorf("expected 'nothing to do' msg: %s", out)
	}
}

func TestRunHNSWRebuildMissingDir(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir()}
	err := runHNSWRebuild(context.Background(), cfg, []string{
		"-dir", filepath.Join(t.TempDir(), "no-such-dir"),
	})
	if err == nil {
		t.Error("expected error opening missing dir")
	}
}

// ---------------------------------------------------------------------------
// runBenchPQ — needs a Pebble dir with persisted HNSW.
// ---------------------------------------------------------------------------

func TestRunBenchPQHappy(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatalf("persist hnsw: %v", err)
	}
	f.Close()

	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		err := runBenchPQ(context.Background(), cfg, []string{
			"-dir", f.dir, "-n", "3", "-k", "2", "-no-pq",
		})
		if err != nil {
			t.Errorf("runBenchPQ: %v", err)
		}
	})
	if !strings.Contains(out, "Recall@") {
		t.Errorf("missing Recall@ output: %s", out)
	}
}

func TestRunBenchPQBadFlags(t *testing.T) {
	cfg := &config.Config{DataDir: t.TempDir()}
	err := runBenchPQ(context.Background(), cfg, []string{"-n", "0"})
	if err == nil {
		t.Error("expected error for -n=0")
	}
}

func TestRunBenchPQNoHNSW(t *testing.T) {
	// Open and close an empty pebble dir — no HNSW persisted.
	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	_ = ps.Close()
	cfg := &config.Config{DataDir: filepath.Dir(dir)}
	err = runBenchPQ(context.Background(), cfg, []string{"-dir", dir, "-n", "2", "-k", "1"})
	if err == nil {
		t.Error("expected error for missing HNSW graph")
	}
}

// ---------------------------------------------------------------------------
// runCompactIndex — sqlite-only path; vacuum=false keeps it fast.
// ---------------------------------------------------------------------------

func TestRunCompactIndexHappy(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runCompactIndex(context.Background(), cfg, []string{"-vacuum=false"})
	if err != nil {
		t.Errorf("runCompactIndex: %v", err)
	}
}

func TestRunCompactIndexWithVacuum(t *testing.T) {
	cfg := seedTinyStore(t)
	err := runCompactIndex(context.Background(), cfg, []string{"-vacuum=true"})
	if err != nil {
		t.Errorf("runCompactIndex with vacuum: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Additional small-function coverage: parseRetrievalFilters, retrievalFilters.allow,
// parseQueryPlan, truncateForPromptLite, sortHitsByDate.
// ---------------------------------------------------------------------------

func TestParseRetrievalFiltersEmpty(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	f, err := parseRetrievalFilters(req)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(f.include) != 0 || len(f.exclude) != 0 || f.dateActive {
		t.Errorf("expected empty filter, got %+v", f)
	}
}

func TestParseRetrievalFiltersWithDates(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/x?include_domains=a.com&exclude_domains=b.com&since=2025-01-01&until=2025-12-31", nil)
	f, err := parseRetrievalFilters(req)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(f.include) != 1 || f.include[0] != "a.com" {
		t.Errorf("include = %v", f.include)
	}
	if !f.dateActive {
		t.Error("dateActive should be true")
	}
}

func TestParseRetrievalFiltersBadSince(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x?since=NOPE", nil)
	if _, err := parseRetrievalFilters(req); err == nil {
		t.Error("expected error")
	}
}

func TestParseRetrievalFiltersBadUntil(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x?until=NOPE", nil)
	if _, err := parseRetrievalFilters(req); err == nil {
		t.Error("expected error")
	}
}

func TestRetrievalFiltersAllowDomain(t *testing.T) {
	f := retrievalFilters{include: []string{"a.com"}}
	if !f.allow("https://a.com/x", time.Time{}) {
		t.Error("a.com should be allowed")
	}
	if f.allow("https://b.com/x", time.Time{}) {
		t.Error("b.com should be blocked")
	}
}

func TestRetrievalFiltersAllowExclude(t *testing.T) {
	f := retrievalFilters{exclude: []string{"b.com"}}
	if !f.allow("https://a.com/x", time.Time{}) {
		t.Error("a.com should pass exclude")
	}
	if f.allow("https://b.com/x", time.Time{}) {
		t.Error("b.com should be blocked by exclude")
	}
}

func TestRetrievalFiltersAllowDateUnknown(t *testing.T) {
	f := retrievalFilters{
		dateActive: true,
		since:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	// Zero publishedAt → blocked under any active date filter.
	if f.allow("https://a.com/x", time.Time{}) {
		t.Error("zero publishedAt should be blocked when dateActive")
	}
}

func TestRetrievalFiltersAllowDateInRange(t *testing.T) {
	f := retrievalFilters{
		dateActive: true,
		since:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		until:      time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	mid := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	if !f.allow("https://a.com/x", mid) {
		t.Error("mid-year date should pass since/until window")
	}
	before := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if f.allow("https://a.com/x", before) {
		t.Error("before-since date should be blocked")
	}
	after := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if f.allow("https://a.com/x", after) {
		t.Error("after-until date should be blocked")
	}
}

func TestParseQueryPlanValid(t *testing.T) {
	raw := `{"intent":"research","queries":["q1","q2"],"retriever":"hybrid"}`
	p := parseQueryPlan(raw, "orig")
	if len(p.Queries) != 2 || p.Queries[0] != "q1" {
		t.Errorf("queries = %v", p.Queries)
	}
	if p.Retriever != "hybrid" {
		t.Errorf("retriever = %q", p.Retriever)
	}
}

func TestParseQueryPlanGarbageFallback(t *testing.T) {
	p := parseQueryPlan("not-json", "fallback-query")
	if len(p.Queries) != 1 || p.Queries[0] != "fallback-query" {
		t.Errorf("fallback queries = %v", p.Queries)
	}
	if p.Retriever != "hybrid" {
		t.Errorf("default retriever = %q", p.Retriever)
	}
}

func TestParseQueryPlanFencedJSON(t *testing.T) {
	raw := "```json\n{\"queries\":[\"a\"],\"retriever\":\"bm25\"}\n```"
	p := parseQueryPlan(raw, "orig")
	if len(p.Queries) != 1 || p.Queries[0] != "a" {
		t.Errorf("queries = %v", p.Queries)
	}
	if p.Retriever != "bm25" {
		t.Errorf("retriever = %q", p.Retriever)
	}
}

func TestParseQueryPlanCapsQueries(t *testing.T) {
	raw := `{"queries":["1","2","3","4","5","6","7"]}`
	p := parseQueryPlan(raw, "x")
	if len(p.Queries) != 5 {
		t.Errorf("queries cap = %d, want 5", len(p.Queries))
	}
}

func TestParseQueryPlanInvalidRetriever(t *testing.T) {
	raw := `{"queries":["a"],"retriever":"weird"}`
	p := parseQueryPlan(raw, "x")
	if p.Retriever != "hybrid" {
		t.Errorf("retriever fallback = %q, want hybrid", p.Retriever)
	}
}

func TestParseQueryPlanRejectsBadSinceDays(t *testing.T) {
	neg := -1
	huge := 10000
	for _, val := range []int{neg, huge} {
		v := val
		raw := fmt.Sprintf(`{"queries":["a"],"since_days":%d}`, v)
		p := parseQueryPlan(raw, "x")
		if p.SinceDays != nil {
			t.Errorf("since_days=%d should be rejected, got %v", v, *p.SinceDays)
		}
	}
}

func TestTruncateForPromptLiteShort(t *testing.T) {
	if got := truncateForPromptLite("abc", 10); got != "abc" {
		t.Errorf("got %q", got)
	}
}

func TestTruncateForPromptLiteTruncates(t *testing.T) {
	got := truncateForPromptLite("0123456789", 4)
	if got != "0123…" {
		t.Errorf("got %q", got)
	}
}

func TestSortHitsByDateDesc(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	hits := []searchHit{
		{URL: "a", PublishedAt: &t1},
		{URL: "b", PublishedAt: nil},
		{URL: "c", PublishedAt: &t3},
		{URL: "d", PublishedAt: &t2},
	}
	sortHitsByDate(hits, false) // desc
	// Expected: c (Dec), d (Jun), a (Jan), b (nil at end)
	if hits[0].URL != "c" || hits[1].URL != "d" || hits[2].URL != "a" || hits[3].URL != "b" {
		t.Errorf("desc order wrong: %v", hits)
	}
}

func TestSortHitsByDateAsc(t *testing.T) {
	t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	hits := []searchHit{
		{URL: "b", PublishedAt: &t2},
		{URL: "a", PublishedAt: &t1},
	}
	sortHitsByDate(hits, true) // asc
	if hits[0].URL != "a" {
		t.Errorf("asc order wrong: %v", hits)
	}
}

// ---------------------------------------------------------------------------
// handleResearch + handleQuery — both require chat; both should reach the
// mock and return a JSON body containing the canned "fake response" answer.
// ---------------------------------------------------------------------------

func TestHandleResearchHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	// Plan stage of /research expects a sub-question list; the doc says
	// fallback is the original q when parsing fails, so canned "fake response"
	// is fine — handler will use [q] as the sole sub-query.
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/research?q=raft+consensus&k=2", nil)
	rec := httptest.NewRecorder()
	srv.handleResearch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if mock.ChatCalls() < 2 {
		// plan + synth = 2
		t.Errorf("expected at least 2 chat calls (plan + synth), got %d", mock.ChatCalls())
	}
}

func TestHandleResearchNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/research?q=x", nil)
	rec := httptest.NewRecorder()
	srv.handleResearch(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestHandleResearchMissingQ(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/research", nil)
	rec := httptest.NewRecorder()
	srv.handleResearch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestHandleQueryHappy(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "0")
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/query?q=raft+consensus&k=3", nil)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleQueryNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/query?q=x", nil)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("status = %d, want 501", rec.Code)
	}
}

func TestHandleQueryMissingQ(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	rec := httptest.NewRecorder()
	srv.handleQuery(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// paraphraseQuery — direct unit test against mock chat.
// ---------------------------------------------------------------------------

func TestParaphraseQueryHappy(t *testing.T) {
	mock := openaiTestServer(t)
	mock.SetChatResponse(`["alt phrasing one", "alt phrasing two"]`)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	got := srv.paraphraseQuery(context.Background(), "original query", 2)
	if len(got) != 2 {
		t.Fatalf("paraphrases = %v", got)
	}
	if got[0] != "alt phrasing one" {
		t.Errorf("first paraphrase = %q", got[0])
	}
}

func TestParaphraseQueryNoChat(t *testing.T) {
	f := populatedPebbleStore(t)
	srv := f.makeServer(nil)
	got := srv.paraphraseQuery(context.Background(), "q", 2)
	if got != nil {
		t.Errorf("expected nil when chat nil, got %v", got)
	}
}

func TestParaphraseQueryCacheHit(t *testing.T) {
	mock := openaiTestServer(t)
	mock.SetChatResponse(`["one","two"]`)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	a := srv.paraphraseQuery(context.Background(), "q", 2)
	b := srv.paraphraseQuery(context.Background(), "q", 2)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("a=%v b=%v", a, b)
	}
	if mock.ChatCalls() != 1 {
		t.Errorf("expected cached second call, chat calls = %d", mock.ChatCalls())
	}
}

func TestParaphraseQueryUnparseableResponse(t *testing.T) {
	mock := openaiTestServer(t)
	mock.SetChatResponse("definitely not json")
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	if got := srv.paraphraseQuery(context.Background(), "q", 2); got != nil {
		t.Errorf("expected nil on bad response, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// doChat / doChatStream / doRerank — counter wrappers.
// ---------------------------------------------------------------------------

func TestDoChatCountsAttempts(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	before := srv.chatAttempts.Load()
	_, err := srv.doChat(context.Background(), srv.chat,
		[]embed.ChatMsg{{Role: "user", Content: "hi"}})
	if err != nil {
		t.Fatalf("doChat: %v", err)
	}
	if srv.chatAttempts.Load() != before+1 {
		t.Errorf("attempts not incremented")
	}
}

// ---------------------------------------------------------------------------
// runPebbleInfo — text + JSON modes against populated store.
// ---------------------------------------------------------------------------

func TestRunPebbleInfoText(t *testing.T) {
	f := populatedPebbleStore(t)
	f.Close()
	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		if err := runPebbleInfo(context.Background(), cfg, []string{"-dir", f.dir}); err != nil {
			t.Errorf("runPebbleInfo: %v", err)
		}
	})
	if !strings.Contains(out, "PebbleStore:") {
		t.Errorf("missing header: %s", out)
	}
	if !strings.Contains(out, "documents:") {
		t.Errorf("missing documents line: %s", out)
	}
}

func TestRunPebbleInfoJSON(t *testing.T) {
	f := populatedPebbleStore(t)
	f.Close()
	cfg := &config.Config{DataDir: filepath.Dir(f.dir)}
	out := captureStdoutCosift(t, func() {
		if err := runPebbleInfo(context.Background(), cfg, []string{"-dir", f.dir, "-json"}); err != nil {
			t.Errorf("runPebbleInfo json: %v", err)
		}
	})
	// Should be valid JSON.
	var any map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &any); err != nil {
		t.Errorf("output not JSON: %v\n%s", err, out)
	}
}

// (Pebble auto-creates missing dirs, so the missing-dir error path isn't
// reachable from runPebbleInfo. No test for it.)

// ---------------------------------------------------------------------------
// runCrawlStatus — sqlite-only path with seeded fixture.
// ---------------------------------------------------------------------------

func TestRunCrawlStatusHappy(t *testing.T) {
	cfg := seedTinyStore(t)
	out := captureStdoutCosift(t, func() {
		if err := runCrawlStatus(context.Background(), cfg, []string{"-hosts", "3", "-errors", "2"}); err != nil {
			t.Errorf("runCrawlStatus: %v", err)
		}
	})
	if !strings.Contains(out, "crawl status") {
		t.Errorf("missing crawl status header: %s", out)
	}
}

// ---------------------------------------------------------------------------
// applyMMRPermutation — null cases (no hnsw, single URL, lambda >= 1).
// ---------------------------------------------------------------------------

func TestApplyMMRPermutationDegenerate(t *testing.T) {
	srv := &pebbleHTTP{} // no hnsw, no embedder
	if got := srv.applyMMRPermutation(context.Background(), []string{"a", "b"}, "q", 0.5); got != nil {
		t.Errorf("no hnsw → got %v, want nil", got)
	}
}

func TestApplyMMRPermutationSingleURL(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	if got := srv.applyMMRPermutation(context.Background(), []string{"only-one"}, "q", 0.5); got != nil {
		t.Errorf("single URL → got %v, want nil", got)
	}
}

func TestApplyMMRPermutationLambdaOne(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	if got := srv.applyMMRPermutation(context.Background(),
		[]string{"a", "b"}, "q", 1.0); got != nil {
		t.Errorf("lambda >= 1 → got %v, want nil", got)
	}
}

func TestApplyMMRPermutationHappy(t *testing.T) {
	mock := openaiTestServer(t)
	f := populatedPebbleStore(t)
	srv := f.makeServer(mock)
	// Use real URLs from fixture so the HNSW lookup hits.
	urls := f.docs[:3]
	got := srv.applyMMRPermutation(context.Background(), urls, "consensus algorithm", 0.5)
	if got == nil {
		t.Fatal("expected non-nil permutation")
	}
	if len(got) != len(urls) {
		t.Errorf("got %d, want %d", len(got), len(urls))
	}
}
