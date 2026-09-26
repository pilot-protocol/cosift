package articles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/pilot-protocol/cosift/internal/store"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const testDim = 32

var (
	synthProd       = v1.Principal{ID: "synth-prod", Kind: v1.KindOIDC, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesWrite, v1.ScopeRetrieveRead}}
	synthStaging    = v1.Principal{ID: "synth-staging", Kind: v1.KindOIDC, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesWrite, v1.ScopeRetrieveRead}}
	resolverProd    = v1.Principal{ID: "resolver-prod", Kind: v1.KindOIDC, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesStub, v1.ScopeRetrieveRead}}
	resolverStaging = v1.Principal{ID: "resolver-staging", Kind: v1.KindOIDC, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesStub, v1.ScopeRetrieveRead}}
	mcpProd         = v1.Principal{ID: "mcp-prod", Kind: v1.KindOIDC, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesRead}, CountsReads: true}
	mcpStaging      = v1.Principal{ID: "mcp-staging", Kind: v1.KindOIDC, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesRead}}
	wiki            = v1.Principal{ID: "community-wiki", Kind: v1.KindKey, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesRead}}
	dashProd        = v1.Principal{ID: "dash-prod", Kind: v1.KindKey, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesReadAll, v1.ScopeArticlesModerate}}
	dashStaging     = v1.Principal{ID: "dash-staging", Kind: v1.KindKey, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesReadAll, v1.ScopeArticlesModerate}}
	goliveAdmin     = v1.Principal{ID: "golive-admin", Kind: v1.KindKey, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesAdmin}}

	allPrincipals = []v1.Principal{synthProd, synthStaging, resolverProd, resolverStaging, mcpProd, mcpStaging, wiki, dashProd, dashStaging, goliveAdmin}
)

// fakeEmbedder returns fixed vectors for known texts and a hash-seeded
// random vector otherwise.
type fakeEmbedder struct {
	mu    sync.Mutex
	vecs  map[string][]float32
	calls [][]string
	block chan struct{}
	fail  error
	model string
}

func newFakeEmbedder() *fakeEmbedder {
	return &fakeEmbedder{vecs: map[string][]float32{}, model: "fake-embed-v1"}
}

func (f *fakeEmbedder) Model() string { return f.model }
func (f *fakeEmbedder) Dim() int      { return testDim }

func (f *fakeEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), texts...))
	block, failErr := f.block, f.fail
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if failErr != nil {
		return nil, failErr
	}
	out := make([][]float32, len(texts))
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, t := range texts {
		if v, ok := f.vecs[t]; ok {
			out[i] = append([]float32(nil), v...)
			continue
		}
		out[i] = hashVec(t)
	}
	return out, nil
}

func (f *fakeEmbedder) set(text string, v []float32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.vecs[text] = v
}

func (f *fakeEmbedder) setBlock(ch chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = ch
}

func (f *fakeEmbedder) embedded(text string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		for _, t := range c {
			if t == text {
				return true
			}
		}
	}
	return false
}

func hashVec(t string) []float32 {
	sum := sha256.Sum256([]byte(t))
	r := rand.New(rand.NewPCG(binary.LittleEndian.Uint64(sum[:8]), binary.LittleEndian.Uint64(sum[8:16])))
	v := make([]float32, testDim)
	for i := range v {
		v[i] = float32(r.NormFloat64())
	}
	_ = normalize(v)
	return v
}

// axis is the unit vector along dimension i; mix has cosine c with axis(i).
func axis(i int) []float32 {
	v := make([]float32, testDim)
	v[i] = 1
	return v
}

func mix(i, j int, c float64) []float32 {
	v := make([]float32, testDim)
	v[i] = float32(c)
	v[j] = float32(math.Sqrt(1 - c*c))
	return v
}

type fakeCorpus struct{ urls map[string]bool }

func (c fakeCorpus) HasDocument(_ context.Context, u string) (bool, error) { return c.urls[u], nil }

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type harness struct {
	t      *testing.T
	dir    string
	ps     *store.PebbleStore
	s      *Store
	emb    *fakeEmbedder
	pol    *v1.FakePolicy
	corpus fakeCorpus
	clock  *fakeClock
	mux    *http.ServeMux
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{
		t:      t,
		dir:    t.TempDir(),
		emb:    newFakeEmbedder(),
		pol:    v1.NewFakePolicy(allPrincipals...),
		corpus: fakeCorpus{urls: map[string]bool{"https://example.org/doc": true, "https://example.org/other": true}},
		clock:  &fakeClock{t: time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)},
	}
	h.open(nil)
	t.Cleanup(h.shutdown)
	return h
}

func (h *harness) open(beforeRebuild func(*Store)) {
	h.t.Helper()
	ps, err := store.OpenPebble(h.dir + "/db")
	if err != nil {
		h.t.Fatal(err)
	}
	s, err := Open(Options{DB: ps.DB(), Embedder: h.emb, Policy: h.pol, Corpus: h.corpus, ThresholdsPath: h.dir + "/articles.json", Logf: func(string, ...any) {}})
	if err != nil {
		h.t.Fatal(err)
	}
	s.now = h.clock.Now
	h.ps, h.s = ps, s
	h.mux = http.NewServeMux()
	for _, rt := range s.Routes() {
		h.mux.Handle(rt.Pattern(), rt.Handler)
	}
	if beforeRebuild != nil {
		beforeRebuild(s)
		return
	}
	if err := s.Rebuild(context.Background()); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) shutdown() {
	if h.s != nil {
		if err := h.s.Close(); err != nil {
			h.t.Errorf("Close: %v", err)
		}
		if err := h.ps.Close(); err != nil {
			h.t.Errorf("pebble Close: %v", err)
		}
		h.s = nil
	}
}

func (h *harness) restart() {
	h.t.Helper()
	h.shutdown()
	h.open(nil)
}

func (h *harness) do(p v1.Principal, method, path string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var rd *bytes.Reader
	switch b := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case string:
		rd = bytes.NewReader([]byte(b))
	case []byte:
		rd = bytes.NewReader(b)
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			h.t.Fatal(err)
		}
		rd = bytes.NewReader(enc)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, v1.RequestWithPrincipal(req, p))
	return rec
}

// call does a request and decodes the JSON response, failing on another status.
func (h *harness) call(p v1.Principal, method, path string, body any, want int) map[string]any {
	h.t.Helper()
	rec := h.do(p, method, path, body)
	if rec.Code != want {
		h.t.Fatalf("%s %s as %s: status %d, want %d; body %s", method, path, p.ID, rec.Code, want, rec.Body.String())
	}
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			h.t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
	}
	return out
}

// expectError asserts an error status and code.
func (h *harness) expectError(p v1.Principal, method, path string, body any, status int, code string) map[string]any {
	h.t.Helper()
	out := h.call(p, method, path, body, status)
	if out["code"] != code {
		h.t.Fatalf("%s %s as %s: code %v, want %s; body %v", method, path, p.ID, out["code"], code, out)
	}
	return out
}

func (h *harness) raw(key []byte) ([]byte, bool) {
	h.t.Helper()
	v, closer, err := h.ps.DB().Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false
	}
	if err != nil {
		h.t.Fatal(err)
	}
	defer closer.Close()
	return append([]byte(nil), v...), true
}

func (h *harness) put(p v1.Principal, id string, body map[string]any, want int) map[string]any {
	h.t.Helper()
	return h.call(p, http.MethodPut, "/v1/articles/"+id, body, want)
}

func (h *harness) moderate(p v1.Principal, id, action, reason string, want int) map[string]any {
	h.t.Helper()
	body := map[string]any{"action": action, "by": "operator"}
	if reason != "" {
		body["reason_code"] = reason
	}
	return h.call(p, http.MethodPost, "/v1/articles/"+id+"/status", body, want)
}

func (h *harness) match(p v1.Principal, body map[string]any, want int) map[string]any {
	h.t.Helper()
	return h.call(p, http.MethodPost, "/v1/articles/match", body, want)
}

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func ulid(n int) string {
	b := []byte("01J8ZC2Q7W4X9M3K5N6P8R0000")
	for i := 25; i >= 22 && n > 0; i-- {
		b[i] = crockford[n%32]
		n /= 32
	}
	return string(b)
}

func articleBody(title string) map[string]any {
	return map[string]any{
		"status":       "published",
		"title":        title,
		"lead":         "About " + title + ". It is a subject an agent can use on its own.",
		"body_md":      "## Overview\n\n" + strings.Repeat("dense factual prose ", 110) + "[1]\n\n## Key facts\n\n- A fact [1]\n",
		"citations":    []any{map[string]any{"n": 1, "url": "https://example.org/doc", "title": "Doc", "host": "example.org", "quote": "A verbatim quote.", "content_sha": strings.Repeat("a", 64)}},
		"quality_tier": "strong",
		"vertical":     "dev-docs",
		"sensitivity":  map[string]any{"class": "none", "reason": ""},
		"models":       map[string]any{"triage": "triage-model", "writer": "writer-model"},
		"build":        map[string]any{"sources_considered": 4, "sources_used": 1, "retrieval_params": map[string]any{"bm25_k": 30}, "cost_usd": 0.01, "dense_degraded": false},
	}
}

func with(b map[string]any, kv ...any) map[string]any {
	out := map[string]any{}
	for k, v := range b {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		k := kv[i].(string)
		if kv[i+1] == nil {
			delete(out, k)
		} else {
			out[k] = kv[i+1]
		}
	}
	return out
}

func stubBodyFor(title string, topics ...string) map[string]any {
	return map[string]any{"status": "pending", "title": title, "vertical": "dev-docs", "topic_ids": topics}
}

func topic(n int) string { return fmt.Sprintf("8624225e%08x", n) }

func article(out map[string]any) map[string]any {
	a, _ := out["article"].(map[string]any)
	return a
}
