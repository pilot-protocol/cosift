package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	v1TestPepper = "v1-test-pepper-0123456789abcdef0123456789"
	v1SynthSub   = "200000000000000000001"
	v1SynthEmail = "synth-prod@cosift-test.iam.gserviceaccount.com"
)

type v1Fixture struct {
	t      *testing.T
	srv    *pebbleHTTP
	ps     *store.PebbleStore
	svc    *svcauth.Service
	signer *svcauthtest.Signer
	key    string
	logs   *bytes.Buffer
	cfg    map[string]any
	path   string
}

type fixedEmbedder struct {
	dim  int
	err  error
	mu   sync.Mutex
	seen []string
}

func (e *fixedEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	e.seen = append(e.seen, texts...)
	e.mu.Unlock()
	if e.err != nil {
		return nil, e.err
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = deterministicVec(t, e.dim)
	}
	return out, nil
}
func (e *fixedEmbedder) Model() string { return "fixed" }
func (e *fixedEmbedder) Dim() int      { return e.dim }

var v1Corpus = []struct {
	url, title, text string
	published        time.Time
}{
	{"https://x.example/raft", "Raft consensus protocol", "Raft is a distributed consensus algorithm. Leader election. Log replication.", time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"https://x.example/paxos", "Paxos algorithm", "Paxos is the classical distributed consensus algorithm. Proposers, acceptors, learners.", time.Now().Add(-24 * time.Hour)},
	{"https://en.wikipedia.org/wiki/Consensus", "Consensus (computer science)", "Consensus algorithms let distributed processes agree. Raft and Paxos are consensus algorithms.", time.Time{}},
	{"https://x.example/zephyr", "Zephyrcore", "Zephyrcore is a small runtime written in Rust.", time.Time{}},
	{"https://x.example/inventors", "Inventor biographies", "An inventor and founder is the creator of an origin story.", time.Time{}},
	{"https://x.example/cooking", "How to boil pasta", "Boil water with salt. Drop pasta. Stir occasionally.", time.Time{}},
}

// newV1Fixture is a Pebble-backed pebbleHTTP with BM25, a graph and an
// uncached embedder, behind a svcauth listener handler with a test config.
func newV1Fixture(t *testing.T) *v1Fixture {
	t.Helper()
	t.Setenv("COSIFT_BM25_MIN_IDF", "0")
	dim := 8
	ps, err := store.OpenPebble(filepath.Join(t.TempDir(), "pebble"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Close() })
	idx := index.NewPebbleBM25(ps).WithAuthority(authority.New())
	g := index.NewHNSW(dim)
	ctx := context.Background()
	for _, d := range v1Corpus {
		id, err := ps.UpsertDocument(ctx, &store.Document{URL: d.url, Title: d.title, Text: d.text, Lang: "en", FetchedAt: time.Now(), PublishedAt: d.published})
		if err != nil {
			t.Fatal(err)
		}
		if err := idx.IndexDocument(ctx, id, d.title, d.text); err != nil {
			t.Fatal(err)
		}
		g.Add(d.url, d.title, deterministicVec(d.title, dim))
	}
	srv := &pebbleHTTP{store: ps, idx: idx, authority: authority.New(), v1Embedder: &fixedEmbedder{dim: dim}}
	srv.hnswAt.Store(g)
	f := &v1Fixture{t: t, srv: srv, ps: ps, signer: svcauthtest.NewSigner("kid-1"), logs: &bytes.Buffer{}}
	key, keyID, err := svcauth.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	f.key = key
	f.cfg = map[string]any{"schema_version": 1, "listen": freeAddr(t), "principals": []any{
		map[string]any{"id": "synth-prod", "kind": "oidc", "sub": v1SynthSub, "email": v1SynthEmail, "env": "prod", "scopes": []string{"articles:read", "articles:write", "retrieve:read"}, "rpm": 6000},
		map[string]any{"id": "community-wiki", "kind": "key", "keys": []any{map[string]any{"key_id": keyID, "digest": svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), key))}}, "env": "prod", "scopes": []string{"articles:read"}, "rpm": 6000},
	}}
	f.path = filepath.Join(t.TempDir(), "service-auth.json")
	f.writeConfig(f.cfg)
	return f
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func (f *v1Fixture) writeConfig(v any) {
	b, _ := json.Marshal(v)
	if err := os.WriteFile(f.path, b, 0o640); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(f.path, 0o640); err != nil {
		f.t.Fatal(err)
	}
}

func (f *v1Fixture) options(rd *v1.Readiness) svcauth.Options {
	src := svcauthtest.NewSource(svcauthtest.JWKS(f.signer), 21600)
	return svcauth.Options{Path: f.path, OwnerUID: uint32(os.Getuid()), Pepper: []byte(v1TestPepper), Readiness: rd, Logger: log.New(f.logs, "", 0), CertSource: src.Fetch}
}

// listener mounts routes (default: retrieval plus the article stubs) on a
// Service with the fixture's config installed, without opening a socket.
func (f *v1Fixture) listener(routes []v1.Route) *svcauth.Service {
	f.t.Helper()
	rd := &v1.Readiness{}
	rd.SetReady()
	svc, err := svcauth.New(f.options(rd))
	if err != nil {
		f.t.Fatal(err)
	}
	if routes == nil {
		routes = append(f.srv.v1RetrievalRoutes(), articleStubRoutes()...)
	}
	if err := svc.Mount(routes); err != nil {
		f.t.Fatal(err)
	}
	svc.Start(context.Background())
	f.t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	if svc.Addr() == "" {
		f.t.Fatalf("listener down:\n%s", f.logs.String())
	}
	f.svc = svc
	return svc
}

func (f *v1Fixture) token() string {
	return f.signer.Token(svcauthtest.Claims(v1SynthSub, v1SynthEmail, time.Now()))
}

// articleStubRoutes is the ARTICLES.md §4 table with no-op handlers.
func articleStubRoutes() []v1.Route {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { v1.WriteJSON(w, http.StatusOK, map[string]any{}) })
	read := []v1.Scope{v1.ScopeArticlesRead, v1.ScopeArticlesReadAll}
	return []v1.Route{
		{Method: "POST", Path: "/v1/articles/match", Scopes: []v1.Scope{v1.ScopeArticlesRead}, BodyLimit: 8 << 10, Handler: ok},
		{Method: "GET", Path: "/v1/articles/{id}", Scopes: read, Handler: ok},
		{Method: "GET", Path: "/v1/articles/by-slug/{slug}", Scopes: read, Handler: ok},
		{Method: "GET", Path: "/v1/articles", Scopes: read, Handler: ok},
		{Method: "PUT", Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesWrite, v1.ScopeArticlesStub}, BodyLimit: 512 << 10, Handler: ok},
		{Method: "DELETE", Path: "/v1/articles/{id}", Scopes: []v1.Scope{v1.ScopeArticlesStub}, Handler: ok},
		{Method: "POST", Path: "/v1/articles/{id}/status", Scopes: []v1.Scope{v1.ScopeArticlesModerate}, BodyLimit: 4 << 10, Handler: ok},
		{Method: "POST", Path: "/v1/articles/purge-prelive", Scopes: []v1.Scope{v1.ScopeArticlesAdmin}, BodyLimit: 1 << 10, Handler: ok},
		{Method: "GET", Path: "/v1/articles/stats", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: ok},
		{Method: "GET", Path: "/v1/article-versions/{id}", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: ok},
		{Method: "GET", Path: "/v1/article-versions/{id}/{version}", Scopes: []v1.Scope{v1.ScopeArticlesReadAll}, Handler: ok},
	}
}

// v1Call runs a /v1 retrieval handler directly with the synth principal.
func (f *v1Fixture) v1Call(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/search", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r = v1.RequestWithPrincipal(r, v1.Principal{ID: "synth-prod", Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeRetrieveRead}})
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

type v1SearchOut struct {
	Retriever string        `json:"retriever"`
	K         int           `json:"k"`
	Hits      []v1SearchHit `json:"hits"`
	TookMS    *int64        `json:"took_ms"`
}

func (f *v1Fixture) search(body string) (int, v1SearchOut, *httptest.ResponseRecorder) {
	w := f.v1Call(f.srv.handleV1Search, body)
	var out v1SearchOut
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			f.t.Fatal(err)
		}
	}
	return w.Code, out, w
}

func v1ErrCode(t *testing.T, w *httptest.ResponseRecorder) v1.Error {
	t.Helper()
	var e v1.Error
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("%s: %v", w.Body.String(), err)
	}
	return e
}

func TestV1SearchBM25(t *testing.T) {
	f := newV1Fixture(t)
	code, out, w := f.search(`{"q":"distributed consensus","k":2}`)
	if code != http.StatusOK || out.Retriever != "bm25" || out.K != 2 || len(out.Hits) != 2 || out.TookMS == nil {
		t.Fatalf("%d %s", code, w.Body.String())
	}
	raw, err := f.srv.idx.Search(context.Background(), "distributed consensus", 2)
	if err != nil {
		t.Fatal(err)
	}
	for i, h := range out.Hits {
		if h.Rank != i+1 || h.URL != raw[i].URL || h.Score != raw[i].Score || h.Title == "" {
			t.Fatalf("hit %d %+v, raw %+v", i, h, raw[i])
		}
	}
	if code, out, _ := f.search(`{"q":"consensus"}`); code != 200 || out.K != 30 {
		t.Fatalf("default k: %d %+v", code, out)
	}
	var sawDate, sawNone bool
	_, out, w = f.search(`{"q":"consensus algorithm","k":10}`)
	for _, h := range out.Hits {
		sawDate = sawDate || h.PublishedAt != nil
		sawNone = sawNone || h.PublishedAt == nil
	}
	if !sawDate || !sawNone || strings.Contains(w.Body.String(), `"published_at":"0001`) {
		t.Fatalf("published_at omitted when unknown: %s", w.Body.String())
	}
}

// AR §8 "Search": no decay whatever COSIFT_DEFAULT_DECAY_DAYS says, and the
// fixture is one where /search would decay.
func TestV1SearchNoDecay(t *testing.T) {
	t.Setenv("COSIFT_DEFAULT_DECAY_DAYS", "30")
	f := newV1Fixture(t)
	_, out, _ := f.search(`{"q":"distributed consensus algorithm","k":5}`)
	raw, _ := f.srv.idx.Search(context.Background(), "distributed consensus algorithm", 5)
	if len(out.Hits) != len(raw) {
		t.Fatalf("%d hits, raw %d", len(out.Hits), len(raw))
	}
	for i := range raw {
		if out.Hits[i].Score != raw[i].Score {
			t.Fatalf("hit %d score %v, raw %v", i, out.Hits[i].Score, raw[i].Score)
		}
	}
	w := httptest.NewRecorder()
	f.srv.handleSearch(w, httptest.NewRequest("GET", "/search?q=distributed+consensus+algorithm&k=5", nil))
	var classic searchResponse
	_ = json.Unmarshal(w.Body.Bytes(), &classic)
	if !strings.Contains(classic.Retriever, "decay") {
		t.Fatalf("/search did not decay this fixture: %s", w.Body.String())
	}
}

// AR §8 "Search": an entity-shaped query is not rewritten.
func TestV1SearchNoExpansion(t *testing.T) {
	f := newV1Fixture(t)
	q := "who created zephyrcore"
	_, out, _ := f.search(`{"q":"` + q + `","k":10}`)
	raw, _ := f.srv.idx.Search(context.Background(), q, 10)
	if len(out.Hits) != len(raw) {
		t.Fatalf("%d hits, raw %d", len(out.Hits), len(raw))
	}
	for i := range raw {
		if out.Hits[i].URL != raw[i].URL {
			t.Fatalf("hit %d %s, raw %s", i, out.Hits[i].URL, raw[i].URL)
		}
		if out.Hits[i].URL == "https://x.example/inventors" {
			t.Fatal("the rewrite's terms matched")
		}
	}
	expanded, eff, _ := f.srv.retrieveWithExpansionIdx(context.Background(), f.srv.idx, q, 10, "")
	found := false
	for _, h := range expanded {
		found = found || h.URL == "https://x.example/inventors"
	}
	if eff == q || !found {
		t.Fatalf("the fixture does not expose the rewrite (%q)", eff)
	}
}

// AR §8 "Search": /v1/search never writes the query log.
func TestV1SearchWritesNoQueryLog(t *testing.T) {
	f := newV1Fixture(t)
	qlog := filepath.Join(t.TempDir(), "qlog.jsonl")
	fh, err := os.OpenFile(qlog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	f.srv.qlogFile = fh
	svc := f.listener(nil)
	for _, body := range []string{`{"q":"consensus"}`, `{"q":"consensus","retriever":"dense"}`, `{"q":""}`} {
		req, _ := http.NewRequest("POST", "http://"+svc.Addr()+"/v1/search", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+f.token())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if b, _ := os.ReadFile(qlog); len(b) != 0 {
		t.Fatalf("query log: %s", b)
	}
	f.srv.qlog(f.srv.handleSearch)(httptest.NewRecorder(), httptest.NewRequest("GET", "/search?q=consensus", nil))
	if b, _ := os.ReadFile(qlog); len(b) == 0 {
		t.Fatal("the query log in this fixture records nothing; the assertion above is vacuous")
	}
}

func TestV1SearchValidation(t *testing.T) {
	f := newV1Fixture(t)
	cases := []struct {
		body              string
		code              int
		errCode, field, r string
	}{
		{`{}`, 422, "invalid_field", "q", "required"},
		{`{"q":""}`, 422, "invalid_field", "q", "required"},
		{`{"q":"` + strings.Repeat("a", 513) + `"}`, 422, "invalid_field", "q", "too_long"},
		{`{"q":"a\u0007b"}`, 422, "invalid_field", "q", "invalid_chars"},
		{`{"q":"a​b"}`, 422, "invalid_field", "q", "invalid_chars"},
		{`{"q":"a\nb"}`, 422, "invalid_field", "q", "invalid_chars"},
		{`{"q":"x","k":0}`, 422, "invalid_field", "k", "range"},
		{`{"q":"x","k":51}`, 422, "invalid_field", "k", "range"},
		{`{"q":"x","retriever":"hybrid"}`, 422, "invalid_field", "retriever", "enum"},
		{`{"q":"x","k":"5"}`, 400, "invalid_body", "", ""},
		{`{"q":"x","decay":"30"}`, 400, "unknown_field", "decay", ""},
		{`{"q":"x","rerank":true}`, 400, "unknown_field", "rerank", ""},
		{`{"q":"x","expand":"hyde"}`, 400, "unknown_field", "expand", ""},
		{`{"q":"x","mmr":0.5}`, 400, "unknown_field", "mmr", ""},
		{`{"q":"x","include_domains":"a.com"}`, 400, "unknown_field", "include_domains", ""},
		{`{"q":"x","site":"a.com"}`, 400, "unknown_field", "site", ""},
		{`{"q":"x","since":"2020-01-01"}`, 400, "unknown_field", "since", ""},
		{`[1]`, 400, "invalid_body", "", ""},
	}
	for _, tc := range cases {
		w := f.v1Call(f.srv.handleV1Search, tc.body)
		e := v1ErrCode(t, w)
		if w.Code != tc.code || e.Code != tc.errCode || e.Field != tc.field || e.Rule != tc.r {
			t.Errorf("%.60s: %d %+v", tc.body, w.Code, e)
		}
		if strings.Contains(w.Body.String(), "aaaa") {
			t.Errorf("error echoes the value: %s", w.Body.String())
		}
	}
	if code, _, _ := f.search(`{"q":"` + strings.Repeat("a", 512) + `","k":50}`); code != 200 {
		t.Fatalf("512-byte q, k 50: %d", code)
	}
	r := httptest.NewRequest("POST", "/v1/search", strings.NewReader(`{"q":"x"}`))
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	f.srv.handleV1Search(w, r)
	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain: %d", w.Code)
	}
	empty := &pebbleHTTP{}
	w = f.v1Call(empty.handleV1Search, `{"q":"x"}`)
	if e := v1ErrCode(t, w); w.Code != 503 || e.Code != "index_unavailable" {
		t.Fatalf("no store: %d %+v", w.Code, e)
	}
}

func TestV1SearchDense(t *testing.T) {
	f := newV1Fixture(t)
	code, out, w := f.search(`{"q":"Paxos algorithm","k":3,"retriever":"dense"}`)
	if code != 200 || out.Retriever != "dense" || len(out.Hits) != 3 {
		t.Fatalf("%d %s", code, w.Body.String())
	}
	g := f.srv.hnsw()
	want := f.srv.applyAuthorityToDense(g.Search(context.Background(), deterministicVec("Paxos algorithm", 8), 3+denseFetchSlack()))
	for i, h := range out.Hits {
		if h.URL != want[i].URL || h.Score != want[i].Score {
			t.Fatalf("hit %d %+v, want %+v", i, h, want[i])
		}
	}
	if m := authority.New().Multiplier("en.wikipedia.org"); m == 1 {
		t.Fatal("the fixture's authority scorer is flat; the multiplier assertion is vacuous")
	}
	if e := f.srv.v1Embedder.(*fixedEmbedder); len(e.seen) != 1 || e.seen[0] != "Paxos algorithm" {
		t.Fatalf("embedded %q", e.seen)
	}
}

// AR §4.11: dense is 503 dense_unavailable with no graph, no embedder, a
// failing embedder or the graph lock unavailable, and never falls back.
func TestV1SearchDenseUnavailable(t *testing.T) {
	f := newV1Fixture(t)
	check := func(stage string) {
		t.Helper()
		start := time.Now()
		w := f.v1Call(f.srv.handleV1Search, `{"q":"Paxos","retriever":"dense"}`)
		if e := v1ErrCode(t, w); w.Code != 503 || e.Code != "dense_unavailable" || w.Header().Get("Retry-After") == "" {
			t.Fatalf("%s: %d %s", stage, w.Code, w.Body.String())
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("%s: waited %s", stage, d)
		}
	}
	emb := f.srv.v1Embedder
	f.srv.v1Embedder = nil
	f.srv.embedder = emb
	check("no uncached embedder")
	f.srv.v1Embedder = &fixedEmbedder{dim: 8, err: context.DeadlineExceeded}
	check("embedder failing")
	f.srv.v1Embedder = emb
	g := f.srv.hnsw()
	f.srv.hnswAt.Store(nil)
	check("no graph")
	f.srv.hnswAt.Store(g)
	if code, _, _ := f.search(`{"q":"Paxos","retriever":"dense"}`); code != 200 {
		t.Fatalf("restored: %d", code)
	}
}

// AR §4.11: the compaction's in-memory phase holds the graph write lock, so
// dense is 503 then; its persist phase does not make dense unavailable.
func TestV1SearchDenseDuringCompaction(t *testing.T) {
	f := newV1Fixture(t)
	g := f.srv.hnsw()
	if g.MarkURLPassagesInvalid("https://x.example/cooking") == 0 {
		t.Fatal("no node invalidated")
	}
	inMemory, persist := make(chan struct{}), make(chan struct{})
	release1, release2 := make(chan struct{}), make(chan struct{})
	var once1, once2 sync.Once
	done := make(chan error, 1)
	go func() {
		_, err := g.CompactPersist(context.Background(), f.ps, false, nil, func(p index.CompactProgress) {
			switch p.Phase {
			case "compact:url-index":
				once1.Do(func() { close(inMemory); <-release1 })
			case "persist":
				once2.Do(func() { close(persist); <-release2 })
			}
		})
		done <- err
	}()
	<-inMemory
	start := time.Now()
	w := f.v1Call(f.srv.handleV1Search, `{"q":"Paxos","retriever":"dense"}`)
	if e := v1ErrCode(t, w); w.Code != 503 || e.Code != "dense_unavailable" {
		t.Fatalf("during the in-memory phase: %d %s", w.Code, w.Body.String())
	}
	if d := time.Since(start); d < v1DenseLockWait || d > time.Second {
		t.Fatalf("gave up after %s", d)
	}
	if code, out, _ := f.search(`{"q":"Paxos consensus"}`); code != 200 || len(out.Hits) == 0 {
		t.Fatalf("bm25 during the compaction: %d", code)
	}
	close(release1)
	<-persist
	if code, out, w := f.search(`{"q":"Paxos algorithm","k":3,"retriever":"dense"}`); code != 200 || len(out.Hits) == 0 {
		t.Fatalf("during the persist phase: %d %s", code, w.Body.String())
	}
	close(release2)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestV1Contents(t *testing.T) {
	f := newV1Fixture(t)
	big := strings.Repeat("é", 200<<10)
	if _, err := f.ps.UpsertDocument(context.Background(), &store.Document{URL: "https://x.example/big", Title: "Big", Text: "a" + big, FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	w := f.v1Call(f.srv.handleV1Contents, `{"urls":["https://x.example/paxos","https://example.org/missing","https://x.example/big"]}`)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	var out struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 3 {
		t.Fatalf("%d results", len(out.Results))
	}
	p, miss, b := out.Results[0], out.Results[1], out.Results[2]
	if p["url"] != "https://x.example/paxos" || p["found"] != true || p["title"] != "Paxos algorithm" || p["lang"] != "en" || p["truncated"] != false || p["published_at"] == nil || p["fetched_at"] == nil || !strings.HasPrefix(p["text"].(string), "Paxos is") {
		t.Fatalf("found item %v", p)
	}
	if len(miss) != 2 || miss["url"] != "https://example.org/missing" || miss["found"] != false {
		t.Fatalf("missing item %v", miss)
	}
	text := b["text"].(string)
	if b["truncated"] != true || len(text) > v1MaxTextBytes || len(text) < v1MaxTextBytes-1 || !strings.HasSuffix(text, "é") {
		t.Fatalf("truncated item: %d bytes, truncated=%v", len(text), b["truncated"])
	}
	if _, ok := b["published_at"]; ok {
		t.Fatal("unknown published_at not omitted")
	}
	many := `{"urls":[` + strings.TrimSuffix(strings.Repeat(`"https://x.example/a",`, 21), ",") + `]}`
	for _, tc := range []struct {
		body, rule string
		code       int
	}{
		{`{}`, "required", 422},
		{`{"urls":[]}`, "required", 422},
		{many, "too_many", 422},
		{`{"urls":[""]}`, "length", 422},
		{`{"urls":["https://x.example/` + strings.Repeat("a", 2048) + `"]}`, "length", 422},
		{`{"urls":["x"],"text":true}`, "", 400},
	} {
		w := f.v1Call(f.srv.handleV1Contents, tc.body)
		if e := v1ErrCode(t, w); w.Code != tc.code || e.Rule != tc.rule {
			t.Errorf("%.50s: %d %+v", tc.body, w.Code, e)
		}
	}
	twenty := `{"urls":[` + strings.TrimSuffix(strings.Repeat(`"https://x.example/a",`, 20), ",") + `]}`
	if w := f.v1Call(f.srv.handleV1Contents, twenty); w.Code != 200 {
		t.Fatalf("20 urls: %d", w.Code)
	}
}

func TestCutUTF8(t *testing.T) {
	for _, tc := range []struct {
		s    string
		n    int
		want string
		cut  bool
	}{
		{"abc", 3, "abc", false},
		{"abcd", 3, "abc", true},
		{"aé", 2, "a", true},
		{"日本", 4, "日", true},
		{"日本", 6, "日本", false},
	} {
		if got, cut := cutUTF8(tc.s, tc.n); got != tc.want || cut != tc.cut {
			t.Errorf("cutUTF8(%q, %d) = %q %v", tc.s, tc.n, got, cut)
		}
	}
}

var muxPattern = regexp.MustCompile(`mux\.HandleFunc\("([A-Z]+) (/[^"]*)"`)

// §1 / §10.6: every route of the :7777 mux, as registered in serve_setup.go,
// is the JSON 404 on the /v1 listener, even with a valid credential.
func TestPort7777RoutesUnreachableOnV1(t *testing.T) {
	src, err := os.ReadFile("serve_setup.go")
	if err != nil {
		t.Fatal(err)
	}
	matches := muxPattern.FindAllStringSubmatch(string(src), -1)
	if len(matches) < 50 {
		t.Fatalf("found only %d :7777 patterns", len(matches))
	}
	f := newV1Fixture(t)
	svc := f.listener(nil)
	fill := regexp.MustCompile(`\{[a-z]+(\.\.\.)?\}`)
	for _, m := range matches {
		path := fill.ReplaceAllString(m[2], "x")
		for _, cred := range []string{f.token(), f.key} {
			req, _ := http.NewRequest(m[1], "http://"+svc.Addr()+path, strings.NewReader(`{}`))
			req.Header.Set("Authorization", "Bearer "+cred)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 404 || !strings.Contains(string(body), `"code":"not_found"`) || resp.Header.Get("Location") != "" {
				t.Errorf("%s %s reached: %d %s", m[1], path, resp.StatusCode, body)
			}
		}
	}
	if strings.Count(f.logs.String(), "route=unmatched status=404") != 2*len(matches) {
		t.Fatalf("not every request was unmatched:\n%s", f.logs.String())
	}
}

// AR §4: the full table, retrieval plus the article routes, registers on one
// mux.
func TestV1RouteTableRegisters(t *testing.T) {
	f := newV1Fixture(t)
	routes := append(f.srv.v1RetrievalRoutes(), articleStubRoutes()...)
	mux := http.NewServeMux()
	for _, rt := range routes {
		mux.Handle(rt.Pattern(), rt.Handler)
	}
	svc, err := svcauth.New(f.options(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Mount(routes); err != nil {
		t.Fatal(err)
	}
	for _, rt := range f.srv.v1RetrievalRoutes() {
		if !rt.Ungated || len(rt.Scopes) != 1 || rt.Scopes[0] != v1.ScopeRetrieveRead {
			t.Fatalf("%s: %+v", rt.Pattern(), rt)
		}
	}
}

func TestV1RetrievalThroughListener(t *testing.T) {
	f := newV1Fixture(t)
	svc := f.listener(nil)
	post := func(path, body, cred string) (int, string) {
		req, _ := http.NewRequest("POST", "http://"+svc.Addr()+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if cred != "" {
			req.Header.Set("Authorization", "Bearer "+cred)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := post("/v1/search", `{"q":"consensus"}`, f.token()); code != 200 || !strings.Contains(body, `"retriever":"bm25"`) {
		t.Fatalf("search: %d %s", code, body)
	}
	if code, body := post("/v1/contents", `{"urls":["https://x.example/raft"]}`, f.token()); code != 200 || !strings.Contains(body, `"found":true`) {
		t.Fatalf("contents: %d %s", code, body)
	}
	if code, _ := post("/v1/search", `{"q":"consensus"}`, f.key); code != 403 {
		t.Fatalf("search with an articles:read key: %d", code)
	}
	if code, _ := post("/v1/search", `{"q":"`+strings.Repeat("a", 9<<10)+`"}`, f.token()); code != 413 {
		t.Fatalf("9 KiB search body: %d", code)
	}
	if code, _ := post("/v1/contents", `{"urls":["`+strings.Repeat("a", 17<<10)+`"]}`, f.token()); code != 413 {
		t.Fatalf("17 KiB contents body: %d", code)
	}
	if code, _ := post("/v1/search", `{"q":"consensus"}`, ""); code != 401 {
		t.Fatalf("no credential: %d", code)
	}
}

// §1.2: a SIGHUP with a good file starts a listener that was down, a bad and
// a deleted file keep the previous config.
func TestSIGHUPReload(t *testing.T) {
	f := newV1Fixture(t)
	good := f.cfg
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}
	oldPath, oldUID, oldSrc := svcAuthPath, svcAuthOwnerUID, v1CertSource
	svcAuthPath, svcAuthOwnerUID = f.path, uint32(os.Getuid())
	v1CertSource = svcauthtest.NewSource(svcauthtest.JWKS(f.signer), 21600).Fetch
	t.Cleanup(func() { svcAuthPath, svcAuthOwnerUID, v1CertSource = oldPath, oldUID, oldSrc })
	t.Setenv("COSIFT_SVC_PEPPER", v1TestPepper)
	var rs v1.Reloaders
	stop := handleSIGHUP(&rs)
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := f.srv.startV1(ctx, &config.Config{}, &rs)
	defer l.stop()
	if l.svc.Addr() != "" {
		t.Fatal("listening without a config")
	}
	reloads := func(result string) string {
		var b bytes.Buffer
		l.svc.Metrics.WritePrometheus(&b)
		return regexp.MustCompile(`cosift_v1_config_reloads_total\{result="` + result + `"\} (\d+)`).FindStringSubmatch(b.String())[1]
	}
	hup := func(result, want string) {
		t.Helper()
		if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for reloads(result) != want {
			if time.Now().After(deadline) {
				t.Fatalf("reloads{%s} = %s, want %s", result, reloads(result), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	search := func() int {
		req, _ := http.NewRequest("POST", "http://"+l.svc.Addr()+"/v1/search", strings.NewReader(`{"q":"consensus"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+f.token())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	f.writeConfig(good)
	hup("ok", "1")
	if l.svc.Addr() == "" {
		t.Fatal("a SIGHUP with a valid file did not start the listener")
	}
	deadline := time.Now().Add(5 * time.Second)
	for search() != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("the started listener never served")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.writeConfig(map[string]any{"schema_version": 1})
	hup("error", "1")
	if code := search(); code != http.StatusOK {
		t.Fatalf("after a bad file: %d", code)
	}
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}
	hup("error", "2")
	if code := search(); code != http.StatusOK {
		t.Fatalf("after a deleted file: %d", code)
	}
	restricted := map[string]any{"schema_version": 1, "principals": []any{good["principals"].([]any)[1]}}
	f.writeConfig(restricted)
	hup("ok", "2")
	if code := search(); code != http.StatusUnauthorized {
		t.Fatalf("after removing the principal: %d", code)
	}
}

func TestServeUnitReloadLines(t *testing.T) {
	b, err := os.ReadFile("../../scripts/cosift-serve.service")
	if err != nil {
		t.Fatal(err)
	}
	section := ""
	found := map[string]string{}
	for _, l := range strings.Split(string(b), "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "[") {
			section = l
			continue
		}
		if l == "ExecReload=/bin/kill -HUP $MAINPID" || l == "RestartForceExitStatus=SIGHUP" {
			found[l] = section
		}
	}
	for _, want := range []string{"ExecReload=/bin/kill -HUP $MAINPID", "RestartForceExitStatus=SIGHUP"} {
		if found[want] != "[Service]" {
			t.Errorf("%s not in [Service] (%q)", want, found[want])
		}
	}
}

func TestMetricsIncludeV1(t *testing.T) {
	f := newV1Fixture(t)
	f.srv.v1svc = f.listener(nil)
	w := httptest.NewRecorder()
	f.srv.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, want := range []string{"cosift_v1_config_reloads_total", "cosift_v1_server_errors_total", "cosift_v1_cert_refresh_total"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}
