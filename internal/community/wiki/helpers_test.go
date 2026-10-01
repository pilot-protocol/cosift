package wiki

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testKey = "csk_0123456789abcdef_ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRST"

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type reply struct {
	status   int
	body     string
	ctype    string
	location string
	delay    time.Duration
	block    chan struct{}
}

func jsonReply(status int, v any) reply {
	b, _ := json.Marshal(v)
	return reply{status: status, body: string(b)}
}

func errReply(status int, code string) reply {
	return jsonReply(status, map[string]any{"error": http.StatusText(status), "status": status, "code": code, "detail": "x"})
}

type call struct {
	method, path, query string
	header              http.Header
}

// fakeV1 stands in for the engine's /v1 listener on loopback.
type fakeV1 struct {
	*httptest.Server
	mu    sync.Mutex
	slugs map[string]reply
	items []map[string]any
	pages map[int]reply
	calls []call
}

func newFakeV1(t *testing.T) *fakeV1 {
	f := &fakeV1{slugs: map[string]reply{}, pages: map[int]reply{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeV1) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.calls = append(f.calls, call{r.Method, r.URL.EscapedPath(), r.URL.RawQuery, r.Header.Clone()})
	var rep reply
	var ok bool
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/articles/by-slug/"):
		rep, ok = f.slugs[strings.TrimPrefix(r.URL.Path, "/v1/articles/by-slug/")]
		if !ok {
			rep, ok = errReply(404, "not_found"), true
		}
	case r.URL.Path == "/v1/articles":
		rep, ok = f.listPage(r.URL.Query().Get("cursor"))
	}
	f.mu.Unlock()
	if !ok {
		rep = errReply(404, "not_found")
	}
	if rep.block != nil {
		<-rep.block
	}
	if rep.delay > 0 {
		time.Sleep(rep.delay)
	}
	if rep.ctype == "" {
		rep.ctype = "application/json"
	}
	w.Header().Set("Content-Type", rep.ctype)
	w.Header().Set("Cache-Control", "no-store")
	if rep.location != "" {
		w.Header().Set("Location", rep.location)
	}
	w.WriteHeader(rep.status)
	_, _ = io.WriteString(w, rep.body)
}

// listPage pages f.items two at a time; pages[n] overrides page n (0-based).
func (f *fakeV1) listPage(cursor string) (reply, bool) {
	n := 0
	if cursor != "" {
		if _, err := fmt.Sscanf(cursor, "page%d", &n); err != nil {
			return errReply(400, "invalid_cursor"), true
		}
	}
	if rep, ok := f.pages[n]; ok {
		return rep, true
	}
	lo, hi := n*2, min(n*2+2, len(f.items))
	items := f.items[min(lo, len(f.items)):hi]
	var next any
	if hi < len(f.items) {
		next = fmt.Sprintf("page%d", n+1)
	}
	return jsonReply(200, map[string]any{"items": append([]map[string]any{}, items...), "next_cursor": next}), true
}

func (f *fakeV1) set(slug string, rep reply) {
	f.mu.Lock()
	f.slugs[slug] = rep
	f.mu.Unlock()
}

func (f *fakeV1) list(items ...map[string]any) {
	f.mu.Lock()
	f.items = items
	f.mu.Unlock()
}

func (f *fakeV1) failPage(n int, rep reply) {
	f.mu.Lock()
	f.pages[n] = rep
	f.mu.Unlock()
}

func (f *fakeV1) clearPages() {
	f.mu.Lock()
	f.pages = map[int]reply{}
	f.mu.Unlock()
}

func (f *fakeV1) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeV1) slugCalls(slug string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.path == "/v1/articles/by-slug/"+slug {
			n++
		}
	}
	return n
}

func (f *fakeV1) allCalls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

type harness struct {
	t     *testing.T
	w     *Wiki
	mux   *http.ServeMux
	v1    *fakeV1
	clock *clock
	logs  *logBuf
}

type logBuf struct {
	mu    sync.Mutex
	lines []string
}

func (l *logBuf) printf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *logBuf) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func testConfig(engineURL string, c *clock, logs *logBuf) Config {
	return Config{Public: true, EngineURL: engineURL, EngineKey: testKey, ReportMailto: "reports@example.org",
		PublicURL: "https://cosift.example", ClientIP: remoteIP, Now: c.Now, Logf: logs.printf}
}

func newHarness(t *testing.T, edit ...func(*Config)) *harness {
	t.Helper()
	h := &harness{t: t, v1: newFakeV1(t), clock: newClock(), logs: &logBuf{}}
	cfg := testConfig(h.v1.URL, h.clock, h.logs)
	for _, e := range edit {
		e(&cfg)
	}
	w, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	h.mux = http.NewServeMux()
	w.Mount(h.mux)
	return h
}

func (h *harness) get(path string, headers ...string) *httptest.ResponseRecorder {
	h.t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		switch headers[i] {
		case "RemoteAddr":
			r.RemoteAddr = headers[i+1]
			continue
		case "Host":
			r.Host = headers[i+1]
			continue
		}
		r.Header.Add(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, r)
	return rec
}

func (h *harness) refresh() {
	h.t.Helper()
	if err := h.w.Refresh(context.Background()); err != nil {
		h.t.Fatal(err)
	}
}

func cacheControl(rec *httptest.ResponseRecorder) string { return rec.Header().Get("Cache-Control") }

func wantPage(t *testing.T, rec *httptest.ResponseRecorder, status int, cache string) {
	t.Helper()
	if rec.Code != status || cacheControl(rec) != cache {
		t.Fatalf("got %d %q, want %d %q", rec.Code, cacheControl(rec), status, cache)
	}
}

func public(n int) string { return fmt.Sprintf("public, max-age=%d, s-maxage=%d", n, n) }

var fixtureID = func() func() string {
	var mu sync.Mutex
	n := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
		b := []byte("01J8ZC2Q7W4X9M3K5N6P8R0000")
		for i, k := 25, n; i >= 20 && k > 0; i, k = i-1, k/32 {
			b[i] = crockford[k%32]
		}
		return string(b)
	}
}()

// published is a public projection record.
func published(slug, title string) map[string]any {
	return map[string]any{
		"id": fixtureID(), "slug": slug, "title": title,
		"lead":    "A lead about " + title + ". It stands on its own [1].",
		"body_md": "## Overview\n\nDense factual prose about " + title + " [1].\n\n## Key facts\n\n- A fact [2]\n",
		"citations": []any{
			map[string]any{"n": 1, "url": "https://docs.example.org/guide", "title": "Guide", "host": "docs.example.org", "quote": "A verbatim quote."},
			map[string]any{"n": 2, "url": "https://news.example.com/item?id=7", "title": "Item", "host": "news.example.com", "quote": "Another quote."},
		},
		"quality_tier": "strong", "promoted": true, "vertical": "dev-docs", "status": "published",
		"ai_generated": true, "version": 3, "created_at": "2026-09-29T10:00:00Z", "updated_at": "2026-10-06T10:00:00Z",
	}
}

func stub(slug, title string) map[string]any {
	return map[string]any{"id": fixtureID(), "slug": slug, "title": title, "vertical": "research", "status": "pending", "created_at": "2026-10-01T08:00:00Z"}
}

func item(rec map[string]any) map[string]any {
	out := map[string]any{"readers_7d": 4, "version": 3}
	for _, k := range []string{"id", "slug", "title", "vertical", "quality_tier", "promoted", "status", "updated_at"} {
		out[k] = rec[k]
	}
	return out
}

func with(m map[string]any, kv ...any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

// inspect is the test-only cache inspector: the slug's entry and whether it is live.
func inspect(w *Wiki, slug string) (entry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := w.pages[slug]
	if e == nil {
		return entry{}, false
	}
	_, live := e.remaining(w.now())
	return *e, live
}

func negatives(w *Wiki) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.negatives
}

func known(w *Wiki, slug string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snap != nil && w.snap.known[slug]
}

func (w *Wiki) ready() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snap != nil
}
