package articles

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestRoutesUnavailableUntilRebuilt(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	charged := h.pol.Charges("synth-prod")
	h.shutdown()
	release := make(chan struct{})
	h.open(func(s *Store) {
		s.rebuildHook = func() { <-release }
		go func() { _ = s.Rebuild(context.Background()) }()
	})
	id := ulid(1)
	routes := []struct {
		p            v1.Principal
		method, path string
		body         any
	}{
		{mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "x"}},
		{wiki, http.MethodGet, "/v1/articles/" + id, nil},
		{wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil},
		{wiki, http.MethodGet, "/v1/articles", nil},
		{dashProd, http.MethodGet, "/v1/articles/stats", nil},
		{synthProd, http.MethodPut, "/v1/articles/" + ulid(2), articleBody("Go memory model")},
		{resolverProd, http.MethodDelete, "/v1/articles/" + id, nil},
		{dashProd, http.MethodPost, "/v1/articles/" + id + "/status", map[string]any{"action": "hold", "by": "op"}},
		{goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{}},
		{dashProd, http.MethodGet, "/v1/article-versions/" + id, nil},
		{dashProd, http.MethodGet, "/v1/article-versions/" + id + "/1", nil},
	}
	if len(routes) != len(h.s.Routes()) {
		t.Fatalf("%d probes for %d routes", len(routes), len(h.s.Routes()))
	}
	for _, c := range routes {
		rec := h.do(c.p, c.method, c.path, c.body)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "5" || !strings.Contains(rec.Body.String(), "index_unavailable") {
			t.Errorf("%s %s during rebuild: %d %s", c.method, c.path, rec.Code, rec.Body.String())
		}
	}
	if _, ok := h.raw(recordKey(ulid(2))); ok {
		t.Error("a PUT during the rebuild was written")
	}
	if h.pol.Charges("synth-prod") != charged {
		t.Error("a PUT during the rebuild was charged")
	}
	close(release)
	waitFor(t, h.s.Readiness().Ready)
	h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
}

func TestRouteTable(t *testing.T) {
	h := newHarness(t)
	mux := http.NewServeMux()
	for _, rt := range h.s.Routes() {
		if rt.Ungated || rt.Handler == nil || len(rt.Scopes) == 0 {
			t.Errorf("%s: %+v", rt.Pattern(), rt)
		}
		mux.Handle(rt.Pattern(), rt.Handler)
	}
	mux.Handle("POST /v1/search", http.NotFoundHandler())
	mux.Handle("POST /v1/contents", http.NotFoundHandler())
	limits := map[string]int64{
		"POST /v1/articles/match": 8 << 10, "PUT /v1/articles/{id}": 512 << 10, "POST /v1/articles/{id}/status": 4 << 10,
		"POST /v1/articles/purge-prelive": 1 << 10,
	}
	for _, rt := range h.s.Routes() {
		if rt.BodyLimit != limits[rt.Pattern()] {
			t.Errorf("%s body limit %d", rt.Pattern(), rt.BodyLimit)
		}
	}
	probes := map[string]string{
		"GET /v1/articles/stats":                     "GET /v1/articles/stats",
		"GET /v1/articles/by-slug/versions":          "GET /v1/articles/by-slug/{slug}",
		"GET /v1/articles/" + ulid(1):                "GET /v1/articles/{id}",
		"GET /v1/article-versions/" + ulid(1):        "GET /v1/article-versions/{id}",
		"GET /v1/article-versions/" + ulid(1) + "/2": "GET /v1/article-versions/{id}/{version}",
		"POST /v1/articles/purge-prelive":            "POST /v1/articles/purge-prelive",
		"POST /v1/articles/match":                    "POST /v1/articles/match",
		"POST /v1/articles/" + ulid(1) + "/status":   "POST /v1/articles/{id}/status",
	}
	for probe, want := range probes {
		method, path, _ := strings.Cut(probe, " ")
		req, _ := http.NewRequest(method, path, nil)
		if _, pattern := mux.Handler(req); pattern != want {
			t.Errorf("%s matched %q, want %q", probe, pattern, want)
		}
	}
}

func TestCountersFlushAndRestart(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("covered", mix(1, 0, 0.9))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	readers := func() float64 {
		return h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["readers_7d"].(float64)
	}
	count := func(r string) {
		h.match(mcpProd, map[string]any{"q": "covered", "reader": strings.Repeat(r, 32)}, http.StatusOK)
	}
	count("a")
	count("b")
	if _, ok := h.raw(countersKey(id)); ok {
		t.Error("counters written before a flush")
	}
	h.restart()
	if got := readers(); got != 2 {
		t.Fatalf("readers after restart = %v", got)
	}
	var c countersJSON
	_ = json.Unmarshal(mustRaw(t, h, countersKey(id)), &c)
	if c.V != 1 || c.Days["2026-09-29"] != 2 {
		t.Errorf("R c = %+v", c)
	}
	count("c")
	h.clock.advance(24 * time.Hour)
	count("a")
	if got := readers(); got != 4 {
		t.Errorf("readers over two days = %v", got)
	}
	byDay := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["readers_by_day"].(map[string]any)
	if byDay["2026-09-29"] != 3.0 || byDay["2026-09-30"] != 1.0 {
		t.Errorf("readers_by_day = %v", byDay)
	}
	h.clock.advance(7 * 24 * time.Hour)
	if got := readers(); got != 0 {
		t.Errorf("readers_7d a week later = %v", got)
	}
	h.clock.advance(14 * 24 * time.Hour)
	count("d")
	h.restart()
	var trimmed countersJSON
	_ = json.Unmarshal(mustRaw(t, h, countersKey(id)), &trimmed)
	if len(trimmed.Days) != 1 || trimmed.Days["2026-10-21"] != 1 {
		t.Errorf("days kept past 14: %v", trimmed.Days)
	}
}

func TestPeriodicCounterFlush(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("covered", mix(1, 0, 0.9))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	h.shutdown()
	h.open(func(s *Store) {
		s.flushEvery = 20 * time.Millisecond
		if err := s.Rebuild(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	h.match(mcpProd, map[string]any{"q": "covered", "reader": strings.Repeat("a", 32)}, http.StatusOK)
	waitFor(t, func() bool { _, ok := h.raw(countersKey(id)); return ok })
}

func TestShutdownOrder(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("covered", mix(1, 0, 0.9))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.shutdown()
	h.open(func(s *Store) {
		s.flushEvery = 5 * time.Millisecond
		if err := s.Rebuild(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	h.match(mcpProd, map[string]any{"q": "covered", "reader": strings.Repeat("a", 32)}, http.StatusOK)
	h.moderate(dashProd, ulid(2), "tombstone", "privacy", http.StatusOK)
	if err := h.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.raw(countersKey(id)); !ok {
		t.Error("Close did not flush the counters")
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Kafka consumer groups"), http.StatusServiceUnavailable, "index_unavailable")
	if err := h.ps.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := h.s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	h.s = nil
}

func TestReembedsOtherModel(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "aliases", []string{"Async executors in Rust"}), http.StatusCreated)
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.moderate(dashProd, ulid(2), "tombstone", "privacy", http.StatusOK)
	h.shutdown()
	h.emb.model = "fake-embed-v2"
	block := make(chan struct{})
	h.emb.setBlock(block)
	h.open(nil)
	st := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)
	if st["matrix_rows"] != 0.0 || st["fingerprints"].(map[string]any)["other_model"] != 1.0 || st["fingerprints"].(map[string]any)["active"] != 0.0 {
		t.Errorf("stats before re-embed = %v", st)
	}
	close(block)
	waitFor(t, func() bool {
		return h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["matrix_rows"] == 3.0
	})
	model, _, rows, err := decodeEmbeddings(mustRaw(t, h, embeddingKey(id)))
	if err != nil || model != "fake-embed-v2" || len(rows) != 3 {
		t.Errorf("R e = %s %d %v", model, len(rows), err)
	}
	h.restart()
	if st := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK); st["matrix_rows"] != 3.0 || st["fingerprints"].(map[string]any)["other_model"] != 1.0 {
		t.Errorf("stats after restart = %v", st)
	}
}

// Rebuild closes its iterator once: a second close would close whichever
// iterator reused the pooled struct. Meaningful without -race only.
func TestRebuildClosesIteratorOnce(t *testing.T) {
	for round := range 20 {
		db, err := pebble.Open(t.TempDir(), &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Set(metaKey("schema"), []byte(`{"articles":1}`), pebble.Sync)
		for i := range 10 {
			_ = db.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"), pebble.NoSync)
		}
		var victim *pebble.Iterator
		logf := func(format string, _ ...any) {
			if victim == nil && strings.HasPrefix(format, "articles: index ready") {
				victim, _ = db.NewIter(&pebble.IterOptions{LowerBound: []byte("k"), UpperBound: []byte("l")})
			}
		}
		s, err := Open(Options{DB: db, Policy: v1.NewFakePolicy(), ThresholdsPath: t.TempDir() + "/none.json", Logf: logf})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Rebuild(context.Background()); err != nil {
			t.Fatal(err)
		}
		n := 0
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("round %d: another iterator was closed under its owner: %v", round, p)
				}
			}()
			for ok := victim.First(); ok; ok = victim.Next() {
				n++
			}
		}()
		if n != 10 {
			t.Fatalf("round %d: another iterator saw %d of 10 keys", round, n)
		}
		_ = victim.Close()
		_ = s.Close()
		_ = db.Close()
	}
}

type capturedLog struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturedLog) logf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
}

func (c *capturedLog) count(sub string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, l := range c.lines {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// reopenWithModel restarts h under another embedding model with a fast re-embed back-off.
func reopenWithModel(h *harness, model string, log *capturedLog) {
	h.shutdown()
	h.emb.model = model
	h.open(func(s *Store) {
		s.reembedBase = 10 * time.Millisecond
		s.logf = log.logf
		if err := s.Rebuild(context.Background()); err != nil {
			h.t.Fatal(err)
		}
	})
}

func matrixRows(h *harness) float64 {
	return h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["matrix_rows"].(float64)
}

func TestReembedRetriesUntilEmbedderRecovers(t *testing.T) {
	h := newHarness(t)
	body := articleBody("Rust async runtimes")
	h.put(synthProd, ulid(1), body, http.StatusCreated)
	h.emb.setFailIf(func([]string) bool { return true })
	log := &capturedLog{}
	reopenWithModel(h, "fake-embed-v2", log)
	waitFor(t, func() bool { return log.count("re-embed: embedder_unavailable") >= 2 })
	if matrixRows(h) != 0 {
		t.Fatal("rows served before the re-embed")
	}
	h.emb.setFailIf(nil)
	waitFor(t, func() bool { return matrixRows(h) == 2 })
	for _, secret := range []string{"echoed input", "Rust async runtimes", body["lead"].(string)} {
		if n := log.count(secret); n != 0 {
			t.Errorf("%d log lines carry %q", n, secret)
		}
	}
}

func TestReembedSurvivesModeration(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "status", "held"), http.StatusCreated)
	block := make(chan struct{})
	h.emb.setBlock(block)
	reopenWithModel(h, "fake-embed-v2", &capturedLog{})
	waitFor(t, func() bool { return h.emb.embedded("Rust async runtimes") })
	h.moderate(dashProd, ulid(1), "approve", "", http.StatusOK)
	close(block)
	waitFor(t, func() bool { return matrixRows(h) == 2 })
	if n := h.emb.count("Rust async runtimes"); n != 2 {
		t.Errorf("the title was embedded %d times, want 2: the create and one re-embed", n)
	}
	if out := h.match(mcpProd, map[string]any{"q": "Rust async runtimes"}, http.StatusOK); out["verdict"] != "covered" {
		t.Errorf("match after the re-embed = %v", out)
	}
}
