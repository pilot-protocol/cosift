package crawler

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/store"
)

// stubEmbedder counts calls and returns a deterministic non-zero vector.
type stubEmbedder struct {
	dim   int
	calls int
}

func (s *stubEmbedder) Model() string { return "stub-emb" }
func (s *stubEmbedder) Dim() int      { return s.dim }
func (s *stubEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	s.calls++
	out := make([][]float32, len(texts))
	for i := range texts {
		v := make([]float32, s.dim)
		v[0] = 1.0
		out[i] = v
	}
	return out, nil
}

func newStoreT(t *testing.T) *store.Store {
	t.Helper()
	// Iter 134: OpenMemory — no on-disk artifacts. Crawler tests already use
	// MaxConcurrent=1 (line 63 etc.), so the SetMaxOpenConns(1) cap from
	// iter-133's OpenMemory doesn't contend.
	s, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

const sampleHTML = `<!doctype html>
<html><head><title>Test Page</title></head>
<body><main><h1>Header</h1><p>This is the body text about Go programming.</p></main></body>
</html>`

func TestCrawlEmbedsAndPersistsPassage(t *testing.T) {
	// httptest server returning a tiny HTML page.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()

	s := newStoreT(t)
	emb := &stubEmbedder{dim: 8}

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0           // don't follow links
	cfg.PerHostDelayMs = 0     // fast test
	cfg.MaxConcurrent = 1      // deterministic
	cfg.RespectRobots = false  // skip the per-host /robots.txt fetch
	c := New(cfg, s).WithEmbedder(emb)

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Doc landed in the store.
	doc, err := s.GetDocByURL(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("get doc: %v", err)
	}
	if doc.Title != "Test Page" {
		t.Errorf("title: got %q", doc.Title)
	}

	// Passage written under the stub embedder's model.
	n, err := s.CountPassagesByModel(context.Background(), "stub-emb")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("passage count: got %d want 1", n)
	}
	if emb.calls != 1 {
		t.Errorf("embed calls: got %d want 1", emb.calls)
	}
}

func TestCrawlContentHashSkipsReembedOnUnchangedRecrawl(t *testing.T) {
	// First crawl + Recrawl with unchanged content → embedder called once total.
	// Exercises the content-hash skip in processClaimed via the Recrawl path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()

	s := newStoreT(t)
	emb := &stubEmbedder{dim: 8}

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, s).WithEmbedder(emb)

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if emb.calls != 1 {
		t.Fatalf("first crawl: got %d embed calls want 1", emb.calls)
	}

	// Force re-crawl. Content unchanged → embedder must NOT be called again.
	if err := c.Recrawl(context.Background(), srv.URL); err != nil {
		t.Fatalf("recrawl: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if emb.calls != 1 {
		t.Errorf("recrawl with unchanged content should skip embed; got %d total embed calls", emb.calls)
	}
}

func TestCrawlContentChangedReembedsOnRecrawl(t *testing.T) {
	// Recrawl with CHANGED content → embedder must run again.
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", "text/html")
		if count == 1 {
			_, _ = w.Write([]byte(`<html><head><title>v1</title></head><body><main>first version content about widgets</main></body></html>`))
		} else {
			_, _ = w.Write([]byte(`<html><head><title>v2</title></head><body><main>completely different second version about something else entirely</main></body></html>`))
		}
	}))
	defer srv.Close()

	s := newStoreT(t)
	emb := &stubEmbedder{dim: 8}

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, s).WithEmbedder(emb)

	_ = c.Seed(srv.URL)
	_ = c.Run(context.Background())

	_ = c.Recrawl(context.Background(), srv.URL)
	_ = c.Run(context.Background())

	if emb.calls != 2 {
		t.Errorf("recrawl with changed content should re-embed; got %d embed calls want 2", emb.calls)
	}
}

func TestCrawlConditionalGET304SkipsBodyAndEmbed(t *testing.T) {
	// First request: serve 200 with an ETag. Second (post-Recrawl) request:
	// expect If-None-Match in the headers and respond with 304. The crawler
	// should NOT parse or embed on 304.
	calls := 0
	var sawIfNoneMatch string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("ETag", `"v1"`)
			w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(sampleHTML))
			return
		}
		sawIfNoneMatch = r.Header.Get("If-None-Match")
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()

	s := newStoreT(t)
	emb := &stubEmbedder{dim: 8}

	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, s).WithEmbedder(emb)

	_ = c.Seed(srv.URL)
	_ = c.Run(context.Background())
	if emb.calls != 1 {
		t.Fatalf("first crawl embed calls: got %d want 1", emb.calls)
	}

	// Force re-crawl. Server returns 304 — embed must NOT fire.
	_ = c.Recrawl(context.Background(), srv.URL)
	_ = c.Run(context.Background())

	if emb.calls != 1 {
		t.Errorf("304 path should not embed; got %d total embed calls", emb.calls)
	}
	if sawIfNoneMatch != `"v1"` {
		t.Errorf("conditional header not sent: got If-None-Match=%q", sawIfNoneMatch)
	}
	if calls != 2 {
		t.Errorf("expected 2 HTTP requests, got %d", calls)
	}
}

func TestCrawlWithoutEmbedderSkipsPassages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, s)

	_ = c.Seed(srv.URL)
	_ = c.Run(context.Background())

	n, _ := s.CountPassagesByModel(context.Background(), "any-model")
	if n != 0 {
		t.Errorf("without embedder, passage count should be 0, got %d", n)
	}
	// But the BM25 doc still exists.
	if _, err := s.GetDocByURL(context.Background(), srv.URL); err != nil {
		t.Errorf("doc still expected: %v", err)
	}
}

// Iter 146: per-host ChunkSize override wins over global ChunkSize. Crawl
// the same long doc twice — once with no override (uses global) and once with
// a per-host override matching the test server's host. The per-host override
// must apply, producing a different passage count.
func TestCrawlPerHostChunkSizeOverride(t *testing.T) {
	longText := "<!doctype html><html><head><title>Long</title></head><body><main>"
	for i := 0; i < 200; i++ {
		longText += "wordtoken "
	}
	longText += "</main></body></html>"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(longText))
	}))
	defer srv.Close()

	// Extract the host from the test-server URL — iter 146's match key.
	host := strings.TrimPrefix(srv.URL, "http://")

	run := func(globalSize int, perHost map[string]int) int {
		s := newStoreT(t)
		emb := &stubEmbedder{dim: 4}
		cfg := config.Default().Crawler
		cfg.MaxDepth = 0
		cfg.PerHostDelayMs = 0
		cfg.MaxConcurrent = 1
		cfg.RespectRobots = false
		cfg.ChunkSize = globalSize
		cfg.PerHostChunkSize = perHost
		c := New(cfg, s).WithEmbedder(emb)
		if err := c.Seed(srv.URL); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := c.Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		n, _ := s.CountPassagesByModel(context.Background(), "stub-emb")
		return int(n)
	}

	// (1) Global=500 (one chunk on ~200 words). No per-host. Baseline.
	baselineN := run(500, nil)
	// (2) Global=500 still, but per-host override for our host = 50.
	overrideN := run(500, map[string]int{host: 50})
	// (3) Per-host override for a DIFFERENT host — global wins.
	otherHostN := run(500, map[string]int{"unrelated.example.com": 50})

	if overrideN <= baselineN {
		t.Errorf("per-host ChunkSize=50 should produce more passages than global=500; got baseline=%d override=%d", baselineN, overrideN)
	}
	if otherHostN != baselineN {
		t.Errorf("per-host map keyed on a DIFFERENT host should not affect this host; got baseline=%d other=%d", baselineN, otherHostN)
	}
}

// Iter 142: cfg.Crawler.ChunkSize override produces more chunks than default.
// Default chunker (320 word target) emits one chunk on the 200-word sample;
// setting ChunkSize=50 must split it into multiple passages.
func TestCrawlChunkSizeOverrideEmitsMorePassages(t *testing.T) {
	// Build a long sample (200+ words) so chunk count is sensitive to ChunkSize.
	longText := "<!doctype html><html><head><title>Long Test</title></head><body><main>"
	for i := 0; i < 200; i++ {
		longText += "wordtoken "
	}
	longText += "</main></body></html>"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(longText))
	}))
	defer srv.Close()

	run := func(chunkSize, overlap int) int {
		s := newStoreT(t)
		emb := &stubEmbedder{dim: 4}
		cfg := config.Default().Crawler
		cfg.MaxDepth = 0
		cfg.PerHostDelayMs = 0
		cfg.MaxConcurrent = 1
		cfg.RespectRobots = false
		cfg.ChunkSize = chunkSize
		cfg.ChunkOverlap = overlap
		c := New(cfg, s).WithEmbedder(emb)
		if err := c.Seed(srv.URL); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := c.Run(context.Background()); err != nil {
			t.Fatalf("run: %v", err)
		}
		n, _ := s.CountPassagesByModel(context.Background(), "stub-emb")
		return int(n)
	}

	defaultN := run(0, 0)       // default chunker (320/64) → 1 chunk on ~200 words
	smallN := run(50, 10)       // small override → more chunks

	if smallN <= defaultN {
		t.Errorf("ChunkSize=50 should yield more passages than the default; got default=%d small=%d", defaultN, smallN)
	}
	if smallN < 3 {
		t.Errorf("ChunkSize=50 on ~200 words should produce at least 3 chunks, got %d", smallN)
	}
}

// Iter 141: server gzip-encodes the response. The crawler must transparently
// decompress (via Go's http.Transport auto-handling) and parse the original
// HTML. Pre-iter-141 the crawler manually set Accept-Encoding: gzip, which
// disabled Go's auto-decompression — the body would arrive as raw gzipped
// bytes and Title extraction would silently fail. This locks in the fix.
func TestCrawlGzipEncodedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte(sampleHTML))
		_ = gz.Close()
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	s := newStoreT(t)
	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 1
	cfg.RespectRobots = false
	c := New(cfg, s)

	if err := c.Seed(srv.URL); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	doc, err := s.GetDocByURL(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("get doc: %v", err)
	}
	if doc.Title != "Test Page" {
		t.Errorf("gzip-decoded title: got %q want %q (Transport must transparently decompress)", doc.Title, "Test Page")
	}
	if !strings.Contains(doc.Text, "body text about Go programming") {
		t.Errorf("gzip-decoded body text missing: %q", doc.Text)
	}
}
