package crawler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/store"
)

// slowEmbedder takes a fixed amount of time per call and honors context
// cancellation the way a real HTTP-backed embedder does.
type slowEmbedder struct {
	dim   int
	delay time.Duration

	mu     sync.Mutex
	calls  int
	failed int
}

func (s *slowEmbedder) Model() string { return "slow-emb" }
func (s *slowEmbedder) Dim() int      { return s.dim }

func (s *slowEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	t := time.NewTimer(s.delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.failed++
		s.mu.Unlock()
		return nil, ctx.Err()
	case <-t.C:
	}
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	out := make([][]float32, len(texts))
	for i := range texts {
		v := make([]float32, s.dim)
		for j := range v {
			v[j] = 0.1
		}
		out[i] = v
	}
	return out, nil
}

// countingWriter records passage writes and reports zero prior passages from
// MarkURLInvalid, which is the branch that drives the zombie-reclaim
// diagnostic counter.
type countingWriter struct {
	inner PassageWriter
	// invalidateDelay holds each worker inside the reclaim block so that all
	// of them reach the diagnostic counter at roughly the same instant.
	invalidateDelay time.Duration

	mu       sync.Mutex
	upserts  int
	invalids int
}

func (c *countingWriter) UpsertPassage(ctx context.Context, p *store.Passage) error {
	if err := c.inner.UpsertPassage(ctx, p); err != nil {
		return err
	}
	c.mu.Lock()
	c.upserts++
	c.mu.Unlock()
	return nil
}

func (c *countingWriter) MarkURLInvalid(ctx context.Context, url string) (int, error) {
	if c.invalidateDelay > 0 {
		time.Sleep(c.invalidateDelay)
	}
	c.mu.Lock()
	c.invalids++
	c.mu.Unlock()
	return 0, nil
}

func (c *countingWriter) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.upserts, c.invalids
}

// pagesServer serves n distinct HTML pages under /p<i>.
func pagesServer(t *testing.T, n int) (*httptest.Server, []string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<!doctype html><html><head><title>Page %s</title></head>
<body><main><h1>Heading %s</h1><p>Body text for path %s about distributed indexing and retrieval.</p></main></body></html>`,
			r.URL.Path, r.URL.Path, r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	urls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		urls = append(urls, fmt.Sprintf("%s/p%d", srv.URL, i))
	}
	return srv, urls
}

func fastCrawlCfg() config.Crawler {
	cfg := config.Default().Crawler
	cfg.MaxDepth = 0
	cfg.PerHostDelayMs = 0
	cfg.MaxConcurrent = 8
	cfg.RespectRobots = false
	return cfg
}

// TestEmbedPoolFinishesQueuedJobsAfterCrawlContextCancel checks that the
// decoupled embed pool keeps a usable context after the crawl's own context is
// cancelled (the terminator cancels it as soon as the frontier drains). Jobs
// already sitting in the queue must still be embedded and written, not failed.
func TestEmbedPoolFinishesQueuedJobsAfterCrawlContextCancel(t *testing.T) {
	const pages = 40

	t.Setenv("COSIFT_EMBED_DECOUPLE_WORKERS", "1")
	t.Setenv("COSIFT_EMBED_DECOUPLE_BUFFER", "256")

	_, urls := pagesServer(t, pages)
	s := newStoreT(t)
	emb := &slowEmbedder{dim: 8, delay: 100 * time.Millisecond}
	cw := &countingWriter{inner: s}

	c := New(fastCrawlCfg(), s).WithEmbedder(emb).WithPassageWriter(cw)
	for _, u := range urls {
		if err := c.Seed(u); err != nil {
			t.Fatalf("seed %s: %v", u, err)
		}
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	queued := c.embedQueued.Load()
	done := c.embedDone.Load()
	failed := c.embedFailed.Load()
	dropped := c.embedDropped.Load()

	if queued == 0 {
		t.Fatalf("no embed jobs were queued — decoupled path did not engage")
	}
	if failed != 0 {
		t.Errorf("embed jobs failed after the crawl context was cancelled: failed=%d (queued=%d done=%d)",
			failed, queued, done)
	}
	if done != queued {
		t.Errorf("embed pool completed %d of %d queued jobs (failed=%d dropped=%d)",
			done, queued, failed, dropped)
	}
	if up, _ := cw.counts(); up == 0 {
		t.Errorf("no passages were written")
	}
}

// TestEnqueueEmbedJobWaitsForCapacity checks that a momentarily full queue
// makes the producer wait for space rather than discarding the document's
// passages outright.
func TestEnqueueEmbedJobWaitsForCapacity(t *testing.T) {
	c := newBare(config.Default().Crawler)
	c.embedQ = make(chan *embedJob, 1)
	c.embedQ <- &embedJob{url: "https://x.example/filler"}

	t.Setenv("COSIFT_EMBED_ENQUEUE_WAIT_MS", "2000")

	go func() {
		time.Sleep(50 * time.Millisecond)
		<-c.embedQ
	}()

	start := time.Now()
	if !c.enqueueEmbedJob(context.Background(), &embedJob{url: "https://x.example/real"}) {
		t.Fatalf("job rejected even though capacity freed up after %s", time.Since(start))
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("returned in %s — did not actually wait for capacity", elapsed)
	}
}

// TestEnqueueEmbedJobGivesUpWhenQueueStaysFull keeps the wall clock bounded:
// a permanently full queue must not block a crawl worker indefinitely.
func TestEnqueueEmbedJobGivesUpWhenQueueStaysFull(t *testing.T) {
	c := newBare(config.Default().Crawler)
	c.embedQ = make(chan *embedJob, 1)
	c.embedQ <- &embedJob{url: "https://x.example/filler"}

	t.Setenv("COSIFT_EMBED_ENQUEUE_WAIT_MS", "50")

	start := time.Now()
	if c.enqueueEmbedJob(context.Background(), &embedJob{url: "https://x.example/real"}) {
		t.Fatalf("job accepted into a full queue")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("enqueue took %s — wait budget not honored", elapsed)
	}
}

// TestEmbedJobExpiredHonoursDrainDeadline covers the shutdown bound: once the
// drain deadline has passed, remaining queued work is abandoned.
func TestEmbedJobExpiredHonoursDrainDeadline(t *testing.T) {
	c := newBare(config.Default().Crawler)
	if c.embedJobExpired() {
		t.Fatalf("expired with no deadline set")
	}
	c.embedDrainUntil.Store(time.Now().Add(time.Hour).UnixNano())
	if c.embedJobExpired() {
		t.Fatalf("expired with an hour of budget left")
	}
	c.embedDrainUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if !c.embedJobExpired() {
		t.Fatalf("did not expire past the deadline")
	}
}

// TestZombieReclaimDiagnosticCounterConcurrent drives the zombie-reclaim
// diagnostic path from many crawl workers at once. Run under -race.
func TestZombieReclaimDiagnosticCounterConcurrent(t *testing.T) {
	const pages = 60

	t.Setenv("COSIFT_ZOMBIE_RECLAIM", "1")
	t.Setenv("COSIFT_EMBED_DECOUPLE_WORKERS", "0")

	_, urls := pagesServer(t, pages)
	s := newStoreT(t)
	// slowEmbedder with no delay: the package's stubEmbedder keeps an
	// unsynchronized call counter and is only safe at MaxConcurrent=1.
	emb := &slowEmbedder{dim: 8}
	cw := &countingWriter{inner: s, invalidateDelay: 15 * time.Millisecond}

	cfg := fastCrawlCfg()
	cfg.MaxConcurrent = 16
	c := New(cfg, s).WithEmbedder(emb).WithPassageWriter(cw)
	for _, u := range urls {
		if err := c.Seed(u); err != nil {
			t.Fatalf("seed %s: %v", u, err)
		}
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	if _, inv := cw.counts(); inv == 0 {
		t.Fatalf("zombie-reclaim path never ran — counter was not exercised")
	}
	if got := c.zombieDebugLogged.Load(); got == 0 {
		t.Fatalf("diagnostic counter never incremented")
	}
}
