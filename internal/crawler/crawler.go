// Package crawler is a small, dependency-light web crawler.
//
// scope: fetch, parse, store, robots.txt, persistent frontier.
// Concurrent fetcher pool + per-host rate limiting land in
package crawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

// Crawler orchestrates fetch → parse → index over a persistent frontier.
type Crawler struct {
	cfg           config.Crawler
	store         CrawlerStore
	idx           LexicalIndexer
	passageWriter PassageWriter // optional; nil = no vector write (Pebble path uses HNSW directly)
	http          *http.Client
	robots        *Robots
	embedder      embed.Embedder // optional; nil = lexical-only ingest

	// When
	// route returns ownsLocally=false, the crawler calls forward(url,
	// peerAddr) instead of pushing to its own frontier. Both nil = single-
	// node (every URL is local).
	route   RouteFn
	forward ForwardFn

	// First time we see a host,
	// background-fetch its /sitemap.xml and enqueue everything it lists.
	// Subsequent URLs from the same host skip the lookup. Empty / unset
	// keeps manual /admin/sitemap-import behavior unchanged.
	autoSitemapMu   sync.Mutex
	autoSitemapSeen map[string]struct{}

	// bounded diagnostic counter for the zombie-reclaim path.
	// Logs the first few zero-hit cases so we can confirm the code path
	// is actually reached even when no prior passages exist. Cheap (just
	// an int read on the hot path; not atomic — diagnostic only).
	zombieDebugLogged int

	// per-host error-rate tracking via sync.Map + atomic
	// counters. With 512 workers we cannot afford a single write lock
	// on every claim's completion — that bottlenecked and cost
	// ~25% throughput. sync.Map.LoadOrStore lets us avoid the lock on
	// the steady-state path (host already in map).
	hostStats sync.Map // host (string) → *hostFetchStats
}

type hostFetchStats struct {
	attempts  atomic.Int32
	successes atomic.Int32
}

// New constructs a SQLite-backed crawler. Caller owns store lifecycle.
// Backward-compatible with and prior callers; *store.Store
// satisfies both CrawlerStore and PassageWriter so the SQLite vector-
// write path is auto-attached.
func New(cfg config.Crawler, s *store.Store) *Crawler {
	c := newBare(cfg)
	c.store = s
	c.idx = index.NewBM25(s)
	c.passageWriter = s
	return c
}

// NewWithBackend constructs a crawler against arbitrary CrawlerStore +
// LexicalIndexer implementations — the Pebble entry point. Caller passes
// a *store.PebbleStore plus an *index.PebbleBM25 (or any other implementor).
// Vector indexing during crawl is opt-in via WithPassageWriter; the
// default Pebble-side flow writes BM25 only.
func NewWithBackend(cfg config.Crawler, s CrawlerStore, idx LexicalIndexer) *Crawler {
	c := newBare(cfg)
	c.store = s
	c.idx = idx
	// If the concrete store happens to satisfy PassageWriter (the SQLite
	// case), auto-attach so callers don't have to wire it explicitly.
	if pw, ok := s.(PassageWriter); ok {
		c.passageWriter = pw
	}
	return c
}

func newBare(cfg config.Crawler) *Crawler {
	// Defaults
	// (MaxIdleConns=50, MaxConnsPerHost=2) throttle the crawler at any
	// host with multiple in-flight URLs. Bumping to 500 idle and 16/host
	// matches what real crawlers run.
	// The previous 15s waited too
	// long on dead/slow hosts — at 512 workers each stuck fetch holds a slot,
	// throttling overall docs/min throughput more than tail-latency hurts.
	// Most healthy servers send headers in <1s; 6s gives plenty of margin
	// for slow-but-real ones while shedding the long tail. Override via
	// COSIFT_FETCH_HEADER_TIMEOUT_MS.
	respHeaderTimeout := 6 * time.Second
	if v := os.Getenv("COSIFT_FETCH_HEADER_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			respHeaderTimeout = time.Duration(n) * time.Millisecond
		}
	}
	// The
	// previous 16-per-host was right when MaxConcurrent was ~50; at 512
	// workers + multi-worker CF pool, 16-per-host capped throughput at
	// ~2K docs/min because crawler workers spent most time queued waiting
	// for an HTTP conn to free. 128 per host gives plenty of headroom
	// while staying well below file-descriptor limits.
	maxConnsPerHost := 128
	if v := os.Getenv("COSIFT_MAX_CONNS_PER_HOST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConnsPerHost = n
		}
	}
	transport := &http.Transport{
		MaxIdleConns:          2000,
		MaxIdleConnsPerHost:   maxConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
		ResponseHeaderTimeout: respHeaderTimeout,
	}
	// Each request picks a random proxy
	// from cfg.Proxies; empty list = direct connection.
	if proxies := parseProxies(cfg.Proxies); len(proxies) > 0 {
		var pmu sync.Mutex
		var prng = rand.New(rand.NewSource(time.Now().UnixNano()))
		transport.Proxy = func(req *http.Request) (*url.URL, error) {
			pmu.Lock()
			idx := prng.Intn(len(proxies))
			pmu.Unlock()
			return proxies[idx], nil
		}
		log.Printf("crawler: proxy pool enabled (%d proxies)", len(proxies))
	}
	// optional remote fetcher (CF Worker pool, etc.). When
	// configured, wraps the transport so every outbound GET goes through
	// the worker. Crawler logic is unchanged; only the network egress
	// shifts. Falls back to direct fetch for non-GET (robots, etc.).
	var rt http.RoundTripper = transport
	// prefer the pool field when set; fall back to the singular URL.
	urls := cfg.RemoteFetcherURLs
	if len(urls) == 0 && cfg.RemoteFetcherURL != "" {
		urls = []string{cfg.RemoteFetcherURL}
	}
	if len(urls) > 0 {
		rt = newRemoteFetcherTransport(urls, cfg.RemoteFetcherToken, transport)
		log.Printf("crawler: remote fetcher enabled (%d workers in pool)", len(urls))
	}
	// 30s overall timeout was generous to a fault — most useful
	// fetches finish in <3s. Drop to 12s so dead URLs free up the worker
	// faster. Override via COSIFT_FETCH_TIMEOUT_MS.
	overallTimeout := 12 * time.Second
	if v := os.Getenv("COSIFT_FETCH_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			overallTimeout = time.Duration(n) * time.Millisecond
		}
	}
	httpClient := &http.Client{
		Timeout:   overallTimeout,
		Transport: rt,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
	var robots *Robots
	if cfg.RespectRobots {
		robots = NewRobots(httpClient, cfg.UserAgent)
	}
	return &Crawler{cfg: cfg, http: httpClient, robots: robots}
}

// maybeAutoSitemap kicks off a background sitemap discovery the first
// time we see a host. Subsequent URLs from the same host return
// immediately. Disabled when COSIFT_AUTO_SITEMAP=false.
func (c *Crawler) maybeAutoSitemap(ctx context.Context, u *url.URL) {
	if u == nil || u.Host == "" {
		return
	}
	if !c.cfg.AutoSitemap {
		return
	}
	c.autoSitemapMu.Lock()
	if c.autoSitemapSeen == nil {
		c.autoSitemapSeen = make(map[string]struct{})
	}
	if _, ok := c.autoSitemapSeen[u.Host]; ok {
		c.autoSitemapMu.Unlock()
		return
	}
	c.autoSitemapSeen[u.Host] = struct{}{}
	c.autoSitemapMu.Unlock()

	scheme := u.Scheme
	if scheme == "" {
		scheme = "https"
	}
	host := u.Host
	go func() {
		// Detach from the request ctx (which gets canceled when the
		// triggering URL's processing finishes) so the goroutine has
		// time to finish a possibly-slow sitemap-index walk. Cap at 5 min.
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		smURL := scheme + "://" + host + "/sitemap.xml"
		n, err := c.SeedSitemap(sctx, smURL)
		if err != nil {
			// Most hosts don't have /sitemap.xml at the root — 404 / 403 /
			// connection-refused are routine. Silence them.
			return
		}
		if n > 0 {
			log.Printf("auto-sitemap %q: enqueued %d URLs", host, n)
		}
	}()
}

// parseProxies turns config.Crawler.Proxies (string URLs) into a slice of
// *url.URL ready to be returned from http.Transport.Proxy. Malformed
// entries are logged and skipped.
func parseProxies(raw []string) []*url.URL {
	out := make([]*url.URL, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		u, err := url.Parse(s)
		if err != nil || u.Host == "" {
			log.Printf("crawler: skipping malformed proxy %q: %v", s, err)
			continue
		}
		out = append(out, u)
	}
	return out
}

// WithEmbedder enables dense indexing during crawl: every successfully indexed
// document also gets one passage embedding written to the store. Embedding
// failure is non-fatal — the BM25 index still gets the document.
func (c *Crawler) WithEmbedder(e embed.Embedder) *Crawler {
	c.embedder = e
	return c
}

// WithPassageWriter overrides the auto-attached PassageWriter. Use this on
// the Pebble path to bridge passage vectors into an HNSW index.
func (c *Crawler) WithPassageWriter(pw PassageWriter) *Crawler {
	c.passageWriter = pw
	return c
}

// RouteFn decides which physical shard owns a given canonical URL. The
// returned ownsLocally=true means "index here." When false, peerAddr is the
// host:port of the shard that should own the URL — the crawler forwards
// the URL there via POST /admin/crawl-enqueue.
type RouteFn func(canonURL string) (ownsLocally bool, peerAddr string)

// ForwardFn is the function the crawler calls when a discovered URL belongs
// to another shard. Implementation lives outside the crawler package (in
// pebble_serve.go) so the crawler stays HTTP-client-free.
type ForwardFn func(ctx context.Context, canonURL, peerAddr string) error

// WithRouter wires sharding-aware URL routing. When nil (default), every
// URL is owned locally — preserves the single-node code path.
func (c *Crawler) WithRouter(route RouteFn, forward ForwardFn) *Crawler {
	c.route = route
	c.forward = forward
	return c
}

// Seed pushes a URL onto the persistent frontier at depth 0.
//
// `INSERT OR IGNORE` semantics: if the URL is already in the frontier (queued,
// in-flight, done, or errored), Seed is a no-op. To force a refresh, use Recrawl.
func (c *Crawler) Seed(rawURL string) error {
	canon, err := canonicalize(rawURL)
	if err != nil {
		return err
	}
	if !c.allowedDomain(canon) {
		return fmt.Errorf("seed %s not allowed by include/exclude rules", canon)
	}
	return c.store.PushFrontier(context.Background(), canon, 0, 1.0)
}

// Recrawl re-enqueues a URL even if it was previously crawled. Status flips
// to 'queued', attempts resets. Combined with the content-hash dedup in
// processClaimed, an unchanged page costs one HTTP request and zero embedding
// calls; a changed page is fully re-indexed.
func (c *Crawler) Recrawl(ctx context.Context, rawURL string) error {
	canon, err := canonicalize(rawURL)
	if err != nil {
		return err
	}
	if !c.allowedDomain(canon) {
		return fmt.Errorf("recrawl %s not allowed by include/exclude rules", canon)
	}
	return c.store.RecrawlURL(ctx, canon)
}

// Run processes the frontier with a pool of workers until the frontier is
// drained for several consecutive polls (or ctx is cancelled).
//
// Termination: a coordinator goroutine watches frontier stats. When
// queued + in_flight == 0 for ~1.5s, the run context is cancelled and workers exit.
func (c *Crawler) Run(ctx context.Context) error {
	if err := c.store.RecoverInFlight(ctx); err != nil {
		return fmt.Errorf("recover in-flight: %w", err)
	}

	workers := c.cfg.MaxConcurrent
	if workers <= 0 {
		workers = 1
	}
	// build per-host override map from int ms → time.Duration.
	// Nil overrides map is safe (delayFor returns the default for any host).
	var overrides map[string]time.Duration
	if len(c.cfg.PerHostOverrides) > 0 {
		overrides = make(map[string]time.Duration, len(c.cfg.PerHostOverrides))
		for host, ms := range c.cfg.PerHostOverrides {
			overrides[host] = time.Duration(ms) * time.Millisecond
		}
	}
	gate := newHostGate(time.Duration(c.cfg.PerHostDelayMs)*time.Millisecond, overrides)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go c.worker(runCtx, &wg, gate)
	}
	go c.terminator(runCtx, cancel)

	// Pebble's single-writer lock blocks
	// `cosift stats -backend=pebble` from any sidecar process during a live
	// crawl. The crawler — which holds the lock — periodically writes a
	// small status.json file alongside the data dir so operators can
	// `cat status.json` (or jq it, watch -n it) without contending.
	if c.cfg.StatusFile != "" {
		go c.statusDumper(runCtx, c.cfg.StatusFile)
	}

	wg.Wait()
	return nil
}

// statusDumper writes a JSON snapshot of crawl progress every 10s to path.
// Cheap: just reads the Iter running counters from the store.
// Stops when ctx is cancelled.
func (c *Crawler) statusDumper(ctx context.Context, path string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("statusDumper panicked, recovering: %v", r)
		}
	}()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// capture started_at + indexed_docs_at_start on the first poll so
	// `cosift status-file -target N` can compute an ETA from rate-since-start
	// without needing a second observation point.
	startedAt := time.Now()
	var indexedAtStart int64
	var indexedAtStartSet bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fStats, err := c.store.GetFrontierStats(ctx)
			if err != nil {
				continue
			}
			// include indexed-doc count + avg doc length when the
			// store has cheap O(1) accessors for them (PebbleStore via iter
			// 207's running counters). SQLite Store doesn't expose
			// CorpusStats today; the type-assert short-circuits and the
			// fields stay 0 — operators reading the JSON treat 0 as "not
			// available for this backend" via the omitempty tag.
			var indexedDocs int64
			var avgDocLen float64
			if pebbleLike, ok := c.store.(interface {
				CorpusStats(ctx context.Context) (int64, int64, error)
			}); ok {
				if sumLen, count, e := pebbleLike.CorpusStats(ctx); e == nil && count > 0 {
					indexedDocs = count
					avgDocLen = float64(sumLen) / float64(count)
				}
			}
			if !indexedAtStartSet {
				indexedAtStart = indexedDocs
				indexedAtStartSet = true
			}
			doc := struct {
				Queued             int64     `json:"frontier_queued"`
				InFlight           int64     `json:"frontier_in_flight"`
				Done               int64     `json:"frontier_done"`
				Errored            int64     `json:"frontier_errored"`
				IndexedDocs        int64     `json:"indexed_docs,omitempty"`
				IndexedDocsAtStart int64     `json:"indexed_docs_at_start,omitempty"`
				AvgDocLen          float64   `json:"avg_doc_len,omitempty"`
				StartedAt          time.Time `json:"started_at,omitempty"`
				WrittenAt          time.Time `json:"written_at"`
			}{
				Queued:             fStats.Queued,
				InFlight:           fStats.InFlight,
				Done:               fStats.Done,
				Errored:            fStats.Errored,
				IndexedDocs:        indexedDocs,
				IndexedDocsAtStart: indexedAtStart,
				AvgDocLen:          avgDocLen,
				StartedAt:          startedAt,
				WrittenAt:          time.Now(),
			}
			buf, err := json.Marshal(doc)
			if err != nil {
				continue
			}
			// Write+rename for atomic publish — readers never see a partial file.
			tmp := path + ".tmp"
			if err := os.WriteFile(tmp, buf, 0o644); err != nil {
				continue
			}
			_ = os.Rename(tmp, path)
		}
	}
}

func (c *Crawler) worker(ctx context.Context, wg *sync.WaitGroup, gate *hostGate) {
	defer wg.Done()
	// A single un-recovered panic in
	// any worker (network library, parser corner case, Pebble write under
	// load) would silently exit the entire crawl process without a stack
	// trace, leaving no signal in crawl.log for diagnosis. Recover at the
	// worker boundary, log the stack, and let sibling workers continue.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("crawler worker panicked, recovering: %v\n%s", r, debug.Stack())
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		item, ok, err := c.store.ClaimFrontier(ctx)
		if err != nil {
			log.Printf("claim: %v", err)
			sleepCtx(ctx, 100*time.Millisecond)
			continue
		}
		if !ok {
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}
		err = c.processClaimed(ctx, item, gate)
		if u, perr := url.Parse(item.URL); perr == nil && u.Host != "" {
			c.recordHostResult(u.Host, err == nil)
		}
		if err != nil {
			log.Printf("crawl %s: %v", item.URL, err)
			_ = c.store.FailFrontier(ctx, item.URL, err.Error())
			continue
		}
		_ = c.store.CompleteFrontier(ctx, item.URL)
	}
}

// isHostBlacklisted reports whether a host has seen enough failures to
// short-circuit further claims. Defaults: require ≥20 attempts before
// judging (cold-start protection), blacklist when success ratio < 20%.
// lock-free via sync.Map + atomic load.
func (c *Crawler) isHostBlacklisted(host string) bool {
	if host == "" {
		return false
	}
	v, ok := c.hostStats.Load(host)
	if !ok {
		return false
	}
	s := v.(*hostFetchStats)
	att := s.attempts.Load()
	if att < 20 {
		return false
	}
	succ := s.successes.Load()
	return float64(succ)/float64(att) < 0.20
}

// recordHostResult updates per-host success/attempt counters. Called from
// the worker loop after each processClaimed return. lock-free
// hot path via sync.Map + atomic counter increment. Only the first call
// for a given host pays the LoadOrStore cost; subsequent calls just bump
// the atomic int32 counters.
func (c *Crawler) recordHostResult(host string, success bool) {
	if host == "" {
		return
	}
	v, _ := c.hostStats.LoadOrStore(host, &hostFetchStats{})
	s := v.(*hostFetchStats)
	s.attempts.Add(1)
	if success {
		s.successes.Add(1)
	}
}

// terminator cancels runCtx once the frontier has stayed empty (no queued, no
// in-flight) across several polls. Three empties at 500ms = ~1.5s of quiet.
func (c *Crawler) terminator(ctx context.Context, cancel context.CancelFunc) {
	empty := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
		fs, err := c.store.GetFrontierStats(ctx)
		if err != nil {
			continue
		}
		if fs.Queued == 0 && fs.InFlight == 0 {
			empty++
			if empty >= 3 {
				cancel()
				return
			}
		} else {
			empty = 0
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// FetchAndIndexNow bypasses the persistent frontier and runs the
// fetch-parse-index pipeline synchronously for a single URL. Returns the
// crawler's per-URL error semantics: nil on success (incl. 304-not-modified
// and content-hash-no-change), error string from the inner stage on failure.
// Used by admin endpoints to inject genuinely-novel seeds without waiting
// for the round-robin cursor to find them in a multi-million-entry frontier.
// Side effect: discovered links ARE pushed to the frontier via enqueueLinks
// (this is desired — seed URLs should fan out further crawls).
func (c *Crawler) FetchAndIndexNow(ctx context.Context, url string) error {
	// Build a one-off host gate so per-host delay still applies if multiple
	// FetchAndIndexNow calls overlap. The gate is fresh — the worker pool's
	// own gate is goroutine-local to Run(). For a single direct fetch the
	// overhead is just one mutex+map.
	gate := newHostGate(time.Duration(c.cfg.PerHostDelayMs)*time.Millisecond, nil)
	item := store.FrontierItem{URL: url, Depth: 0, Priority: 1.0}
	return c.processClaimed(ctx, item, gate)
}

func (c *Crawler) processClaimed(ctx context.Context, item store.FrontierItem, gate *hostGate) error {
	u, _ := url.Parse(item.URL)
	// Prior enqueueLinks already
	// filters via allowedDomain, but stale frontier entries from before
	// an exclude was added (or imported from sitemap before the rule)
	// keep getting claimed and consuming round-robin cycles per host.
	// Fast-skip them here so they get marked done immediately, freeing
	// the cursor to reach quality hosts. Returning nil signals success
	// to the worker so the frontier entry transitions queued → done.
	if !c.allowedDomain(item.URL) {
		return nil
	}
	// After we've tried a host N times
	// and seen <20% success, fast-skip subsequent claims to that host.
	// Burning workers on the same dead hosts (auth walls, dead Tumblr,
	// rate-limited APIs) is the single biggest waste of throughput.
	if u != nil && c.isHostBlacklisted(u.Host) {
		return nil
	}
	if u != nil {
		// first time we see a host, fire-and-forget a sitemap
		// discovery. Compounds: each new host typically brings hundreds-
		// to-thousands of URLs in one batch.
		c.maybeAutoSitemap(ctx, u)
		if err := gate.Wait(ctx, u.Host); err != nil {
			return err
		}
	}
	if c.robots != nil {
		allowed, robotsDelay, err := c.robots.Allowed(ctx, item.URL)
		if err != nil {
			return fmt.Errorf("robots: %w", err)
		}
		if !allowed {
			return errors.New("blocked by robots.txt")
		}
		// Honor Crawl-delay if it exceeds our per-host gate's interval.
		// use the effective per-host delay (gate's override or
		// default), not the global default — operators who set a longer
		// override for a host should have that override compared against
		// robots.txt's Crawl-delay, not the global setting.
		if robotsDelay > 0 && u != nil {
			gateDelay := gate.delayFor(u.Host)
			if robotsDelay > gateDelay {
				sleepCtx(ctx, robotsDelay-gateDelay)
			}
		}
	}

	// Look up prior doc once: needed for both conditional GET (validators) and
	// content-hash dedup later. Missing is fine — empty prior means full fetch.
	prior, _ := c.store.GetDocByURL(ctx, item.URL)

	// If we already have a
	// healthy copy of this URL fetched within COSIFT_REFETCH_AFTER_HOURS,
	// skip the HTTP call entirely. Saves the 200ms-2s fetch cost on
	// revisits at the cost of missing freshly-updated content during the
	// staleness window. Default 0 = disabled (every revisit issues a
	// conditional GET, matching behavior).
	if prior != nil && len(prior.ContentSHA) > 0 {
		refetchHours := 0
		if v := os.Getenv("COSIFT_REFETCH_AFTER_HOURS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				refetchHours = n
			}
		}
		if refetchHours > 0 && time.Since(prior.FetchedAt) < time.Duration(refetchHours)*time.Hour {
			prior.FetchedAt = time.Now()
			_, _ = c.store.UpsertDocument(ctx, prior)
			return nil
		}
	}

	res, err := c.fetch(ctx, item.URL, prior)
	if err != nil {
		return err
	}

	// 304 Not Modified: server confirmed nothing changed. Update validators
	// + fetched_at, skip parse / BM25 / embed. Zero body bandwidth.
	if res.notModified && prior != nil {
		prior.FetchedAt = time.Now()
		if res.etag != "" {
			prior.ETag = res.etag
		}
		if res.lastModified != "" {
			prior.LastModified = res.lastModified
		}
		if _, e := c.store.UpsertDocument(ctx, prior); e != nil {
			return e
		}
		return nil
	}

	finalURL := res.finalURL
	// Dispatch parser by content-type. added PDF support; HTML/XML
	// remains the default both for legacy responses and for missing/empty
	// Content-Type headers (the lenient case Parse already handles).
	var parsed *ParsedDoc
	var perr error
	if strings.Contains(res.contentType, "pdf") {
		// ledongthuc/pdf has an unbounded-allocation bug in
		// (*buffer).readArray on malformed PDFs — heap profile showed it
		// growing at 2.7 GB/sec until the OOM-killer fired.
		// The child
		// process has a 500 MiB soft memory limit (debug.SetMemoryLimit)
		// and a 30 s wall-clock cap; if it OOMs or hangs, only the child
		// dies. The parent (crawler) returns a clean error and moves on.
		// Disable with COSIFT_CRAWL_PDF=false; default is sandboxed-on.
		if os.Getenv("COSIFT_CRAWL_PDF") == "false" {
			return errors.New("pdf: parsing disabled (unset COSIFT_CRAWL_PDF=false to re-enable)")
		}
		parsed, perr = ParsePDFSandboxed(ctx, res.body, finalURL, "", 0)
	} else {
		parsed, perr = Parse(res.body, finalURL)
	}
	if perr != nil {
		return perr
	}
	if strings.TrimSpace(parsed.Text) == "" {
		return errors.New("empty content")
	}
	// See
	// config.Crawler.MinTextLen — defaults to 0 (off) so existing
	// deployments keep current behavior.
	if c.cfg.MinTextLen > 0 && len(parsed.Text) < c.cfg.MinTextLen {
		return errors.New("text below min_text_len")
	}

	finalU, _ := url.Parse(finalURL)
	sha := sha256.Sum256([]byte(parsed.Text))

	// Content-hash dedup: if the server gave us a 200 but the content_hash matches,
	// the index work is already done. Update validators + fetched_at and exit.
	// Catches servers that don't send ETag/Last-Modified (so 304 isn't available).
	if existing := prior; existing != nil {
		if bytes.Equal(existing.ContentSHA, sha[:]) {
			existing.FetchedAt = time.Now()
			if res.etag != "" {
				existing.ETag = res.etag
			}
			if res.lastModified != "" {
				existing.LastModified = res.lastModified
			}
			if _, e := c.store.UpsertDocument(ctx, existing); e != nil {
				return e
			}
			return nil
		}
	}

	// Content is new-or-changed at this point (the unchanged paths above
	// returned early). Propagate prior change history; bookkeeping bumps after upsert.
	var changeCount int64
	if prior != nil {
		changeCount = prior.ChangeCount
	}
	doc := &store.Document{
		URL:           finalURL,
		Domain:        finalU.Host,
		Title:         parsed.Title,
		Text:          parsed.Text,
		Lang:          parsed.Lang,
		Source:        "crawl",
		Quality:       0.5,
		FetchedAt:     time.Now(),
		ContentSHA:    sha[:],
		ETag:          res.etag,
		LastModified:  res.lastModified,
		ChangeCount:   changeCount + 1,
		LastChangedAt: time.Now().Unix(),
		PublishedAt:   parsed.PublishedAt, // JSON-LD datePublished (or zero if absent)
		Author:        parsed.AuthorName,  // JSON-LD author.name (empty if absent)
		Image:         parsed.Image,       // og:image / twitter:image / JSON-LD image (empty if absent)
		Favicon:       parsed.Favicon,     // <link rel="icon"> resolved absolute (empty if absent)
	}
	id, err := c.store.UpsertDocument(ctx, doc)
	if err != nil {
		return err
	}
	if err := c.idx.IndexDocument(ctx, id, parsed.Title, parsed.Text); err != nil {
		return err
	}

	// Dense indexing — optional, non-fatal. Multi-passage: chunk into ~512-token
	// windows (320 words ~= 512 BPE tokens) with 64-word overlap, embed each
	// chunk, write one row per passage. On embedder failure we log and continue;
	// the BM25 doc is already indexed.
	if c.embedder != nil {
		// per-host overrides win over global ChunkSize/ChunkOverlap;
		// both fall through to NewChunker defaults via's NewChunkerWith.
		// Host matched against the originally-requested URL (item.URL) —
		// redirects don't inherit.
		host := ""
		if u, err := url.Parse(item.URL); err == nil {
			host = u.Host
		}
		chunker := index.NewChunkerWith(c.chunkSizeFor(host), c.chunkOverlapFor(host))
		chunks := chunker.Chunk(parsed.Title + "\n\n" + parsed.Text)
		if len(chunks) > 0 {
			// cap each text by approximate token count, not raw
			// bytes. nomic-embed-text has a 2048-token model context. A naive
			// byte cap blows past the limit on dense scripts (CJK: ~1
			// token/char × 3 bytes/char → 6000 bytes = ~2000 tokens, fails).
			//
			// Heuristic: every byte ≤ 0x7F (ASCII / Latin-1) is ~0.25 tokens
			// (1 token / 4 chars rule); every wider rune is ~1 token. Cap at
			// 1500 estimated tokens (safe margin under 2048).
			const tokenCap = 1500
			texts := make([]string, len(chunks))
			for i, ch := range chunks {
				texts[i] = truncateForEmbed(ch.Text, tokenCap)
			}
			vecs, embErr := c.embedder.Embed(ctx, texts)
			if embErr != nil {
				log.Printf("embed %s: %v", item.URL, embErr)
			} else if len(vecs) == len(chunks) {
				// vector writes go through the optional PassageWriter
				// so Pebble-backed crawlers (no SQL passages table) can opt out
				// or supply their own HNSW bridge.
				// prefer the optional Batch interface so the underlying
				// writer can take its lock once for all chunks of this doc.
				// Batched writes held
				// the HNSW write lock for ~40 ms per acquisition (vs 5 ms),
				// blocking /search readers behind each batch. Per-chunk
				// writes give readers more chances to slip in. Same total
				// lock time, smaller bursts.
				if c.passageWriter != nil {
					// When this URL was
					// previously crawled, the prior generation of chunks
					// still lives in the HNSW graph (same url, stale vecs).
					// Mark them invalid before adding the fresh set so the
					// graph doesn't accumulate generations. Gated by env
					// COSIFT_ZOMBIE_RECLAIM=1 until soaked; off-by-default
					// preserves prior behavior bit-for-bit.
					if os.Getenv("COSIFT_ZOMBIE_RECLAIM") == "1" {
						inv, ok := c.passageWriter.(URLInvalidator)
						if !ok {
							log.Printf("zombie-reclaim: passageWriter %T does NOT implement URLInvalidator (one-time check)", c.passageWriter)
						} else {
							n, err := inv.MarkURLInvalid(ctx, item.URL)
							switch {
							case err != nil:
								log.Printf("zombie-reclaim %s: %v", item.URL, err)
							case n > 0:
								log.Printf("zombie-reclaim %s: invalidated %d stale passages", item.URL, n)
							case c.zombieDebugLogged < 5:
								// Diagnostic: first few zero-hit cases to confirm code path is reached.
								log.Printf("zombie-reclaim %s: 0 prior passages (diagnostic)", item.URL)
								c.zombieDebugLogged++
							}
						}
					}
					for i, ch := range chunks {
						p := &store.Passage{
							DocID:     id,
							Offset:    ch.Offset,
							Length:    ch.Length,
							Model:     c.embedder.Model(),
							Embedding: vecs[i],
						}
						if upErr := c.passageWriter.UpsertPassage(ctx, p); upErr != nil {
							log.Printf("save passage %s offset=%d: %v", item.URL, ch.Offset, upErr)
						}
					}
				}
			}
		}
	}

	// depth-cap check moved from parent-level to per-link in
	// enqueueLinks. Per-host overrides (cfg.PerHostMaxDepth) can exceed OR
	// undercut the default, so the authoritative check is per-link against
	// the child URL's host cap.
	c.enqueueLinks(ctx, parsed.Links, item.Depth+1)
	return nil
}

// maxDepthFor returns the depth cap for `host` — override if present in
// cfg.PerHostMaxDepth, default cfg.MaxDepth otherwise.
func (c *Crawler) maxDepthFor(host string) int {
	if d, ok := c.cfg.PerHostMaxDepth[host]; ok {
		return d
	}
	return c.cfg.MaxDepth
}

// chunkSizeFor returns the chunker size for `host` — override if present in
// cfg.PerHostChunkSize, default cfg.ChunkSize otherwise. Zero (no
// per-host AND no global override) signals the caller to use NewChunker's
// built-in default — same fallback shape as
func (c *Crawler) chunkSizeFor(host string) int {
	if s, ok := c.cfg.PerHostChunkSize[host]; ok && s > 0 {
		return s
	}
	return c.cfg.ChunkSize
}

// chunkOverlapFor returns the chunker overlap for `host` — override if present
// in cfg.PerHostChunkOverlap, default cfg.ChunkOverlap otherwise.
func (c *Crawler) chunkOverlapFor(host string) int {
	if o, ok := c.cfg.PerHostChunkOverlap[host]; ok && o > 0 {
		return o
	}
	return c.cfg.ChunkOverlap
}

// maxBodyBytesFor returns the body-size cap for `host` — override if present
// in cfg.PerHostMaxBodyBytes, default cfg.MaxBodyBytes otherwise.
// Returns the 5MB safe default if BOTH the override and default are zero/unset
// (preserves fallback in fetch).
func (c *Crawler) maxBodyBytesFor(host string) int64 {
	if b, ok := c.cfg.PerHostMaxBodyBytes[host]; ok && b > 0 {
		return b
	}
	if c.cfg.MaxBodyBytes > 0 {
		return c.cfg.MaxBodyBytes
	}
	return 5 << 20
}

func (c *Crawler) enqueueLinks(ctx context.Context, links []string, depth int) {
	// First pass canonicalizes + filters by
	// domain + depth, then batches a single host-count query, then enqueues
	// only links whose host hasn't hit cfg.MaxURLsPerHost. Without this cap,
	// fanout-heavy hosts (github.com, en.wikipedia.org) grow their queue
	// unboundedly to tens of thousands of URLs, starving the worker pool.
	type candidate struct {
		canon string
		host  string
	}
	candidates := make([]candidate, 0, len(links))
	hosts := make(map[string]struct{})
	for _, l := range links {
		canon, err := canonicalize(l)
		if err != nil {
			continue
		}
		if !c.allowedDomain(canon) {
			continue
		}
		// per-link depth check against the CHILD's host cap.
		// A child on a host with override=1 is dropped if depth would exceed 1,
		// even if the default MaxDepth is much higher (and vice versa).
		u, err := url.Parse(canon)
		if err != nil {
			continue
		}
		if depth > c.maxDepthFor(u.Host) {
			continue
		}
		h := strings.ToLower(u.Host)
		candidates = append(candidates, candidate{canon: canon, host: h})
		hosts[h] = struct{}{}
	}
	if len(candidates) == 0 {
		return
	}

	// Per-host cap: one batched COUNT(*) GROUP BY host, then in-memory
	// accounting as we enqueue. The accounting needs to track BOTH the count
	// already in the queue AND additions made in this batch — otherwise a
	// 300-link page with 280 github links would push all 280 past the cap.
	hostCap := c.cfg.MaxURLsPerHost
	var queuedPerHost map[string]int
	if hostCap > 0 {
		hostList := make([]string, 0, len(hosts))
		for h := range hosts {
			hostList = append(hostList, h)
		}
		var err error
		queuedPerHost, err = c.store.CountQueuedPerHost(ctx, hostList)
		if err != nil {
			// Falling back to no-cap is preferable to dropping links outright
			// on a transient SQLite error.
			queuedPerHost = map[string]int{}
		}
	}

	// The 24h refetch-skip path in
	// processClaimed bails out instantly when prior.FetchedAt is recent,
	// but only AFTER claiming the URL — so the frontier fills with re-claim
	// candidates that immediately bail, starving never-seen URLs in the same
	// queue. Filter here so re-claims of fresh docs never enter the frontier
	// in the first place. Gated by COSIFT_PREFER_NEW_URLS=1; default off
	// preserves prior behavior. Cost: one GetDocByURL per candidate (cheap
	// hash lookup in Pebble's 'd' family).
	preferNew := os.Getenv("COSIFT_PREFER_NEW_URLS") == "1"
	var freshWindow time.Duration
	if preferNew {
		hours := 24
		if v := os.Getenv("COSIFT_REFETCH_AFTER_HOURS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				hours = n
			}
		}
		freshWindow = time.Duration(hours) * time.Hour
	}

	for _, cand := range candidates {
		if hostCap > 0 && queuedPerHost[cand.host] >= hostCap {
			continue
		}
		if preferNew {
			if prior, _ := c.store.GetDocByURL(ctx, cand.canon); prior != nil && !prior.FetchedAt.IsZero() && time.Since(prior.FetchedAt) < freshWindow {
				continue // already indexed within freshness window — skip enqueue
			}
		}
		// The
		// route fn returns ownsLocally=true for single-node and same-shard;
		// otherwise we forward via HTTP (peer-side calls PushFrontier).
		if c.route != nil {
			owns, peer := c.route(cand.canon)
			if !owns && peer != "" && c.forward != nil {
				if err := c.forward(ctx, cand.canon, peer); err != nil {
					log.Printf("crawler: forward %s to peer %s: %v", cand.canon, peer, err)
				}
				continue
			}
		}
		// PushFrontier is INSERT OR IGNORE — dedup is at the persistent layer.
		_ = c.store.PushFrontier(ctx, cand.canon, depth, 0.5)
		if hostCap > 0 {
			queuedPerHost[cand.host]++
		}
	}
}

func (c *Crawler) allowedDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	// URL-substring blocklist. Cheaper than domain-suffix checks
	// because it short-circuits before URL.Parse runs again anywhere; runs
	// once per candidate. Operator-curated so false-positives are their
	// call.
	for _, pat := range c.cfg.ExcludeURLPatterns {
		if pat != "" && strings.Contains(rawURL, pat) {
			return false
		}
	}
	h := u.Host
	for _, d := range c.cfg.ExcludeDomains {
		if strings.HasSuffix(h, d) {
			return false
		}
	}
	if len(c.cfg.IncludeDomains) == 0 {
		return true
	}
	for _, d := range c.cfg.IncludeDomains {
		if strings.HasSuffix(h, d) {
			return true
		}
	}
	return false
}

// fetchResult bundles the fetch outcome so processClaimed can distinguish
// "fresh body to parse" from "server said 304, nothing changed".
type fetchResult struct {
	body         []byte
	finalURL     string
	contentType  string // Content-Type from response — drives parser dispatch (HTML vs PDF).
	notModified  bool   // server returned 304
	etag         string // ETag from response, if any
	lastModified string // Last-Modified from response, if any
}

// fetch returns the fresh body OR a notModified signal (with empty body).
// Sends conditional headers when the existing doc carries validators —
// turns re-crawls of unchanged stable pages into 304s with zero body bandwidth.
func (c *Crawler) fetch(ctx context.Context, u string, prior *store.Document) (*fetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/pdf")
	// Go's http.Transport sets
	// "gzip" automatically AND transparently decompresses on read. Setting the
	// header explicitly disables that auto-decompression — the body would arrive
	// gzipped and the parser would receive garbage on any CDN-fronted host.
	if prior != nil {
		if prior.ETag != "" {
			req.Header.Set("If-None-Match", prior.ETag)
		}
		if prior.LastModified != "" {
			req.Header.Set("If-Modified-Since", prior.LastModified)
		}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	finalURL := resp.Request.URL.String()
	if resp.StatusCode == http.StatusNotModified {
		return &fetchResult{finalURL: finalURL, notModified: true,
			etag: resp.Header.Get("ETag"), lastModified: resp.Header.Get("Last-Modified")}, nil
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "html") && !strings.Contains(ct, "xml") && !strings.Contains(ct, "pdf") && ct != "" {
		return nil, fmt.Errorf("non-html/xml/pdf content-type: %s", ct)
	}

	// Parse the originally-requested URL's
	// host (not finalURL after redirects) — operators configure overrides for
	// hosts they explicitly trust to deliver large bodies; a redirect to a
	// different host shouldn't inherit the trust.
	limit := int64(5 << 20)
	if reqURL, parseErr := url.Parse(u); parseErr == nil {
		limit = c.maxBodyBytesFor(reqURL.Host)
	} else if c.cfg.MaxBodyBytes > 0 {
		limit = c.cfg.MaxBodyBytes
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, err
	}
	return &fetchResult{
		body: body, finalURL: finalURL, contentType: ct,
		etag: resp.Header.Get("ETag"), lastModified: resp.Header.Get("Last-Modified"),
	}, nil
}

// canonicalize trims fragments, normalizes host casing, and strips
// common tracking params (utm_*, fbclid, gclid, mc_cid, mc_eid).
// Returns an error for non-http(s) schemes.
func canonicalize(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	u.Fragment = ""
	u.Host = strings.ToLower(u.Host)

	q := u.Query()
	for k := range q {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "utm_") || lk == "fbclid" || lk == "gclid" || lk == "mc_cid" || lk == "mc_eid" {
			q.Del(k)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// truncateForEmbed caps text by ESTIMATED token count for nomic-embed-text's
// 2048-token model context. Each ASCII byte counts as ~0.25 tokens
// (~4 chars/token rule), each wider rune ~1 token (BERT WordPiece tends to
// split a Chinese / Japanese / Korean / Arabic char into its own token).
// Stops at the first whitespace before the cap when possible to avoid
// bisecting a token.
func truncateForEmbed(text string, maxTokens int) string {
	// The token estimate
	// (0.35/byte for ASCII) underestimates dense source code, CI logs,
	// stack traces, and JSON-heavy content where punctuation creates many
	// micro-tokens. nomic-embed-text's 2048-token budget translates to
	// roughly 2500 bytes in the worst case; cap at 2500 to stay safely
	// under without re-tokenizing in-process.
	const byteCap = 2500
	var tokens float64
	var lastSpaceByte = -1
	for i, r := range text {
		if r > 0x7F {
			tokens++
		} else {
			tokens += 0.35
		}
		if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
			lastSpaceByte = i
		}
		if tokens >= float64(maxTokens) || i >= byteCap {
			cut := i
			if lastSpaceByte > i/2 {
				cut = lastSpaceByte
			}
			return text[:cut]
		}
	}
	return text
}
