// Package crawler is a small, dependency-light web crawler.
//
// Iter 3 scope: fetch, parse, store, robots.txt, persistent frontier.
// Concurrent fetcher pool + per-host rate limiting land in iter 4.
package crawler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/embed"
	"github.com/calinteodor/cosift/internal/index"
	"github.com/calinteodor/cosift/internal/store"
)

// Crawler orchestrates fetch → parse → index over a persistent frontier.
type Crawler struct {
	cfg      config.Crawler
	store    *store.Store
	idx      *index.BM25
	http     *http.Client
	robots   *Robots
	embedder embed.Embedder // optional; nil = lexical-only ingest
}

// New constructs a crawler against a store. Caller owns store lifecycle.
func New(cfg config.Crawler, s *store.Store) *Crawler {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          50,
			MaxConnsPerHost:       2,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
			ResponseHeaderTimeout: 15 * time.Second,
		},
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
	return &Crawler{
		cfg:    cfg,
		store:  s,
		idx:    index.NewBM25(s),
		http:   httpClient,
		robots: robots,
	}
}

// WithEmbedder enables dense indexing during crawl: every successfully indexed
// document also gets one passage embedding written to the store. Embedding
// failure is non-fatal — the BM25 index still gets the document.
func (c *Crawler) WithEmbedder(e embed.Embedder) *Crawler {
	c.embedder = e
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
	// Iter 128: build per-host override map from int ms → time.Duration.
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

	wg.Wait()
	return nil
}

func (c *Crawler) worker(ctx context.Context, wg *sync.WaitGroup, gate *hostGate) {
	defer wg.Done()
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
		if err := c.processClaimed(ctx, item, gate); err != nil {
			log.Printf("crawl %s: %v", item.URL, err)
			_ = c.store.FailFrontier(ctx, item.URL, err.Error())
			continue
		}
		_ = c.store.CompleteFrontier(ctx, item.URL)
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

func (c *Crawler) processClaimed(ctx context.Context, item store.FrontierItem, gate *hostGate) error {
	u, _ := url.Parse(item.URL)
	if u != nil {
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
		// Iter 128: use the effective per-host delay (gate's override or
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
		if _, err := c.store.UpsertDocument(ctx, prior); err != nil {
			return err
		}
		return nil
	}

	finalURL := res.finalURL
	// Dispatch parser by content-type. Iter 73 added PDF support; HTML/XML
	// remains the default both for legacy responses and for missing/empty
	// Content-Type headers (the lenient case Parse already handles).
	var parsed *ParsedDoc
	var perr error
	if strings.Contains(res.contentType, "pdf") {
		parsed, perr = ParsePDF(res.body, finalURL)
	} else {
		parsed, perr = Parse(res.body, finalURL)
	}
	if perr != nil {
		return perr
	}
	if strings.TrimSpace(parsed.Text) == "" {
		return errors.New("empty content")
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
			if _, err := c.store.UpsertDocument(ctx, existing); err != nil {
				return err
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
		PublishedAt:   parsed.PublishedAt, // iter 77: JSON-LD datePublished (or zero if absent)
		Author:        parsed.AuthorName, // iter 150: JSON-LD author.name (empty if absent)
		Image:         parsed.Image,      // iter 155: og:image / twitter:image / JSON-LD image (empty if absent)
		Favicon:       parsed.Favicon,    // iter 156: <link rel="icon"> resolved absolute (empty if absent)
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
		// Iter 146: per-host overrides win over global ChunkSize/ChunkOverlap;
		// both fall through to NewChunker defaults via iter-147's NewChunkerWith.
		// Host matched against the originally-requested URL (item.URL) —
		// redirects don't inherit (iter 130's per-host body-size policy).
		host := ""
		if u, err := url.Parse(item.URL); err == nil {
			host = u.Host
		}
		chunker := index.NewChunkerWith(c.chunkSizeFor(host), c.chunkOverlapFor(host))
		chunks := chunker.Chunk(parsed.Title + "\n\n" + parsed.Text)
		if len(chunks) > 0 {
			texts := make([]string, len(chunks))
			for i, ch := range chunks {
				texts[i] = ch.Text
			}
			vecs, embErr := c.embedder.Embed(ctx, texts)
			if embErr != nil {
				log.Printf("embed %s: %v", item.URL, embErr)
			} else if len(vecs) == len(chunks) {
				for i, ch := range chunks {
					p := &store.Passage{
						DocID:     id,
						Offset:    ch.Offset,
						Length:    ch.Length,
						Model:     c.embedder.Model(),
						Embedding: vecs[i],
					}
					if upErr := c.store.UpsertPassage(ctx, p); upErr != nil {
						log.Printf("save passage %s offset=%d: %v", item.URL, ch.Offset, upErr)
					}
				}
			}
		}
	}

	// Iter 129: depth-cap check moved from parent-level to per-link in
	// enqueueLinks. Per-host overrides (cfg.PerHostMaxDepth) can exceed OR
	// undercut the default, so the authoritative check is per-link against
	// the child URL's host cap.
	c.enqueueLinks(ctx, parsed.Links, item.Depth+1)
	return nil
}

// maxDepthFor returns the depth cap for `host` — override if present in
// cfg.PerHostMaxDepth, default cfg.MaxDepth otherwise. Iter 129.
func (c *Crawler) maxDepthFor(host string) int {
	if d, ok := c.cfg.PerHostMaxDepth[host]; ok {
		return d
	}
	return c.cfg.MaxDepth
}

// chunkSizeFor returns the chunker size for `host` — override if present in
// cfg.PerHostChunkSize, default cfg.ChunkSize otherwise. Iter 146. Zero (no
// per-host AND no global override) signals the caller to use NewChunker's
// built-in default — same fallback shape as iter-142.
func (c *Crawler) chunkSizeFor(host string) int {
	if s, ok := c.cfg.PerHostChunkSize[host]; ok && s > 0 {
		return s
	}
	return c.cfg.ChunkSize
}

// chunkOverlapFor returns the chunker overlap for `host` — override if present
// in cfg.PerHostChunkOverlap, default cfg.ChunkOverlap otherwise. Iter 146.
func (c *Crawler) chunkOverlapFor(host string) int {
	if o, ok := c.cfg.PerHostChunkOverlap[host]; ok && o > 0 {
		return o
	}
	return c.cfg.ChunkOverlap
}

// maxBodyBytesFor returns the body-size cap for `host` — override if present
// in cfg.PerHostMaxBodyBytes, default cfg.MaxBodyBytes otherwise. Iter 130.
// Returns the 5MB safe default if BOTH the override and default are zero/unset
// (preserves iter-1 fallback in fetch).
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
	for _, l := range links {
		canon, err := canonicalize(l)
		if err != nil {
			continue
		}
		if !c.allowedDomain(canon) {
			continue
		}
		// Iter 129: per-link depth check against the CHILD's host cap.
		// A child on a host with override=1 is dropped if depth would exceed 1,
		// even if the default MaxDepth is much higher (and vice versa).
		u, err := url.Parse(canon)
		if err == nil && depth > c.maxDepthFor(u.Host) {
			continue
		}
		// PushFrontier is INSERT OR IGNORE — dedup is at the persistent layer.
		_ = c.store.PushFrontier(ctx, canon, depth, 0.5)
	}
}

func (c *Crawler) allowedDomain(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
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
	contentType  string // Content-Type from response — drives parser dispatch (HTML vs PDF). Iter 73.
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
	// Iter 141: do NOT manually set Accept-Encoding. Go's http.Transport sets
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

	// Iter 130: per-host body-size cap. Parse the originally-requested URL's
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
