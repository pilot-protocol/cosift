package main

import (
	"compress/gzip"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// handleCrawlEnqueue accepts a single URL forwarded from a peer shard and
// pushes it onto the local crawler's frontier. Auth via cfg.Cluster.
// PeerAuthToken Bearer header.
type crawlEnqueueReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleCrawlEnqueue(w http.ResponseWriter, r *http.Request) {
	// Auth
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
		return
	}
	if s.crawlSeed == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req crawlEnqueueReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"...\"}")
		return
	}
	if err := s.crawlSeed(req.URL); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queued": req.URL})
}

// handleAllowDomain promotes a domain into the crawler's runtime dynamic
// allowlist so subsequently-crawled URLs from it pass allowedDomain(). Used by
// the HN/Reddit harvesters to grow the crawlable set organically once a
// link-target domain recurs above their frequency threshold. Persisted so it
// survives restart. Body: {"domain":"example.com"}.
func (s *pebbleHTTP) handleAllowDomain(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
		return
	}
	if s.crawlAllowDomain == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler")
		return
	}
	var req struct {
		Domain string `json:"domain"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Domain == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"domain\": \"example.com\"}")
		return
	}
	if err := s.crawlAllowDomain(req.Domain); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"allowed": strings.ToLower(strings.TrimSpace(req.Domain))})
}

// handleFrontierClear wipes the entire frontier in a single Pebble
// DeleteRange — every queued URL, primary + secondary index. Used when
// the frontier is so polluted by spam-discovery crawls that
// claim-time exclude rules can't drain it in reasonable time. Operator
// re-seeds via seeds-file restart or /admin/sitemap-import calls.
//
// Sees the same PeerAuthToken as the other admin endpoints. Returns the
// approximate count of deleted entries (DeleteRange is O(tombstones),
// not O(N), so the count is reported as -1 to signal "swept range").
func (s *pebbleHTTP) handleFrontierClear(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
		return
	}
	if err := s.store.ClearFrontier(r.Context()); err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": true})
}

// handleFrontierPurgeHost deletes every queued frontier entry whose host
// matches the supplied value. Used to drain blacklisted hosts that the
// round-robin cursor would take days to chew through one-by-one (the
// cursor visits each host once per cycle; a 5M-entry host needs 5M
// cycles to drain, days of cycle time at typical claim rates).
type frontierPurgeReq struct {
	Host string `json:"host"`
}

func (s *pebbleHTTP) handleFrontierPurgeHost(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid peer token")
		return
	}
	var req frontierPurgeReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\": \"...\"}")
		return
	}
	n, err := s.store.PurgeFrontierByHost(r.Context(), req.Host)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"purged": n, "host": req.Host})
}

// handleSitemapImport fetches a sitemap.xml (or sitemap-index, one level
// of recursion) and pushes every <loc> entry to the live frontier. Same
// auth as crawl-enqueue. Synchronous — returns when all URLs are queued.
// For very large sitemaps (>50K URLs) the call may take seconds.
type sitemapImportReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleSitemapImport(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedSitemap == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req sitemapImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"https://.../sitemap.xml\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedSitemap(r.Context(), req.URL)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("sitemap-import: queued %d URLs from %s in %s", n, req.URL, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"sitemap": req.URL,
		"queued":  n,
		"elapsed": time.Since(t0).String(),
	})
}

// handleRecrawlSitemap fetches a sitemap.xml and calls Recrawl on every URL,
// resetting done/errored entries back to queued so the crawler re-visits them.
// Use this when a domain was previously blocked by include rules and its frontier
// entries are stuck in errored state — sitemap-import's INSERT OR IGNORE won't
// re-queue them, but Recrawl will.
func (s *pebbleHTTP) handleRecrawlSitemap(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlRecrawl == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler")
		return
	}
	var req sitemapImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, `expected {"url": "https://.../sitemap.xml"}`)
		return
	}
	// Fetch and parse the sitemap.
	hc := &http.Client{Timeout: 20 * time.Second}
	resp, err := hc.Get(req.URL)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "fetch sitemap: "+err.Error())
		return
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	// Extract <loc> entries — simple text scan avoids XML namespace headaches.
	// Handles both pretty-printed (one tag per line) and compact sitemaps
	// where <url><loc>…</loc></url> all appear on one line.
	t0 := time.Now()
	var reset, skipped int
	src := string(raw)
	for {
		start := strings.Index(src, "<loc>")
		if start < 0 {
			break
		}
		src = src[start+len("<loc>"):]
		end := strings.Index(src, "</loc>")
		if end < 0 {
			break
		}
		u := strings.TrimSpace(src[:end])
		src = src[end+len("</loc>"):]
		if err := s.crawlRecrawl(r.Context(), u); err != nil {
			skipped++
		} else {
			reset++
		}
	}
	log.Printf("recrawl-sitemap: reset %d, skipped %d from %s in %s", reset, skipped, req.URL, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"sitemap": req.URL,
		"reset":   reset,
		"skipped": skipped,
		"elapsed": time.Since(t0).String(),
	})
}

// handleCrawlNow runs the fetch-parse-index pipeline synchronously for one
// or more URLs, bypassing the persistent frontier entirely. Use this when
// the round-robin cursor in a large frontier (10M+) would take hours to
// reach an explicitly-seeded URL — direct fetch + UpsertDocument + chunk +
// embed all happen in this request. Returns per-URL outcomes.
type crawlNowReq struct {
	URLs []string `json:"urls"`
	URL  string   `json:"url,omitempty"`
}

func (s *pebbleHTTP) handleCrawlNow(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlFetchNow == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req crawlNowReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := json.Unmarshal(body, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "expected {\"urls\":[...]} or {\"url\":\"...\"}")
		return
	}
	urls := req.URLs
	if req.URL != "" {
		urls = append(urls, req.URL)
	}
	if len(urls) == 0 {
		writeProblem(w, http.StatusBadRequest, "no URLs in body")
		return
	}
	results := make([]map[string]any, 0, len(urls))
	t0 := time.Now()
	for _, u := range urls {
		err := s.crawlFetchNow(r.Context(), u)
		out := map[string]any{"url": u}
		if err != nil {
			out["error"] = err.Error()
		} else {
			out["ok"] = true
		}
		results = append(results, out)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"results": results,
		"elapsed": time.Since(t0).String(),
	})
}

// handleSitePack discovers a site's sitemaps + RSS feeds and bulk-enqueues
// everything found. Targets the "I want to index THIS site" operator flow
// without editing cosift.json + restarting + waiting for the cursor.
// Discovery order:
//  1. GET https://<host>/robots.txt — look for `Sitemap:` directives
//  2. Fallback to canonical /sitemap.xml + /sitemap_index.xml
//  3. RSS at common paths: /feed, /feed.xml, /rss, /rss.xml, /atom.xml, /feed/atom
//
// Each found resource is run through the existing SeedSitemap / SeedRSS
// primitives. Returns per-resource counts.
type sitePackReq struct {
	Host string `json:"host"` // e.g. "example.com" or "blog.example.com" — no scheme
}

func (s *pebbleHTTP) handleSitePack(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedSitemap == nil || s.crawlSeedRSS == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req sitePackReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\":\"example.com\"}")
		return
	}
	host, ok := normalizeBareHost(req.Host)
	if !ok {
		writeProblem(w, http.StatusBadRequest, "host must be a bare hostname like example.com")
		return
	}
	base := "https://" + host
	hc := &http.Client{Timeout: 20 * time.Second}

	type result struct {
		Source  string `json:"source"` // "robots-sitemap" | "fallback-sitemap" | "rss"
		URL     string `json:"url"`
		Indexed int    `json:"indexed"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, 0, 8)
	t0 := time.Now()

	candidateSitemaps, fromRobots := discoverSitemaps(r.Context(), hc, base)
	for _, su := range candidateSitemaps {
		n, err := s.crawlSeedSitemap(r.Context(), su)
		res := result{URL: su, Indexed: n}
		if fromRobots {
			res.Source = "robots-sitemap"
		} else {
			res.Source = "fallback-sitemap"
		}
		if err != nil {
			res.Error = err.Error()
		}
		results = append(results, res)
	}
	// Step 3: common RSS paths.
	for _, p := range []string{"/feed", "/feed.xml", "/rss", "/rss.xml", "/atom.xml", "/feed/atom"} {
		fu := base + p
		n, err := s.crawlSeedRSS(r.Context(), fu)
		// Silently skip RSS paths that 404 / don't parse — most sites have
		// only one of the listed paths.
		if err != nil && n == 0 {
			continue
		}
		results = append(results, result{Source: "rss", URL: fu, Indexed: n})
	}

	total := 0
	for _, r := range results {
		total += r.Indexed
	}
	log.Printf("site-pack: %s discovered %d resources, enqueued %d URLs in %s",
		host, len(results), total, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"host":          host,
		"resources":     len(results),
		"total_indexed": total,
		"elapsed":       time.Since(t0).String(),
		"results":       results,
	})
}

// normalizeBareHost strips scheme/trailing-slash from a host or URL and
// returns the bare hostname. ok is false when the result still contains a
// path segment (so callers can reject "example.com/foo" as a host).
func normalizeBareHost(s string) (host string, ok bool) {
	host = strings.TrimSpace(s)
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimSuffix(host, "/")
	host = strings.ToLower(host)
	if host == "" || strings.Contains(host, "/") {
		return "", false
	}
	return host, true
}

// discoverSitemaps returns candidate sitemap URLs for a site, given its base
// origin (e.g. "https://example.com"). It prefers Sitemap: directives in
// robots.txt; when robots.txt yields none it falls back to a small ordered
// list of canonical/CMS paths. fromRobots reports which source was used.
func discoverSitemaps(ctx context.Context, hc *http.Client, base string) (sitemaps []string, fromRobots bool) {
	if hc == nil {
		hc = &http.Client{Timeout: 20 * time.Second}
	}
	if req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/robots.txt", nil); err == nil {
		if rresp, err := hc.Do(req); err == nil {
			if rresp.StatusCode < 400 {
				rbody, _ := io.ReadAll(io.LimitReader(rresp.Body, 2<<20))
				for _, line := range strings.Split(string(rbody), "\n") {
					line = strings.TrimSpace(line)
					if strings.HasPrefix(strings.ToLower(line), "sitemap:") {
						if val := strings.TrimSpace(line[len("sitemap:"):]); val != "" {
							sitemaps = append(sitemaps, val)
						}
					}
				}
			}
			rresp.Body.Close()
		}
	}
	if len(sitemaps) > 0 {
		return sitemaps, true
	}
	// /sitemap.xml is the canonical spec but many CMSes (WordPress, Yoast,
	// Ghost, Hugo themes) ship at non-canonical paths. Try a small ordered
	// list before giving up: /sitemap.xml (most common), then WordPress's
	// /wp-sitemap.xml + Yoast's per-content-type splits, then index variants.
	for _, p := range []string{
		"/sitemap.xml",
		"/wp-sitemap.xml",    // WordPress 5.5+
		"/sitemap_index.xml", // Yoast SEO
		"/post-sitemap.xml",  // Yoast posts
		"/page-sitemap.xml",  // Yoast pages
		"/sitemap-index.xml", // some CMSes hyphenate
		"/sitemap.xml.gz",    // gzipped variant (sitemap.go handles .gz)
	} {
		sitemaps = append(sitemaps, base+p)
	}
	return sitemaps, false
}

// parseLaneName maps a friendly lane name to a frontier lane byte. The empty
// string and unknown values default to the high-priority submitted lane,
// which is the point of /admin/site-submit: jump a site to the front.
func parseLaneName(s string) byte {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "refresh":
		return store.LaneRefresh
	case "discovered":
		return store.LaneDiscovered
	case "bulk":
		return store.LaneBulk
	default: // "", "priority", "submitted", or anything unrecognized
		return store.LaneSubmitted
	}
}

func laneName(lane byte) string {
	switch lane {
	case store.LaneSubmitted:
		return "submitted"
	case store.LaneRefresh:
		return "refresh"
	case store.LaneDiscovered:
		return "discovered"
	case store.LaneBulk:
		return "bulk"
	default:
		return "submitted"
	}
}

// handleSiteSubmit discovers every URL of a website (via robots.txt sitemaps,
// then canonical fallbacks) and pushes them all onto the live crawl frontier
// in a chosen priority lane — by default the high-priority "submitted" lane,
// so the whole site jumps ahead of the generic discovery backlog. Same auth
// as the other admin endpoints. Synchronous; large sitemaps take seconds.
//
// Body: {"host":"pilotprotocol.network", "lane":"priority"}
//
//	lane: "priority" (default) | "refresh" | "discovered" | "bulk"
type siteSubmitReq struct {
	Host string `json:"host"`
	Lane string `json:"lane,omitempty"`
}

func (s *pebbleHTTP) handleSiteSubmit(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedSitemapLane == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req siteSubmitReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\":\"example.com\", \"lane\":\"priority\"}")
		return
	}
	host, ok := normalizeBareHost(req.Host)
	if !ok {
		writeProblem(w, http.StatusBadRequest, "host must be a bare hostname like example.com")
		return
	}
	lane := parseLaneName(req.Lane)
	base := "https://" + host
	hc := &http.Client{Timeout: 20 * time.Second}
	t0 := time.Now()

	type result struct {
		Source string `json:"source"` // "robots-sitemap" | "fallback-sitemap"
		URL    string `json:"url"`
		Queued int    `json:"queued"`
		Error  string `json:"error,omitempty"`
	}
	candidateSitemaps, fromRobots := discoverSitemaps(r.Context(), hc, base)
	source := "fallback-sitemap"
	if fromRobots {
		source = "robots-sitemap"
	}
	results := make([]result, 0, len(candidateSitemaps))
	total := 0
	for _, su := range candidateSitemaps {
		n, err := s.crawlSeedSitemapLane(r.Context(), su, lane)
		res := result{Source: source, URL: su, Queued: n}
		if err != nil {
			res.Error = err.Error()
		}
		total += n
		results = append(results, res)
	}
	log.Printf("site-submit: %s → lane=%s discovered %d sitemap(s), queued %d URLs in %s",
		host, laneName(lane), len(results), total, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"host":         host,
		"lane":         laneName(lane),
		"sitemaps":     len(results),
		"total_queued": total,
		"elapsed":      time.Since(t0).String(),
		"results":      results,
	})
}

// handleWETImportBulk fetches a CommonCrawl `wet.paths.gz` manifest,
// takes the first N entries (or skip+take), and runs `/admin/wet-import`
// against each one in parallel. Lets operators bulk-ingest a release with
// one call instead of repeatedly POSTing per file. Synchronous — blocks
// until all N finish, returns total docs indexed.
//
// Example body:
//
//	{"manifest_url":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-51/wet.paths.gz",
//	 "count":4, "skip":0, "concurrency":2, "lexical_only":true}
type wetImportBulkReq struct {
	ManifestURL string `json:"manifest_url"`
	Count       int    `json:"count"`
	Skip        int    `json:"skip,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	LexicalOnly bool   `json:"lexical_only,omitempty"`
	DedupeFresh bool   `json:"dedupe_fresh,omitempty"`
}

func (s *pebbleHTTP) handleWETImportBulk(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedWET == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req wetImportBulkReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.ManifestURL == "" || req.Count <= 0 {
		writeProblem(w, http.StatusBadRequest, "expected {\"manifest_url\":\"https://.../wet.paths.gz\",\"count\":N}")
		return
	}
	if req.Concurrency <= 0 {
		req.Concurrency = 2
	}
	if req.Concurrency > 8 {
		req.Concurrency = 8 // beyond this the pebble write lock dominates anyway
	}

	// Fetch the manifest. wet.paths.gz is a gzipped newline-delimited list
	// of relative paths under data.commoncrawl.org.
	mreq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, req.ManifestURL, http.NoBody)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "build manifest request: "+err.Error())
		return
	}
	mreq.Header.Set("User-Agent", "cosift-bulk-import")
	mresp, err := (&http.Client{Timeout: 60 * time.Second}).Do(mreq)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "fetch manifest: "+err.Error())
		return
	}
	defer mresp.Body.Close()
	if mresp.StatusCode >= 400 {
		writeProblem(w, http.StatusBadGateway, fmt.Sprintf("manifest http %d", mresp.StatusCode))
		return
	}
	zr, err := gzip.NewReader(mresp.Body)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "gunzip manifest: "+err.Error())
		return
	}
	defer zr.Close()
	mbody, err := io.ReadAll(io.LimitReader(zr, 32<<20))
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "read manifest: "+err.Error())
		return
	}
	// Each line is a path like "crawl-data/CC-MAIN-.../wet/...warc.wet.gz".
	// Prepend the data.commoncrawl.org base.
	all := strings.Split(strings.TrimSpace(string(mbody)), "\n")
	if req.Skip >= len(all) {
		writeProblem(w, http.StatusBadRequest, fmt.Sprintf("skip=%d exceeds manifest size %d", req.Skip, len(all)))
		return
	}
	end := req.Skip + req.Count
	if end > len(all) {
		end = len(all)
	}
	paths := all[req.Skip:end]

	// Run imports in parallel, bounded by concurrency.
	type result struct {
		URL     string `json:"url"`
		Indexed int    `json:"indexed"`
		Elapsed string `json:"elapsed"`
		Error   string `json:"error,omitempty"`
	}
	results := make([]result, len(paths))
	sem := make(chan struct{}, req.Concurrency)
	var wg sync.WaitGroup
	t0 := time.Now()
	for i, p := range paths {
		i, p := i, strings.TrimSpace(p)
		if p == "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			full := "https://data.commoncrawl.org/" + p
			start := time.Now()
			n, err := s.crawlSeedWET(r.Context(), full, req.DedupeFresh, req.LexicalOnly)
			results[i].URL = full
			results[i].Indexed = n
			results[i].Elapsed = time.Since(start).Round(time.Second).String()
			if err != nil {
				results[i].Error = err.Error()
			}
		}()
	}
	wg.Wait()

	total := 0
	for _, r := range results {
		total += r.Indexed
	}
	log.Printf("wet-import-bulk: indexed %d docs from %d WET files in %s", total, len(paths), time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"manifest_url":  req.ManifestURL,
		"files":         len(paths),
		"total_indexed": total,
		"elapsed":       time.Since(t0).String(),
		"results":       results,
	})
}

// handleWETImport streams a CommonCrawl WET file (gzipped, pre-extracted
// plain text per URL) and runs each record through UpsertDocument + BM25
// indexing + chunk + embed. Bypasses the fetch-and-parse pipeline entirely
// because WET bodies are already extracted text — typically 50-100× faster
// than open-web crawling.
//
// Example: POST {"url":"https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-09/segments/.../wet/CC-MAIN-...wet.gz"}
type wetImportReq struct {
	URL         string `json:"url"`
	DedupeFresh bool   `json:"dedupe_fresh,omitempty"` // skip URLs already-fresh in famDoc
	LexicalOnly bool   `json:"lexical_only,omitempty"` // BM25-only ingest, defer dense embed to /admin/embed-backfill
}

func (s *pebbleHTTP) handleWETImport(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedWET == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req wetImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\":\"https://.../...warc.wet.gz\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedWET(r.Context(), req.URL, req.DedupeFresh, req.LexicalOnly)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("wet-import: indexed %d docs from %s in %s", n, req.URL, time.Since(t0).Round(time.Second))
	writeJSON(w, http.StatusOK, map[string]any{
		"wet":     req.URL,
		"indexed": n,
		"elapsed": time.Since(t0).String(),
	})
}

// handleFrontierDemoteHost re-keys every queued URL for a host into a
// different lane. The escape hatch for the cloud.google.com problem:
// 2.8M queued URLs on one host (65% of the queue) starve host-fair
// claim slots from fresher lanes. Demote to lane 3 (bulk, 5% weight)
// and lane 1/2 actually get the work.
//
// POST body: {"host": "cloud.google.com", "lane": 3}
type frontierDemoteHostReq struct {
	Host string `json:"host"`
	Lane int    `json:"lane"`
}

func (s *pebbleHTTP) handleFrontierDemoteHost(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	ps, ok := any(s.store).(*store.PebbleStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "lanes are PebbleStore-only")
		return
	}
	var req frontierDemoteHostReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.Host == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"host\":\"foo.com\",\"lane\":0..3}")
		return
	}
	if req.Lane < 0 || req.Lane > 3 {
		writeProblem(w, http.StatusBadRequest, "lane must be 0..3")
		return
	}
	t0 := time.Now()
	n, err := ps.DemoteHostToLane(r.Context(), req.Host, byte(req.Lane))
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("frontier-demote-host: moved %d URLs (%s -> lane %d) in %s", n, req.Host, req.Lane, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"host":    req.Host,
		"lane":    req.Lane,
		"moved":   n,
		"elapsed": time.Since(t0).String(),
	})
}

// handleFrontierPurgeStaleInFlight clears the stale 'i' secondary keys
// left over from the pre-fix RecoverInFlight bug. Pre-fix, every restart
// re-queued in-flight URLs via the LEGACY 'q' index only and skipped the
// lane-aware 'i' delete, so each restart leaked the URL's lane-aware 'i'
// key. GetLaneStats then reported impossibly-high in_flight counts
// (>max_concurrent). Idempotent — re-running is a no-op once clean.
func (s *pebbleHTTP) handleFrontierPurgeStaleInFlight(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	ps, ok := any(s.store).(*store.PebbleStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "PebbleStore-only")
		return
	}
	t0 := time.Now()
	n, err := ps.PurgeStaleInFlight(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("frontier-purge-stale-inflight: purged %d keys in %s", n, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{"purged": n, "elapsed": time.Since(t0).String()})
}

// handleRSSImport fetches an RSS 2.0 or Atom feed and pushes every <item>/
// <entry> link to the live frontier. Same auth shape as sitemap-import.
// Designed to be cron-friendly: idempotent against the frontier (re-seeding
// the same feed only adds newly-listed items).
type rssImportReq struct {
	URL string `json:"url"`
}

func (s *pebbleHTTP) handleRSSImport(w http.ResponseWriter, r *http.Request) {
	if !peerTokenOK(r, s.cluster.PeerAuthToken) {
		writeProblem(w, http.StatusUnauthorized, "missing or invalid admin token")
		return
	}
	if s.crawlSeedRSS == nil {
		writeProblem(w, http.StatusNotImplemented, "this shard has no in-serve crawler (-crawl-seeds-file not set)")
		return
	}
	var req rssImportReq
	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err := json.Unmarshal(body, &req); err != nil || req.URL == "" {
		writeProblem(w, http.StatusBadRequest, "expected {\"url\": \"https://.../feed.xml\"}")
		return
	}
	t0 := time.Now()
	n, err := s.crawlSeedRSS(r.Context(), req.URL)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("rss-import: queued %d URLs from %s in %s", n, req.URL, time.Since(t0).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, map[string]any{
		"feed":    req.URL,
		"queued":  n,
		"elapsed": time.Since(t0).String(),
	})
}
