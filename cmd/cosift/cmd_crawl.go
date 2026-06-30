package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/authority"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

func runCrawl(ctx context.Context, cfg *config.Config, args []string) error {
	// hoisted PebbleStore handle so the embedder wiring below can
	// reach it to attach the HNSW bridge after the backend switch.
	var pebbleStoreForCrawl *store.PebbleStore
	fs := flag.NewFlagSet("crawl", flag.ExitOnError)
	refresh := fs.Bool("refresh", false, "force re-crawl of URLs already in the frontier")
	sitemap := fs.String("sitemap", "", "URL of a sitemap.xml (or sitemap index) to seed from")
	seedsFile := fs.String("seeds-file", "", "path to a text file with one seed URL per line (blank lines and # comments ignored)")
	backend := fs.String("backend", "sqlite", "storage backend: sqlite (default) | pebble")
	duration := fs.Duration("duration", 0, "stop the crawl cleanly after this much time (0 = run until frontier empty or SIGTERM)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	urls := fs.Args()
	// -seeds-file lets operators target specific websites in bulk
	// without stuffing dozens of URLs on the command line. Each non-blank,
	// non-comment line is treated as a seed.
	if *seedsFile != "" {
		buf, err := os.ReadFile(*seedsFile)
		if err != nil {
			return fmt.Errorf("read -seeds-file %s: %w", *seedsFile, err)
		}
		for _, line := range strings.Split(string(buf), "\n") {
			s := strings.TrimSpace(line)
			if s == "" || strings.HasPrefix(s, "#") {
				continue
			}
			urls = append(urls, s)
		}
		log.Printf("crawler: loaded %d seeds from %s (after dedup of positional args, total queued = %d)", len(urls)-fs.NArg(), *seedsFile, len(urls))
	}
	if len(urls) == 0 && *sitemap == "" {
		return errors.New("crawl: at least one URL, -seeds-file, or -sitemap is required")
	}

	// Wraps the caller's ctx with a
	// timeout; workers see ctx.Err() != nil and exit cleanly. Pebble flushes
	// on Close (via the deferred ps.Close() below) so durability is preserved.
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
		log.Printf("crawler: bounded run, will stop after %s", *duration)
	}

	var c *crawler.Crawler
	switch *backend {
	case "sqlite", "":
		s, err := store.Open(cfg.DataDir)
		if err != nil {
			return err
		}
		defer s.Close()
		c = crawler.New(cfg.Crawler, s)
	case "pebble":
		// route through the  Pebble path. The
		// data dir layout under cfg.DataDir for Pebble: a sibling "pebble"
		// subdir so SQLite and Pebble stores can coexist during migration.
		pebbleDir := filepath.Join(cfg.DataDir, "pebble")
		var err error
		pebbleStoreForCrawl, err = openPebbleOrFriendlyErr(pebbleDir)
		if err != nil {
			return err
		}
		defer pebbleStoreForCrawl.Close()
		c = crawler.NewWithBackend(cfg.Crawler, pebbleStoreForCrawl, index.NewPebbleBM25(pebbleStoreForCrawl))
		log.Printf("crawler: pebble backend at %s", pebbleDir)
	default:
		return fmt.Errorf("crawl: unknown -backend %q (want: sqlite | pebble)", *backend)
	}

	// Authority-weighted frontier priority: load Tranco / Majestic CSVs when
	// configured. The scorer is read-only after loading — safe to share with
	// the crawler's concurrent worker pool.
	if cfg.Crawler.AuthorityPriority {
		scorer := authority.New()
		if cfg.Crawler.AuthorityTrancoCSV != "" {
			f, ferr := os.Open(cfg.Crawler.AuthorityTrancoCSV)
			if ferr != nil {
				log.Printf("crawler: authority: open tranco csv %s: %v (skipping)", cfg.Crawler.AuthorityTrancoCSV, ferr)
			} else {
				n, lerr := scorer.LoadTranco(f)
				_ = f.Close()
				if lerr != nil {
					log.Printf("crawler: authority: load tranco: %v (skipping)", lerr)
				} else {
					log.Printf("crawler: authority: loaded %d tranco entries from %s", n, cfg.Crawler.AuthorityTrancoCSV)
				}
			}
		}
		if cfg.Crawler.AuthorityMajesticCSV != "" {
			f, ferr := os.Open(cfg.Crawler.AuthorityMajesticCSV)
			if ferr != nil {
				log.Printf("crawler: authority: open majestic csv %s: %v (skipping)", cfg.Crawler.AuthorityMajesticCSV, ferr)
			} else {
				n, lerr := scorer.LoadMajestic(f)
				_ = f.Close()
				if lerr != nil {
					log.Printf("crawler: authority: load majestic: %v (skipping)", lerr)
				} else {
					log.Printf("crawler: authority: loaded %d majestic entries from %s", n, cfg.Crawler.AuthorityMajesticCSV)
				}
			}
		}
		// Even without CSV data the embedded whitelist in authority.New() gives
		// known-good sites (arxiv, MDN, Wikipedia, kernel.org, …) a priority boost.
		c = c.WithAuthority(scorer)
		log.Printf("crawler: authority-weighted frontier priority enabled (tranco=%s majestic=%s)",
			cfg.Crawler.AuthorityTrancoCSV, cfg.Crawler.AuthorityMajesticCSV)
	}

	// Auto-wire embedder when configured. For the Pebble backend, also
	// build and persist an HNSW graph via the hnswPassageWriter bridge
	// — that's the path /search?retriever=dense
	// needs and that pre was a documented no-op.
	// API key is OPTIONAL when cfg.Embeddings.URL points at a
	// custom endpoint (Ollama / vLLM / TEI / etc — local self-hosted
	// embedders don't need a Bearer token). Required only when hitting the
	// default OpenAI endpoint.
	if cfg.Embeddings.Model != "" {
		apiKey := resolveEmbedAPIKey()
		needsKey := cfg.Embeddings.URL == "" // default OpenAI requires a key
		if apiKey != "" || !needsKey {
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, cfg.Embeddings.Model, dim)
			c = c.WithEmbedder(emb)
			authStatus := "anonymous"
			if apiKey != "" {
				authStatus = "bearer-token"
			}
			log.Printf("crawler: dense embeddings enabled (model=%s, dim=%d, auth=%s)", cfg.Embeddings.Model, dim, authStatus)
			if pebbleStoreForCrawl != nil {
				h := index.NewHNSW(dim)
				c = c.WithPassageWriter(&hnswPassageWriter{ps: pebbleStoreForCrawl, hnsw: h})
				// periodic checkpoint every COSIFT_HNSW_CHECKPOINT_SEC
				// (default 60s). Without this, a crash / SIGKILL / time-cap
				// deadline mid-crawl loses all in-memory vectors because Persist
				// only runs at deferred end-of-crawl. Goroutine exits when
				// ctx.Done() fires (either deadline or SIGTERM).
				checkpointEvery := 60 * time.Second
				if v := os.Getenv("COSIFT_HNSW_CHECKPOINT_SEC"); v != "" {
					if s, err := strconv.Atoi(v); err == nil && s >= 5 {
						checkpointEvery = time.Duration(s) * time.Second
					}
				}
				log.Printf("crawler: HNSW vector index attached (in-memory build, checkpoint every %s, final persist at crawl end)", checkpointEvery)
				ckpDone := make(chan struct{})
				go func() {
					defer close(ckpDone)
					t := time.NewTicker(checkpointEvery)
					defer t.Stop()
					var lastN int
					for {
						select {
						case <-ctx.Done():
							return
						case <-t.C:
							n := h.Len()
							if n == 0 || n == lastN {
								continue // nothing new to persist
							}
							log.Printf("crawler: HNSW checkpoint at %d nodes (delta=%d) ...", n, n-lastN)
							if err := h.Persist(context.Background(), pebbleStoreForCrawl); err != nil {
								log.Printf("crawler: HNSW checkpoint failed: %v", err)
								continue
							}
							lastN = n
						}
					}
				}()
				defer func() {
					<-ckpDone // ensure ticker goroutine exits before final persist
					if h.Len() == 0 {
						return
					}
					log.Printf("crawler: final HNSW persist (%d nodes, dim=%d) to Pebble...", h.Len(), dim)
					if err := h.Persist(context.Background(), pebbleStoreForCrawl); err != nil {
						log.Printf("crawler: HNSW persist failed: %v", err)
					} else {
						log.Printf("crawler: HNSW persist complete")
					}
				}()
			}
		} else {
			log.Printf("warning: embeddings configured (default OpenAI endpoint) but no OPENAI_API_KEY in env; crawling BM25 only")
		}
	}
	if *sitemap != "" {
		n, err := c.SeedSitemap(ctx, *sitemap)
		if err != nil {
			return fmt.Errorf("sitemap %s: %w", *sitemap, err)
		}
		log.Printf("seeded %d URLs from sitemap %s", n, *sitemap)
	}
	for _, u := range urls {
		if *refresh {
			if err := c.Recrawl(ctx, u); err != nil {
				return fmt.Errorf("recrawl %s: %w", u, err)
			}
		} else {
			if err := c.Seed(u); err != nil {
				return fmt.Errorf("seed %s: %w", u, err)
			}
		}
	}
	return c.Run(ctx)
}

// runRefreshDue re-queues URLs whose adaptive interval has elapsed.
// Combined with conditional GET, a refresh-due pass over a stable corpus
// mostly costs 304 round-trips — no body, no parse, no embed.
//
// One-shot by default. Pass `-interval` to loop forever — useful when running
// as a systemd service or inside the Docker image (no extra cron needed).
func runRefreshDue(ctx context.Context, cfg *config.Config, args []string) (err error) {
	fs := flag.NewFlagSet("refresh-due", flag.ExitOnError)
	minH := fs.Duration("min", 1*time.Hour, "minimum re-crawl interval")
	maxH := fs.Duration("max", 30*24*time.Hour, "maximum re-crawl interval")
	limit := fs.Int("limit", 100, "max URLs to enqueue per pass")
	dryRun := fs.Bool("dry-run", false, "print URLs that would be refreshed, don't enqueue")
	interval := fs.Duration("interval", 0, "loop with this delay between passes (0 = one-shot)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	pass := func() error {
		due, err := s.DueForRefresh(ctx, *minH, *maxH, *limit)
		if err != nil {
			return err
		}
		if len(due) == 0 {
			log.Printf("refresh-due: 0 URLs due")
			return nil
		}
		log.Printf("refresh-due: %d URLs due (limit %d, min=%s max=%s)", len(due), *limit, *minH, *maxH)
		if *dryRun {
			for _, u := range due {
				fmt.Println(u)
			}
			return nil
		}
		c := crawler.New(cfg.Crawler, s)
		for _, u := range due {
			if err := c.Recrawl(ctx, u); err != nil {
				log.Printf("recrawl %s: %v", u, err)
			}
		}
		return c.Run(ctx)
	}

	if *interval <= 0 {
		return pass()
	}
	// Daemon mode: pass, sleep, pass, ... until ctx cancelled.
	for {
		if err := pass(); err != nil {
			log.Printf("refresh-due pass: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*interval):
		}
	}
}

// siteToHost extracts the hostname from a URL or returns the input verbatim if
// it's already a bare host (e.g. "docs.example.com" vs "https://docs.example.com").
// Used by `cosift init -site` to populate include_domains.
func siteToHost(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("empty site")
	}
	// If no scheme, treat as bare host and validate.
	if !strings.Contains(s, "://") {
		// reject if it looks like a path or has whitespace
		if strings.ContainsAny(s, " /\t\n") {
			return "", fmt.Errorf("not a hostname: %q", s)
		}
		return s, nil
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("could not extract host from %q", s)
	}
	return u.Host, nil
}

// runCheckRobots reports whether each URL is crawlable under the site's
// robots.txt, plus any Crawl-delay. Lets operators plan crawls without
// hand-inspecting the site's robots.txt.
//
// Wraps the existing internal/crawler.Robots so the CLI uses exactly the
// same enforcement logic the real crawler would. No drift.
func runCheckRobots(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("check-robots", flag.ExitOnError)
	userAgent := fs.String("user-agent", cfg.Crawler.UserAgent, "User-Agent the check pretends to be (defaults to cfg.crawler.user_agent)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	urls := fs.Args()
	if len(urls) == 0 {
		return errors.New("usage: cosift check-robots [-user-agent UA] <url...>")
	}
	if *userAgent == "" {
		*userAgent = "CosiftBot/0.0 (+https://github.com/pilot-protocol/cosift)"
	}
	httpClient := &http.Client{Timeout: 15 * time.Second}
	r := crawler.NewRobots(httpClient, *userAgent)

	fmt.Printf("user-agent: %s\n", *userAgent)

	// Probe robots.txt for each unique host before running per-URL checks.
	// Robots.Allowed returns (true, ...) when robots.txt is unreachable
	// (graceful degradation suits the crawler), but the operator running
	// check-robots wants to see "this host has no reachable robots.txt"
	// explicitly. Group URLs by host so we only probe each host once.
	hosts := uniqueHosts(urls)
	fmt.Println()
	for _, h := range hosts {
		probeURL := h + "/robots.txt"
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, http.NoBody)
		req.Header.Set("User-Agent", *userAgent)
		resp, err := httpClient.Do(req)
		if err != nil {
			fmt.Printf("robots.txt   %s — UNREACHABLE: %v (crawler will assume ALLOWED for everything on this host)\n", probeURL, err)
			continue
		}
		_ = resp.Body.Close()
		switch {
		case resp.StatusCode == 200:
			fmt.Printf("robots.txt   %s — OK (status 200)\n", probeURL)
		case resp.StatusCode == 404:
			fmt.Printf("robots.txt   %s — 404, no rules (crawler will assume ALLOWED)\n", probeURL)
		default:
			fmt.Printf("robots.txt   %s — status %d (crawler will assume ALLOWED on non-2xx/404)\n", probeURL, resp.StatusCode)
		}
		// surface Sitemap: directives from robots.txt. Modern
		// crawlers auto-discover sitemaps this way; this output lets operators
		// see what `cosift crawl -sitemap <URL>` could seed from.
		if sitemaps := r.Sitemaps(ctx, h); len(sitemaps) > 0 {
			for _, sm := range sitemaps {
				fmt.Printf("  Sitemap:   %s\n", sm)
			}
		}
	}
	fmt.Println()
	for _, u := range urls {
		allowed, crawlDelay, err := r.Allowed(ctx, u)
		if err != nil {
			fmt.Printf("[ERR ]  %s — %v\n", u, err)
			continue
		}
		mark := "[OK  ]"
		if !allowed {
			mark = "[DENY]"
		}
		extra := ""
		if crawlDelay > 0 {
			extra = fmt.Sprintf("  crawl-delay=%s", crawlDelay)
		}
		fmt.Printf("%s  %s%s\n", mark, u, extra)
	}
	return nil
}

// uniqueHosts extracts the scheme+host part from each URL, deduped + in order.
// Used by check-robots to probe each host's robots.txt exactly once.
func uniqueHosts(urls []string) []string {
	seen := make(map[string]bool)
	out := []string{}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			continue
		}
		key := u.Scheme + "://" + u.Host
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	return out
}

// runCrawlErrors lists recently-errored frontier URLs with their failure reason.
// operator diagnostic for "why are 142 URLs in error state?" without
// requiring SQLite shell access. Pure read-only; no side effects on the index.
//
// The error column is's addition to the frontier schema; pre
// errored URLs will have an empty reason.
func runCrawlErrors(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("crawl-errors", flag.ExitOnError)
	limit := fs.Int("limit", 50, "max number of errored URLs to list (most recent first)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	errs, err := s.ListErroredFrontier(ctx, *limit)
	if err != nil {
		return fmt.Errorf("list errored: %w", err)
	}
	if len(errs) == 0 {
		fmt.Println("no errored frontier entries.")
		return nil
	}
	fmt.Printf("%d errored frontier entries (most recent first):\n", len(errs))
	fmt.Println()
	for _, e := range errs {
		reason := e.LastError
		if reason == "" {
			reason = "(no reason recorded — entry predates)"
		}
		fmt.Printf("  attempts=%d  %s\n    → %s\n", e.Attempts, e.URL, reason)
	}
	return nil
}
