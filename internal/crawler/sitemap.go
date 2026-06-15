package crawler

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/store"
)

// Sitemap parser, intentionally minimal: handles the standard urlset shape,
// the sitemapindex (one level of recursion to follow), and gzipped sitemaps
// declared by Content-Encoding (stdlib net/http auto-decompresses).
//
// Spec we honor (https://www.sitemaps.org/protocol.html):
//   - <urlset> with <url><loc>...</loc></url> children
//   - <sitemapindex> with <sitemap><loc>...</loc></sitemap> children
//
// What we deliberately ignore:
//   - <lastmod> / <changefreq> / <priority>: useful for re-crawl scheduling,
//     not for first seeding. Add later if a frontier prioritizer needs them.
//   - <image:image>, <video:video>, <news:news> extensions: not relevant to
//     general text retrieval.
//   - .txt sitemaps (one URL per line). Trivial to add; defer until we see one.

// SeedSitemap fetches a sitemap, parses out URLs, and pushes each to the
// persistent frontier at depth 0. Sitemap indices are followed up to two
// levels deep — news sites (theguardian, NYT, BBC etc.) commonly
// publish a master /sitemap.xml that points at a sitemap-index of monthly
// indexes that each point at daily sitemaps. One-level recursion missed
// 99%+ of their URLs. Two levels covers the standard "index → sub-index →
// urlset" depth without risking runaway fanout. Cap stays at 2 to keep
// pathological deeply-nested adversarial sitemaps bounded.
//
// Politeness: each fetched sitemap counts as one HTTP request, so the same
// per-host gate semantics as a normal crawl apply if you seed multiple
// sitemaps to the same host. The host gate isn't wired into SeedSitemap
// directly — it's expected this runs at startup, not in a hot loop.
//
// Returns the number of URLs enqueued.
//
// Sitemap-imported URLs go into the refresh lane: callers run sitemap-import
// to refresh known-good sources (kubernetes.io, docs.python.org, etc.), so
// prioritize over generic discovery. Use SeedSitemapLane to land them in a
// different lane (e.g. store.LaneSubmitted for an operator-driven priority
// site submission).
func (c *Crawler) SeedSitemap(ctx context.Context, sitemapURL string) (int, error) {
	return c.SeedSitemapLane(ctx, sitemapURL, store.LaneRefresh)
}

// SeedSitemapLane fetches a sitemap (or sitemap-index, two levels of
// recursion) and pushes every <loc> URL into the given priority lane.
//
// Bypass include_domains the same way SeedRSS does: the operator explicitly
// requested this sitemap, so trust its URLs regardless of the curated crawler
// allowlist.
func (c *Crawler) SeedSitemapLane(ctx context.Context, sitemapURL string, lane byte) (int, error) {
	// stream URLs into the frontier via callback instead of
	// materializing the full URL list. The prior approach accumulated
	// every URL across the entire recursive sitemap-index walk into a
	// single `all` slice, which for a multi-million-URL wired.com-style
	// site held 100+ GB across in-flight SeedSitemap calls (heap profile
	// showed strings.Builder.Write at 107 GB). Streaming bounds heap to
	// O(current sitemap size) regardless of nesting depth or total URLs.
	n := 0
	// Buffer URLs into 1024-item batches and flush via PushFrontierBatch.
	// Single mu acquire per batch instead of per URL — at scale (MDN,
	// kubernetes.io sitemaps with 100K+ URLs) this is the difference
	// between a sitemap-import that returns in seconds vs an hour.
	const batchSize = 1024
	buf := make([]store.FrontierPushItem, 0, batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		w, perr := c.store.PushFrontierBatch(context.Background(), buf)
		if perr == nil {
			n += w
		}
		buf = buf[:0]
	}
	err := c.fetchSitemapStream(ctx, sitemapURL, 2, func(u string) {
		canon, cerr := canonicalize(u)
		if cerr != nil {
			return
		}
		buf = append(buf, store.FrontierPushItem{URL: canon, Depth: 0, Lane: lane, Priority: 1.0})
		if len(buf) >= batchSize {
			flush()
		}
	})
	flush()
	return n, err
}

// fetchSitemapStream pulls a sitemap (urlset or sitemapindex), pushes
// every discovered URL through emit, and recurses into sub-sitemaps up
// to depthRemaining levels. Bounded heap: per-call working set is one
// sitemap's parsed token stream + transient locBuf string. URLs aren't
// accumulated; they flow straight to the emit callback (typically
// c.Seed which writes to the persistent frontier).
func (c *Crawler) fetchSitemapStream(ctx context.Context, url string, depthRemaining int, emit func(string)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "application/xml, text/xml")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("sitemap http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return err
	}
	if strings.HasSuffix(strings.ToLower(url), ".gz") ||
		(len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b) {
		zr, zerr := gzip.NewReader(bytes.NewReader(body))
		if zerr != nil {
			return fmt.Errorf("sitemap gunzip: %w", zerr)
		}
		decompressed, derr := io.ReadAll(io.LimitReader(zr, 200<<20))
		_ = zr.Close()
		if derr != nil {
			return fmt.Errorf("sitemap gunzip read: %w", derr)
		}
		body = decompressed
	}
	rootName, err := xmlRootName(body)
	if err != nil {
		return err
	}
	switch rootName {
	case "urlset":
		return streamSitemapEmit(body, "url", emit)
	case "sitemapindex":
		if depthRemaining <= 0 {
			return nil
		}
		// For sitemap-index: collect child sitemap URLs streamingly,
		// then recurse into each. Child URLs themselves are bounded
		// (typically <1000 per index), so collecting them in a small
		// slice is fine — we just can't collect the EXPANSION.
		var childURLs []string
		if err := streamSitemapEmit(body, "sitemap", func(u string) {
			childURLs = append(childURLs, u)
		}); err != nil {
			return err
		}
		// Free the body before recursing (each child call rebuilds its own)
		body = nil
		for _, loc := range childURLs {
			if err := c.fetchSitemapStream(ctx, loc, depthRemaining-1, emit); err != nil {
				continue
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	default:
		return fmt.Errorf("unsupported sitemap root: %s", rootName)
	}
}

// streamSitemapEmit decodes the sitemap XML token-by-token and pushes
// each <loc> string through emit. Used by both urlset (emits article
// URLs) and sitemapindex (emits child sitemap URLs).
func streamSitemapEmit(body []byte, itemTag string, emit func(string)) error {
	dec := xml.NewDecoder(bytes.NewReader(body))
	var inItem, inLoc bool
	var locBuf strings.Builder
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case itemTag:
				inItem = true
			case "loc":
				if inItem {
					inLoc = true
					locBuf.Reset()
				}
			}
		case xml.CharData:
			if inLoc {
				locBuf.Write(t)
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "loc":
				if inLoc {
					if s := strings.TrimSpace(locBuf.String()); s != "" {
						emit(s)
					}
					inLoc = false
				}
			case itemTag:
				inItem = false
			}
		}
	}
	return nil
}

// xmlRootName returns the local name of the first start element.
func xmlRootName(body []byte) (string, error) {
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	for {
		tok, err := dec.Token()
		if err != nil {
			return "", fmt.Errorf("xml peek: %w", err)
		}
		if se, ok := tok.(xml.StartElement); ok {
			return se.Name.Local, nil
		}
	}
}
