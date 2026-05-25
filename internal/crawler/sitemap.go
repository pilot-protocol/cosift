package crawler

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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

type sitemapURLSet struct {
	XMLName xml.Name      `xml:"urlset"`
	URLs    []sitemapURLT `xml:"url"`
}

type sitemapURLT struct {
	Loc string `xml:"loc"`
}

type sitemapIndex struct {
	XMLName  xml.Name           `xml:"sitemapindex"`
	Sitemaps []sitemapURLT      `xml:"sitemap"`
}

// SeedSitemap fetches a sitemap, parses out URLs, and pushes each to the
// persistent frontier at depth 0. Sitemap indices are followed one level deep
// (industry standard depth: most sites with index files use exactly one level
// of nesting).
//
// Politeness: each fetched sitemap counts as one HTTP request, so the same
// per-host gate semantics as a normal crawl apply if you seed multiple
// sitemaps to the same host. The host gate isn't wired into SeedSitemap
// directly — it's expected this runs at startup, not in a hot loop.
//
// Returns the number of URLs enqueued.
func (c *Crawler) SeedSitemap(ctx context.Context, sitemapURL string) (int, error) {
	urls, err := c.fetchSitemap(ctx, sitemapURL, 1)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range urls {
		if err := c.Seed(u); err != nil {
			// Skip individual seed failures (domain rules, scheme); don't fail the batch.
			continue
		}
		n++
	}
	return n, nil
}

// fetchSitemap returns flat URL list. depthRemaining controls index recursion.
func (c *Crawler) fetchSitemap(ctx context.Context, url string, depthRemaining int) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "application/xml, text/xml")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("sitemap http %d", resp.StatusCode)
	}
	// Cap at 50 MB. Real sitemaps are usually <10 MB (the spec limit before split).
	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return nil, err
	}

	// Peek at root element. xml.Decoder gives us this without parsing everything.
	rootName, err := xmlRootName(body)
	if err != nil {
		return nil, err
	}

	switch rootName {
	case "urlset":
		var us sitemapURLSet
		if err := xml.Unmarshal(body, &us); err != nil {
			return nil, fmt.Errorf("parse urlset: %w", err)
		}
		out := make([]string, 0, len(us.URLs))
		for _, u := range us.URLs {
			loc := strings.TrimSpace(u.Loc)
			if loc != "" {
				out = append(out, loc)
			}
		}
		return out, nil

	case "sitemapindex":
		if depthRemaining <= 0 {
			return nil, nil // refuse to recurse further
		}
		var idx sitemapIndex
		if err := xml.Unmarshal(body, &idx); err != nil {
			return nil, fmt.Errorf("parse sitemapindex: %w", err)
		}
		var all []string
		for _, sm := range idx.Sitemaps {
			loc := strings.TrimSpace(sm.Loc)
			if loc == "" {
				continue
			}
			child, err := c.fetchSitemap(ctx, loc, depthRemaining-1)
			if err != nil {
				continue // skip bad child sitemaps
			}
			all = append(all, child...)
			// Be polite between sibling sitemap fetches.
			time.Sleep(50 * time.Millisecond)
		}
		return all, nil

	default:
		return nil, fmt.Errorf("unsupported sitemap root: %s", rootName)
	}
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
