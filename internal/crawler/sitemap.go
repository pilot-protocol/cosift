package crawler

import (
	"bytes"
	"compress/gzip"
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
	XMLName  xml.Name      `xml:"sitemapindex"`
	Sitemaps []sitemapURLT `xml:"sitemap"`
}

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
func (c *Crawler) SeedSitemap(ctx context.Context, sitemapURL string) (int, error) {
	// stream URLs into the frontier via callback instead of
	// materializing the full URL list. The prior approach accumulated
	// every URL across the entire recursive sitemap-index walk into a
	// single `all` slice, which for a multi-million-URL wired.com-style
	// site held 100+ GB across in-flight SeedSitemap calls (heap profile
	// showed strings.Builder.Write at 107 GB). Streaming bounds heap to
	// O(current sitemap size) regardless of nesting depth or total URLs.
	n := 0
	err := c.fetchSitemapStream(ctx, sitemapURL, 2, func(u string) {
		if seedErr := c.Seed(u); seedErr == nil {
			n++
		}
	})
	return n, err
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

	// Go's net/http auto-decompresses responses
	// with Content-Encoding: gzip, but sitemap.xml.gz URLs typically arrive
	// with Content-Encoding: identity (or absent) and the gzip framing is
	// part of the BODY. Detect via either the URL extension or magic bytes.
	if strings.HasSuffix(strings.ToLower(url), ".gz") ||
		(len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b) {
		zr, zerr := gzip.NewReader(bytes.NewReader(body))
		if zerr != nil {
			return nil, fmt.Errorf("sitemap gunzip: %w", zerr)
		}
		decompressed, derr := io.ReadAll(io.LimitReader(zr, 200<<20))
		_ = zr.Close()
		if derr != nil {
			return nil, fmt.Errorf("sitemap gunzip read: %w", derr)
		}
		body = decompressed
	}

	// wrapper kept for any non-streaming callers; new internal
	// fetchSitemapStream is the leak-free path. Callers prefer streaming.
	_ = body
	_ = depthRemaining
	return nil, fmt.Errorf("fetchSitemap deprecated, use fetchSitemapStream")
}

// fetchSitemapStream pulls a sitemap (urlset or sitemapindex), pushes
// every discovered URL through emit, and recurses into sub-sitemaps up
// to depthRemaining levels. Bounded heap: per-call working set is one
// sitemap's parsed token stream + transient locBuf string. URLs aren't
// accumulated; they flow straight to the emit callback (typically
// c.Seed which writes to the persistent frontier).
func (c *Crawler) fetchSitemapStream(ctx context.Context, url string, depthRemaining int, emit func(string)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
		if err == io.EOF {
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
