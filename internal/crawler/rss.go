package crawler

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// RSS / Atom feed seeding. Parallel to sitemap.go in spirit: fetch a feed,
// pull out the article links, push each to the frontier. The point isn't
// "subscribe to the firehose" — it's giving operators a no-deps way to
// inject genuinely-novel URLs that the existing link graph hasn't surfaced.
//
// Crawler corpora plateau when every URL the cursor claims has already been
// indexed. The auto-sitemap path compounds that — sitemaps of seen hosts
// surface URLs we already have. RSS/Atom feeds dated-by-design produce a
// rolling list of brand-new article URLs from sources that often DO allow
// machine clients (RSS is explicitly machine-readable; far fewer sites bot-
// block /feed than block /article).
//
// We intentionally do NOT subscribe / poll on a timer. That'd add a goroutine
// per feed and create another moving part. Operators who want freshness can
// hit /admin/rss-import on cron (or every cosift restart re-fetches via the
// seeds-file).
//
// What we deliberately ignore:
//   - <pubDate> / <updated>: useful for "only new since last fetch" semantics,
//     but the frontier already dedups (INSERT OR IGNORE on URL), so re-seeding
//     the same feed on cron costs ~one no-op iter and stays correct.
//   - <enclosure>: media-only items aren't text-retrieval candidates.
//   - <content:encoded>, <description>: extracting bodies straight from the
//     feed would skip the proper HTML fetch+chunk flow. Just enqueue the link.

// rss2Feed is the RSS 2.0 channel/item shape. The 0.91/0.92 dialects nest the
// same way; 1.0 (RDF) is rare enough we treat it as Atom-or-fail.
type rss2Feed struct {
	XMLName xml.Name    `xml:"rss"`
	Channel rss2Channel `xml:"channel"`
}

type rss2Channel struct {
	Items []rss2Item `xml:"item"`
}

type rss2Item struct {
	Link string `xml:"link"`
}

// atomFeed is the Atom 1.0 shape. <link> is an empty element with href attr.
// Some feeds emit multiple <link> per entry (alternate, self, enclosure);
// we take the first href that looks like an http(s) URL — typically the
// alternate (article) link.
type atomFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Links []atomLink `xml:"link"`
}

type atomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

// SeedRSS fetches a feed URL and enqueues every article link it advertises.
// Returns the count enqueued (after per-link domain/cap filtering). Idempotent
// against the frontier — re-seeding the same feed pushes only the newly-added
// items.
func (c *Crawler) SeedRSS(ctx context.Context, feedURL string) (int, error) {
	urls, err := c.fetchRSS(ctx, feedURL)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range urls {
		if err := c.Seed(u); err != nil {
			continue
		}
		n++
	}
	return n, nil
}

// fetchRSS pulls the feed body and parses either RSS2 or Atom. Returns the
// flat list of article URLs. Tolerates each format failing — only fails if
// BOTH parsers can't make sense of the body.
func (c *Crawler) fetchRSS(ctx context.Context, feedURL string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "application/atom+xml, application/rss+xml, application/xml, text/xml")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("rss http %d", resp.StatusCode)
	}
	// Cap at 50 MB. Real feeds are <1 MB; the cap exists for hostile servers.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return nil, err
	}

	// Try RSS 2.0 first (the dominant flavor). Fall back to Atom on mismatch.
	var rss rss2Feed
	if err := xml.Unmarshal(body, &rss); err == nil && len(rss.Channel.Items) > 0 {
		out := make([]string, 0, len(rss.Channel.Items))
		for _, it := range rss.Channel.Items {
			if link := strings.TrimSpace(it.Link); link != "" {
				out = append(out, link)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	var atom atomFeed
	if err := xml.Unmarshal(body, &atom); err == nil && len(atom.Entries) > 0 {
		out := make([]string, 0, len(atom.Entries))
		for _, e := range atom.Entries {
			// Prefer rel="alternate" (article URL); fall back to first http(s) link.
			pick := ""
			for _, l := range e.Links {
				href := strings.TrimSpace(l.Href)
				if !strings.HasPrefix(href, "http") {
					continue
				}
				if l.Rel == "alternate" || l.Rel == "" {
					pick = href
					break
				}
				if pick == "" {
					pick = href
				}
			}
			if pick != "" {
				out = append(out, pick)
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, fmt.Errorf("rss: not parseable as RSS2 or Atom (or no items)")
}
