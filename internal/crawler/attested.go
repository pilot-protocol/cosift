package crawler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pilot-protocol/cosift/internal/adultfilter"
	"github.com/pilot-protocol/cosift/internal/store"
)

// ErrCopySkipped means the crawl path declined to fetch the URL at all.
var ErrCopySkipped = errors.New("page not fetched")

// AttestedDoc is a page that a quorum of workers or the server's own fetch vouches for.
type AttestedDoc struct {
	// URL is the URL the document is stored under; LeaseURL is the frontier row it completes.
	URL, LeaseURL     string
	Title, Text, Lang string
	// Artifact holds worker vectors for Text; they are used only when they verify.
	Artifact   *LocalArtifact
	VerifiedBy string // "server" or "quorum"
}

// IndexAttested stores and indexes an attested page, and completes its frontier row. indexed is
// false when the same URL already holds the same content; contentNovel is true only when no live
// document has the content and the content index is complete. A quorum's page replaces only an
// earlier quorum page of its own lease URL, never a crawled or server-vouched document.
func (c *Crawler) IndexAttested(ctx context.Context, d AttestedDoc) (indexed, contentNovel bool, err error) {
	if d.VerifiedBy != "server" && d.VerifiedBy != "quorum" {
		return false, false, fmt.Errorf("attested page needs a verifier")
	}
	lease := d.LeaseURL
	if lease == "" {
		lease = d.URL
	}
	defer c.takeCompletedInline(lease)
	prior, err := c.store.GetDocByURL(ctx, d.URL)
	if err != nil {
		if !docNotFound(err) {
			return false, false, err
		}
		prior = nil
	}
	if d.VerifiedBy == "quorum" && prior != nil && !(prior.Source == "attested" && prior.VerifiedBy == "quorum" && d.URL == lease) {
		return false, false, nil
	}
	emb := c.embedder
	if d.Artifact != nil && c.embedder != nil {
		a := *d.Artifact
		a.Title, a.Text = d.Title, d.Text
		if verified, verr := VerifyWorkVectors(ctx, &a, c.embedder); verr == nil {
			emb = verified
		}
	}
	page := &fetchedPage{finalURL: d.URL, parsed: &ParsedDoc{Title: d.Title, Text: d.Text, Lang: d.Lang}, prior: prior}
	return c.indexParsed(ctx, store.FrontierItem{URL: lease}, page, indexOpts{attested: true, verifiedBy: d.VerifiedBy, embedder: emb})
}

// CopyPage is the server's own fetch of a URL, parsed and not stored.
type CopyPage struct {
	FinalURL, Title, Text, Lang string
}

// FetchCopy fetches raw through the crawl transport, with its proxies, robots rules and host spacing,
// as an unconditional GET, and returns the parsed page without indexing it.
func (c *Crawler) FetchCopy(ctx context.Context, raw string) (*CopyPage, error) {
	if !c.allowedDomain(raw) {
		return nil, ErrCopySkipped
	}
	c.copyGateOnce.Do(func() {
		c.copyGate = newHostGate(time.Duration(c.cfg.PerHostDelayMs)*time.Millisecond, nil)
	})
	page, err := c.fetchParsed(ctx, store.FrontierItem{URL: raw}, c.copyGate, false)
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, ErrCopySkipped
	}
	return &CopyPage{FinalURL: page.finalURL, Title: page.parsed.Title, Text: page.parsed.Text, Lang: page.parsed.Lang}, nil
}

// AllowedURL reports whether the crawler's domain policy and the adult filter admit the URL.
func (c *Crawler) AllowedURL(raw string) bool {
	return c.allowedDomain(raw) && !adultfilter.IsAdult("", "", raw)
}
