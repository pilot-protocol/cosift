package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// FetchContribution uses the same index and embedding budget as the bulk
// crawler, but an independent direct-only transport. It never discovers links
// or sitemaps, so unreviewed descendants cannot enter the bulk frontier.
// The community database is the durable queue; callers retry transient errors.
// The result reports whether this URL was absent before the accepted fetch.
type ContributionReceipt struct {
	Indexed     bool   `json:"indexed"`
	Novel       bool   `json:"novel"`
	ContentHash string `json:"content_hash"`
}

// ApprovedContentHash binds a moderation decision to the exact parsed document.
func ApprovedContentHash(title, text string) string {
	sum := sha256.Sum256([]byte(title + "\x00" + text))
	return hex.EncodeToString(sum[:])
}

func ValidApprovedContentHash(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}

type approvedContentKey struct{}

// ContributionURL returns the document key used when indexing a contribution.
func ContributionURL(raw string) (string, error) { return canonicalize(raw) }

func (c *Crawler) FetchContribution(ctx context.Context, raw string, artifact *LocalArtifact, approvedHash string) (ContributionReceipt, error) {
	if !ValidApprovedContentHash(approvedHash) {
		return ContributionReceipt{}, fmt.Errorf("%w: approved content hash is required", ErrContributionRejected)
	}
	ctx = context.WithValue(ctx, approvedContentKey{}, approvedHash)
	canon, err := canonicalize(raw)
	if err != nil {
		return ContributionReceipt{}, err
	}
	if !c.allowedDomain(canon) {
		return ContributionReceipt{}, fmt.Errorf("%w: contribution domain is excluded by crawler policy", ErrContributionRejected)
	}
	c.contributionOnce.Do(func() {
		cfg := c.cfg
		cfg.PublicOnly = true
		cfg.FilterAdult = true
		cfg.RespectRobots = true
		cfg.Proxies = nil
		cfg.RemoteFetcherURL = ""
		cfg.RemoteFetcherURLs = nil
		cfg.RemoteFetcherToken = ""
		cfg.AutoSitemap = false
		cfg.DisableLinkFollowing = true
		// Parent policy is checked above, including runtime domain promotions.
		cfg.IncludeDomains = nil
		cfg.MaxBodyBytes = 2 << 20
		cfg.PerHostMaxBodyBytes = nil
		c.contributionCrawler = NewWithBackend(cfg, c.store, c.idx).
			WithEmbedder(c.embedder).WithPassageWriter(c.passageWriter)
		c.contributionSlots = make(chan struct{}, 2)
	})
	select {
	case c.contributionSlots <- struct{}{}:
		defer func() { <-c.contributionSlots }()
	case <-ctx.Done():
		return ContributionReceipt{}, ctx.Err()
	}
	prior, _ := c.store.GetDocByURL(ctx, canon)
	safe := c.contributionCrawler
	if artifact != nil {
		if artifact.URL != raw {
			return ContributionReceipt{}, fmt.Errorf("%w: artifact URL mismatch", ErrContributionRejected)
		}
		allowed, _, err := safe.robots.Allowed(ctx, canon)
		if err != nil || !allowed {
			return ContributionReceipt{}, fmt.Errorf("webpage does not permit validation")
		}
		page, err := FetchOne(ctx, safe.http, safe.cfg.UserAgent, canon, 2<<20)
		if err != nil {
			return ContributionReceipt{}, err
		}
		if ApprovedContentHash(page.Title, page.Text) != approvedHash {
			return ContributionReceipt{}, fmt.Errorf("%w: webpage changed after moderation", ErrContributionRejected)
		}
		verified, err := VerifyArtifact(ctx, artifact, page.Title, page.Text, c.embedder)
		if err != nil {
			return ContributionReceipt{}, err
		}
		safe = NewWithBackend(safe.cfg, c.store, c.idx).WithEmbedder(verified).WithPassageWriter(c.passageWriter)
	}
	defer safe.takeCompletedInline(canon)
	if err := safe.FetchAndIndexNow(ctx, canon); err != nil {
		return ContributionReceipt{}, err
	}
	doc, err := c.store.GetDocByURL(ctx, canon)
	if err != nil || doc == nil {
		return ContributionReceipt{}, fmt.Errorf("contribution did not produce an indexable document")
	}
	if ApprovedContentHash(doc.Title, doc.Text) != approvedHash {
		return ContributionReceipt{}, fmt.Errorf("%w: stored webpage differs from moderated content", ErrContributionRejected)
	}
	sum := sha256.Sum256([]byte(doc.Text))
	return ContributionReceipt{Indexed: true, Novel: prior == nil, ContentHash: hex.EncodeToString(sum[:])}, nil
}
