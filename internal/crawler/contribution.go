package crawler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"net/url"

	"github.com/pilot-protocol/cosift/internal/store"
)

// ContributionReceipt reports one accepted contribution. Novel means new
// content: no document at either URL or with the same hash, on a complete index.
type ContributionReceipt struct {
	Indexed              bool   `json:"indexed"`
	Novel                bool   `json:"novel"`
	ContentHash          string `json:"content_hash"`
	FinalURL             string `json:"final_url,omitempty"`
	ContentIndexComplete bool   `json:"content_index_complete"`
}

// NoveltyDecision is the verdict taken just before a contribution's first write.
type NoveltyDecision struct {
	FinalURL string `json:"final_url"`
	Novel    bool   `json:"novel"`
}

type noveltyRecorderKey struct{}

// WithNoveltyRecorder has contribution fetches report to fn before writing; an
// error from fn aborts the write.
func WithNoveltyRecorder(ctx context.Context, fn func(NoveltyDecision) error) context.Context {
	return context.WithValue(ctx, noveltyRecorderKey{}, fn)
}

// ReportNovelty delivers a verdict to the recorder installed on ctx, if any.
func ReportNovelty(ctx context.Context, d NoveltyDecision) error {
	if fn, ok := ctx.Value(noveltyRecorderKey{}).(func(NoveltyDecision) error); ok {
		return fn(d)
	}
	return nil
}

type (
	contributionStateKey struct{}
	redirectPolicyKey    struct{}
)

// withoutRedirectPolicy exempts a request from the hop policy: a refused robots.txt
// redirect would read as "no robots.txt" and allow everything.
func withoutRedirectPolicy(ctx context.Context) context.Context {
	return context.WithValue(ctx, redirectPolicyKey{}, nil)
}

// docNotFound recognises a missing document from either store backend.
func docNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, sql.ErrNoRows)
}

// contributionState carries what a contribution fetch learned out of the
// shared crawl pipeline.
type contributionState struct {
	docURL  string // URL of the document holding the fetched content
	decided bool
	novel   bool
}

// decideNovelty rules on a contribution about to be written; existing is the
// document already stored under its submitted or final URL.
func (c *Crawler) decideNovelty(ctx context.Context, st *contributionState, existing *store.Document, finalURL string, sha []byte) error {
	novel := false
	if existing == nil {
		var err error
		if novel, err = c.contentNovel(ctx, finalURL, sha); err != nil {
			return err
		}
	}
	st.decided, st.novel = true, novel
	return ReportNovelty(ctx, NoveltyDecision{FinalURL: finalURL, Novel: novel})
}

// contentNovel reports whether the content is new: the index is complete, no
// live document carries the hash, and no document whose hash cannot be compared
// sits at a variant of the URL.
func (c *Crawler) contentNovel(ctx context.Context, finalURL string, sha []byte) (bool, error) {
	ci, ok := c.store.(ContentIndexer)
	if !ok {
		return false, nil
	}
	if complete, err := ci.ContentIndexComplete(ctx); err != nil || !complete {
		return false, err
	}
	if live, err := ci.HasLiveContent(ctx, sha, 0); err != nil || live {
		return false, err
	}
	held, err := ci.HasUncomparableVariant(ctx, finalURL, 0)
	return !held, err
}

// contributionHopAllowed applies the crawler's domain policy and the intake
// URL rules to every redirect hop, the final URL being the last hop. Ports and
// resolved addresses are enforced by the public dialer on each connection.
func (c *Crawler) contributionHopAllowed(raw string) error {
	reject := func(why string) error { return fmt.Errorf("%w: redirect target %s", ErrContributionRejected, why) }
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return reject("is not a public web address")
	}
	if _, err := netip.ParseAddr(u.Hostname()); err == nil {
		return reject("is an IP address")
	}
	if !c.allowedDomain(raw) {
		return reject("is excluded by crawler policy")
	}
	return nil
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

// FetchContribution uses the same index and embedding budget as the bulk
// crawler, but an independent direct-only transport. It never discovers links
// or sitemaps, so unreviewed descendants cannot enter the bulk frontier.
// The community database is the durable queue; callers retry transient errors.
func (c *Crawler) FetchContribution(ctx context.Context, raw string, artifact *LocalArtifact, approvedHash string) (ContributionReceipt, error) {
	if !ValidApprovedContentHash(approvedHash) {
		return ContributionReceipt{}, fmt.Errorf("%w: approved content hash is required", ErrContributionRejected)
	}
	st := &contributionState{}
	ctx = context.WithValue(ctx, approvedContentKey{}, approvedHash)
	ctx = context.WithValue(ctx, contributionStateKey{}, st)
	ctx = context.WithValue(ctx, redirectPolicyKey{}, c.contributionHopAllowed)
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
		if errors.Is(err, errNonPublicTarget) {
			err = fmt.Errorf("%w: %v", ErrContributionRejected, err)
		}
		return ContributionReceipt{}, err
	}
	docURL := canon
	if st.docURL != "" {
		docURL = st.docURL
	}
	doc, err := c.store.GetDocByURL(ctx, docURL)
	if err != nil || doc == nil {
		return ContributionReceipt{}, fmt.Errorf("contribution did not produce an indexable document")
	}
	if ApprovedContentHash(doc.Title, doc.Text) != approvedHash {
		return ContributionReceipt{}, fmt.Errorf("%w: stored webpage differs from moderated content", ErrContributionRejected)
	}
	sum := sha256.Sum256([]byte(doc.Text))
	receipt := ContributionReceipt{Indexed: true, Novel: st.decided && st.novel, ContentHash: hex.EncodeToString(sum[:]), FinalURL: docURL}
	if ci, ok := c.store.(ContentIndexer); ok {
		if receipt.ContentIndexComplete, err = ci.ContentIndexComplete(ctx); err != nil {
			return ContributionReceipt{}, err
		}
	}
	return receipt, nil
}
