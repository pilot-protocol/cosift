package sharedaccount

import (
	"context"
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

// Member is a joined contributor as My contributions shows them; both documents are read through field masks.
type Member struct {
	CID, Handle string
	// Fetch is counters.fetch_* with the prefix dropped.
	Fetch map[string]int64
}

type RefusalCode struct{ Code, Field string }

// ContributionRecord is the part of a contributions document a contributor may see; text and judge verdicts are never requested.
type ContributionRecord struct {
	ID, Kind, Status string
	StatusAt         time.Time
	Title            string
	Refusals         []RefusalCode
	LostReason       string
	URLs             []string
	RecheckAt        time.Time
	SourcesPushed    bool
	ArticleID        string
	Version          int64
	Credits          int64
}

type ContributionSource interface {
	// Member returns ErrNotFound when the account has not joined.
	Member(ctx context.Context, uid string) (Member, error)
	Contributions(ctx context.Context, cid string, limit int) ([]ContributionRecord, error)
}

var _ ContributionSource = (*Client)(nil)

var fetchCounters = []string{"claimed", "submitted", "verified", "mismatch", "rejected", "credits"}

// maskedDoc reads one document through a field mask; ErrNotFound when it does not exist.
func maskedDoc(ctx context.Context, q firestore.Query) (map[string]any, error) {
	it := q.Limit(1).Documents(ctx)
	defer it.Stop()
	doc, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	return doc.Data(), nil
}

// accountQuery selects the contributor link of one account and nothing else.
func accountQuery(c *Client, uid string) firestore.Query {
	return c.db.Collection("accounts").Where(firestore.DocumentID, "==", c.db.Collection("accounts").Doc(uid)).Select("contributor_cid")
}

// contributorQuery selects the handle, owner and fetch counters of one contributor.
func contributorQuery(c *Client, cid string) firestore.Query {
	paths := []firestore.FieldPath{{"handle"}, {"uid"}}
	for _, name := range fetchCounters {
		paths = append(paths, firestore.FieldPath{"counters", "fetch_" + name})
	}
	return c.db.Collection("contributors").Where(firestore.DocumentID, "==", c.db.Collection("contributors").Doc(cid)).SelectPaths(paths...)
}

func (c *Client) Member(ctx context.Context, uid string) (Member, error) {
	if !UIDPattern.MatchString(uid) {
		return Member{}, ErrInvalid
	}
	acct, err := maskedDoc(ctx, accountQuery(c, uid))
	if err != nil {
		return Member{}, err
	}
	id, ok := acct["contributor_cid"].(string)
	if !ok || !cidPattern.MatchString(id) {
		return Member{}, ErrNotFound
	}
	data, err := maskedDoc(ctx, contributorQuery(c, id))
	if err != nil {
		return Member{}, err
	}
	return parseMember(id, uid, data)
}

func parseMember(cid, uid string, data map[string]any) (Member, error) {
	if owner, _ := data["uid"].(string); owner != uid {
		return Member{}, ErrNotFound
	}
	m := Member{CID: cid, Fetch: map[string]int64{}}
	m.Handle, _ = data["handle"].(string)
	counters, _ := data["counters"].(map[string]any)
	for _, name := range fetchCounters {
		n, _ := counters["fetch_"+name].(int64)
		m.Fetch[name] = n
	}
	return m, nil
}

// contributionQuery asks for the contributor's newest documents with a field mask.
func contributionQuery(c *Client, cid string, limit int) firestore.Query {
	paths := []firestore.FieldPath{{"kind"}, {"status"}, {"status_at"}, {"generic_title"}, {"submission", "title"}, {"refusal", "codes"}, {"lost_reason"},
		{"needs_sources", "urls"}, {"needs_sources", "recheck_at"}, {"sources_pushed"}, {"result", "article_id"}, {"result", "version"}, {"result", "credits"}}
	return c.db.Collection("contributions").Where("cid", "==", cid).OrderBy("created_at", firestore.Desc).Limit(limit).SelectPaths(paths...)
}

func (c *Client) Contributions(ctx context.Context, cid string, limit int) ([]ContributionRecord, error) {
	if !cidPattern.MatchString(cid) || limit < 1 {
		return nil, ErrInvalid
	}
	it := contributionQuery(c, cid, limit).Documents(ctx)
	defer it.Stop()
	var out []ContributionRecord
	for {
		doc, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, ErrUnavailable
		}
		out = append(out, parseContribution(doc.Ref.ID, doc.Data()))
	}
}

// parseContribution reads each member for its expected type and drops one of another type.
func parseContribution(id string, data map[string]any) ContributionRecord {
	r := ContributionRecord{ID: id}
	r.Kind, _ = data["kind"].(string)
	r.Status, _ = data["status"].(string)
	r.StatusAt, _ = data["status_at"].(time.Time)
	r.StatusAt = r.StatusAt.UTC()
	r.LostReason, _ = data["lost_reason"].(string)
	r.SourcesPushed, _ = data["sources_pushed"].(bool)
	if r.Title, _ = data["generic_title"].(string); r.Title == "" {
		sub, _ := data["submission"].(map[string]any)
		r.Title, _ = sub["title"].(string)
	}
	refusal, _ := data["refusal"].(map[string]any)
	list, _ := refusal["codes"].([]any)
	for _, v := range list {
		entry, _ := v.(map[string]any)
		code, _ := entry["code"].(string)
		field, _ := entry["field"].(string)
		if code != "" {
			r.Refusals = append(r.Refusals, RefusalCode{Code: code, Field: field})
		}
	}
	needs, _ := data["needs_sources"].(map[string]any)
	urls, _ := needs["urls"].([]any)
	for _, v := range urls {
		if s, ok := v.(string); ok && !strings.ContainsAny(s, "\r\n") {
			r.URLs = append(r.URLs, s)
		}
	}
	if at, ok := needs["recheck_at"].(time.Time); ok {
		r.RecheckAt = at.UTC()
	}
	res, _ := data["result"].(map[string]any)
	r.ArticleID, _ = res["article_id"].(string)
	r.Version, _ = res["version"].(int64)
	r.Credits, _ = res["credits"].(int64)
	return r
}
