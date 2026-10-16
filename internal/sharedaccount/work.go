package sharedaccount

import (
	"context"
	"maps"
	"slices"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JoinState is what the work broker needs of an account: its contributor id and the contributor's ban and terms version.
type JoinState struct {
	CID          string // empty when the account has not joined
	Banned       bool
	TermsVersion int64
}

type WorkSource interface {
	JoinState(ctx context.Context, uid string) (JoinState, error)
	AddCounters(ctx context.Context, cid string, add map[string]int64) error
	RaiseAlert(ctx context.Context, a Alert) error
}

var counterFields = map[string]bool{"fetch_claimed": true, "fetch_submitted": true, "fetch_verified": true, "fetch_mismatch": true, "fetch_rejected": true, "fetch_credits": true}

var _ WorkSource = (*Client)(nil)

func NewWorkSource(db *firestore.Client) WorkSource { return &Client{db: db} }

// JoinState reads accounts/{uid}.contributor_cid and the contributor it names; an inconsistent pair is unavailable, never a join.
func (c *Client) JoinState(ctx context.Context, uid string) (JoinState, error) {
	account, err := c.db.Collection("accounts").Doc(uid).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return JoinState{}, nil
	}
	if err != nil {
		return JoinState{}, ErrUnavailable
	}
	raw, present := account.Data()["contributor_cid"]
	if !present || raw == nil {
		return JoinState{}, nil
	}
	cid, ok := raw.(string)
	if !ok || !cidPattern.MatchString(cid) {
		return JoinState{}, ErrUnavailable
	}
	doc, err := c.db.Collection("contributors").Doc(cid).Get(ctx)
	if err != nil {
		return JoinState{}, ErrUnavailable
	}
	data := doc.Data()
	if owner, _ := data["uid"].(string); owner != uid {
		return JoinState{}, ErrUnavailable
	}
	out := JoinState{CID: cid}
	if banned, present := data["banned"]; present && banned != nil {
		if out.Banned, ok = banned.(bool); !ok {
			return JoinState{}, ErrUnavailable
		}
	}
	if terms, present := data["terms_version"]; present && terms != nil {
		if out.TermsVersion, ok = terms.(int64); !ok {
			return JoinState{}, ErrUnavailable
		}
	}
	return out, nil
}

// AddCounters increments counters.fetch_* with a field-masked update; it writes no other field and never creates the contributor.
func (c *Client) AddCounters(ctx context.Context, cid string, add map[string]int64) error {
	if !cidPattern.MatchString(cid) {
		return ErrInvalid
	}
	updates := make([]firestore.Update, 0, len(add))
	for _, name := range slices.Sorted(maps.Keys(add)) {
		if !counterFields[name] {
			return ErrInvalid
		}
		updates = append(updates, firestore.Update{Path: "counters." + name, Value: firestore.Increment(add[name])})
	}
	if len(updates) == 0 {
		return nil
	}
	_, err := c.db.Collection("contributors").Doc(cid).Update(ctx, updates)
	switch {
	case err == nil:
		return nil
	case status.Code(err) == codes.NotFound:
		return ErrNotFound
	}
	return ErrUnavailable
}
