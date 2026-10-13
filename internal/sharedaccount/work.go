package sharedaccount

import (
	"context"

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
}

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
