package sharedaccount

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotFound   = errors.New("shared Cosift document not found")
	ErrNotPending = errors.New("award is no longer pending")
)

type AwardRef struct {
	ContributionID, ArticleID, Day, RequestID string
	Version                                   int64
}

// Award is a credit_awards document. Valid is false when a member has the wrong type; such an award is never applied.
type Award struct {
	ID        string
	UID, CID  string
	Amount    int64
	Reason    string
	Ref       AwardRef
	Status    string
	CreatedAt time.Time
	// Applied is set when applied_at, applied_delta or ledger_id is present.
	Applied bool
	Valid   bool
}

type AwardPosition struct {
	CreatedAt time.Time
	ID        string
}

// AwardResult is how an award leaves the pending state: "applied" with its ledger id, or "skipped" with a reason.
type AwardResult struct {
	Status       string
	AppliedDelta int64
	LedgerID     string
	SkipReason   string
}

type Contributor struct {
	UID    string
	Banned bool
}

type AwardSource interface {
	PendingAwards(ctx context.Context, after AwardPosition, limit int) ([]Award, error)
	MarkApplied(ctx context.Context, id string, result AwardResult) error
	Contributor(ctx context.Context, cid string) (Contributor, error)
	Account(ctx context.Context, uid string) (Identity, error)
}

var _ AwardSource = (*Client)(nil)

// ParseAward reads a document's data without trusting its types.
func ParseAward(id string, data map[string]any) Award {
	a := Award{ID: id, Valid: true}
	str := func(key string) string {
		v, ok := data[key].(string)
		if !ok {
			a.Valid = false
		}
		return v
	}
	a.UID, a.CID, a.Reason, a.Status = str("uid"), str("cid"), str("reason"), str("status")
	if v, ok := data["amount"].(int64); ok {
		a.Amount = v
	} else {
		a.Valid = false
	}
	if v, ok := data["created_at"].(time.Time); ok {
		a.CreatedAt = v.UTC()
	} else {
		a.Valid = false
	}
	if ref, ok := data["ref"].(map[string]any); ok {
		for key, dst := range map[string]*string{"contribution_id": &a.Ref.ContributionID, "article_id": &a.Ref.ArticleID, "day": &a.Ref.Day, "request_id": &a.Ref.RequestID} {
			if raw, present := ref[key]; present {
				if s, isString := raw.(string); isString {
					*dst = s
				} else {
					a.Valid = false
				}
			}
		}
		if raw, present := ref["version"]; present {
			if n, isInt := raw.(int64); isInt {
				a.Ref.Version = n
			} else {
				a.Valid = false
			}
		}
	} else {
		a.Valid = false
	}
	for _, key := range []string{"applied_at", "applied_delta", "ledger_id"} {
		if v, present := data[key]; present && v != nil {
			a.Applied = true
		}
	}
	return a
}

func (c *Client) PendingAwards(ctx context.Context, after AwardPosition, limit int) ([]Award, error) {
	q := c.db.Collection("credit_awards").Where("status", "==", "pending").OrderBy("created_at", firestore.Asc).OrderBy(firestore.DocumentID, firestore.Asc).Limit(limit)
	if !after.CreatedAt.IsZero() {
		q = q.StartAfter(after.CreatedAt, after.ID)
	}
	it := q.Documents(ctx)
	defer it.Stop()
	var out []Award
	for {
		doc, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, ErrUnavailable
		}
		out = append(out, ParseAward(doc.Ref.ID, doc.Data()))
	}
}

// MarkApplied re-reads the award in a transaction and writes only while it is still pending.
func (c *Client) MarkApplied(ctx context.Context, id string, r AwardResult) error {
	ref := c.db.Collection("credit_awards").Doc(id)
	err := c.db.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if current, _ := snap.Data()["status"].(string); current != "pending" {
			return ErrNotPending
		}
		updates := []firestore.Update{{Path: "status", Value: r.Status}, {Path: "applied_at", Value: time.Now().UTC()}, {Path: "applied_delta", Value: r.AppliedDelta}}
		if r.LedgerID != "" {
			updates = append(updates, firestore.Update{Path: "ledger_id", Value: r.LedgerID})
		}
		if r.SkipReason != "" {
			updates = append(updates, firestore.Update{Path: "skip_reason", Value: r.SkipReason})
		}
		return tx.Update(ref, updates)
	})
	switch {
	case err == nil, errors.Is(err, ErrNotFound), errors.Is(err, ErrNotPending):
		return err
	}
	return ErrUnavailable
}

func (c *Client) Contributor(ctx context.Context, cid string) (Contributor, error) {
	doc, err := c.db.Collection("contributors").Doc(cid).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return Contributor{}, ErrNotFound
	}
	if err != nil {
		return Contributor{}, ErrUnavailable
	}
	data := doc.Data()
	uid, ok := data["uid"].(string)
	if !ok {
		return Contributor{}, ErrUnavailable
	}
	out := Contributor{UID: uid}
	if banned, present := data["banned"]; present && banned != nil {
		if out.Banned, ok = banned.(bool); !ok {
			return Contributor{}, ErrUnavailable
		}
	}
	return out, nil
}

func (c *Client) Account(ctx context.Context, uid string) (Identity, error) {
	return googleStore{db: c.db}.Account(ctx, uid)
}
