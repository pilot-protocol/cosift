package sharedaccount

import (
	"context"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaiseAlert writes one occurrence: it creates the row, or counts it and reopens a resolved row; an acked row stays acked.
func (c *Client) RaiseAlert(ctx context.Context, a Alert) error {
	if err := a.validate(); err != nil {
		return err
	}
	now := a.Now.UTC()
	ref := c.db.Collection("alerts").Doc(AlertDedupKey(a.Kind, a.Subject))
	err := c.db.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			doc := map[string]any{
				"schema_version": int64(1), "dedup_key": ref.ID, "kind": a.Kind, "severity": a.Severity,
				"subject": a.Subject, "subject_kind": a.SubjectKind, "title": a.Title, "detail": a.Detail,
				"source": alertSource, "status": "firing", "first_seen_at": now, "last_seen_at": now,
				"count": int64(1), "reopened_count": int64(0),
			}
			if len(a.Labels) > 0 {
				doc["labels"] = a.Labels
			}
			return tx.Create(ref, doc)
		}
		if err != nil {
			return err
		}
		var labels any = firestore.Delete
		if len(a.Labels) > 0 {
			labels = a.Labels
		}
		updates := []firestore.Update{
			{Path: "schema_version", Value: int64(1)}, {Path: "dedup_key", Value: ref.ID}, {Path: "kind", Value: a.Kind},
			{Path: "severity", Value: a.Severity}, {Path: "subject", Value: a.Subject}, {Path: "subject_kind", Value: a.SubjectKind},
			{Path: "title", Value: a.Title}, {Path: "detail", Value: a.Detail}, {Path: "labels", Value: labels},
			{Path: "source", Value: alertSource}, {Path: "last_seen_at", Value: now}, {Path: "count", Value: firestore.Increment(1)},
		}
		if current, _ := snap.Data()["status"].(string); current == "resolved" {
			updates = append(updates,
				firestore.Update{Path: "status", Value: "firing"},
				firestore.Update{Path: "reopened_count", Value: firestore.Increment(1)},
				firestore.Update{Path: "acked_at", Value: firestore.Delete}, firestore.Update{Path: "acked_by", Value: firestore.Delete},
				firestore.Update{Path: "resolved_at", Value: firestore.Delete}, firestore.Update{Path: "resolved_by", Value: firestore.Delete})
		}
		return tx.Update(ref, updates)
	})
	if err != nil {
		return ErrUnavailable
	}
	return nil
}
