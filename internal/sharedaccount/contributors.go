package sharedaccount

import (
	"context"
	"errors"
	"regexp"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
)

var cidPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// firestoreIn is the most values one "in" filter takes.
const firestoreIn = 30

// ContributorHandle is the public part of a contributor: nothing else of the document is requested.
type ContributorHandle struct {
	Handle string
	Banned bool
}

type HandleSource interface {
	Handles(ctx context.Context, cids []string) (map[string]ContributorHandle, error)
}

var _ HandleSource = (*Client)(nil)

var handleFields = []string{"handle", "banned"}

// Handles reads handle and banned for each cid with a field mask; a cid with no document is absent from the result.
func (c *Client) Handles(ctx context.Context, cids []string) (map[string]ContributorHandle, error) {
	for _, cid := range cids {
		if !cidPattern.MatchString(cid) {
			return nil, ErrInvalid
		}
	}
	out := map[string]ContributorHandle{}
	for start := 0; start < len(cids); start += firestoreIn {
		var refs []*firestore.DocumentRef
		for _, cid := range cids[start:min(start+firestoreIn, len(cids))] {
			refs = append(refs, c.db.Collection("contributors").Doc(cid))
		}
		it := c.db.Collection("contributors").Where(firestore.DocumentID, "in", refs).Select(handleFields...).Documents(ctx)
		for {
			doc, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				it.Stop()
				return nil, ErrUnavailable
			}
			h, ok := parseHandle(doc.Data())
			if !ok {
				it.Stop()
				return nil, ErrUnavailable
			}
			out[doc.Ref.ID] = h
		}
		it.Stop()
	}
	return out, nil
}

// parseHandle fails on a member outside the mask; a banned member of the wrong type counts as banned.
func parseHandle(data map[string]any) (ContributorHandle, bool) {
	var h ContributorHandle
	for key, v := range data {
		switch key {
		case "handle":
			h.Handle, _ = v.(string)
		case "banned":
			if v != nil {
				banned, isBool := v.(bool)
				h.Banned = banned || !isBool
			}
		default:
			return ContributorHandle{}, false
		}
	}
	return h, true
}
