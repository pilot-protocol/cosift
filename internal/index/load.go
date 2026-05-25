package index

import (
	"context"

	"github.com/calinteodor/cosift/internal/store"
)

// LoadVectorIndex builds an in-memory VectorIndex from all passages in the
// store for the given model. Returns an empty index (no error) if there are
// no passages — that's the "nothing indexed yet" state, not a failure.
func LoadVectorIndex(ctx context.Context, s *store.Store, model string, dim int) (*VectorIndex, error) {
	rows, err := s.LoadPassagesByModel(ctx, model)
	if err != nil {
		return nil, err
	}
	vi := NewVectorIndex(dim)
	for _, r := range rows {
		vi.AddPassage(r.URL, r.Title, r.Offset, r.Length, r.Vec)
	}
	return vi, nil
}
