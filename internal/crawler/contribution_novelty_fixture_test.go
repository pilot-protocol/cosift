package crawler

import (
	"context"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func completeContentIndex(t *testing.T, ps *store.PebbleStore) {
	t.Helper()
	if _, err := ps.BackfillContentSHA(context.Background(), false, nil); err != nil {
		t.Fatal(err)
	}
}
