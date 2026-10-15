package main

import (
	"context"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func completeContentIndex(t *testing.T, db *store.PebbleStore) {
	t.Helper()
	if _, err := db.BackfillContentSHA(context.Background(), false, nil); err != nil {
		t.Fatal(err)
	}
}
