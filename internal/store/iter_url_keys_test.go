package store

import (
	"context"
	"testing"
	"time"
)

// TestIterURLKeys — yields exactly the live 'u' keys: soft-deleted URLs are
// excluded, early-stop and ctx cancel are honored.
func TestIterURLKeys(t *testing.T) {
	p := newPebbleStore(t)
	ctx := context.Background()

	urls := []string{"https://a.example/1", "https://b.example/2", "https://c.example/3"}
	ids := make([]int64, len(urls))
	for i, u := range urls {
		id, err := p.UpsertDocument(ctx, &Document{
			URL: u, Domain: "example", Title: "t", Text: "body", FetchedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", u, err)
		}
		ids[i] = id
	}
	if ok, err := p.SoftDeleteDocument(ctx, ids[1], urls[1]); err != nil || !ok {
		t.Fatalf("soft delete: ok=%v err=%v", ok, err)
	}

	got := map[string]bool{}
	if err := p.IterURLKeys(ctx, func(url string) bool {
		got[url] = true
		return true
	}); err != nil {
		t.Fatalf("iter: %v", err)
	}
	if len(got) != 2 || !got[urls[0]] || !got[urls[2]] {
		t.Fatalf("expected exactly {%s, %s}, got %v", urls[0], urls[2], got)
	}
	if got[urls[1]] {
		t.Fatalf("soft-deleted URL still yielded")
	}

	n := 0
	if err := p.IterURLKeys(ctx, func(string) bool {
		n++
		return false
	}); err != nil {
		t.Fatalf("early-stop iter: %v", err)
	}
	if n != 1 {
		t.Fatalf("early stop should visit exactly 1, visited %d", n)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.IterURLKeys(cctx, func(string) bool { return true }); err == nil {
		t.Fatalf("canceled ctx should error")
	}
}
