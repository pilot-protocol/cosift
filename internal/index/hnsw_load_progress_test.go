package index

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

// LoadHNSWProgress reports progress on its node cadence and aborts promptly
// when ctx is cancelled mid-decode. loadCheckEvery is lowered so the 6-node
// fixture exercises the mid-loop progress + cancel path.
func TestLoadHNSWProgressMidLoopAbort(t *testing.T) {
	old := loadCheckEvery
	loadCheckEvery = 1
	defer func() { loadCheckEvery = old }()

	dir := filepath.Join(t.TempDir(), "pebble")
	ps, err := store.OpenPebble(dir)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	defer ps.Close()
	ctx := context.Background()

	const dim = 8
	g := NewHNSW(dim)
	for i := 0; i < 6; i++ {
		v := make([]float32, dim)
		v[i%dim] = 1
		g.AddPassage(fmt.Sprintf("https://x/%d", i), fmt.Sprintf("doc %d", i), i*10, 5, v)
	}
	if err := g.Persist(ctx, ps); err != nil {
		t.Fatalf("persist: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	var calls int
	got, ok, err := LoadHNSWProgress(cctx, ps, func(loaded, total uint64) {
		calls++
		if calls == 2 {
			cancel() // cancel mid-decode
		}
	})
	if calls < 2 {
		t.Fatalf("progress fired %d times, want >=2", calls)
	}
	if err == nil || ok || got != nil {
		t.Fatalf("mid-loop abort: want (nil,false,err), got (%v,%v,%v)", got, ok, err)
	}
}
