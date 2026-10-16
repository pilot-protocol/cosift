package wiki

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeHandles struct {
	mu       sync.Mutex
	known    map[string]Handle
	err      error
	calls    [][]string
	deadline bool
}

func (f *fakeHandles) Handles(ctx context.Context, cids []string) (map[string]Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, append([]string(nil), cids...))
	_, has := ctx.Deadline()
	f.deadline = has
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]Handle{}
	for _, c := range cids {
		if h, ok := f.known[c]; ok {
			out[c] = h
		}
	}
	return out, nil
}

func (f *fakeHandles) set(cid string, h Handle) {
	f.mu.Lock()
	f.known[cid] = h
	f.mu.Unlock()
}

func (f *fakeHandles) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

const (
	cidA = "0f1e2d3c4b5a6978"
	cidB = "5b8a1c4e9d207f36"
	cidC = "1111111111111111"
)

func withHandles(t *testing.T, f HandleSource) *harness {
	return newHarness(t, func(c *Config) { c.Handles = f })
}

func TestNamesResolveInOrderWithOneBatchedLookup(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "_edge_"}}}
	h := withHandles(t, src)
	got, degraded := h.w.names([]string{cidB, cidA, cidB})
	if degraded || fmt.Sprint(got) != "[@_edge_ @tidal-fan @_edge_]" {
		t.Fatalf("%v degraded=%v", got, degraded)
	}
	if len(src.calls) != 1 || fmt.Sprint(src.calls[0]) != fmt.Sprint([]string{cidB, cidA}) || !src.deadline {
		t.Fatalf("lookups %v, deadline %v", src.calls, src.deadline)
	}
}

func TestBannedUnknownAndMalformedHandlesShowAsACommunityMember(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{
		cidA: {Name: "tidal-fan", Banned: true},
		cidB: {Name: "Bad Name"},
	}}
	h := withHandles(t, src)
	got, degraded := h.w.names([]string{cidA, cidB, cidC})
	if degraded || fmt.Sprint(got) != "[a community member a community member a community member]" {
		t.Fatalf("%v degraded=%v", got, degraded)
	}
	for _, bad := range []string{"ab", "a b", "<script>", "ABC", "x‮y", "aaaaaaaaaaaaaaaaaaaaaaaaa"} {
		if displayName(Handle{Name: bad}, true) != anonymous {
			t.Fatalf("handle %q was shown", bad)
		}
	}
}

func TestLocalModeNeverAsksAndShowsEveryoneAnonymously(t *testing.T) {
	h := newHarness(t)
	got, degraded := h.w.names([]string{cidA, cidB})
	if degraded || fmt.Sprint(got) != "[a community member a community member]" {
		t.Fatalf("%v degraded=%v", got, degraded)
	}
}

func TestHandlesAreCachedUntilTheTTLAndARenameShowsAfterIt(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "old-name"}}}
	h := withHandles(t, src)
	h.w.names([]string{cidA})
	h.clock.Add(handleTTL - time.Second)
	if got, _ := h.w.names([]string{cidA}); got[0] != "@old-name" || src.callCount() != 1 {
		t.Fatalf("%v after %d lookups", got, src.callCount())
	}
	src.set(cidA, Handle{Name: "new-name"})
	h.clock.Add(time.Second)
	if got, _ := h.w.names([]string{cidA}); got[0] != "@new-name" || src.callCount() != 2 {
		t.Fatalf("%v after %d lookups", got, src.callCount())
	}
}

func TestUnknownAndBannedContributorsAreCachedToo(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan", Banned: true}}}
	h := withHandles(t, src)
	h.w.names([]string{cidA, cidC})
	h.w.names([]string{cidA, cidC})
	if src.callCount() != 1 {
		t.Fatalf("%d lookups", src.callCount())
	}
}

func TestOnlyUncachedContributorsAreLookedUp(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}, cidB: {Name: "other"}}}
	h := withHandles(t, src)
	h.w.names([]string{cidA})
	h.w.names([]string{cidA, cidB})
	if len(src.calls) != 2 || fmt.Sprint(src.calls[1]) != fmt.Sprint([]string{cidB}) {
		t.Fatalf("lookups %v", src.calls)
	}
}

func TestAFailingSourceDegradesWithoutCachingTheAnswer(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}, err: errors.New("unavailable")}
	h := withHandles(t, src)
	got, degraded := h.w.names([]string{cidA})
	if !degraded || got[0] != anonymous || h.w.handles.size() != 0 {
		t.Fatalf("%v degraded=%v cached=%d", got, degraded, h.w.handles.size())
	}
	src.mu.Lock()
	src.err = nil
	src.mu.Unlock()
	if got, degraded = h.w.names([]string{cidA}); degraded || got[0] != "@tidal-fan" {
		t.Fatalf("after recovery %v degraded=%v", got, degraded)
	}
}

func TestAnExpiredEntryIsNotServedWhenTheSourceFails(t *testing.T) {
	src := &fakeHandles{known: map[string]Handle{cidA: {Name: "tidal-fan"}}}
	h := withHandles(t, src)
	h.w.names([]string{cidA})
	h.clock.Add(handleTTL)
	src.mu.Lock()
	src.err = errors.New("unavailable")
	src.mu.Unlock()
	if got, degraded := h.w.names([]string{cidA}); !degraded || got[0] != anonymous {
		t.Fatalf("%v degraded=%v", got, degraded)
	}
}

func TestHandleCacheIsBounded(t *testing.T) {
	var c handleCache
	now := time.Now()
	for i := 0; i < handleCap+500; i++ {
		c.put(map[string]string{fmt.Sprintf("%016x", i): "@x"}, now)
	}
	if c.size() > handleCap {
		t.Fatalf("%d entries", c.size())
	}
	if name, ok := c.get(fmt.Sprintf("%016x", handleCap+499), now); !ok || name != "@x" {
		t.Fatal("the newest entry was not kept")
	}
}

func TestContributorCIDsAcceptOnlyAWellFormedList(t *testing.T) {
	long := make([]any, 65)
	for i := range long {
		long[i] = cidA
	}
	for name, tc := range map[string]struct {
		in   []any
		want int
	}{
		"empty":         {[]any{}, 0},
		"two":           {[]any{cidA, cidB}, 2},
		"upper case":    {[]any{cidA, "0F1E2D3C4B5A6978"}, 0},
		"short":         {[]any{"abc"}, 0},
		"not a string":  {[]any{cidA, 7.0}, 0},
		"too many":      {long, 0},
		"exactly sixty": {long[:64], 64},
	} {
		if got := contributorCIDs(tc.in); len(got) != tc.want {
			t.Errorf("%s: %d cids, want %d", name, len(got), tc.want)
		}
	}
}
