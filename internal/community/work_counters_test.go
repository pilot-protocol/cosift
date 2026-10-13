package community

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func TestCountersAreSummedPerContributorAndFlushedOnce(t *testing.T) {
	s, f, _ := gateRig(t)
	s.bumpFetch(testCID, fetchClaimed, 3)
	s.bumpFetch(testCID, fetchClaimed, 2)
	s.bumpFetch(testCID, fetchSubmitted, 1)
	s.bumpFetch("1111111111111111", fetchCredits, 4)
	s.flushFetchCounters(context.Background())
	if f.counter(testCID, "fetch_claimed") != 5 || f.counter(testCID, "fetch_submitted") != 1 || f.counter("1111111111111111", "fetch_credits") != 4 || f.adds != 2 {
		t.Fatalf("%v after %d calls", f.counters, f.adds)
	}
	s.flushFetchCounters(context.Background())
	if f.adds != 2 {
		t.Fatalf("an empty buffer wrote: %d calls", f.adds)
	}
}

func TestCountersSkipAMissingContributorAndKeepWhatATransientErrorRefused(t *testing.T) {
	s, f, _ := gateRig(t)
	ctx := context.Background()
	f.missing[testCID] = true
	s.bumpFetch(testCID, fetchVerified, 1)
	s.flushFetchCounters(ctx)
	f.missing[testCID] = false
	s.flushFetchCounters(ctx)
	if f.counter(testCID, "fetch_verified") != 0 {
		t.Fatal("a missing contributor's counters were retried")
	}
	f.addErr = sharedaccount.ErrUnavailable
	s.bumpFetch(testCID, fetchVerified, 2)
	s.flushFetchCounters(ctx)
	s.bumpFetch(testCID, fetchVerified, 1)
	f.addErr = nil
	s.flushFetchCounters(ctx)
	if got := f.counter(testCID, "fetch_verified"); got != 3 {
		t.Fatalf("%d after an unavailable flush, want 3", got)
	}
}

func TestCountersLoseNothingWhenIncrementsRaceFlushesThatFailAtRandom(t *testing.T) {
	s, f, _ := gateRig(t)
	ctx := context.Background()
	var calls int
	var mu sync.Mutex
	f.stall = func(string) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		f.mu.Lock()
		if n%3 == 0 {
			f.addErr = sharedaccount.ErrUnavailable
		} else {
			f.addErr = nil
		}
		f.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	var wg sync.WaitGroup
	const workers, perWorker = 8, 200
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				s.bumpFetch(testCID, fetchClaimed, 1)
				s.bumpFetch(fmt.Sprintf("%016x", i%5), fetchSubmitted, 1)
				if i%25 == 0 {
					s.flushFetchCounters(ctx)
				}
			}
		}()
	}
	wg.Wait()
	f.stall = nil
	f.addErr = nil
	s.flushFetchCounters(ctx)
	if got := f.counter(testCID, "fetch_claimed"); got != workers*perWorker {
		t.Fatalf("claimed %d, want %d", got, workers*perWorker)
	}
	var submitted int64
	for i := 0; i < 5; i++ {
		submitted += f.counter(fmt.Sprintf("%016x", i), "fetch_submitted")
	}
	if submitted != workers*perWorker {
		t.Fatalf("submitted %d, want %d", submitted, workers*perWorker)
	}
}

func TestCountersBufferIsBounded(t *testing.T) {
	s, _, _ := gateRig(t)
	for i := 0; i < fetchCounterCIDs+500; i++ {
		s.bumpFetch(fmt.Sprintf("%016x", i), fetchClaimed, 1)
	}
	if n := s.work.counters.size(); n > fetchCounterCIDs {
		t.Fatalf("%d contributors buffered", n)
	}
	s.bumpFetch(testCID, fetchClaimed, 1)
}
