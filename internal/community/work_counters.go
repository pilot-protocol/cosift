package community

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	fetchClaimed   = "fetch_claimed"
	fetchSubmitted = "fetch_submitted"
	fetchVerified  = "fetch_verified"
	fetchMismatch  = "fetch_mismatch"
	fetchRejected  = "fetch_rejected"
	fetchCredits   = "fetch_credits"

	fetchCounterCIDs = 50000
	counterFlushWait = 10 * time.Second
)

// fetchCounters buffers increments per contributor between flushes; a crash loses at most one interval.
type fetchCounters struct {
	mu  sync.Mutex
	add map[string]map[string]int64
}

func (c *fetchCounters) bump(cid, field string, n int64) {
	if cid == "" || n == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.add == nil {
		c.add = map[string]map[string]int64{}
	}
	fields := c.add[cid]
	if fields == nil {
		if len(c.add) >= fetchCounterCIDs {
			return
		}
		fields = map[string]int64{}
		c.add[cid] = fields
	}
	fields[field] += n
}

func (c *fetchCounters) take() map[string]map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	taken := c.add
	c.add = nil
	return taken
}

func (c *fetchCounters) restore(cid string, add map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.add == nil {
		c.add = map[string]map[string]int64{}
	}
	if c.add[cid] == nil {
		c.add[cid] = map[string]int64{}
	}
	for field, n := range add {
		c.add[cid][field] += n
	}
}

func (c *fetchCounters) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.add)
}

func (s *Server) bumpFetch(cid, field string, n int64) { s.work.counters.bump(cid, field, n) }

// flushFetchCounters writes each contributor's buffered increments; a missing contributor is dropped, any other failure keeps them for the next flush.
func (s *Server) flushFetchCounters(ctx context.Context) {
	failed := 0
	for cid, add := range s.work.counters.take() {
		callCtx, cancel := context.WithTimeout(ctx, counterFlushWait)
		err := s.work.source.AddCounters(callCtx, cid, add)
		cancel()
		if err != nil && !errors.Is(err, sharedaccount.ErrNotFound) && !errors.Is(err, sharedaccount.ErrInvalid) {
			s.work.counters.restore(cid, add)
			failed++
		}
	}
	if failed > 0 {
		log.Printf("community: fetch_counters_flush_failed contributors=%d", failed)
	}
}
