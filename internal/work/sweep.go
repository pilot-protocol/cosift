package work

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	refetchTimeout   = 60 * time.Second
	indexRetryDelay  = time.Minute
	maxRefetchBackof = 24 * time.Hour
)

// Run feeds the pool and sweeps until ctx ends.
func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Duration(s.cfg.SweepIntervalS) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.Sweep(ctx)
			case <-s.wake:
				s.runModerations(ctx)
			}
		}
	}()
	go func() {
		defer wg.Done()
		t := time.NewTicker(time.Duration(s.cfg.ErroredScanIntervalS) * time.Second)
		defer t.Stop()
		for {
			s.Feed(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	wg.Wait()
}

// Sweep runs one pass: leases lapse, the TTL expires old records, due server fetches and moderations run.
func (s *Service) Sweep(ctx context.Context) {
	now := s.now()
	s.mu.Lock()
	s.expireLeases(now)
	s.tidy(now)
	s.mu.Unlock()
	s.expire(now)
	s.runRefetches(ctx)
	s.runModerations(ctx)
}

// tidy forgets expired host spacing and parks; mu must be held.
func (s *Service) tidy(now time.Time) {
	for h, at := range s.hostNext {
		if !now.Before(at) {
			delete(s.hostNext, h)
		}
	}
	for u, until := range s.parked {
		if !now.Before(until) {
			delete(s.parked, u)
		}
	}
}

// expire ends records older than the TTL and deletes the finished ones, with their texts, events and parks.
func (s *Service) expire(now time.Time) {
	ttl := s.exclusionTTL()
	s.mu.Lock()
	defer s.mu.Unlock()
	type item struct {
		url string
		rec *Record
	}
	var old []item
	if err := s.forEachRecord(func(url string, r *Record) bool {
		if now.Sub(r.FirstAt) >= ttl {
			old = append(old, item{url, r})
		}
		return true
	}); err != nil {
		s.logf("work: store scan failed")
		return
	}
	for _, it := range old {
		tx := s.begin()
		sha := urlSHA(it.url)
		switch it.rec.State {
		case StateAwaiting, StateModerating, StateDeferred:
			it.rec.State, it.rec.Copy = StateExpired, CopyNone
			_ = tx.event(sha, "expired", otherWorkers(it.rec, -1), false)
			_ = tx.delText(sha)
			_ = tx.delParts(sha)
			_ = tx.putRecord(it.url, it.rec)
			delete(s.pool, it.url)
		default:
			_ = tx.delText(sha)
			_ = tx.delRecord(it.url)
		}
		if err := tx.commit(); err != nil {
			s.logf("work: store write failed")
		}
	}
	s.sweepEvents(now, ttl)
}

// sweepEvents deletes settled events and parks past their time.
func (s *Service) sweepEvents(now time.Time, ttl time.Duration) {
	b := s.db.NewBatch()
	defer b.Close()
	for _, ev := range s.eventsAfter(0, 1<<30) {
		if now.Sub(ev.At) < ttl {
			break
		}
		_ = b.Delete(keyEvent(ev.Seq), nil)
	}
	s.parksPast(now, b)
	_ = b.Commit(pebbleSync)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func toCrawlerArtifact(a *Artifact) *crawler.LocalArtifact {
	if a == nil {
		return nil
	}
	return &crawler.LocalArtifact{Model: a.Model, Chunks: a.Chunks}
}
