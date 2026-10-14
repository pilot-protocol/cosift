package work

import (
	"context"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// runRefetches runs the due server fetches, at most refetch_concurrency at a time.
func (s *Service) runRefetches(ctx context.Context) {
	now := s.now()
	s.mu.Lock()
	var due []string
	if len(s.texts) < s.cfg.ModerationQueueMax {
		_ = s.forEachRecord(func(url string, r *Record) bool {
			if _, busy := s.inflight["r:"+url]; r.State == StateAwaiting && !r.RefetchAt.After(now) && !busy && len(due) < 4*s.cfg.RefetchConcurrency {
				due = append(due, url)
				s.inflight["r:"+url] = struct{}{}
			}
			return true
		})
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.cfg.RefetchConcurrency)
	for _, url := range due {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.refetchOne(ctx, url)
		}()
	}
	wg.Wait()
}

// validCopy canonicalises the server's copy and holds it to the same bar as an attestation.
func (s *Service) validCopy(p Page) (Text, string, bool) {
	canon, err := s.deps.Canonical(p.FinalURL)
	if err != nil || !s.deps.Allowed(canon) || len(p.Text) < minTextBytes || (s.deps.BotCheck != nil && s.deps.BotCheck(p.Title, p.Text)) {
		return Text{}, "", false
	}
	if _, ok := crawler.Simhash(p.Text); !ok {
		return Text{}, "", false
	}
	return Text{FinalURL: canon, Title: p.Title, Text: p.Text, Lang: p.Lang}, sha256Hex(p.Text), true
}

func (s *Service) refetchOne(ctx context.Context, url string) {
	fctx, cancel := context.WithTimeout(ctx, refetchTimeout)
	page, err := s.deps.Fetch(fctx, url)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer delete(s.inflight, "r:"+url)
	rec, rerr := s.getRecord(url)
	if rerr != nil || rec == nil || rec.State != StateAwaiting {
		return
	}
	now := s.now()
	text, hash, ok := Text{}, "", false
	if err == nil {
		text, hash, ok = s.validCopy(page)
	}
	tx := s.begin()
	switch {
	case !ok:
		rec.RefetchFailures++
		rec.RefetchAt = now.Add(refetchBackoff(rec.RefetchFailures)).UTC()
		s.metrics.refetch.add("fail")
	case len(s.texts) >= s.cfg.ModerationQueueMax:
		rec.RefetchAt = now.Add(time.Minute).UTC()
		s.metrics.refetch.add("busy")
	default:
		sum, _ := crawler.Simhash(text.Text)
		if gi := s.closestGroup(rec, sum); gi >= 0 {
			s.verifyLocked(tx, url, rec, gi, &text, "server", hash, now)
			s.metrics.refetch.add("match")
		} else {
			s.mismatchLocked(tx, url, rec, &text, hash, now)
			s.metrics.refetch.add("mismatch")
		}
		delete(s.pool, url)
	}
	if s.stage(tx, url, rec) != nil {
		return
	}
}

// refetchBackoff is one hour doubling to a day.
func refetchBackoff(failures int) time.Duration {
	d := time.Hour
	for i := 1; i < failures && d < maxRefetchBackof; i++ {
		d *= 2
	}
	return min(d, maxRefetchBackof)
}

// closestGroup is the group whose anchor is within the bound of sum, nearest first and largest on a tie; -1 if none.
func (s *Service) closestGroup(rec *Record, sum uint64) int {
	best, bestDist := -1, s.cfg.SimhashMax+1
	for i, g := range rec.Groups {
		anchor, ok := crawler.ParseSimhash(g.Anchor)
		if !ok {
			continue
		}
		d := crawler.SimhashDistance(anchor, sum)
		if d < bestDist || (d == bestDist && best >= 0 && len(g.Members) > len(rec.Groups[best].Members)) {
			best, bestDist = i, d
		}
	}
	return best
}

// mismatchLocked settles a record whose server copy matches no group: every attesting worker is a mismatch
// and the server's copy is moderated and indexed under copy.
func (s *Service) mismatchLocked(tx *txn, url string, rec *Record, text *Text, hash string, now time.Time) {
	rec.State, rec.FinalURL, rec.ContentHash = StateMismatch, text.FinalURL, hash
	_ = tx.putText(urlSHA(url), text)
	_ = tx.event(urlSHA(url), "mismatch", otherWorkers(rec, -1), false)
	if _, reached := s.ceiling(now); reached {
		rec.Copy, rec.CopyRetryAt = CopyDeferred, nextMidnight(now)
		return
	}
	rec.Copy, rec.CopyRetryAt = CopyModerating, time.Time{}
	s.kick()
}
