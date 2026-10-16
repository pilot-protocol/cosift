package work

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// moderationBackoff is ten minutes doubling with each failure.
func moderationBackoff(failures int) time.Duration {
	return 10 * time.Minute << (failures - 1)
}

// target is which text of a record a moderation concerns.
type target struct {
	url  string
	copy bool
}

// runModerations moderates and indexes the due texts, at most moderation_concurrency at a time.
func (s *Service) runModerations(ctx context.Context) {
	now := s.now()
	s.mu.Lock()
	var due []target
	_ = s.forEachRecord(func(url string, r *Record) bool {
		switch {
		case r.State == StateMismatch && (r.Copy == CopyModerating || r.Copy == CopyDeferred) && !r.CopyRetryAt.After(now):
			due = append(due, target{url, true})
		case (r.State == StateModerating || r.State == StateDeferred) && !r.RetryAt.After(now):
			due = append(due, target{url, false})
		}
		return true
	})
	var run []target
	for _, t := range due {
		if _, busy := s.inflight["m:"+t.url]; !busy {
			s.inflight["m:"+t.url] = struct{}{}
			run = append(run, t)
		}
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	sem := make(chan struct{}, s.cfg.ModerationConcurrency)
	for _, t := range run {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.moderateOne(ctx, t)
		}()
	}
	wg.Wait()
}

type outcome struct {
	verdict  Verdict
	err      error
	indexErr error
	novel    bool
}

func (s *Service) moderateOne(ctx context.Context, t target) {
	s.mu.Lock()
	rec, err := s.getRecord(t.url)
	var text *Text
	if err == nil && rec != nil {
		text, err = s.getText(urlSHA(t.url))
	}
	by := "server"
	if rec != nil && !t.copy {
		by = rec.VerifiedBy
	}
	s.mu.Unlock()
	if err != nil || rec == nil || text == nil {
		s.mu.Lock()
		delete(s.inflight, "m:"+t.url)
		s.mu.Unlock()
		return
	}
	var out outcome
	out.verdict, out.err = s.deps.Moderate(ctx, ModerationDoc{URL: text.FinalURL, Title: text.Title, Text: text.Text}, s.reserve)
	if out.err == nil && out.verdict.Decision == "allow" && !s.gone(t) {
		var indexed bool
		indexed, out.novel, out.indexErr = s.deps.Index(ctx, crawler.AttestedDoc{URL: text.FinalURL, LeaseURL: t.url, Title: text.Title,
			Text: text.Text, Lang: text.Lang, Artifact: toCrawlerArtifact(text.Artifact), VerifiedBy: by})
		if indexed && out.indexErr == nil && s.gone(t) {
			s.unindex(ctx, text.FinalURL)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer delete(s.inflight, "m:"+t.url)
	s.applyModeration(t, out)
}

// gone reports whether the record a moderation was started for has been purged since: it is absent or no longer awaits it.
func (s *Service) gone(t target) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.getRecord(t.url)
	return err == nil && (rec == nil || !live(rec, t))
}

// unindex takes out a page that finished indexing after its record was purged.
func (s *Service) unindex(ctx context.Context, url string) {
	if _, _, err := s.deps.Purge(ctx, url, true); err != nil {
		s.logf("work: a purged page could not be taken out of the index")
		return
	}
	s.logf("work: a page indexed during a purge was taken out of the index")
}

func live(rec *Record, t target) bool {
	return (!t.copy && (rec.State == StateModerating || rec.State == StateDeferred)) ||
		(t.copy && rec.State == StateMismatch && (rec.Copy == CopyModerating || rec.Copy == CopyDeferred))
}

// applyModeration moves the record on after its moderation; mu must be held.
func (s *Service) applyModeration(t target, out outcome) {
	rec, err := s.getRecord(t.url)
	now := s.now()
	if err != nil || rec == nil {
		return
	}
	if !live(rec, t) {
		return
	}
	sha := urlSHA(t.url)
	tx := s.begin()
	retry, failures := &rec.RetryAt, &rec.ModerationFailures
	setDone := func() { rec.State = StateDone }
	setRejected := func() { rec.State = StateRejected }
	setDeferred := func() { rec.State = StateDeferred }
	setModerating := func() { rec.State = StateModerating }
	if t.copy {
		retry, failures = &rec.CopyRetryAt, &rec.CopyFailures
		setDone = func() { rec.Copy = CopyDone }
		setRejected = func() { rec.Copy = CopyRejected }
		setDeferred = func() { rec.Copy = CopyDeferred }
		setModerating = func() { rec.Copy = CopyModerating }
	}
	var ceiling *CeilingError
	switch {
	case errors.As(out.err, &ceiling):
		setDeferred()
		*retry = nextMidnight(now)
		s.metrics.moderations.add("ceiling")
	case out.err != nil:
		*failures++
		s.metrics.moderations.add("error")
		if *failures >= s.cfg.ModerationAttemptsMax {
			setRejected()
			rec.RejectReason = "moderation_failed"
			_ = tx.delText(sha)
			if !t.copy {
				_ = tx.event(sha, "rejected", otherWorkers(rec, -1), false)
			}
		} else {
			setModerating()
			*retry = now.Add(moderationBackoff(*failures)).UTC()
		}
	case out.verdict.Decision != "allow":
		setRejected()
		rec.RejectReason = out.verdict.Category
		_ = tx.delText(sha)
		if !t.copy {
			_ = tx.event(sha, "rejected", otherWorkers(rec, -1), false)
		}
		s.metrics.moderations.add("reject")
	case out.indexErr != nil:
		setModerating()
		*retry = now.Add(indexRetryDelay).UTC()
		s.metrics.moderations.add("index_error")
	default:
		setDone()
		_ = tx.delText(sha)
		if !t.copy {
			rec.ContentNovel = out.novel
			_ = tx.event(sha, "verified", workersOf(rec.Groups[rec.VerifiedGroup]), out.novel)
		}
		s.metrics.moderations.add("allow")
	}
	_ = s.stage(tx, t.url, rec)
}
