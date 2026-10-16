package work

import (
	"context"
	"net/url"
	"regexp"
	"strings"

	"github.com/cockroachdb/pebble"
)

// blocking matches the errors a site gives a crawler that it turns away: a 403 or 429, a bot-check or
// challenge page, a timeout. A 404, a DNS failure or a parse failure is no reason to ask a person to fetch.
var blocking = regexp.MustCompile(`(?i)\bhttp (403|429)\b|rate limited|challenge|captcha|bot[- ]check|just a moment|timeout|timed out|deadline exceeded`)

var errURL = regexp.MustCompile(`https?://[^\s"]+`)

// BlockingError reports whether a stored frontier error is of a class a worker's own network may get past.
// The URL a transport error quotes, and its host, say nothing about the failure and are left out.
func BlockingError(msg string) bool {
	msg = strings.ToLower(msg)
	for _, raw := range errURL.FindAllString(msg, -1) {
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			msg = strings.ReplaceAll(msg, u.Hostname(), "")
		}
	}
	return blocking.MatchString(errURL.ReplaceAllString(msg, ""))
}

type feedResult int

const (
	fedAdded feedResult = iota
	fedDuplicate
	fedBadURL
	fedPolicy
	fedFull
)

// consider puts one URL in the pool if it passes the policy and is not already known.
func (s *Service) consider(ctx context.Context, raw string, class Class) feedResult {
	canon, err := s.deps.Canonical(raw)
	if err != nil {
		return fedBadURL
	}
	if !s.deps.Allowed(canon) {
		return fedPolicy
	}
	if s.deps.InCorpus != nil {
		if in, err := s.deps.InCorpus(ctx, canon); err != nil || in {
			return fedDuplicate
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if until, ok := s.parked[canon]; ok && now.Before(until) || s.isParked(urlSHA(canon)) {
		return fedDuplicate
	}
	if _, ok := s.pool[canon]; ok {
		return fedDuplicate
	}
	rec, err := s.getRecord(canon)
	if err != nil || (rec != nil && rec.State != StateAwaiting) {
		return fedDuplicate
	}
	switch added, full := s.poolAdd(canon, class, rec); {
	case added:
		return fedAdded
	case full:
		return fedFull
	}
	return fedDuplicate
}

// Feed runs one pass of the errored feeder and, when it is on, the frontier feeder.
func (s *Service) Feed(ctx context.Context) {
	if s.deps.Frontier == nil {
		return
	}
	s.mu.Lock()
	after := s.errCursor
	s.mu.Unlock()
	rows, next, end, err := s.deps.Frontier.Errored(ctx, after, s.cfg.ErroredScanBatch)
	if err != nil {
		s.logf("work: errored scan failed")
		return
	}
	for _, r := range rows {
		if BlockingError(r.LastError) {
			s.consider(ctx, r.URL, ClassErrored)
		}
	}
	if end {
		next = ""
	}
	s.mu.Lock()
	s.errCursor = next
	s.mu.Unlock()
	_ = s.db.Set(keyPoolCur, []byte(next), pebble.Sync)
	if !s.frontier {
		return
	}
	s.mu.Lock()
	after = s.frontierCursor
	room := s.cfg.PoolMax/2 - s.classSize(ClassFrontier)
	s.mu.Unlock()
	if room <= 0 {
		return
	}
	rows, next, end, err = s.deps.Frontier.Queued(ctx, after, s.cfg.ErroredScanBatch)
	if err != nil {
		s.logf("work: frontier scan failed")
		return
	}
	for _, r := range rows {
		if s.consider(ctx, r.URL, ClassFrontier) != fedAdded {
			continue
		}
		if room--; room == 0 {
			next, end = r.URL, false
			break
		}
	}
	if end {
		next = ""
	}
	s.mu.Lock()
	s.frontierCursor = next
	s.mu.Unlock()
}

// classSize counts the pool entries of a class; mu must be held.
func (s *Service) classSize(c Class) int {
	n := 0
	for _, e := range s.pool {
		if e.class == c {
			n++
		}
	}
	return n
}

// PushResult counts what a push did with each URL.
type PushResult struct {
	Accepted, Duplicates int
	Rejected             []PushRejection
}

// PushRejection names the URL (by index) a push refused and the rule it broke.
type PushRejection struct {
	Index int
	Rule  string
}

// Push puts explicit URLs in the pool as the highest-priority class.
func (s *Service) Push(ctx context.Context, urls []string) (PushResult, error) {
	switch {
	case len(urls) == 0:
		return PushResult{}, invalidField("urls", "required")
	case len(urls) > 100:
		return PushResult{}, invalidField("urls", "too_many")
	}
	res := PushResult{Rejected: []PushRejection{}}
	for i, raw := range urls {
		var rule string
		switch s.consider(ctx, strings.TrimSpace(raw), ClassPushed) {
		case fedAdded:
			res.Accepted++
		case fedDuplicate:
			res.Duplicates++
		case fedBadURL:
			rule = "url"
		case fedPolicy:
			rule = "policy"
		case fedFull:
			rule = "pool_full"
		}
		if rule != "" {
			res.Rejected = append(res.Rejected, PushRejection{Index: i, Rule: rule})
		}
	}
	return res, nil
}
