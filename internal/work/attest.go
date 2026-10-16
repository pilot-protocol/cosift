package work

import (
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// matches reports whether a joins g: within the Hamming bound of its anchor, from a worker and a
// network no member has.
func (s *Service) matches(g Group, a Attestation) bool {
	anchor, ok1 := crawler.ParseSimhash(g.Anchor)
	sum, ok2 := crawler.ParseSimhash(a.Simhash)
	if !ok1 || !ok2 || crawler.SimhashDistance(anchor, sum) > s.cfg.SimhashMax {
		return false
	}
	for _, m := range g.Members {
		if m.Worker == a.Worker || m.Net == a.Net {
			return false
		}
	}
	return true
}

// joins reports whether a joins group gi on a record of the class. An errored record also holds a to the
// anchor's title, language and the in-order agreement of WORK 6.5, which it returns for the caller to index.
func (s *Service) joins(url string, class Class, gi int, g Group, a Attestation, sp *splitText) (agreement, bool, error) {
	if !s.matches(g, a) {
		return agreement{}, false, nil
	}
	if class != ClassErrored {
		return agreement{}, true, nil
	}
	if sp == nil || len(g.Members) == 0 {
		return agreement{}, false, nil
	}
	anchor := g.Members[0]
	if a.TitleSha != anchor.TitleSha || a.Lang != anchor.Lang {
		return agreement{}, false, nil
	}
	parts, err := s.getParts(urlSHA(url), gi)
	if err != nil {
		s.logf("work: store read failed")
		return agreement{}, false, errUnavailable
	}
	ag := sp.agreeWith(parts, g.PartsN)
	return ag, floorMet(ag.matched, a.Len) && floorMet(ag.matched, anchor.Len) && len(ag.text) >= minAgreedBytes, nil
}

// newGroup starts a group with a as its anchor; sp is the anchor's text of an errored record.
func newGroup(a Attestation, sp *splitText) Group {
	g := Group{Anchor: a.Simhash, Members: []Attestation{a}}
	if sp != nil {
		g.PartsN = sp.total
	}
	return g
}

// startGroup appends a group anchored by a and stages the anchor's parts in the same batch.
func startGroup(tx *txn, url string, rec *Record, a Attestation, sp *splitText) {
	rec.Groups = append(rec.Groups, newGroup(a, sp))
	if sp != nil {
		_ = tx.putParts(urlSHA(url), len(rec.Groups)-1, sp.hashes)
	}
}

// place adds a to the first group it joins, or starts one; the caller stages the parts of a group it started.
func (s *Service) place(url string, rec *Record, a Attestation, sp *splitText) (gi int, started bool, ag agreement, err error) {
	for i := range rec.Groups {
		agreed, ok, err := s.joins(url, rec.Class, i, rec.Groups[i], a, sp)
		if err != nil {
			return 0, false, agreement{}, err
		}
		if ok {
			rec.Groups[i].Members = append(rec.Groups[i].Members, a)
			return i, false, agreed, nil
		}
	}
	rec.Groups = append(rec.Groups, newGroup(a, sp))
	return len(rec.Groups) - 1, true, agreement{}, nil
}

func workersOf(g Group) []string {
	out := make([]string, len(g.Members))
	for i, m := range g.Members {
		out[i] = m.Worker
	}
	return out
}

// otherWorkers lists the workers outside group keep, or every worker when keep is negative.
func otherWorkers(rec *Record, keep int) []string {
	var out []string
	for i, g := range rec.Groups {
		if i != keep {
			out = append(out, workersOf(g)...)
		}
	}
	return out
}

func awaiting(rec *Record) SubmitResult {
	until := rec.WindowUntil
	return SubmitResult{Verdict: "awaiting_quorum", Agreeing: []string{}, WindowExpiresAt: &until}
}

// attestLocked records an attestation against its URL's record and answers it.
func (s *Service) attestLocked(l *lease, a Attestation, att *attestation, now time.Time) (SubmitResult, error) {
	rec, err := s.getRecord(l.url)
	if err != nil {
		s.logf("work: store read failed")
		return SubmitResult{}, errUnavailable
	}
	if rec == nil {
		window := now.Add(time.Duration(s.cfg.QuorumWindowS) * time.Second).UTC()
		rec = &Record{V: 1, State: StateAwaiting, Copy: CopyNone, Class: l.class, FirstAt: now.UTC(), WindowUntil: window, RefetchAt: window}
	}
	var sp *splitText
	if rec.Class == ClassErrored {
		sp = att.parts()
		a.TitleSha, a.Lang, a.Len = titleSha(att.text.Title), att.text.Lang, len(att.text.Text)
	}
	if rec.State != StateAwaiting {
		return s.lateLocked(l, rec, a, sp)
	}
	gi, started, ag, err := s.place(l.url, rec, a, sp)
	if err != nil {
		return SubmitResult{}, err
	}
	tx := s.begin()
	if started && sp != nil {
		_ = tx.putParts(urlSHA(l.url), gi, sp.hashes)
	}
	res := awaiting(rec)
	quorate := len(rec.Groups[gi].Members) >= 2
	verified := false
	if quorate {
		if rec.Class == ClassErrored {
			if len(s.texts) < s.cfg.ModerationQueueMax {
				text := att.text
				text.Title = normaliseTitle(text.Title)
				text.Text = ag.text
				res = s.verifyLocked(tx, l.url, rec, gi, &text, "quorum", sha256Hex(text.Text), now)
				verified = true
			}
		} else if rec.RefetchFailures == 0 && rec.RefetchAt.After(now) {
			rec.RefetchAt = now.UTC()
		}
	}
	if err := s.stage(tx, l.url, rec); err != nil {
		return SubmitResult{}, err
	}
	s.consume(l, att.hash, false)
	if e := s.pool[l.url]; e != nil {
		e.attestations++
		e.quorate = e.quorate || (quorate && rec.Class != ClassErrored)
	}
	if verified {
		delete(s.pool, l.url)
	}
	return res, nil
}

// stage writes the record and commits the transition.
func (s *Service) stage(tx *txn, url string, rec *Record) error {
	if err := tx.putRecord(url, rec); err != nil {
		tx.discard()
		return errUnavailable
	}
	if err := tx.commit(); err != nil {
		s.logf("work: store write failed")
		return errUnavailable
	}
	return nil
}

// verifyLocked marks group gi verified with the text to index and queues it for moderation,
// deferring it at the ceiling. The workers of every other group are settled as mismatches.
func (s *Service) verifyLocked(tx *txn, url string, rec *Record, gi int, text *Text, by, hash string, now time.Time) SubmitResult {
	if by == "quorum" { // a worker's final_url is its own claim: the pool URL is what two accounts vouched for
		text.FinalURL = url
	}
	rec.VerifiedGroup, rec.VerifiedBy, rec.FinalURL, rec.ContentHash = gi, by, text.FinalURL, hash
	_ = tx.putText(urlSHA(url), text)
	_ = tx.event(urlSHA(url), "mismatch", otherWorkers(rec, gi), false)
	if _, reached := s.ceiling(now); reached {
		rec.State, rec.RetryAt = StateDeferred, nextMidnight(now)
		return SubmitResult{Verdict: "deferred", Agreeing: []string{}, Reason: "moderation_ceiling"}
	}
	rec.State, rec.RetryAt = StateModerating, time.Time{}
	s.kick()
	return SubmitResult{Verdict: "verified", Agreeing: workersOf(rec.Groups[gi])}
}

// lateLocked answers an attestation on a record that has left awaiting, the same way every time.
func (s *Service) lateLocked(l *lease, rec *Record, a Attestation, sp *splitText) (SubmitResult, error) {
	sha := urlSHA(l.url)
	tx := s.begin()
	var res SubmitResult
	switch rec.State {
	case StateModerating, StateDeferred, StateDone:
		g := &rec.Groups[rec.VerifiedGroup]
		joined := false
		if rec.VerifiedBy != "" {
			var err error
			if _, joined, err = s.joins(l.url, rec.Class, rec.VerifiedGroup, *g, a, sp); err != nil {
				tx.discard()
				return SubmitResult{}, err
			}
		}
		if joined {
			g.Members = append(g.Members, a)
			res = verifiedAnswer(rec)
			if rec.State == StateDone {
				_ = tx.event(sha, "verified", []string{a.Worker}, rec.ContentNovel)
			}
			break
		}
		fallthrough
	case StateMismatch:
		startGroup(tx, l.url, rec, a, sp)
		_ = tx.event(sha, "mismatch", []string{a.Worker}, false)
		res = SubmitResult{Verdict: "mismatch", Agreeing: []string{}}
	case StateRejected:
		startGroup(tx, l.url, rec, a, sp)
		_ = tx.event(sha, "rejected", []string{a.Worker}, false)
		res = SubmitResult{Verdict: "rejected", Agreeing: []string{}, Reason: rec.RejectReason}
	default:
		tx.discard()
		return SubmitResult{}, errLeaseExpired
	}
	if err := s.stage(tx, l.url, rec); err != nil {
		return SubmitResult{}, err
	}
	s.consume(l, a.Hash, false)
	return res, nil
}

// verifiedAnswer is the answer for a worker in the verified group of rec.
func verifiedAnswer(rec *Record) SubmitResult {
	if rec.State == StateDeferred {
		return SubmitResult{Verdict: "deferred", Agreeing: []string{}, Reason: "moderation_ceiling"}
	}
	return SubmitResult{Verdict: "verified", Agreeing: workersOf(rec.Groups[rec.VerifiedGroup]), Indexed: rec.State == StateDone}
}

// currentVerdict repeats the answer for a worker whose lease was already consumed.
func (s *Service) currentVerdict(url, worker string, now time.Time) (SubmitResult, error) {
	rec, err := s.getRecord(url)
	if err != nil {
		return SubmitResult{}, errUnavailable
	}
	if rec == nil || rec.State == StateExpired {
		return SubmitResult{}, errLeaseExpired
	}
	gi := -1
	for i, g := range rec.Groups {
		for _, m := range g.Members {
			if m.Worker == worker {
				gi = i
			}
		}
	}
	switch {
	case gi < 0:
		return SubmitResult{}, errLeaseExpired
	case rec.State == StateAwaiting:
		return awaiting(rec), nil
	case rec.State == StateRejected:
		return SubmitResult{Verdict: "rejected", Agreeing: []string{}, Reason: rec.RejectReason}, nil
	case rec.VerifiedBy != "" && gi == rec.VerifiedGroup:
		return verifiedAnswer(rec), nil
	}
	return SubmitResult{Verdict: "mismatch", Agreeing: []string{}}, nil
}
