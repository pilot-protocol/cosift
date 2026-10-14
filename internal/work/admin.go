package work

import (
	"context"
	"net/http"
	"sort"
	"time"
)

// PurgeResult counts what a purge removed, or would remove.
type PurgeResult struct {
	DocumentsDeleted, DocumentsKept, RecordsDeleted, TextsDeleted, WorkersSettled int
	ParkedUntil                                                                   time.Time
}

// Purge removes what a forged URL indexed: its attested documents, its record and text, and its leases.
// The URL is parked, and every worker that attested it is settled rejected. apply false only counts.
func (s *Service) Purge(ctx context.Context, raw string, apply bool) (PurgeResult, error) {
	canon, err := s.deps.Canonical(raw)
	if err != nil {
		return PurgeResult{}, &Error{Status: http.StatusBadRequest, Code: "invalid_url"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.getRecord(canon)
	if err != nil {
		return PurgeResult{}, errUnavailable
	}
	urls := []string{canon}
	if rec != nil && rec.FinalURL != "" && rec.FinalURL != canon {
		urls = append(urls, rec.FinalURL)
	}
	var res PurgeResult
	for _, u := range urls {
		deleted, kept, err := s.deps.Purge(ctx, u, apply)
		if err != nil {
			return PurgeResult{}, errUnavailable
		}
		res.DocumentsDeleted += deleted
		res.DocumentsKept += kept
	}
	sha := urlSHA(canon)
	_, hasText := s.texts[sha]
	until := s.now().Add(s.exclusionTTL()).UTC()
	res.ParkedUntil = until
	if hasText {
		res.TextsDeleted = 1
	}
	var workers []string
	if rec != nil {
		res.RecordsDeleted = 1
		workers = distinct(otherWorkers(rec, -1))
		res.WorkersSettled = len(workers)
	}
	if !apply {
		return res, nil
	}
	tx := s.begin()
	if rec != nil {
		_ = tx.delRecord(canon)
		_ = tx.event(sha, "rejected", workers, false)
	}
	_ = tx.delText(sha)
	_ = tx.park(sha, until)
	if err := tx.commit(); err != nil {
		return PurgeResult{}, errUnavailable
	}
	s.parkLocked(canon, until)
	for id, c := range s.consumed {
		if c.url == canon {
			delete(s.consumed, id)
		}
	}
	return res, nil
}

// parkLocked stops the URL being fed or leased until the time, and drops its pool entry and leases.
func (s *Service) parkLocked(canon string, until time.Time) {
	s.parked[canon] = until
	for id, l := range s.leases {
		if l.url == canon {
			s.dropLease(id)
		}
	}
	delete(s.pool, canon)
}

func distinct(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, w := range in {
		if !seen[w] {
			seen[w] = true
			out = append(out, w)
		}
	}
	return out
}

// VoidResult counts what a void changed, or would change.
type VoidResult struct {
	RecordsChanged, AttestationsDropped, RecordsDeleted, URLsParked int
}

// Void removes one worker's attestations from every record still awaiting. apply false only counts.
func (s *Service) Void(ctx context.Context, worker string, apply bool) (VoidResult, error) {
	if !hexID32.MatchString(worker) {
		return VoidResult{}, invalidField("worker", "pattern")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type change struct {
		url string
		rec *Record
	}
	var changes []change
	if err := s.forEachRecord(func(url string, r *Record) bool {
		if r.State == StateAwaiting && holds(r, worker) {
			changes = append(changes, change{url, r})
		}
		return true
	}); err != nil {
		return VoidResult{}, errUnavailable
	}
	var res VoidResult
	now := s.now()
	until := now.Add(s.exclusionTTL()).UTC()
	tx := s.begin()
	var parks []string
	for _, c := range changes {
		res.RecordsChanged++
		dropped, kept, err := s.regroup(c.rec, worker)
		if err != nil {
			tx.discard()
			return VoidResult{}, err
		}
		res.AttestationsDropped += dropped
		sha := urlSHA(c.url)
		switch {
		case len(c.rec.Groups) == 0:
			res.RecordsDeleted++
			_ = tx.delRecord(c.url)
		default:
			if c.rec.Class == ClassErrored {
				if err := s.rekeyParts(tx, sha, kept); err != nil {
					tx.discard()
					return VoidResult{}, err
				}
			}
			if !hasPair(c.rec) {
				res.URLsParked++
				parks = append(parks, c.url)
				_ = tx.park(sha, until)
				if c.rec.Class != ClassErrored && c.rec.RefetchFailures == 0 {
					c.rec.RefetchAt = c.rec.WindowUntil
				}
			}
			_ = tx.putRecord(c.url, c.rec)
		}
	}
	if !apply || res.RecordsChanged == 0 {
		tx.discard()
		return res, nil
	}
	if err := tx.commit(); err != nil {
		return VoidResult{}, errUnavailable
	}
	for _, c := range changes {
		s.resync(c.url, c.rec)
	}
	for _, u := range parks {
		s.parkLocked(u, until)
	}
	return res, nil
}

func holds(r *Record, worker string) bool {
	for _, g := range r.Groups {
		for _, m := range g.Members {
			if m.Worker == worker {
				return true
			}
		}
	}
	return false
}

// hasPair reports whether some group of the record has two members.
func hasPair(r *Record) bool {
	for _, g := range r.Groups {
		if len(g.Members) >= 2 {
			return true
		}
	}
	return false
}

// regroup drops the worker's attestations and groups the rest again in time order; it returns how many it dropped.
// An errored record keeps no parts but its anchors', so its groups are not formed again: a group whose anchor
// was the worker's goes whole and the others lose the worker's attestations; kept lists the groups left, by
// their index before the void.
func (s *Service) regroup(r *Record, worker string) (dropped int, kept []int, err error) {
	if r.Class == ClassErrored {
		dropped, kept = dropFromGroups(r, worker)
		return dropped, kept, nil
	}
	var keep []Attestation
	for _, g := range r.Groups {
		for _, m := range g.Members {
			if m.Worker == worker {
				dropped++
			} else {
				keep = append(keep, m)
			}
		}
	}
	sort.SliceStable(keep, func(i, j int) bool { return keep[i].At.Before(keep[j].At) })
	r.Groups = nil
	for _, a := range keep {
		if _, _, _, err := s.place("", r, a, nil); err != nil {
			return 0, nil, err
		}
	}
	return dropped, nil, nil
}

func dropFromGroups(r *Record, worker string) (dropped int, kept []int) {
	var groups []Group
	for i, g := range r.Groups {
		var members []Attestation
		for _, m := range g.Members {
			if m.Worker == worker {
				dropped++
			} else {
				members = append(members, m)
			}
		}
		if len(members) > 0 && g.Members[0].Worker != worker {
			g.Members = members
			groups = append(groups, g)
			kept = append(kept, i)
		}
	}
	r.Groups = groups
	return dropped, kept
}

// rekeyParts leaves in W p the parts of the groups a void kept, each under its new index.
func (s *Service) rekeyParts(tx *txn, sha string, kept []int) error {
	moved := make([][]uint64, len(kept))
	for i, old := range kept {
		parts, err := s.getParts(sha, old)
		if err != nil {
			s.logf("work: store read failed")
			return errUnavailable
		}
		moved[i] = parts
	}
	_ = tx.delParts(sha)
	for i, parts := range moved {
		_ = tx.putParts(sha, i, parts)
	}
	return nil
}

// resync brings a pool entry in line with a record that changed under it.
func (s *Service) resync(url string, r *Record) {
	e := s.pool[url]
	if e == nil {
		return
	}
	e.workers, e.nets, e.attestations, e.quorate = map[string]time.Time{}, map[string]time.Time{}, 0, false
	for _, g := range r.Groups {
		e.quorate = e.quorate || (len(g.Members) >= 2 && r.Class != ClassErrored)
		for _, m := range g.Members {
			e.workers[m.Worker], e.nets[m.Net] = r.FirstAt, r.FirstAt
			e.attestations++
		}
	}
	for _, l := range e.leases {
		e.workers[l.worker], e.nets[l.net] = l.granted, l.granted
	}
}
