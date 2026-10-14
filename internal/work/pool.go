package work

import (
	"net/url"
	"regexp"
	"strings"
	"time"
)

var (
	hexID32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type pair struct{ worker, net string }

// entry is a pool URL: a copy of the feeder's row, so nothing here changes a frontier row.
type entry struct {
	url, sha, host string
	class          Class
	failed         map[pair]struct{}
	reported       map[pair]struct{}
	workers, nets  map[string]time.Time
	leases         map[string]*lease
	parkedUntil    time.Time
	attestations   int
	quorate        bool
}

type lease struct {
	id, url         string
	worker, net     string
	class           Class
	granted, expiry time.Time
}

// consumedLease remembers a lease whose submission was taken, so a lost response can be repeated.
type consumedLease struct {
	url, worker, net string
	hash             string
	failure          bool
	until            time.Time
}

type classQueue struct {
	ring   []string
	cursor int
	byHost map[string][]*entry
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

func (q *classQueue) add(e *entry) {
	if _, ok := q.byHost[e.host]; !ok {
		q.ring = append(q.ring, e.host)
	}
	q.byHost[e.host] = append(q.byHost[e.host], e)
}

// poolAdd puts a URL in the pool; it reports false when the pool is full.
func (s *Service) poolAdd(canonical string, class Class, rec *Record) (added, full bool) {
	if _, ok := s.pool[canonical]; ok {
		return false, false
	}
	if len(s.pool) >= s.cfg.PoolMax {
		return false, true
	}
	e := &entry{url: canonical, sha: urlSHA(canonical), host: hostOf(canonical), class: class,
		failed: map[pair]struct{}{}, reported: map[pair]struct{}{}, workers: map[string]time.Time{}, nets: map[string]time.Time{}, leases: map[string]*lease{}}
	if rec != nil {
		e.class = rec.Class
		at := rec.FirstAt
		for _, g := range rec.Groups {
			e.quorate = e.quorate || (len(g.Members) >= 2 && rec.Class != ClassErrored)
			for _, m := range g.Members {
				e.workers[m.Worker], e.nets[m.Net] = at, at
				e.attestations++
			}
		}
	}
	s.pool[canonical] = e
	s.queues[e.class].add(e)
	return true, false
}

func (s *Service) poolDrop(canonical string) {
	e := s.pool[canonical]
	if e == nil {
		return
	}
	delete(s.pool, canonical)
	for id := range e.leases {
		s.dropLease(id)
	}
}

func (s *Service) dropLease(id string) {
	l := s.leases[id]
	if l == nil {
		return
	}
	delete(s.leases, id)
	if s.byWorker[l.worker]--; s.byWorker[l.worker] <= 0 {
		delete(s.byWorker, l.worker)
	}
	if e := s.pool[l.url]; e != nil {
		delete(e.leases, id)
	}
}

func (s *Service) exclusionTTL() time.Duration {
	return time.Duration(s.cfg.AttestationTTLH) * time.Hour
}

// eligible reports whether the pair may be leased the entry now.
func (s *Service) eligible(e *entry, p pair, now time.Time) bool {
	if now.Before(e.parkedUntil) || e.quorate || e.attestations >= maxAttestationsPerURL || len(e.leases) >= s.cfg.DistinctLeasesPerURL {
		return false
	}
	ttl := s.exclusionTTL()
	if at, ok := e.workers[p.worker]; ok && now.Sub(at) < ttl {
		return false
	}
	if at, ok := e.nets[p.net]; ok && now.Sub(at) < ttl {
		return false
	}
	return true
}

// maxAttestationsPerURL stops leasing a URL that attracts attestations that never agree.
const maxAttestationsPerURL = 12

// expireLeases returns lapsed leases to the pool, counting one attempt for each distinct pair.
func (s *Service) expireLeases(now time.Time) {
	for id, l := range s.leases {
		if now.Before(l.expiry) {
			continue
		}
		e := s.pool[l.url]
		s.dropLease(id)
		if e != nil {
			s.noteFailure(e, pair{l.worker, l.net}, false, now)
		}
	}
	for id, c := range s.consumed {
		if !now.Before(c.until) {
			delete(s.consumed, id)
		}
	}
}

// noteFailure counts a lapsed lease or a failure report against its pair, and parks the entry once
// six pairs failed it or three pairs reported it.
func (s *Service) noteFailure(e *entry, p pair, report bool, now time.Time) {
	e.failed[p] = struct{}{}
	if report {
		e.reported[p] = struct{}{}
	}
	if len(e.failed) >= s.cfg.MaxAttempts || len(e.reported) >= 3 {
		e.parkedUntil = now.Add(s.exclusionTTL())
		s.parked[e.url] = e.parkedUntil
		delete(s.pool, e.url)
	}
}

// ClaimResult is the answer to a claim.
type ClaimResult struct {
	Leases            []LeaseGrant
	RetryAfterS       int
	ModerationCeiling bool
}

// LeaseGrant is one leased URL.
type LeaseGrant struct {
	ID             string
	URL            string
	Source         Class
	ExpiresAt      time.Time
	ExtractVersion int
}

// Claim leases up to max URLs to the worker on the network.
func (s *Service) Claim(worker, net string, max int) (ClaimResult, error) {
	if !hexID32.MatchString(worker) {
		return ClaimResult{}, invalidField("worker", "pattern")
	}
	if !hexID32.MatchString(net) {
		return ClaimResult{}, invalidField("net", "pattern")
	}
	if max < 1 || max > 32 {
		return ClaimResult{}, invalidField("max", "range")
	}
	now := s.now()
	if retry, reached := s.ceiling(now); reached {
		return ClaimResult{ModerationCeiling: true, RetryAfterS: min(int(retry.Seconds()), 3600)}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLeases(now)
	room := min(max, s.cfg.MaxLeasesPerWorker-s.byWorker[worker], s.cfg.MaxLeasesTotal-len(s.leases))
	var out ClaimResult
	p := pair{worker, net}
	for len(out.Leases) < room {
		e := s.nextEntry(p, now)
		if e == nil {
			break
		}
		l := &lease{id: randomID(), url: e.url, worker: worker, net: net, class: e.class,
			granted: now, expiry: now.Add(time.Duration(s.cfg.LeaseTTLS) * time.Second)}
		s.leases[l.id] = l
		e.leases[l.id] = l
		e.workers[worker], e.nets[net] = now, now
		s.byWorker[worker]++
		s.hostNext[e.host] = now.Add(time.Duration(s.cfg.HostSpacingMS) * time.Millisecond)
		s.metrics.leases.add(string(e.class))
		out.Leases = append(out.Leases, LeaseGrant{ID: l.id, URL: e.url, Source: e.class, ExpiresAt: l.expiry.UTC(), ExtractVersion: extractVersion})
	}
	if len(out.Leases) == 0 {
		out.RetryAfterS = 30
	}
	return out, nil
}

// maxHostScan bounds how many entries of one host a claim looks through.
const maxHostScan = 64

// nextEntry takes the next leasable entry: classes in order, hosts round-robin within a class.
func (s *Service) nextEntry(p pair, now time.Time) *entry {
	for _, c := range classOrder {
		q := s.queues[c]
		for n := 0; n < len(q.ring); n++ {
			if q.cursor >= len(q.ring) {
				q.cursor = 0
			}
			host := q.ring[q.cursor]
			list := s.liveEntries(q, host)
			if len(list) == 0 {
				delete(q.byHost, host)
				q.ring = append(q.ring[:q.cursor], q.ring[q.cursor+1:]...)
				n--
				continue
			}
			q.cursor++
			if now.Before(s.hostNext[host]) {
				continue
			}
			for i, e := range list {
				if i >= maxHostScan {
					break
				}
				if s.eligible(e, p, now) {
					return e
				}
			}
		}
	}
	return nil
}

// liveEntries drops entries that left the pool from a host's queue.
func (s *Service) liveEntries(q *classQueue, host string) []*entry {
	list := q.byHost[host]
	live := list[:0]
	for _, e := range list {
		if s.pool[e.url] == e {
			live = append(live, e)
		}
	}
	q.byHost[host] = live
	return live
}
