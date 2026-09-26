package svcauth

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

type principalState struct {
	cfg PrincipalConfig
	p   v1.Principal
}

type keyRef struct {
	p      *principalState
	digest [32]byte
}

// state is one active configuration, never modified after it is published.
type state struct {
	cfg            *Config
	byID           map[string]*principalState
	bySub          map[string]*principalState
	byKeyID        map[string]keyRef
	stagingWriters []string
}

func newState(cfg *Config) *state {
	st := &state{
		cfg:            cfg,
		byID:           map[string]*principalState{},
		bySub:          map[string]*principalState{},
		byKeyID:        map[string]keyRef{},
		stagingWriters: []string{},
	}
	for _, pc := range cfg.Principals {
		ps := &principalState{cfg: pc, p: pc.principal()}
		st.byID[pc.ID] = ps
		if pc.Kind == v1.KindOIDC {
			st.bySub[pc.Sub] = ps
		}
		for _, k := range pc.Keys {
			st.byKeyID[k.KeyID] = keyRef{p: ps, digest: k.Digest}
		}
		if pc.Env == v1.EnvStaging && pc.Writer() {
			st.stagingWriters = append(st.stagingWriters, pc.ID)
		}
	}
	slices.Sort(st.stagingWriters)
	return st
}

func (st *state) hasKeyPrincipals() bool { return len(st.byKeyID) > 0 }

// Policy is the active service-auth config and the per-principal limits. It
// implements v1.Policy; its methods never take the article store's lock.
type Policy struct {
	active  atomic.Pointer[state]
	now     func() time.Time
	metrics *Metrics

	lockMu sync.Mutex
	lock   sync.Locker

	reqMu sync.Mutex
	req   map[string]*bucket

	wrMu sync.Mutex
	wr   map[string]*writeBudget
}

var _ v1.Policy = (*Policy)(nil)

type writeBudget struct {
	hour bucket
	day  string
	used int
}

func NewPolicy(m *Metrics, now func() time.Time) *Policy {
	if now == nil {
		now = time.Now
	}
	if m == nil {
		m = NewMetrics()
	}
	return &Policy{now: now, metrics: m, req: map[string]*bucket{}, wr: map[string]*writeBudget{}}
}

// SetSwapLocker sets the lock held around every config swap: the article
// store's write lock, so no mutation commits across a reload.
func (p *Policy) SetSwapLocker(l sync.Locker) {
	p.lockMu.Lock()
	p.lock = l
	p.lockMu.Unlock()
}

// Install makes cfg the active config.
func (p *Policy) Install(cfg *Config) {
	st := newState(cfg)
	p.lockMu.Lock()
	l := p.lock
	p.lockMu.Unlock()
	if l != nil {
		l.Lock()
		defer l.Unlock()
	}
	p.active.Store(st)
	p.prune(st)
}

func (p *Policy) prune(st *state) {
	p.reqMu.Lock()
	for id := range p.req {
		if st.byID[id] == nil {
			delete(p.req, id)
		}
	}
	p.reqMu.Unlock()
	p.wrMu.Lock()
	for id := range p.wr {
		if ps := st.byID[id]; ps == nil || !ps.cfg.Writer() {
			delete(p.wr, id)
		}
	}
	p.wrMu.Unlock()
}

func (p *Policy) state() *state { return p.active.Load() }

func (p *Policy) Principal(id string) (v1.Principal, bool) {
	st := p.state()
	if st == nil {
		return v1.Principal{}, false
	}
	ps, ok := st.byID[id]
	if !ok {
		return v1.Principal{}, false
	}
	pr := ps.p
	pr.Scopes = slices.Clone(pr.Scopes)
	return pr, true
}

func (p *Policy) WritesFrozen() bool {
	st := p.state()
	return st == nil || st.cfg.WritesFrozen
}

func (p *Policy) GoliveAt() (time.Time, bool) {
	st := p.state()
	if st == nil || st.cfg.GoliveAt.IsZero() {
		return time.Time{}, false
	}
	return st.cfg.GoliveAt, true
}

func (p *Policy) StagingWriters() []string {
	st := p.state()
	if st == nil {
		return []string{}
	}
	return slices.Clone(st.stagingWriters)
}

// allowRequest draws one unit from the principal's request bucket.
func (p *Policy) allowRequest(ps *principalState) (time.Duration, bool) {
	rate := float64(ps.cfg.RPM) / 60
	now := p.now()
	p.reqMu.Lock()
	defer p.reqMu.Unlock()
	b := p.req[ps.cfg.ID]
	if b == nil {
		b = &bucket{}
		p.req[ps.cfg.ID] = b
	}
	b.refill(now, rate, float64(ps.cfg.Burst))
	if b.tokens < 1 {
		return b.wait(rate), false
	}
	b.tokens--
	return 0, true
}

func (p *Policy) ChargeWrite(id string) (time.Duration, bool) {
	st := p.state()
	if st == nil {
		return 0, false
	}
	ps := st.byID[id]
	if ps == nil || !ps.cfg.Writer() {
		return 0, false
	}
	c := ps.cfg
	rate := float64(c.WritesPerHour) / 3600
	now := p.now()
	p.wrMu.Lock()
	defer p.wrMu.Unlock()
	b := p.budget(id, now)
	b.hour.refill(now, rate, float64(c.WritesBurst))
	var retry time.Duration
	refused := false
	if b.used >= c.WritesPerDay {
		refused = true
		retry = nextUTCMidnight(now).Sub(now)
	}
	if b.hour.tokens < 1 {
		refused = true
		retry = max(retry, b.hour.wait(rate))
	}
	if refused {
		return retry, false
	}
	b.hour.tokens--
	b.used++
	p.metrics.write(id)
	return 0, true
}

func (p *Policy) WritesExhausted(id string) bool {
	st := p.state()
	if st == nil {
		return true
	}
	ps := st.byID[id]
	if ps == nil || !ps.cfg.Writer() {
		return true
	}
	p.wrMu.Lock()
	defer p.wrMu.Unlock()
	return p.budget(id, p.now()).used >= ps.cfg.WritesPerDay
}

// budget returns id's write budget with the daily count reset on a new UTC day.
func (p *Policy) budget(id string, now time.Time) *writeBudget {
	b := p.wr[id]
	if b == nil {
		b = &writeBudget{}
		p.wr[id] = b
	}
	if day := now.UTC().Format(time.DateOnly); b.day != day {
		b.day, b.used = day, 0
	}
	return b
}

func nextUTCMidnight(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC)
}

// bucket is a token bucket; the zero value starts full on first refill.
type bucket struct {
	tokens float64
	last   time.Time
	primed bool
}

func (b *bucket) refill(now time.Time, rate, capacity float64) {
	if !b.primed {
		b.tokens, b.last, b.primed = capacity, now, true
		return
	}
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens += el * rate
		b.last = now
	}
	b.tokens = min(b.tokens, capacity)
}

func (b *bucket) wait(rate float64) time.Duration {
	return time.Duration((1 - b.tokens) / rate * float64(time.Second))
}
