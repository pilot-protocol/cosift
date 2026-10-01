package v1

import (
	"slices"
	"sync"
	"time"
)

// Policy is the active service-auth config as the /v1 handlers read it. Its
// methods must not block: the article store calls them inside its write lock.
// An id missing from the active config cannot write.
type Policy interface {
	Principal(id string) (Principal, bool)
	WritesFrozen() bool
	// ChargeWrite draws one unit from the principal's write bucket and daily
	// allowance, once per write request, after decoding and before any embedding.
	ChargeWrite(id string) (retry time.Duration, ok bool)
	// WritesExhausted reports, without charging, whether today's allowance is spent.
	WritesExhausted(id string) bool
	// StagingWriters returns the sorted ids of staging principals holding
	// articles:write or articles:stub; empty, not nil, when there are none.
	StagingWriters() []string
	GoliveAt() (time.Time, bool)
}

// FakePolicy is a Policy for tests, safe for concurrent use. A principal's
// writes are unlimited until SetWriteBudget is called for it.
type FakePolicy struct {
	mu         sync.Mutex
	principals map[string]Principal
	frozen     bool
	golive     time.Time
	budgets    map[string]fakeBudget
	charges    map[string]int
}

type fakeBudget struct {
	left  int
	retry time.Duration
}

var _ Policy = (*FakePolicy)(nil)

func NewFakePolicy(ps ...Principal) *FakePolicy {
	f := &FakePolicy{}
	f.SetPrincipals(ps...)
	return f
}

// SetPrincipals replaces the active principals, as a reload does.
func (f *FakePolicy) SetPrincipals(ps ...Principal) {
	m := make(map[string]Principal, len(ps))
	for _, p := range ps {
		p.Scopes = slices.Clone(p.Scopes)
		m[p.ID] = p
	}
	f.mu.Lock()
	f.principals = m
	f.mu.Unlock()
}

func (f *FakePolicy) SetWritesFrozen(frozen bool) {
	f.mu.Lock()
	f.frozen = frozen
	f.mu.Unlock()
}

// SetGoliveAt sets golive_at; the zero time clears it.
func (f *FakePolicy) SetGoliveAt(t time.Time) {
	f.mu.Lock()
	f.golive = t
	f.mu.Unlock()
}

// SetWriteBudget allows id n more writes, after which ChargeWrite refuses with retry.
func (f *FakePolicy) SetWriteBudget(id string, n int, retry time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.budgets == nil {
		f.budgets = map[string]fakeBudget{}
	}
	f.budgets[id] = fakeBudget{left: n, retry: retry}
}

// Charges counts the ChargeWrite calls for id, refused ones included.
func (f *FakePolicy) Charges(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.charges[id]
}

func (f *FakePolicy) Principal(id string) (Principal, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.principals[id]
	p.Scopes = slices.Clone(p.Scopes)
	return p, ok
}

func (f *FakePolicy) WritesFrozen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frozen
}

func (f *FakePolicy) ChargeWrite(id string) (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.principals[id]; !ok {
		return 0, false
	}
	if f.charges == nil {
		f.charges = map[string]int{}
	}
	f.charges[id]++
	b, limited := f.budgets[id]
	if !limited {
		return 0, true
	}
	if b.left <= 0 {
		return b.retry, false
	}
	b.left--
	f.budgets[id] = b
	return 0, true
}

func (f *FakePolicy) WritesExhausted(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.principals[id]; !ok {
		return true
	}
	b, limited := f.budgets[id]
	return limited && b.left <= 0
}

func (f *FakePolicy) StagingWriters() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := []string{}
	for id, p := range f.principals {
		if p.Env == EnvStaging && p.HasAny(ScopeArticlesWrite, ScopeArticlesStub) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (f *FakePolicy) GoliveAt() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.golive, !f.golive.IsZero()
}
