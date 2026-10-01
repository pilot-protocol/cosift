package v1

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	synthStaging    = Principal{ID: "synth-staging", Kind: KindOIDC, Env: EnvStaging, Scopes: []Scope{ScopeArticlesRead, ScopeArticlesWrite, ScopeRetrieveRead}}
	resolverStaging = Principal{ID: "resolver-staging", Kind: KindOIDC, Env: EnvStaging, Scopes: []Scope{ScopeArticlesRead, ScopeArticlesStub, ScopeRetrieveRead}}
	mcpStaging      = Principal{ID: "mcp-staging", Kind: KindOIDC, Env: EnvStaging, Scopes: []Scope{ScopeArticlesRead}}
	dashStaging     = Principal{ID: "dash-staging", Kind: KindKey, Env: EnvStaging, Scopes: []Scope{ScopeArticlesReadAll, ScopeArticlesModerate}}
	synthProd       = Principal{ID: "synth-prod", Kind: KindOIDC, Env: EnvProd, Scopes: []Scope{ScopeArticlesRead, ScopeArticlesWrite, ScopeRetrieveRead}}
	resolverProd    = Principal{ID: "resolver-prod", Kind: KindOIDC, Env: EnvProd, Scopes: []Scope{ScopeArticlesRead, ScopeArticlesStub, ScopeRetrieveRead}}
)

func TestFakePolicyPrincipals(t *testing.T) {
	f := NewFakePolicy(synthProd, mcpStaging)
	got, ok := f.Principal("synth-prod")
	if !ok || got.ID != "synth-prod" || !got.Has(ScopeArticlesWrite) || got.Env != EnvProd {
		t.Fatalf("Principal = %+v, %v", got, ok)
	}
	got.Scopes[0] = ScopeArticlesAdmin
	if again, _ := f.Principal("synth-prod"); again.Has(ScopeArticlesAdmin) {
		t.Error("caller's change reached the active config")
	}
	if _, ok := f.Principal("nobody"); ok {
		t.Error("unknown id found")
	}

	f.SetPrincipals(synthStaging)
	if _, ok := f.Principal("synth-prod"); ok {
		t.Error("principal kept after a reload removed it")
	}
	if p, ok := f.Principal("synth-staging"); !ok || p.Env != EnvStaging {
		t.Errorf("reloaded principal = %+v, %v", p, ok)
	}
}

func TestFakePolicyFlags(t *testing.T) {
	f := NewFakePolicy()
	if f.WritesFrozen() {
		t.Error("frozen by default")
	}
	f.SetWritesFrozen(true)
	if !f.WritesFrozen() {
		t.Error("not frozen")
	}
	if _, ok := f.GoliveAt(); ok {
		t.Error("golive_at set by default")
	}
	at := time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC)
	f.SetGoliveAt(at)
	if got, ok := f.GoliveAt(); !ok || !got.Equal(at) {
		t.Errorf("GoliveAt = %v, %v", got, ok)
	}
	f.SetGoliveAt(time.Time{})
	if _, ok := f.GoliveAt(); ok {
		t.Error("golive_at not cleared")
	}
}

func TestFakePolicyWriteBudget(t *testing.T) {
	f := NewFakePolicy(synthProd, resolverProd)
	for range 3 {
		if _, ok := f.ChargeWrite("resolver-prod"); !ok {
			t.Fatal("unlimited principal refused")
		}
	}
	if f.WritesExhausted("resolver-prod") {
		t.Error("unlimited principal exhausted")
	}

	f.SetWriteBudget("synth-prod", 2, 42*time.Minute)
	steps := []struct {
		ok        bool
		retry     time.Duration
		exhausted bool
	}{
		{true, 0, false},
		{true, 0, true},
		{false, 42 * time.Minute, true},
		{false, 42 * time.Minute, true},
	}
	for i, s := range steps {
		retry, ok := f.ChargeWrite("synth-prod")
		if ok != s.ok || retry != s.retry {
			t.Errorf("charge %d = %v, %v; want %v, %v", i, retry, ok, s.retry, s.ok)
		}
		if got := f.WritesExhausted("synth-prod"); got != s.exhausted {
			t.Errorf("after charge %d: WritesExhausted = %v", i, got)
		}
	}
	if got := f.Charges("synth-prod"); got != 4 {
		t.Errorf("Charges = %d, want 4 (refused ones included)", got)
	}

	f.SetWriteBudget("resolver-prod", 1, time.Hour)
	for range 5 {
		f.WritesExhausted("resolver-prod")
	}
	if _, ok := f.ChargeWrite("resolver-prod"); !ok {
		t.Error("WritesExhausted spent the budget")
	}
	if got := f.Charges("resolver-prod"); got != 4 {
		t.Errorf("Charges = %d, want 4", got)
	}

	if _, ok := f.ChargeWrite("nobody"); ok {
		t.Error("unknown id charged")
	}
	if !f.WritesExhausted("nobody") {
		t.Error("unknown id may write")
	}
	if got := f.Charges("nobody"); got != 0 {
		t.Errorf("unknown id Charges = %d", got)
	}
}

func TestFakePolicyStagingWriters(t *testing.T) {
	f := NewFakePolicy(synthProd, resolverProd, mcpStaging, dashStaging)
	if got := f.StagingWriters(); got == nil || len(got) != 0 {
		t.Errorf("StagingWriters = %#v, want empty non-nil", got)
	}
	zeta := Principal{ID: "zeta-staging", Env: EnvStaging, Scopes: []Scope{ScopeArticlesStub}}
	alpha := Principal{ID: "alpha-staging", Env: EnvStaging, Scopes: []Scope{ScopeArticlesWrite}}
	f.SetPrincipals(zeta, synthProd, synthStaging, mcpStaging, resolverStaging, dashStaging, alpha, resolverProd)
	want := []string{"alpha-staging", "resolver-staging", "synth-staging", "zeta-staging"}
	if got := f.StagingWriters(); !slices.Equal(got, want) {
		t.Errorf("StagingWriters = %v, want %v", got, want)
	}
}

func TestFakePolicyConcurrent(t *testing.T) {
	f := NewFakePolicy(synthProd, synthStaging)
	f.SetWriteBudget("synth-prod", 50, time.Second)
	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 25 {
				if _, ok := f.ChargeWrite("synth-prod"); ok {
					granted.Add(1)
				}
				f.WritesExhausted("synth-prod")
				f.Principal("synth-staging")
				f.StagingWriters()
				f.WritesFrozen()
				f.GoliveAt()
			}
		})
	}
	wg.Go(func() {
		for i := range 25 {
			f.SetPrincipals(synthProd, synthStaging)
			f.SetWritesFrozen(i%2 == 0)
			f.SetGoliveAt(time.Unix(int64(i), 0))
		}
	})
	wg.Wait()
	if got := granted.Load(); got != 50 {
		t.Errorf("granted %d writes from a budget of 50", got)
	}
	if got := f.Charges("synth-prod"); got != 200 {
		t.Errorf("Charges = %d, want 200", got)
	}
}
