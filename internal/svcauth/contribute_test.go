package svcauth

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	intakeSub      = "100000000000000000011"
	intakeEmail    = "intake-prod@cosift-test.iam.gserviceaccount.com"
	intakeStgSub   = "100000000000000000012"
	intakeStgEmail = "intake-staging@cosift-test.iam.gserviceaccount.com"
)

func contributeConfig() map[string]any {
	c := baseConfig()
	c["principals"] = append(principalsOf(c),
		map[string]any{"id": "intake-prod", "kind": "oidc", "sub": intakeSub, "email": intakeEmail, "env": "prod", "scopes": []string{"articles:read", "articles:contribute", "retrieve:read"}, "rpm": 300, "writes_per_hour": 120, "writes_burst": 20, "writes_per_day": 700},
		map[string]any{"id": "intake-staging", "kind": "oidc", "sub": intakeStgSub, "email": intakeStgEmail, "env": "staging", "scopes": []string{"articles:read", "articles:contribute"}, "rpm": 60, "writes_per_hour": 5, "writes_burst": 2, "writes_per_day": 5},
	)
	return c
}

func TestParseContributeScope(t *testing.T) {
	cfg, err := Parse(mustJSON(contributeConfig()), testChecks())
	if err != nil {
		t.Fatal(err)
	}
	var p PrincipalConfig
	for _, pc := range cfg.Principals {
		if pc.ID == "intake-prod" {
			p = pc
		}
	}
	if p.ID == "" || !p.Writer() || !slices.Contains(p.Scopes, v1.Scope("articles:contribute")) || p.WritesPerHour != 120 || p.WritesBurst != 20 || p.WritesPerDay != 700 {
		t.Fatalf("intake-prod %+v", p)
	}

	c := contributeConfig()
	delete(principalByID(c, "intake-prod"), "writes_per_hour")
	delete(principalByID(c, "intake-prod"), "writes_burst")
	delete(principalByID(c, "intake-prod"), "writes_per_day")
	cfg, err = Parse(mustJSON(c), testChecks())
	if err != nil {
		t.Fatal(err)
	}
	for _, pc := range cfg.Principals {
		if pc.ID == "intake-prod" && (pc.WritesPerHour != 30 || pc.WritesBurst != 10 || pc.WritesPerDay != 150) {
			t.Fatalf("default budget %+v", pc)
		}
	}
}

func TestParseContributeScopeRefused(t *testing.T) {
	cases := []struct {
		name string
		edit func(map[string]any)
		want string
	}{
		{"beside articles:write", func(c map[string]any) {
			principalByID(c, "intake-prod")["scopes"] = []string{"articles:read", "articles:contribute", "articles:write"}
		}, "articles:contribute may not be combined with articles:write or articles:stub"},
		{"beside articles:stub", func(c map[string]any) {
			principalByID(c, "intake-prod")["scopes"] = []string{"articles:stub", "articles:contribute"}
		}, "articles:contribute may not be combined with articles:write or articles:stub"},
		{"on a key principal", func(c map[string]any) {
			principalByID(c, "community-wiki")["scopes"] = []string{"articles:read", "articles:contribute"}
		}, "articles:contribute not allowed on a key principal"},
		{"writes_* without a writer scope", func(c map[string]any) {
			principalByID(c, "mcp-prod")["writes_per_day"] = 3
		}, "writes_*: only allowed with articles:write, articles:stub or articles:contribute"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := contributeConfig()
			tc.edit(c)
			_, err := Parse(mustJSON(c), testChecks())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestContributeIsBudgetedAndCountedAsStagingWriter(t *testing.T) {
	cfg := contributeConfig()
	cfg["golive_at"] = "2026-10-12T16:00:00Z"
	h := newHarness(t, cfg)
	pol := h.svc.Policy
	if got := strings.Join(pol.StagingWriters(), ","); got != "intake-staging,resolver-staging,synth-staging" {
		t.Fatalf("staging writers %q", got)
	}
	h.clock.t = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for i := range 2 {
		if _, ok := pol.ChargeWrite("intake-staging"); !ok {
			t.Fatalf("charge %d refused", i)
		}
	}
	if _, ok := pol.ChargeWrite("intake-staging"); ok {
		t.Fatal("burst of 2 not enforced for a contribute principal")
	}
	if pol.WritesExhausted("intake-prod") {
		t.Fatal("fresh daily allowance reported spent")
	}
	if _, ok := pol.ChargeWrite("intake-prod"); !ok {
		t.Fatal("intake-prod cannot charge a write")
	}
}

func TestIntakeProdBudgetExhaustsAtTheHourAndDayWhileReadsPass(t *testing.T) {
	h, _ := newFakeArticlesHarness(t, contributeConfig())
	pol := h.svc.Policy
	h.clock.t = time.Now().UTC().Truncate(24 * time.Hour).Add(48 * time.Hour)
	for i := range 20 {
		if _, ok := pol.ChargeWrite("intake-prod"); !ok {
			t.Fatalf("burst charge %d refused", i)
		}
	}
	retry, ok := pol.ChargeWrite("intake-prod")
	if ok || retry != 30*time.Second {
		t.Fatalf("hourly burst of 20: %v %s", ok, retry)
	}
	if pol.WritesExhausted("intake-prod") {
		t.Fatal("an hourly limit is not an exhausted day")
	}
	used := 20
	for ; used < 700; used++ {
		h.clock.Advance(30 * time.Second)
		if _, ok := pol.ChargeWrite("intake-prod"); !ok {
			t.Fatalf("charge %d refused before the daily 700", used)
		}
	}
	h.clock.Advance(30 * time.Second)
	if _, ok := pol.ChargeWrite("intake-prod"); ok {
		t.Fatal("701st write of the day accepted")
	}
	if !pol.WritesExhausted("intake-prod") {
		t.Fatal("daily allowance of 700 spent but not exhausted")
	}
	if !h.svc.certs.refresh(context.Background()) {
		t.Fatal("refresh failed")
	}
	intake := h.token(intakeSub, intakeEmail)
	if w := h.do("GET", "/v1/articles", "", bearerAuth(intake)); w.Code != http.StatusOK {
		t.Fatalf("read with the write budget spent: %d %s", w.Code, w.Body.String())
	}
}
