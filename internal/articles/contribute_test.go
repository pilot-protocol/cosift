package articles

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func cid(n int) string { return fmt.Sprintf("%016x", n+1) }

func contribBody(title string, c string, kv ...any) map[string]any {
	return with(articleBody(title), append([]any{"contributor", c}, kv...)...)
}

func (h *harness) contribute(id string, body map[string]any, want int) map[string]any {
	h.t.Helper()
	return h.put(intakeProd, id, body, want)
}

func (h *harness) contribError(id string, body map[string]any, status int, code string) map[string]any {
	h.t.Helper()
	return h.expectError(intakeProd, http.MethodPut, "/v1/articles/"+id, body, status, code)
}

func wantInvalid(t *testing.T, out map[string]any, field, rule string) {
	t.Helper()
	if out["field"] != field || out["rule"] != rule {
		t.Errorf("field/rule = %v/%v, want %s/%s", out["field"], out["rule"], field, rule)
	}
}

func fullOf(h *harness, id string) map[string]any {
	h.t.Helper()
	return h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
}

func TestContributionShapeRules(t *testing.T) {
	const title = "Rust async runtimes"
	cases := []struct {
		name        string
		p           func() map[string]any
		wantStatus  int
		field, rule string
		asWriter    bool
		wantCode    string
	}{
		{"1 contributor absent", func() map[string]any { return articleBody(title) }, 422, "contributor", "required", false, "invalid_field"},
		{"1 contributor null", func() map[string]any { return with(articleBody(title), "contributor", json_null{}) }, 422, "contributor", "required", false, "invalid_field"},
		{"2 contributor pattern", func() map[string]any { return contribBody(title, "0F1E2D3C4B5A6978") }, 422, "contributor", "pattern", false, "invalid_field"},
		{"2 contributor too short", func() map[string]any { return contribBody(title, "0f1e2d3c") }, 422, "contributor", "pattern", false, "invalid_field"},
		{"2 contributor not a string", func() map[string]any { return with(articleBody(title), "contributor", 7) }, 422, "contributor", "pattern", false, "invalid_field"},
		{"3 write principal sends contributor", func() map[string]any { return contribBody(title, cid(1)) }, 422, "contributor", "field_not_allowed", true, "invalid_field"},
		{"3 write principal sends null contributor", func() map[string]any { return with(articleBody(title), "contributor", json_null{}) }, 422, "contributor", "field_not_allowed", true, "invalid_field"},
		{"4 held", func() map[string]any { return contribBody(title, cid(1), "status", "held") }, 422, "status", "must_be_published", false, "invalid_field"},
		{"4 status absent", func() map[string]any { return contribBody(title, cid(1), "status", nil) }, 422, "status", "must_be_published", false, "invalid_field"},
		{"5 sensitive", func() map[string]any {
			return contribBody(title, cid(1), "sensitivity", map[string]any{"class": "medical_advice", "reason": "x"})
		}, 422, "sensitivity.class", "must_be_none", false, "invalid_field"},
		{"5 sensitivity absent", func() map[string]any { return contribBody(title, cid(1), "sensitivity", nil) }, 422, "sensitivity.class", "must_be_none", false, "invalid_field"},
		{"6 topic_ids", func() map[string]any { return contribBody(title, cid(1), "topic_ids", []string{topic(1)}) }, 422, "topic_ids", "field_not_allowed", false, "invalid_field"},
		{"6 cluster_ids", func() map[string]any { return contribBody(title, cid(1), "cluster_ids", []string{"c013960"}) }, 422, "cluster_ids", "field_not_allowed", false, "invalid_field"},
	}
	for _, tc := range cases {
		for _, existing := range []bool{false, true} {
			name := tc.name + " on an absent id"
			if existing {
				name = tc.name + " on a record"
			}
			t.Run(name, func(t *testing.T) {
				h := newHarness(t)
				id := ulid(1)
				if existing {
					h.put(synthProd, id, articleBody(title), http.StatusCreated)
				}
				p := intakeProd
				if tc.asWriter {
					p = synthProd
				}
				body := tc.p()
				if existing {
					body["expected_version"] = 1
				}
				out := h.expectError(p, http.MethodPut, "/v1/articles/"+id, body, tc.wantStatus, tc.wantCode)
				wantInvalid(t, out, tc.field, tc.rule)
				if !existing {
					if _, ok := h.raw(recordKey(id)); ok {
						t.Error("refused contribution created a record")
					}
				}
			})
		}
	}
}

type json_null struct{}

func (json_null) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func TestContributionRule7ExpectedVersion(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	changed := contribBody("Rust async runtimes", cid(2), "lead", "A revised lead. It differs.")
	wantInvalid(t, h.contribError(id, changed, 422, "invalid_field"), "expected_version", "required")
	if v := fullOf(h, id)["version"]; v != 1.0 {
		t.Errorf("refused contribution wrote version %v", v)
	}
	h.contribute(id, with(changed, "expected_version", 1), http.StatusOK)

	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	wantInvalid(t, h.contribError(ulid(2), contribBody("Go memory model", cid(1)), 422, "invalid_field"), "expected_version", "required")
	if out := h.contribute(ulid(2), with(contribBody("Go memory model", cid(1)), "expected_version", 0), http.StatusOK); out["result"] != "filled" {
		t.Errorf("stub fill = %v", out)
	}
}

func TestContributionCheckOrder(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1)), http.StatusCreated)

	wantInvalid(t, h.contribError(id, contribBody("Rust async runtimes", cid(2), "status", "held", "lead", "Another lead. It differs."), 422, "invalid_field"), "status", "must_be_published")

	same := contribBody("Rust async runtimes", cid(2))
	if out := h.contribute(id, same, http.StatusOK); out["result"] != "unchanged" {
		t.Errorf("same-hash body without expected_version = %v", out)
	}
	if got := fullOf(h, id); !slices.Equal(toStrings(got["contributors"]), []string{cid(1)}) || got["version_contributor"] != cid(1) {
		t.Errorf("unchanged contribution changed attribution: %v %v", got["contributors"], got["version_contributor"])
	}

	h.moderate(dashProd, id, "hold", "quality", http.StatusOK)
	broken := contribBody("Rust async runtimes", cid(2), "status", "held", "sensitivity", map[string]any{"class": "allegation", "reason": ""}, "topic_ids", []string{topic(1)}, "lead", "Another lead. It differs.")
	if out := h.contribError(id, broken, 409, "moderation_locked"); out["current_status"] != "held" {
		t.Errorf("held record = %v", out)
	}
	if out := h.contribError(id, same, 409, "moderation_locked"); out["current_status"] != "held" {
		t.Errorf("held record, same hash = %v", out)
	}
	h.moderate(dashProd, id, "approve", "", http.StatusOK)

	stale := contribBody("Rust async runtimes", cid(2), "quality_tier", "thin", "lead", "Another lead. It differs.", "expected_version", 5)
	if out := h.contribError(id, stale, 409, "version_conflict"); out["current_version"] != 1.0 {
		t.Errorf("stale tier-regressing body = %v", out)
	}
	h.contribError(id, with(stale, "expected_version", 1), 409, "tier_regression")
}

func TestContributionStampsAttribution(t *testing.T) {
	h := newHarness(t)
	a, b := cid(1), cid(2)

	created := ulid(1)
	h.contribute(created, contribBody("Rust async runtimes", a), http.StatusCreated)
	got := fullOf(h, created)
	if got["origin"] != "community" || !slices.Equal(toStrings(got["contributors"]), []string{a}) || got["version_contributor"] != a {
		t.Errorf("create: %v %v %v", got["origin"], got["contributors"], got["version_contributor"])
	}
	pub := h.call(wiki, http.MethodGet, "/v1/articles/"+created, nil, http.StatusOK)
	if pub["origin"] != "community" || !slices.Equal(toStrings(pub["contributors"]), []string{a}) || hasKey(pub, "version_contributor") {
		t.Errorf("public: %v", pub)
	}

	h.contribute(created, contribBody("Rust async runtimes", a, "lead", "Second lead. By the same cid.", "expected_version", 1), http.StatusOK)
	h.contribute(created, contribBody("Rust async runtimes", b, "lead", "Third lead. By another cid.", "expected_version", 2), http.StatusOK)
	h.contribute(created, contribBody("Rust async runtimes", a, "lead", "Fourth lead. By the first again.", "expected_version", 3), http.StatusOK)
	got = fullOf(h, created)
	if !slices.Equal(toStrings(got["contributors"]), []string{a, b}) || got["version_contributor"] != a || got["version"] != 4.0 {
		t.Errorf("update: %v %v v%v", got["contributors"], got["version_contributor"], got["version"])
	}
	for v, want := range map[int]string{1: a, 2: a, 3: b} {
		pv := h.call(dashProd, http.MethodGet, fmt.Sprintf("/v1/article-versions/%s/%d", created, v), nil, http.StatusOK)
		if pv["version_contributor"] != want {
			t.Errorf("version %d contributor = %v, want %s", v, pv["version_contributor"], want)
		}
	}

	filled := ulid(2)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+filled, stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.contribute(filled, with(contribBody("Go memory model", b), "expected_version", 0), http.StatusOK)
	got = fullOf(h, filled)
	if got["origin"] != "community" || !slices.Equal(toStrings(got["contributors"]), []string{b}) || got["version_contributor"] != b {
		t.Errorf("fill: %v %v %v", got["origin"], got["contributors"], got["version_contributor"])
	}
}

func TestContributionOnSynthRecordTurnsItCommunity(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	if got := fullOf(h, id); got["origin"] != "synth" {
		t.Fatalf("origin = %v", got["origin"])
	}
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", "A contributed lead. It differs.", "expected_version", 1), http.StatusOK)
	if got := fullOf(h, id); got["origin"] != "community" {
		t.Errorf("origin after a contribution = %v", got["origin"])
	}
}

func TestSynthPutKeepsAttribution(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", "A contributed lead. It differs.", "expected_version", 1), http.StatusOK)

	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A synth lead. It rebuilds the article.", "expected_version", 2), http.StatusOK)
	got := fullOf(h, id)
	if got["origin"] != "community" || !slices.Equal(toStrings(got["contributors"]), []string{cid(1), cid(2)}) {
		t.Errorf("synth PUT changed origin/contributors: %v %v", got["origin"], got["contributors"])
	}
	if v, ok := got["version_contributor"]; !ok || v != nil {
		t.Errorf("version_contributor = %v (present %v), want null", v, ok)
	}
	if pv := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id+"/2", nil, http.StatusOK); pv["version_contributor"] != cid(2) {
		t.Errorf("prior version contributor = %v", pv["version_contributor"])
	}
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A later synth lead. It rebuilds again."), http.StatusOK)
	if got := fullOf(h, id); got["origin"] != "community" {
		t.Errorf("origin went back to %v", got["origin"])
	}
}

func TestRetriedContributionIsUnchanged(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	body := contribBody("Rust async runtimes", cid(1))
	if out := h.contribute(id, body, http.StatusCreated); out["result"] != "created" {
		t.Fatalf("create = %v", out)
	}
	if out := h.contribute(id, body, http.StatusOK); out["result"] != "unchanged" || article(out)["version"] != 1.0 {
		t.Errorf("retried create = %v", out)
	}
	got := fullOf(h, id)
	if !slices.Equal(toStrings(got["contributors"]), []string{cid(1)}) || got["version_contributor"] != cid(1) {
		t.Errorf("attribution after retry: %v %v", got["contributors"], got["version_contributor"])
	}

	update := contribBody("Rust async runtimes", cid(2), "lead", "An update lead. It differs.", "expected_version", 1)
	h.contribute(id, update, http.StatusOK)
	if out := h.contribute(id, update, http.StatusOK); out["result"] != "unchanged" || article(out)["version"] != 2.0 {
		t.Errorf("retried update = %v", out)
	}
	if got := fullOf(h, id); !slices.Equal(toStrings(got["contributors"]), []string{cid(1), cid(2)}) || got["version_contributor"] != cid(2) {
		t.Errorf("attribution after retried update: %v %v", got["contributors"], got["version_contributor"])
	}
}

func fillContributors(h *harness, id string, n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		body := contribBody("Rust async runtimes", cid(i), "lead", fmt.Sprintf("Lead number %d. It differs each time.", i))
		if i == 0 {
			h.contribute(id, body, http.StatusCreated)
			continue
		}
		h.contribute(id, with(body, "expected_version", i), http.StatusOK)
	}
}

func TestContributorCap(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	fillContributors(h, id, 63)
	if got := fullOf(h, id); len(got["contributors"].([]any)) != 63 || got["version"] != 63.0 {
		t.Fatalf("setup: %d contributors, v%v", len(got["contributors"].([]any)), got["version"])
	}
	h.contribute(id, contribBody("Rust async runtimes", cid(63), "lead", "The sixty-fourth lead. It is the last place.", "expected_version", 63), http.StatusOK)
	got := fullOf(h, id)
	if cs := toStrings(got["contributors"]); len(cs) != 64 || cs[63] != cid(63) {
		t.Fatalf("64th contributor not appended: %d", len(cs))
	}

	refused := contribBody("Rust async runtimes", cid(64), "lead", "The sixty-fifth lead. It has no place.", "expected_version", 64)
	h.contribError(id, refused, 409, "contributors_full")
	after := fullOf(h, id)
	if after["version"] != 64.0 || len(after["contributors"].([]any)) != 64 || after["version_contributor"] != cid(63) || after["lead"] != got["lead"] {
		t.Errorf("refused contribution wrote: v%v %d %v", after["version"], len(after["contributors"].([]any)), after["version_contributor"])
	}
	if _, ok := h.raw(versionKey(id, 64)); ok {
		t.Error("refused contribution wrote a version")
	}

	h.contribute(id, contribBody("Rust async runtimes", cid(5), "lead", "A known cid writes again. The list is unchanged.", "expected_version", 64), http.StatusOK)
	after = fullOf(h, id)
	if len(after["contributors"].([]any)) != 64 || after["version"] != 65.0 || after["version_contributor"] != cid(5) {
		t.Errorf("known cid on a full list: %d v%v %v", len(after["contributors"].([]any)), after["version"], after["version_contributor"])
	}

	same := contribBody("Rust async runtimes", cid(70), "lead", "A known cid writes again. The list is unchanged.")
	if out := h.contribute(id, same, http.StatusOK); out["result"] != "unchanged" {
		t.Errorf("same-hash body from a new cid on a full record = %v", out)
	}
	if len(fullOf(h, id)["contributors"].([]any)) != 64 {
		t.Error("unchanged contribution took a place")
	}
}

func TestContributorCapRace(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	fillContributors(h, id, 63)
	first := contribBody("Rust async runtimes", cid(100), "lead", "First racer lead. It takes the place.", "expected_version", 63)
	second := contribBody("Rust async runtimes", cid(101), "lead", "Second racer lead. It finds none.", "expected_version", 63)
	h.contribute(id, first, http.StatusOK)
	h.contribError(id, second, 409, "version_conflict")
	h.contribError(id, with(second, "expected_version", 64), 409, "contributors_full")
	if cs := toStrings(fullOf(h, id)["contributors"]); len(cs) != 64 || slices.Contains(cs, cid(101)) {
		t.Errorf("contributors = %d", len(cs))
	}
}

func TestContributionTierRegression(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "quality_tier", "ok"), http.StatusCreated)
	h.contribError(id, contribBody("Rust async runtimes", cid(2), "quality_tier", "thin", "lead", "A thinner lead. It differs.", "expected_version", 1), 409, "tier_regression")
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "quality_tier", "ok", "lead", "An equal tier lead. It differs.", "expected_version", 1), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "quality_tier", "strong", "lead", "A higher tier lead. It differs.", "expected_version", 2), http.StatusOK)
	if got := fullOf(h, id); got["quality_tier"] != "strong" || got["version"] != 3.0 {
		t.Errorf("tier = %v v%v", got["quality_tier"], got["version"])
	}
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.put(synthProd, ulid(2), with(articleBody("Go memory model"), "quality_tier", "thin", "lead", "A synth rebuild. It lowers the tier."), http.StatusOK)
}

func TestContributionLockedRecords(t *testing.T) {
	h := newHarness(t)
	for i, action := range []string{"reject", "tombstone"} {
		id := ulid(i + 1)
		title := []string{"Rust async runtimes", "Go memory model"}[i]
		h.contribute(id, contribBody(title, cid(1)), http.StatusCreated)
		h.moderate(dashProd, id, action, "quality", http.StatusOK)
		h.contribError(id, with(contribBody(title, cid(1), "lead", "A new lead. It differs."), "expected_version", 1), 409, "moderation_locked")
	}
}

func TestContributionBudgetAndWriteGates(t *testing.T) {
	h := newHarness(t)
	h.contribute(ulid(1), contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	if n := h.pol.Charges("intake-prod"); n != 1 {
		t.Errorf("charges = %d", n)
	}
	h.contribError(ulid(2), contribBody("Go memory model", "bad"), 422, "invalid_field")
	if n := h.pol.Charges("intake-prod"); n != 2 {
		t.Errorf("refused contribution charges = %d, want 2", n)
	}
	h.pol.SetWriteBudget("intake-prod", 0, 90e9)
	h.contribError(ulid(3), contribBody("Kafka consumer groups", cid(1)), 429, "write_budget")
	h.pol.SetWritesFrozen(true)
	h.contribError(ulid(3), contribBody("Kafka consumer groups", cid(1)), 503, "writes_frozen")
}

func TestContributionAfterGolive(t *testing.T) {
	h := newHarness(t)
	h.pol.SetPrincipals(append([]v1.Principal{intakeStaging}, allPrincipals...)...)
	h.put(intakeStaging, ulid(1), contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 1}, http.StatusOK)
	h.pol.SetPrincipals(append([]v1.Principal{intakeStaging}, allPrincipals...)...)
	h.expectError(intakeStaging, http.MethodPut, "/v1/articles/"+ulid(2), contribBody("Go memory model", cid(1)), http.StatusForbidden, "env_golive")
}

func TestContributionEnvMismatch(t *testing.T) {
	h := newHarness(t)
	h.put(synthStaging, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.contribError(ulid(1), with(contribBody("Rust async runtimes", cid(1), "lead", "A new lead. It differs."), "expected_version", 1), 403, "env_mismatch")
}

func TestBuildMatchAcceptsContribute(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{"q": "Rust async runtimes", "purpose": "build", "topic_ids": []string{topic(1)}}
	if out := h.match(intakeProd, body, http.StatusOK); out["verdict"] != "none" || out["writes"] != "open" {
		t.Errorf("build match = %v", out)
	}
	h.expectError(mcpProd, http.MethodPost, "/v1/articles/match", body, http.StatusForbidden, "missing_scope")
}

func TestContributionRestorePending(t *testing.T) {
	h := newHarness(t)
	h.pol.SetGoliveAt(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC))
	h.contribError(ulid(1), contribBody("Rust async runtimes", cid(1)), 503, "restore_pending")
}

func TestContributionRestorePendingRecheckedInLock(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan map[string]any)
	go func() { done <- inFlight(h, intakeProd, ulid(1), contribBody("Rust async runtimes", cid(1))) }()
	waitFor(t, func() bool { return h.emb.embedded("Rust async runtimes") })
	h.pol.SetGoliveAt(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC))
	close(block)
	if out := <-done; out["code"] != "restore_pending" {
		t.Errorf("contribution released after golive_at appeared = %v", out)
	}
	if _, ok := h.raw(recordKey(ulid(1))); ok {
		t.Error("the contribution landed")
	}
}

func TestContributionAcceptsEmptyTopicAndClusterLists(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	empty := []string{}
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "topic_ids", empty, "cluster_ids", empty), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "topic_ids", empty, "cluster_ids", empty, "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
}

func TestContributionRefusalsKeepTheirOrderOnAFullRecord(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	fillContributors(h, id, 64)
	fresh := cid(200)
	tier := contribBody("Rust async runtimes", fresh, "quality_tier", "thin", "lead", leadOf("thin"), "expected_version", 64)
	h.contribError(id, tier, 409, "tier_regression")
	invalid := contribBody("Rust async runtimes", fresh, "lead", "", "expected_version", 64)
	h.contribError(id, invalid, 422, "invalid_field")
	h.contribError(id, contribBody("Rust async runtimes", fresh, "lead", leadOf("full"), "expected_version", 64), 409, "contributors_full")
}
