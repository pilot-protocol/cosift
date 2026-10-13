package articles

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"
)

func TestRevertRefusedByStatus(t *testing.T) {
	h := newHarness(t)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(9), stubBodyFor("Stub title here", topic(9)), http.StatusCreated)
	if out := h.revertError(ulid(9), revertBodyFor(1), 409, "illegal_transition"); out["current_status"] != "pending" {
		t.Errorf("pending = %v", out)
	}
	for i, action := range []string{"hold", "reject", "tombstone"} {
		id := ulid(i + 1)
		title := []string{"Rust async runtimes", "Go memory model", "Kafka consumer groups"}[i]
		h.put(synthProd, id, articleBody(title), http.StatusCreated)
		h.put(synthProd, id, with(articleBody(title), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
		h.moderate(dashProd, id, action, "quality", http.StatusOK)
		want := map[string]string{"hold": "held", "reject": "rejected", "tombstone": "tombstoned"}[action]
		if out := h.revertError(id, revertBodyFor(1), 409, "illegal_transition"); out["current_status"] != want {
			t.Errorf("%s: %v", action, out)
		}
	}
	h.put(synthStaging, ulid(5), articleBody("Staging rejected title"), http.StatusCreated)
	h.put(synthStaging, ulid(5), with(articleBody("Staging rejected title"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.call(dashStaging, http.MethodPost, "/v1/articles/"+ulid(5)+"/status", map[string]any{"action": "reject", "reason_code": "quality", "by": "op"}, http.StatusOK)
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 1}, http.StatusOK)
	if out := h.revertError(ulid(5), revertBodyFor(1), 409, "illegal_transition"); out["current_status"] != "rejected" {
		t.Errorf("converted residue = %v", out)
	}
	h.revertError(ulid(404), revertBodyFor(1), 404, "not_found")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/not-an-id/revert", revertBodyFor(1), 404, "not_found")
}

func TestRevertTargetRules(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held", "lead", leadOf("held build")), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held", "lead", leadOf("held rebuild"), "expected_version", 1), http.StatusOK)
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("community"), "expected_version", 2), http.StatusOK)
	before := fullOf(h, id)
	wantInvalid(t, h.revertError(id, revertBodyFor(1), 422, "invalid_field"), "to_version", "must_be_published")
	wantInvalid(t, h.revertError(id, revertBodyFor(3), 422, "invalid_field"), "to_version", "range")
	wantInvalid(t, h.revertError(id, revertBodyFor(9), 422, "invalid_field"), "to_version", "range")
	if after := fullOf(h, id); after["version"] != before["version"] || after["lead"] != before["lead"] {
		t.Error("a refused revert wrote")
	}
	if out := h.revert(id, revertBodyFor(2), http.StatusOK); out["result"] != "reverted" {
		t.Errorf("the approved build was refused: %v", out)
	}

	sid := ulid(2)
	sensitive := map[string]any{"class": "medical_advice", "reason": "dosage"}
	h.put(synthProd, sid, with(articleBody("Go memory model"), "status", "held", "sensitivity", sensitive), http.StatusCreated)
	h.moderate(dashProd, sid, "approve", "", http.StatusOK)
	h.put(synthProd, sid, with(articleBody("Go memory model"), "lead", leadOf("clean rebuild"), "expected_version", 1), http.StatusOK)
	wantInvalid(t, h.revertError(sid, revertBodyFor(1), 422, "invalid_field"), "to_version", "must_be_none")
	if fullOf(h, sid)["version"] != 2.0 {
		t.Error("a refused revert wrote")
	}
}

func TestRevertPrunedTargetIsNotFound(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	cur := synthRevise(h, id, 8, 1)
	h.revertError(id, revertBodyFor(1), 404, "not_found")
	if out := h.revert(id, revertBodyFor(cur-5), http.StatusOK); out["result"] != "reverted" {
		t.Errorf("oldest kept version = %v", out)
	}
}

func TestRevertBodyValidation(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	cases := []struct {
		name        string
		body        map[string]any
		status      int
		code        string
		field, rule string
	}{
		{"to_version missing", with(revertBodyFor(2), "to_version", nil), 422, "invalid_field", "to_version", "required"},
		{"to_version zero", revertBodyFor(0), 422, "invalid_field", "to_version", "range"},
		{"to_version string", revertBodyFor(2, "to_version", "2"), 400, "invalid_body", "", ""},
		{"to_version fraction", revertBodyFor(2, "to_version", 1.5), 400, "invalid_body", "", ""},
		{"reason_code missing", with(revertBodyFor(2), "reason_code", nil), 422, "invalid_field", "reason_code", "required"},
		{"reason_code unknown", revertBodyFor(2, "reason_code", "boredom"), 422, "invalid_field", "reason_code", "enum"},
		{"by missing", with(revertBodyFor(2), "by", nil), 422, "invalid_field", "by", "length"},
		{"by too long", revertBodyFor(2, "by", string(long)), 422, "invalid_field", "by", "length"},
		{"request_id empty", revertBodyFor(2, "request_id", ""), 422, "invalid_field", "request_id", "length"},
		{"request_id too long", revertBodyFor(2, "request_id", string(long)), 422, "invalid_field", "request_id", "length"},
		{"request_id characters", revertBodyFor(2, "request_id", "a b"), 422, "invalid_field", "request_id", "pattern"},
		{"unknown field", revertBodyFor(2, "action", "revert"), 400, "unknown_field", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := h.revertError(id, tc.body, tc.status, tc.code)
			if tc.field != "" {
				wantInvalid(t, out, tc.field, tc.rule)
			}
		})
	}
	if fullOf(h, id)["version"] != 4.0 {
		t.Error("a refused revert wrote")
	}
	h.expectError(synthProd, http.MethodPost, "/v1/articles/"+id+"/revert", revertBodyFor(2), http.StatusForbidden, "missing_scope")
	if rec := h.do(dashProd, http.MethodPost, "/v1/articles/"+id+"/revert", "not json"); rec.Code != http.StatusBadRequest {
		t.Errorf("not JSON: %d", rec.Code)
	}
}

func TestRevertKeepsAliasesClaimsAndIgnoresCorpus(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	body := with(articleBody("Rust async runtimes"), "aliases", []string{"Async executors in Rust"}, "topic_ids", []string{topic(1)})
	h.put(synthProd, id, body, http.StatusCreated)
	h.put(synthProd, id, with(body, "lead", leadOf("two"), "aliases", []string{"Tokio and friends"}, "topic_ids", []string{topic(2)}, "expected_version", 1), http.StatusOK)
	delete(h.corpus.urls, "https://example.org/doc")
	out := h.revert(id, revertBodyFor(1), http.StatusOK)
	if out["result"] != "reverted" {
		t.Fatalf("revert = %v", out)
	}
	got := fullOf(h, id)
	if !slices.Equal(toStrings(got["aliases"]), []string{"Async executors in Rust", "Tokio and friends"}) || !slices.Equal(toStrings(got["topic_ids"]), []string{topic(1), topic(2)}) {
		t.Errorf("aliases %v topics %v", got["aliases"], got["topic_ids"])
	}
	for _, tp := range []string{topic(1), topic(2)} {
		if owner, ok := h.raw(topicKey(tp)); !ok || string(owner) != id {
			t.Errorf("claim %s lost", tp)
		}
	}
	if got["title"] != "Rust async runtimes" || got["slug"] != "rust-async-runtimes" {
		t.Errorf("title/slug = %v %v", got["title"], got["slug"])
	}
}

func TestRevertReembedsTheLeadAndKeepsTitleVectors(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set(leadOf("one"), axis(2))
	h.emb.set(leadOf("two"), axis(3))
	h.emb.set("q lead one", axis(2))
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	titleCalls, leadCalls := h.emb.count("Rust async runtimes"), h.emb.count(leadOf("one"))
	if out := h.match(mcpProd, map[string]any{"q": "q lead one"}, http.StatusOK); out["verdict"] == "covered" && out["match"] == "embedding" && out["score"].(float64) > 0.99 {
		t.Fatalf("setup: the old lead already matches: %v", out)
	}
	h.revert(id, revertBodyFor(1), http.StatusOK)
	if h.emb.count("Rust async runtimes") != titleCalls {
		t.Error("the title was embedded again")
	}
	if h.emb.count(leadOf("one")) != leadCalls+1 {
		t.Errorf("the restored lead was embedded %d times", h.emb.count(leadOf("one"))-leadCalls)
	}
	if out := h.match(mcpProd, map[string]any{"q": "q lead one"}, http.StatusOK); out["verdict"] != "covered" || out["score"].(float64) < 0.99 {
		t.Errorf("match after the revert = %v", out)
	}
	h.restart()
	if out := h.match(mcpProd, map[string]any{"q": "q lead one"}, http.StatusOK); out["verdict"] != "covered" {
		t.Errorf("match after a restart = %v", out)
	}
}

func TestRevertEmbedderFailureWritesNothing(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	before := fullOf(h, id)
	h.emb.setFailIf(func([]string) bool { return true })
	out := h.revertError(id, revertBodyFor(2), 503, "embedder_unavailable")
	_ = out
	after := fullOf(h, id)
	if after["version"] != before["version"] || after["lead"] != before["lead"] {
		t.Errorf("a failed revert wrote: v%v", after["version"])
	}
	if _, ok := h.raw(versionKey(id, 4)); ok {
		t.Error("a failed revert wrote a version row")
	}
	h.emb.setFailIf(nil)
	h.revert(id, revertBodyFor(2), http.StatusOK)
}

func TestRevertEnvironmentRules(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.put(synthStaging, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.put(synthStaging, ulid(2), with(articleBody("Go memory model"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+ulid(1)+"/revert", revertBodyFor(1), http.StatusForbidden, "env_mismatch")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(2)+"/revert", revertBodyFor(1), http.StatusForbidden, "env_mismatch")
	h.call(dashStaging, http.MethodPost, "/v1/articles/"+ulid(2)+"/revert", revertBodyFor(1), http.StatusOK)
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 1}, http.StatusOK)
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+ulid(1)+"/revert", revertBodyFor(1), http.StatusForbidden, "env_golive")
}

func TestRevertWorksWhileWritesAreFrozenAndRestorePending(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	h.pol.SetWritesFrozen(true)
	h.pol.SetWriteBudget("dash-prod", 0, time.Hour)
	if out := h.revert(id, revertBodyFor(2), http.StatusOK); out["result"] != "reverted" {
		t.Errorf("frozen revert = %v", out)
	}
	if n := h.pol.Charges("dash-prod"); n != 0 {
		t.Errorf("revert charged %d writes", n)
	}
	h.pol.SetGoliveAt(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC))
	if out := h.revert(id, revertBodyFor(1, "request_id", "pending-1"), http.StatusOK); out["result"] != "reverted" {
		t.Errorf("revert during restore_pending = %v", out)
	}
}

func TestRevertPrunesByTheKeepRule(t *testing.T) {
	h := newHarness(t)
	synth := ulid(1)
	h.put(synthProd, synth, articleBody("Rust async runtimes"), http.StatusCreated)
	cur := synthRevise(h, synth, 6, 1)
	h.revert(synth, revertBodyFor(cur-2), http.StatusOK)
	if got := keptVersions(h, synth); !slices.Equal(got, topRange(cur-4, cur)) {
		t.Errorf("synth kept %v at %d", got, cur+1)
	}

	comm := ulid(2)
	h.contribute(comm, contribBody("Rust async runtimes", cid(0)), http.StatusCreated)
	cur = contribRevise(h, comm, 22, 1)
	h.revert(comm, revertBodyFor(cur-3), http.StatusOK)
	if got := keptVersions(h, comm); !slices.Equal(got, topRange(cur+1-20, cur)) {
		t.Errorf("community kept %v at %d", got, cur+1)
	}
	for _, v := range keptVersions(h, comm) {
		h.call(dashProd, http.MethodGet, fmt.Sprintf("/v1/article-versions/%s/%d", comm, v), nil, http.StatusOK)
	}
}

func TestLaterWriteClearsRevertsTo(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	h.revert(id, revertBodyFor(2), http.StatusOK)
	if fullOf(h, id)["reverts_to"] != 2.0 {
		t.Fatal("setup: no reverts_to")
	}
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("synth after"), "expected_version", 5), http.StatusOK)
	if got := fullOf(h, id); hasKey(got, "reverts_to") {
		t.Errorf("reverts_to survived a later write: %v", got["reverts_to"])
	}
}

func TestRevertRestoresEveryContentField(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	first := articleBody("Rust async runtimes")
	h.put(synthProd, id, first, http.StatusCreated)
	changed := with(first, "body_md", "## Overview\n\nA different body. [1]\n\n## Key facts\n\n- Another fact [1]\n"+first["body_md"].(string)[len("## Overview\n\n"):],
		"citations", []any{map[string]any{"n": 1, "url": "https://example.org/other", "title": "Other", "host": "example.org", "quote": "Another quote.", "content_sha": "b" + first["citations"].([]any)[0].(map[string]any)["content_sha"].(string)[1:]}},
		"quality_tier", "ok", "vertical", "research",
		"models", map[string]any{"triage": "triage-two", "writer": "writer-two"},
		"build", map[string]any{"sources_considered": 9, "sources_used": 3, "retrieval_params": map[string]any{"bm25_k": 10}, "cost_usd": 0.5, "dense_degraded": true},
		"expected_version", 1)
	h.put(synthProd, id, changed, http.StatusOK)
	if out := h.revert(id, revertBodyFor(1), http.StatusOK); out["result"] != "reverted" {
		t.Fatalf("revert = %v", out)
	}
	got, want := fullOf(h, id), h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id+"/1", nil, http.StatusOK)
	for _, k := range []string{"lead", "body_md", "citations", "quality_tier", "vertical", "sensitivity", "models", "build"} {
		a, _ := json.Marshal(got[k])
		b, _ := json.Marshal(want[k])
		if string(a) != string(b) {
			t.Errorf("%s = %s, want %s", k, a, b)
		}
	}
	if got["content_sha256"] != want["content_sha256"] {
		t.Error("content hash differs from the restored version's")
	}
	for _, only := range []string{"body_md", "citations", "quality_tier", "vertical"} {
		h2 := newHarness(t)
		h2.put(synthProd, id, first, http.StatusCreated)
		h2.put(synthProd, id, with(first, only, changed[only], "expected_version", 1), http.StatusOK)
		if out := h2.revert(id, revertBodyFor(1), http.StatusOK); out["result"] != "reverted" {
			t.Errorf("a difference in %s alone was treated as unchanged: %v", only, out)
		}
	}
}

func TestRevertEmbedsOutsideTheWriteLock(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.s.writeMu.Lock()
	done := make(chan map[string]any)
	go func() {
		rec := h.do(dashProd, http.MethodPost, "/v1/articles/"+id+"/revert", revertBodyFor(1))
		done <- map[string]any{"code": rec.Code}
	}()
	waitFor(t, func() bool { return h.emb.embedded(leadOf("one")) })
	h.s.writeMu.Unlock()
	if out := <-done; out["code"] != http.StatusOK {
		t.Errorf("revert = %v", out)
	}
}

func TestRevertChecksScopeBeforeBody(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	h.expectError(synthProd, http.MethodPost, "/v1/articles/"+id+"/revert", map[string]any{"to_version": "x"}, http.StatusForbidden, "missing_scope")
	h.expectError(wiki, http.MethodPost, "/v1/articles/"+id+"/revert", "not json", http.StatusForbidden, "missing_scope")
}

func TestRevertEmbedsOnlyWhenTheLeadDiffers(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	first := with(articleBody("Rust async runtimes"), "lead", leadOf("one"))
	h.put(synthProd, id, first, http.StatusCreated)
	h.put(synthProd, id, with(first, "body_md", "## Overview\n\nA different body. [1]\n\n## Key facts\n\n- Another fact [1]\n"+first["body_md"].(string)[len("## Overview\n\n"):], "expected_version", 1), http.StatusOK)
	h.emb.setFailIf(func([]string) bool { return true })
	calls := len(h.emb.calls)
	if out := h.revert(id, revertBodyFor(1), http.StatusOK); out["result"] != "reverted" {
		t.Fatalf("revert = %v", out)
	}
	if len(h.emb.calls) != calls {
		t.Errorf("a revert with the same lead embedded %d times", len(h.emb.calls)-calls)
	}
	h.put(synthProd, id, with(first, "lead", leadOf("two"), "expected_version", 3), http.StatusServiceUnavailable)
	h.emb.setFailIf(nil)
	h.put(synthProd, id, with(first, "lead", leadOf("two"), "expected_version", 3), http.StatusOK)
	h.emb.setFailIf(func([]string) bool { return true })
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+id+"/revert", revertBodyFor(1), http.StatusServiceUnavailable, "embedder_unavailable")
}
