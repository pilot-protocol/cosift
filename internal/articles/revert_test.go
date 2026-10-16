package articles

import (
	"net/http"
	"slices"
	"testing"
	"time"
)

func revertBodyFor(to int, kv ...any) map[string]any {
	return with(map[string]any{"to_version": to, "reason_code": "quality", "by": "operator"}, kv...)
}

func (h *harness) revert(id string, body map[string]any, want int) map[string]any {
	h.t.Helper()
	return h.call(dashProd, http.MethodPost, "/v1/articles/"+id+"/revert", body, want)
}

func (h *harness) revertError(id string, body map[string]any, status int, code string) map[string]any {
	h.t.Helper()
	return h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/revert", body, status, code)
}

func ints(v any) []int {
	out := []int{}
	for _, x := range v.([]any) {
		out = append(out, int(x.(float64)))
	}
	return out
}

func leadOf(n string) string { return "Lead " + n + ". It differs from the others." }

// history builds v1 synth, v2 A, v3 B, v4 B and returns the id.
func historyABB(h *harness) string {
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("three"), "expected_version", 2), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("four"), "expected_version", 3), http.StatusOK)
	return id
}

func TestRevertRestoresContentAndContributor(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	h.clock.advance(time.Hour)
	out := h.revert(id, revertBodyFor(2), http.StatusOK)
	if out["result"] != "reverted" || out["to_version"] != 2.0 || out["replayed"] != nil {
		t.Fatalf("revert = %v", out)
	}
	if a := article(out); a["version"] != 5.0 || a["status"] != "published" || a["id"] != id {
		t.Errorf("article = %v", a)
	}
	if !slices.Equal(ints(out["reverted_versions"]), []int{3, 4}) || !slices.Equal(toStrings(out["reverted_contributors"]), []string{cid(2)}) {
		t.Errorf("lists = %v %v", out["reverted_versions"], out["reverted_contributors"])
	}
	got := fullOf(h, id)
	if got["lead"] != leadOf("two") || got["version"] != 5.0 || got["version_contributor"] != cid(1) || got["reverts_to"] != 2.0 {
		t.Errorf("record = lead %q v%v contributor %v reverts_to %v", got["lead"], got["version"], got["version_contributor"], got["reverts_to"])
	}
	if got["origin"] != "community" || !slices.Equal(toStrings(got["contributors"]), []string{cid(1), cid(2)}) {
		t.Errorf("attribution = %v %v", got["origin"], got["contributors"])
	}
	m := got["moderation"].(map[string]any)
	if m["action"] != "revert" || m["by"] != "operator" || m["reason_code"] != "quality" || m["to_version"] != 2.0 || !slices.Equal(ints(m["reverted_versions"]), []int{3, 4}) {
		t.Errorf("moderation = %v", m)
	}
	if got["updated_at"] == got["created_at"] || got["status_changed_at"] != got["created_at"] {
		t.Errorf("timestamps: updated %v status_changed %v created %v", got["updated_at"], got["status_changed_at"], got["created_at"])
	}
	if pv := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id+"/4", nil, http.StatusOK); pv["lead"] != leadOf("four") {
		t.Errorf("the version taken out was lost: %v", pv["lead"])
	}
	pub := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if pub["lead"] != leadOf("two") || hasKey(pub, "reverts_to") || hasKey(pub, "version_contributor") {
		t.Errorf("public = %v", pub)
	}
}

func TestRevertListsFollowTheContractTable(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	h.revert(id, revertBodyFor(2), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(3), "lead", leadOf("six"), "expected_version", 5), http.StatusOK)
	out := h.revert(id, revertBodyFor(1), http.StatusOK)
	if a := article(out); a["version"] != 7.0 {
		t.Fatalf("version = %v", a["version"])
	}
	if !slices.Equal(ints(out["reverted_versions"]), []int{2, 6}) || !slices.Equal(toStrings(out["reverted_contributors"]), []string{cid(1), cid(3)}) {
		t.Errorf("lists = %v %v", out["reverted_versions"], out["reverted_contributors"])
	}
	got := fullOf(h, id)
	if v, ok := got["version_contributor"]; !ok || v != nil || got["reverts_to"] != 1.0 || got["lead"] != leadOf("one") {
		t.Errorf("record = contributor %v reverts_to %v lead %q", got["version_contributor"], got["reverts_to"], got["lead"])
	}
	if len(got["contributors"].([]any)) != 3 {
		t.Errorf("contributors shrank: %v", got["contributors"])
	}
}

func TestRevertToARevertCreatedVersionHasEmptyLists(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.revert(id, revertBodyFor(1), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("four"), "expected_version", 3), http.StatusOK)
	out := h.revert(id, revertBodyFor(3), http.StatusOK)
	if out["result"] != "reverted" || len(ints(out["reverted_versions"])) != 1 || ints(out["reverted_versions"])[0] != 4 {
		t.Errorf("revert to the revert-created version = %v", out)
	}
	out = h.revert(id, revertBodyFor(2), http.StatusOK)
	if !slices.Equal(ints(out["reverted_versions"]), []int{}) || len(toStrings(out["reverted_contributors"])) != 0 || out["result"] != "reverted" {
		t.Errorf("lists = %v %v result %v", out["reverted_versions"], out["reverted_contributors"], out["result"])
	}
}

func TestRevertUnchangedWhenContentEqualsTarget(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("one"), "expected_version", 2), http.StatusOK)
	before := fullOf(h, id)
	out := h.revert(id, revertBodyFor(1), http.StatusOK)
	if out["result"] != "unchanged" || len(ints(out["reverted_versions"])) != 0 || len(toStrings(out["reverted_contributors"])) != 0 || article(out)["version"] != 3.0 {
		t.Errorf("revert = %v", out)
	}
	if after := fullOf(h, id); after["version"] != before["version"] || after["version_contributor"] != before["version_contributor"] || after["moderation"] != nil {
		t.Errorf("unchanged revert wrote: %v", after)
	}
}

func TestRevertReplayAndRequestID(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	first := h.revert(id, revertBodyFor(2, "request_id", id+"_01J9D6T1F7G8H9J0K1M2N3P4Q5"), http.StatusOK)
	if first["replayed"] != nil {
		t.Fatalf("first = %v", first)
	}
	replay := h.revert(id, revertBodyFor(2, "request_id", id+"_01J9D6T1F7G8H9J0K1M2N3P4Q5"), http.StatusOK)
	if replay["result"] != "reverted" || replay["replayed"] != true || !slices.Equal(ints(replay["reverted_versions"]), []int{3, 4}) || !slices.Equal(toStrings(replay["reverted_contributors"]), []string{cid(2)}) || article(replay)["version"] != 5.0 {
		t.Errorf("replay = %v", replay)
	}
	if fullOf(h, id)["version"] != 5.0 {
		t.Error("a replay wrote a version")
	}
	other := h.revert(id, revertBodyFor(2, "request_id", "another"), http.StatusOK)
	if other["result"] != "unchanged" || len(ints(other["reverted_versions"])) != 0 {
		t.Errorf("other request id = %v", other)
	}
	none := h.revert(id, revertBodyFor(2), http.StatusOK)
	if none["result"] != "unchanged" || none["replayed"] != nil {
		t.Errorf("no request id = %v", none)
	}
	h.moderate(dashProd, id, "hold", "", http.StatusOK)
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	if again := h.revert(id, revertBodyFor(2, "request_id", id+"_01J9D6T1F7G8H9J0K1M2N3P4Q5"), http.StatusOK); again["replayed"] != nil {
		t.Errorf("replay survived a later moderation action: %v", again)
	}
}

func TestRevertReplayComesBeforeIllegalTransitionAndAfterEnvMismatch(t *testing.T) {
	h := newHarness(t)
	id := historyABB(h)
	rid := id + "_01J9D6T1F7G8H9J0K1M2N3P4Q5"
	h.revert(id, revertBodyFor(2, "request_id", rid), http.StatusOK)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held", "lead", leadOf("rebuilt"), "expected_version", 5), http.StatusOK)
	if got := fullOf(h, id); got["status"] != "held" {
		t.Fatalf("setup: status %v", got["status"])
	}
	replay := h.revert(id, revertBodyFor(2, "request_id", rid), http.StatusOK)
	if replay["replayed"] != true || !slices.Equal(ints(replay["reverted_versions"]), []int{3, 4}) || !slices.Equal(toStrings(replay["reverted_contributors"]), []string{cid(2)}) {
		t.Errorf("replay on a held record = %v", replay)
	}
	h.revertError(id, revertBodyFor(2, "request_id", "another"), http.StatusConflict, "illegal_transition")
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+id+"/revert", revertBodyFor(2, "request_id", rid), http.StatusForbidden, "env_mismatch")
}

func TestRevertKeepsOriginCommunityWhenTheTargetIsSynth(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.revert(id, revertBodyFor(1), http.StatusOK)
	h.restart()
	if got := fullOf(h, id); got["origin"] != "community" || got["lead"] != leadOf("one") {
		t.Errorf("full origin %v lead %q", got["origin"], got["lead"])
	}
	if pub := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); pub["origin"] != "community" {
		t.Errorf("public origin %v", pub["origin"])
	}
}

func TestRevertCreatedVersionsDoNotListTheirInheritedContributor(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.revert(id, revertBodyFor(1), http.StatusOK)
	if got := fullOf(h, id); got["version"] != 3.0 || got["version_contributor"] != cid(1) {
		t.Fatalf("setup: v%v contributor %v", got["version"], got["version_contributor"])
	}
	out := h.revert(id, revertBodyFor(2), http.StatusOK)
	if out["result"] != "reverted" || len(ints(out["reverted_versions"])) != 0 || len(toStrings(out["reverted_contributors"])) != 0 {
		t.Errorf("revert over a revert-created version = %v", out)
	}
}

func TestRevertThatTookNothingOutStillReplays(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1), "lead", leadOf("one")), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.revert(id, revertBodyFor(1), http.StatusOK)
	first := h.revert(id, revertBodyFor(2, "request_id", "empty-1"), http.StatusOK)
	if first["result"] != "reverted" || len(ints(first["reverted_versions"])) != 0 {
		t.Fatalf("first = %v", first)
	}
	replay := h.revert(id, revertBodyFor(2, "request_id", "empty-1"), http.StatusOK)
	if replay["result"] != "reverted" || replay["replayed"] != true || len(ints(replay["reverted_versions"])) != 0 || article(replay)["version"] != 4.0 {
		t.Errorf("replay = %v", replay)
	}
}

func TestRevertStoresEmptyListsAsArrays(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("one")), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", leadOf("two"), "expected_version", 1), http.StatusOK)
	h.revert(id, revertBodyFor(1, "request_id", "empty-1"), http.StatusOK)
	m := fullOf(h, id)["moderation"].(map[string]any)
	if cs, ok := m["reverted_contributors"].([]any); !ok || len(cs) != 0 {
		t.Errorf("stored reverted_contributors = %#v", m["reverted_contributors"])
	}
}
