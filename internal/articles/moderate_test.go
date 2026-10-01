package articles

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
)

// rKeys lists every key of the article family.
func rKeys(h *harness) []string {
	lower, upper := familyBounds()
	it, _ := h.ps.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	defer it.Close()
	var keys []string
	for ok := it.First(); ok; ok = it.Next() {
		keys = append(keys, string(it.Key()))
	}
	sort.Strings(keys)
	return keys
}

func TestTombstoneErases(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	title := "Rust async runtimes"
	h.emb.set(title, axis(1))
	h.emb.set("rust covered", mix(1, 0, 0.9))
	body := with(articleBody(title), "aliases", []string{"Async executors in Rust"}, "topic_ids", []string{topic(1)}, "cluster_ids", []string{"c013960"})
	h.put(synthProd, id, body, http.StatusCreated)
	h.put(synthProd, id, with(body, "lead", "A second lead. It makes a version."), http.StatusOK)
	h.match(mcpProd, map[string]any{"q": "rust covered", "reader": strings.Repeat("a", 32)}, http.StatusOK)
	h.restart()
	lower, upper := versionPrefix(id)
	if countRange(h, lower, upper) != 1 {
		t.Fatal("no prior version to erase")
	}
	for _, k := range [][]byte{embeddingKey(id), countersKey(id)} {
		if _, ok := h.raw(k); !ok {
			t.Fatalf("setup: %q absent", k)
		}
	}
	rowsBefore := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["matrix_rows"].(float64)

	out := h.moderate(dashProd, id, "tombstone", "privacy", http.StatusOK)
	if a := article(out); a["status"] != "tombstoned" || a["has_fingerprint"] != true || a["slug"] != "rust-async-runtimes" {
		t.Fatalf("tombstone = %v", out)
	}
	if countRange(h, lower, upper) != 0 {
		t.Error("R v survived")
	}
	for _, k := range [][]byte{embeddingKey(id), countersKey(id)} {
		if _, ok := h.raw(k); ok {
			t.Errorf("%q survived", k)
		}
	}
	for _, k := range [][]byte{slugKey("rust-async-runtimes"), topicKey(topic(1))} {
		if _, ok := h.raw(k); !ok {
			t.Errorf("%q not kept", k)
		}
	}
	var residue map[string]any
	_ = json.Unmarshal(mustRaw(t, h, recordKey(id)), &residue)
	var keys []string
	for k := range residue {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := []string{"base_slug", "id", "prelive", "reason_code", "residue", "schema", "slug", "status", "status_changed_at", "tombstoned_at", "topic_ids"}
	if !slices.Equal(keys, want) {
		t.Errorf("residue fields = %v", keys)
	}
	raw := mustRaw(t, h, recordKey(id))
	for _, gone := range []string{title, "Async executors", "dense factual prose", "c013960", "example.org", "triage-model"} {
		if bytes.Contains(raw, []byte(gone)) {
			t.Errorf("residue keeps %q", gone)
		}
	}
	_, _, fp, err := decodeFingerprint(mustRaw(t, h, fingerprintKey(id)))
	if err != nil || !slices.Equal(fp, axis(1)) {
		t.Errorf("fingerprint is not the title vector: %v %v", fp, err)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusGone, "gone")
	if out := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusGone); out["detail"] != "article removed" {
		t.Errorf("410 body = %v", out)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusNotFound, "not_found")
	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); f["residue"] != true || f["title"] != nil || f["has_fingerprint"] != true || f["readers_7d"] != 0.0 {
		t.Errorf("full residue = %v", f)
	}
	if rows := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["matrix_rows"].(float64); rows != rowsBefore-3 {
		t.Errorf("matrix rows %v → %v", rowsBefore, rows)
	}
	if out := h.match(mcpProd, map[string]any{"q": "rust covered"}, http.StatusOK); out["verdict"] != "none" {
		t.Errorf("tombstoned article matched: %v", out)
	}
	h.restart()
	h.expectError(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusGone, "gone")
	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); f["readers_7d"] != 0.0 {
		t.Errorf("readers after restart = %v", f["readers_7d"])
	}
}

func TestEraseFingerprintOnlyRemovesFingerprint(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	h.moderate(dashProd, id, "tombstone", "privacy", http.StatusOK)
	before := rKeys(h)
	rec := mustRaw(t, h, recordKey(id))
	out := h.moderate(dashProd, id, "erase_fingerprint", "privacy", http.StatusOK)
	if a := article(out); a["has_fingerprint"] != false || a["status"] != "tombstoned" {
		t.Errorf("erase = %v", out)
	}
	after := rKeys(h)
	removed := slices.DeleteFunc(slices.Clone(before), func(k string) bool { return slices.Contains(after, k) })
	if len(removed) != 1 || removed[0] != string(fingerprintKey(id)) || len(after) != len(before)-1 {
		t.Errorf("removed %q", removed)
	}
	if !bytes.Equal(rec, mustRaw(t, h, recordKey(id))) {
		t.Error("record changed")
	}
	if out := h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "erase_fingerprint", "reason_code": "privacy", "by": "op"}, http.StatusConflict, "illegal_transition"); out["current_status"] != "tombstoned" {
		t.Errorf("second erase = %v", out)
	}
	h.restart()
	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); f["has_fingerprint"] != false {
		t.Errorf("fingerprint back after restart: %v", f)
	}
}

func TestStatusTransitions(t *testing.T) {
	type state struct {
		name  string
		setup func(h *harness, id string)
	}
	states := []state{
		{StatusPending, func(h *harness, id string) {
			h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
		}},
		{StatusPublished, func(h *harness, id string) {
			h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
		}},
		{StatusHeld, func(h *harness, id string) {
			h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held"), http.StatusCreated)
		}},
		{StatusRejected, func(h *harness, id string) {
			h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
			h.moderate(dashProd, id, "reject", "quality", http.StatusOK)
		}},
		{StatusTombstoned, func(h *harness, id string) {
			h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
			h.moderate(dashProd, id, "tombstone", "legal", http.StatusOK)
		}},
	}
	legalTo := map[string]map[string]string{
		StatusPending:    {"tombstone": StatusTombstoned, "delete_stub": ""},
		StatusPublished:  {"hold": StatusHeld, "reject": StatusRejected, "tombstone": StatusTombstoned},
		StatusHeld:       {"approve": StatusPublished, "reject": StatusRejected, "tombstone": StatusTombstoned},
		StatusRejected:   {"hold": StatusHeld, "tombstone": StatusTombstoned},
		StatusTombstoned: {"erase_fingerprint": StatusTombstoned},
	}
	for _, st := range states {
		for _, action := range []string{"approve", "hold", "reject", "tombstone", "erase_fingerprint", "delete_stub"} {
			t.Run(st.name+"/"+action, func(t *testing.T) {
				h := newHarness(t)
				id := ulid(1)
				st.setup(h, id)
				body := map[string]any{"action": action, "reason_code": "other", "by": "op"}
				to, ok := legalTo[st.name][action]
				if !ok {
					out := h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", body, http.StatusConflict, "illegal_transition")
					if out["current_status"] != st.name {
						t.Errorf("current_status = %v", out["current_status"])
					}
					return
				}
				out := h.call(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", body, http.StatusOK)
				a := article(out)
				if to == "" {
					if a["status"] != nil {
						t.Errorf("deleted stub reports %v", a)
					}
					h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", body, http.StatusNotFound, "not_found")
					return
				}
				if a["status"] != to || a["status_changed_at"] == nil {
					t.Errorf("%s → %v", action, a)
				}
			})
		}
	}
}

func TestDeleteStubModeration(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1), topic(2)), http.StatusCreated)
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "delete_stub", "by": "op"}, http.StatusUnprocessableEntity, "invalid_field")
	h.moderate(dashProd, id, "delete_stub", "other", http.StatusOK)
	if keys := rKeys(h); len(keys) != 1 || keys[0] != string(metaKey("schema")) {
		t.Errorf("keys left: %q", keys)
	}
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "delete_stub", "reason_code": "other", "by": "op"}, http.StatusNotFound, "not_found")
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	if a := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusOK); a["id"] != ulid(2) {
		t.Errorf("slug not released: %v", a)
	}
}

func TestStatusValidation(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	cases := []struct {
		body  map[string]any
		field string
	}{
		{map[string]any{"action": "publish", "by": "op"}, "action"},
		{map[string]any{"action": "reject", "by": "op"}, "reason_code"},
		{map[string]any{"action": "tombstone", "by": "op"}, "reason_code"},
		{map[string]any{"action": "erase_fingerprint", "by": "op"}, "reason_code"},
		{map[string]any{"action": "hold", "reason_code": "spite", "by": "op"}, "reason_code"},
		{map[string]any{"action": "hold", "by": ""}, "by"},
		{map[string]any{"action": "hold", "by": strings.Repeat("x", 65)}, "by"},
		{map[string]any{"action": "hold", "by": "tab\there"}, "by"},
	}
	for _, c := range cases {
		if out := h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", c.body, http.StatusUnprocessableEntity, "invalid_field"); out["field"] != c.field {
			t.Errorf("%v: field %v", c.body, out["field"])
		}
	}
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "hold", "by": "op", "reason": "free text"}, http.StatusBadRequest, "unknown_field")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(9)+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusNotFound, "not_found")
	h.expectError(synthProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusForbidden, "missing_scope")
	h.moderate(dashProd, id, "hold", "", http.StatusOK)
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	if out := h.expectError(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "approve", "by": "op"}, http.StatusConflict, "illegal_transition"); out["current_status"] != "published" {
		t.Errorf("no-op = %v", out)
	}
}

func TestModerationEnvBinding(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.put(synthStaging, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	for _, action := range []string{"approve", "hold", "reject", "tombstone", "erase_fingerprint", "delete_stub"} {
		body := map[string]any{"action": action, "reason_code": "other", "by": "op"}
		h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+ulid(1)+"/status", body, http.StatusForbidden, "env_mismatch")
		h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(2)+"/status", body, http.StatusForbidden, "env_mismatch")
	}
	h.moderate(dashStaging, ulid(2), "hold", "", http.StatusOK)
	h.moderate(dashProd, ulid(1), "hold", "", http.StatusOK)
}

// failing fails every embed call that includes one of texts.
func failing(texts ...string) func([]string) bool {
	return func(in []string) bool {
		for _, t := range in {
			if slices.Contains(texts, t) {
				return true
			}
		}
		return false
	}
}

// A16: a takedown after a model change fingerprints the title with the running model.
func TestTakedownFingerprintAfterModelChange(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("Rust async runtime systems", mix(1, 0, 0.8))
	h.emb.set("Go memory model", axis(2))
	h.emb.set("Go memory models explained", mix(2, 0, 0.8))
	a, b := articleBody("Rust async runtimes"), articleBody("Go memory model")
	h.put(synthProd, ulid(1), a, http.StatusCreated)
	h.put(synthStaging, ulid(2), b, http.StatusCreated)
	h.moderate(dashStaging, ulid(2), "reject", "quality", http.StatusOK)
	h.shutdown()
	h.emb.model = "fake-embed-v2"
	h.emb.setFailIf(failing(a["lead"].(string), b["lead"].(string)))
	h.open(nil)

	if out := h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK); article(out)["has_fingerprint"] != true {
		t.Errorf("tombstone = %v", out)
	}
	if m, _, _, err := decodeFingerprint(mustRaw(t, h, fingerprintKey(ulid(1)))); err != nil || m != "fake-embed-v2" {
		t.Errorf("fingerprint model %q, %v", m, err)
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Rust async runtime systems"), http.StatusConflict, "blocked")
	if v, id := verdictOf(buildMatch(h, nil, "Rust async runtime systems")); v != "blocked" || id != ulid(1) {
		t.Errorf("build match of a rewording: %s %s", v, id)
	}

	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 1}, http.StatusOK)
	if m, _, _, err := decodeFingerprint(mustRaw(t, h, fingerprintKey(ulid(2)))); err != nil || m != "fake-embed-v2" {
		t.Errorf("converted rejection fingerprint model %q, %v", m, err)
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(4), articleBody("Go memory models explained"), http.StatusConflict, "blocked")
	if fp := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["fingerprints"].(map[string]any); fp["active"] != 2.0 || fp["other_model"] != 0.0 {
		t.Errorf("fingerprints = %v", fp)
	}
}

// A16: with the embedder down, the takedown still lands; its old-model fingerprint is counted, not compared.
func TestTakedownWithoutEmbedderAfterModelChange(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.shutdown()
	h.emb.model = "fake-embed-v2"
	h.emb.setFailIf(func([]string) bool { return true })
	h.open(nil)
	out := h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)
	if a := article(out); a["status"] != "tombstoned" || a["has_fingerprint"] != false {
		t.Errorf("tombstone = %v", out)
	}
	if fp := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)["fingerprints"].(map[string]any); fp["active"] != 0.0 || fp["other_model"] != 1.0 {
		t.Errorf("fingerprints = %v", fp)
	}
	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK); f["has_fingerprint"] != false {
		t.Errorf("full projection = %v", f)
	}
	h.moderate(dashProd, ulid(1), "erase_fingerprint", "privacy", http.StatusOK)
	if _, ok := h.raw(fingerprintKey(ulid(1))); ok {
		t.Error("the old-model fingerprint could not be erased")
	}
}
