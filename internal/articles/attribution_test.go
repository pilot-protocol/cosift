package articles

import (
	"encoding/json"
	"net/http"
	"slices"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
)

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func hasKey(m map[string]any, k string) bool { _, ok := m[k]; return ok }

func TestPublicProjectionCarriesAttribution(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("q covered", mix(1, 0, 0.9))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)

	want := []string{"ai_generated", "body_md", "citations", "contributors", "created_at", "id", "lead", "origin", "promoted", "quality_tier", "slug", "status", "title", "updated_at", "version", "vertical"}
	byID := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	bySlug := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusOK)
	covered := h.match(mcpProd, map[string]any{"q": "q covered"}, http.StatusOK)["article"].(map[string]any)
	for name, got := range map[string]map[string]any{"get": byID, "by-slug": bySlug, "match": covered} {
		if !slices.Equal(keysOf(got), want) {
			t.Errorf("%s keys = %v, want %v", name, keysOf(got), want)
		}
		if got["origin"] != "synth" || len(got["contributors"].([]any)) != 0 {
			t.Errorf("%s origin/contributors = %v %v", name, got["origin"], got["contributors"])
		}
	}
}

func TestFullProjectionCarriesAttribution(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if full["origin"] != "synth" || len(full["contributors"].([]any)) != 0 {
		t.Errorf("origin/contributors = %v %v", full["origin"], full["contributors"])
	}
	if v, ok := full["version_contributor"]; !ok || v != nil {
		t.Errorf("version_contributor = %v, present %v; want null", v, ok)
	}
	if hasKey(full, "reverts_to") {
		t.Error("reverts_to present on a version no revert created")
	}
}

func TestSynthWriteIgnoresEngineOwnedAttribution(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	body := with(articleBody("Rust async runtimes"), "origin", "community", "contributors", []string{"0f1e2d3c4b5a6978"}, "version_contributor", "0f1e2d3c4b5a6978", "reverts_to", 3)
	h.put(synthProd, id, body, http.StatusCreated)
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if full["origin"] != "synth" || len(full["contributors"].([]any)) != 0 || full["version_contributor"] != nil || hasKey(full, "reverts_to") {
		t.Errorf("body fields were not ignored: %v", full)
	}
}

func TestStubAndResidueCarryNoAttribution(t *testing.T) {
	h := newHarness(t)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Go memory model", topic(1)), http.StatusCreated)
	stub := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/go-memory-model", nil, http.StatusOK)
	fullStub := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK)
	h.put(synthProd, ulid(2), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(2), "tombstone", "privacy", http.StatusOK)
	residue := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(2), nil, http.StatusOK)
	for name, got := range map[string]map[string]any{"stub": stub, "full stub": fullStub, "residue": residue} {
		for _, k := range []string{"origin", "contributors", "version_contributor", "reverts_to"} {
			if hasKey(got, k) {
				t.Errorf("%s carries %s", name, k)
			}
		}
	}
}

func TestPreContributionRecordReadsAsSynth(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	raw, _ := h.raw(recordKey(id))
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"origin", "contributors", "version_contributor", "reverts_to"} {
		delete(m, k)
	}
	legacy, _ := json.Marshal(m)
	if err := h.ps.DB().Set(recordKey(id), legacy, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	h.restart()
	got := h.call(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if got["origin"] != "synth" || len(got["contributors"].([]any)) != 0 {
		t.Errorf("legacy record = %v %v", got["origin"], got["contributors"])
	}
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if full["origin"] != "synth" {
		t.Errorf("legacy full origin = %v", full["origin"])
	}
}
