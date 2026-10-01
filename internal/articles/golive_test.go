package articles

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

var (
	synthStagingRead    = v1.Principal{ID: "synth-staging", Kind: v1.KindOIDC, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesRead}}
	resolverStagingRead = v1.Principal{ID: "resolver-staging", Kind: v1.KindOIDC, Env: v1.EnvStaging, Scopes: []v1.Scope{v1.ScopeArticlesRead}}
)

// inFlight sends a PUT from another goroutine and returns its error body with the status.
func inFlight(h *harness, p v1.Principal, id string, body map[string]any) map[string]any {
	rec := h.do(p, http.MethodPut, "/v1/articles/"+id, body)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	out["http_status"] = rec.Code
	return out
}

// closeStagingWrites is go-live step 1: staging principals lose their write scopes.
func closeStagingWrites(h *harness) {
	h.pol.SetPrincipals(synthProd, synthStagingRead, resolverProd, resolverStagingRead, mcpProd, mcpStaging, wiki, dashProd, dashStaging, goliveAdmin)
}

func purge(h *harness, body map[string]any, want int) map[string]any {
	h.t.Helper()
	return h.call(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", body, want)
}

func TestPurgePrelive(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Staging rejected title", axis(5))
	h.put(synthProd, ulid(1), articleBody("Prod article title"), http.StatusCreated)
	h.put(synthStaging, ulid(2), with(articleBody("Staging published title"), "topic_ids", []string{topic(2)}), http.StatusCreated)
	h.put(synthStaging, ulid(2), with(articleBody("Staging published title"), "lead", "Second lead. A version."), http.StatusOK)
	h.put(synthStaging, ulid(3), with(articleBody("Staging held title"), "status", "held"), http.StatusCreated)
	h.call(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(4), stubBodyFor("Staging stub title", topic(4)), http.StatusCreated)
	h.put(synthStaging, ulid(5), with(articleBody("Staging tombstoned title"), "topic_ids", []string{topic(5)}), http.StatusCreated)
	h.moderate(dashStaging, ulid(5), "tombstone", "privacy", http.StatusOK)
	h.put(synthStaging, ulid(6), with(articleBody("Staging rejected title"), "topic_ids", []string{topic(6)}), http.StatusCreated)
	h.moderate(dashStaging, ulid(6), "reject", "quality", http.StatusOK)
	h.put(synthProd, ulid(7), articleBody("Prod tombstoned title"), http.StatusCreated)
	h.moderate(dashProd, ulid(7), "tombstone", "legal", http.StatusOK)

	dry := purge(h, map[string]any{}, http.StatusOK)
	if dry["apply"] != false || dry["records"] != 5.0 {
		t.Fatalf("dry run = %v", dry)
	}
	del, conv, keys := dry["delete"].(map[string]any), dry["convert"].(map[string]any), dry["keys"].(map[string]any)
	if del["published"] != 1.0 || del["held"] != 1.0 || del["pending"] != 1.0 || conv["tombstoned"] != 1.0 || conv["rejected"] != 1.0 {
		t.Errorf("split = %v %v", del, conv)
	}
	if keys["a"] != 3.0 || keys["s"] != 3.0 || keys["t"] != 2.0 || keys["v"] != 1.0 || keys["e"] != 3.0 {
		t.Errorf("keys = %v", keys)
	}
	if dry["matrix_rows"] != 6.0 {
		t.Errorf("matrix_rows = %v", dry["matrix_rows"])
	}
	if w := toStrings(dry["staging_writers"]); !slices.Equal(w, []string{"resolver-staging", "synth-staging"}) {
		t.Errorf("staging_writers = %v", w)
	}

	out := h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": 5}, http.StatusConflict, "staging_writers_present")
	if p := toStrings(out["principals"]); len(p) != 2 {
		t.Errorf("principals = %v", p)
	}
	closeStagingWrites(h)
	if out := h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": 4}, http.StatusConflict, "count_changed"); out["records"] != 5.0 {
		t.Errorf("count_changed = %v", out)
	}
	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true}, http.StatusUnprocessableEntity, "invalid_field")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{}, http.StatusForbidden, "missing_scope")
	if _, ok := h.raw(metaKey("golive")); ok {
		t.Fatal("refused purge wrote the flag")
	}

	applied := purge(h, map[string]any{"apply": true, "expect_records": 5}, http.StatusOK)
	if applied["apply"] != true || applied["records"] != 5.0 || applied["golive_at"] == nil {
		t.Fatalf("apply = %v", applied)
	}
	for _, id := range []string{ulid(2), ulid(3), ulid(4)} {
		if _, ok := h.raw(recordKey(id)); ok {
			t.Errorf("%s survived the purge", id)
		}
	}
	for _, k := range [][]byte{topicKey(topic(2)), topicKey(topic(4)), slugKey("staging-published-title")} {
		if _, ok := h.raw(k); ok {
			t.Errorf("%q survived", k)
		}
	}
	var rej map[string]any
	_ = json.Unmarshal(mustRaw(t, h, recordKey(ulid(6))), &rej)
	if rej["status"] != "rejected" || rej["residue"] != true || rej["prelive"] != false || rej["base_slug"] != "staging-rejected-title" || rej["reason_code"] != "quality" || rej["title"] != nil {
		t.Errorf("converted rejection = %v", rej)
	}
	for _, k := range [][]byte{topicKey(topic(6)), slugKey("staging-rejected-title"), fingerprintKey(ulid(6)), topicKey(topic(5)), fingerprintKey(ulid(5))} {
		if _, ok := h.raw(k); !ok {
			t.Errorf("%q not kept", k)
		}
	}
	if _, _, fp, _ := decodeFingerprint(mustRaw(t, h, fingerprintKey(ulid(6)))); !slices.Equal(fp, axis(5)) {
		t.Error("converted rejection's fingerprint is not its title vector")
	}
	if _, ok := h.raw(embeddingKey(ulid(6))); ok {
		t.Error("converted rejection kept R e")
	}

	stats := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)
	res := stats["residues"].(map[string]any)
	if stats["prelive_records"] != 0.0 || stats["prelive_keys"] != 0.0 || stats["orphan_keys"] != 0.0 || res["tombstoned"] != 2.0 || res["rejected"] != 1.0 {
		t.Errorf("stats = %v", stats)
	}
	if g := stats["golive"].(map[string]any); g["at"] != applied["golive_at"] {
		t.Errorf("golive = %v", g)
	}

	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{}, http.StatusGone, "golive")
	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": 0}, http.StatusGone, "golive")
	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": false, "x": 1}, http.StatusGone, "golive")
	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", "not json", http.StatusGone, "golive")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{}, http.StatusForbidden, "missing_scope")
	h.restart()
	h.expectError(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{}, http.StatusGone, "golive")

	h.expectError(wiki, http.MethodGet, "/v1/articles/by-slug/staging-rejected-title", nil, http.StatusNotFound, "not_found")
	h.expectError(wiki, http.MethodGet, "/v1/articles/by-slug/staging-tombstoned-title", nil, http.StatusGone, "gone")
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(8), articleBody("Staging rejected title"), http.StatusConflict, "blocked"); out["article_id"] != ulid(6) {
		t.Errorf("prod create over a converted rejection = %v", out)
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(8), with(articleBody("Another prod title"), "topic_ids", []string{topic(6)}), http.StatusConflict, "topic_claimed")
	h.emb.set("staging rejected reworded", mix(5, 0, 0.8))
	if v, id := verdictOf(buildMatch(h, nil, "staging rejected reworded")); v != "blocked" || id != ulid(6) {
		t.Errorf("rewording of a converted rejection: %s %s", v, id)
	}
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(6)+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusConflict, "illegal_transition")
	h.moderate(dashProd, ulid(6), "tombstone", "privacy", http.StatusOK)

	h.pol.SetPrincipals(allPrincipals...)
	h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(9), articleBody("Staging after golive"), http.StatusForbidden, "env_golive")
	h.expectError(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(9), stubBodyFor("Staging after golive", topic(9)), http.StatusForbidden, "env_golive")
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+ulid(1)+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusForbidden, "env_golive")
	h.expectError(dashStaging, http.MethodPost, "/v1/articles/"+ulid(9)+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusForbidden, "env_golive")
	h.put(synthProd, ulid(9), articleBody("Prod after golive"), http.StatusCreated)
}

func TestStagingWriteInFlightAcrossPurge(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan map[string]any)
	go func() {
		done <- inFlight(h, synthStaging, ulid(1), articleBody("Staging in flight"))
	}()
	waitFor(t, func() bool { return h.emb.embedded("Staging in flight") })
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 0}, http.StatusOK)
	close(block)
	if out := <-done; out["code"] != "env_golive" || out["http_status"] != http.StatusForbidden {
		t.Errorf("in-flight staging write = %v", out)
	}
	if _, ok := h.raw(recordKey(ulid(1))); ok {
		t.Error("in-flight staging write landed after the purge")
	}
}

func TestReloadRemovingScopeRefusesInFlightWrite(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan map[string]any)
	go func() {
		done <- inFlight(h, synthProd, ulid(1), articleBody("Rust async runtimes"))
	}()
	waitFor(t, func() bool { return h.emb.embedded("Rust async runtimes") })
	h.pol.SetPrincipals(v1.Principal{ID: "synth-prod", Kind: v1.KindOIDC, Env: v1.EnvProd, Scopes: []v1.Scope{v1.ScopeArticlesRead}})
	close(block)
	if out := <-done; out["code"] != "missing_scope" || out["http_status"] != http.StatusForbidden {
		t.Errorf("write after its scope was removed = %v", out)
	}
	if _, ok := h.raw(recordKey(ulid(1))); ok {
		t.Error("write landed after its scope was removed")
	}
}

func TestRestorePending(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.pol.SetGoliveAt(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC))
	refused := []struct {
		p            v1.Principal
		method, path string
		body         any
	}{
		{mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "x"}},
		{synthProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}},
		{wiki, http.MethodGet, "/v1/articles/" + ulid(1), nil},
		{wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil},
		{wiki, http.MethodGet, "/v1/articles", nil},
		{synthProd, http.MethodPut, "/v1/articles/" + ulid(3), articleBody("Kafka consumer groups")},
		{resolverProd, http.MethodPut, "/v1/articles/" + ulid(4), stubBodyFor("Python packaging tools", topic(4))},
		{resolverProd, http.MethodDelete, "/v1/articles/" + ulid(2), nil},
	}
	for _, c := range refused {
		rec := h.do(c.p, c.method, c.path, c.body)
		if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
			t.Errorf("%s %s: %d %s", c.method, c.path, rec.Code, rec.Body.String())
			continue
		}
		var e map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &e)
		if e["code"] != "restore_pending" {
			t.Errorf("%s %s: %v", c.method, c.path, e)
		}
	}
	h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK)
	h.call(dashProd, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusOK)
	h.call(dashProd, http.MethodGet, "/v1/articles", nil, http.StatusOK)
	h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK)
	h.call(dashProd, http.MethodGet, "/v1/article-versions/"+ulid(1), nil, http.StatusOK)
	h.moderate(dashProd, ulid(1), "hold", "", http.StatusOK)
	purge(h, map[string]any{}, http.StatusOK)
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 0}, http.StatusOK)
	h.match(mcpProd, map[string]any{"q": "x"}, http.StatusOK)
	h.call(wiki, http.MethodGet, "/v1/articles", nil, http.StatusOK)
}

// E01: read_all is not bound, so the prod dashboard sees prelive records.
func TestReadAllSeesPrelive(t *testing.T) {
	h := newHarness(t)
	h.put(synthStaging, ulid(1), articleBody("Staging article title"), http.StatusCreated)
	h.call(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Staging stub title", topic(2)), http.StatusCreated)
	h.put(synthProd, ulid(3), articleBody("Prod article title"), http.StatusCreated)

	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK); f["prelive"] != true {
		t.Errorf("dash-prod get = %v", f)
	}
	h.call(dashProd, http.MethodGet, "/v1/articles/by-slug/staging-stub-title", nil, http.StatusOK)
	list := h.call(dashProd, http.MethodGet, "/v1/articles?prelive=true", nil, http.StatusOK)
	if items := list["items"].([]any); len(items) != 2 || items[0].(map[string]any)["prelive"] != true {
		t.Errorf("dash-prod prelive list = %v", list)
	}
	if st := h.call(dashProd, http.MethodGet, "/v1/articles/stats", nil, http.StatusOK); st["prelive_records"] != 2.0 || st["prelive_keys"].(float64) < 6 {
		t.Errorf("stats = %v", st)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusNotFound, "not_found")
	h.expectError(wiki, http.MethodGet, "/v1/articles/by-slug/staging-stub-title", nil, http.StatusNotFound, "not_found")
	if items := h.call(wiki, http.MethodGet, "/v1/articles", nil, http.StatusOK)["items"].([]any); len(items) != 1 {
		t.Errorf("prod read list = %v", items)
	}
	if items := h.call(mcpStaging, http.MethodGet, "/v1/articles", nil, http.StatusOK)["items"].([]any); len(items) != 2 {
		t.Errorf("staging read list = %v", items)
	}
	h.call(mcpStaging, http.MethodGet, "/v1/articles/by-slug/staging-stub-title", nil, http.StatusOK)
}

// A record created by another environment while a write waits on the embedder.
func TestEnvRecheckedInLock(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan map[string]any)
	go func() {
		done <- inFlight(h, synthStaging, ulid(1), articleBody("Rust async runtimes"))
	}()
	waitFor(t, func() bool { return h.emb.embedded("Rust async runtimes") })
	h.emb.setBlock(nil)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	close(block)
	if out := <-done; out["code"] != "env_mismatch" || out["http_status"] != http.StatusForbidden {
		t.Errorf("staging fill of a prod stub = %v", out)
	}
	if r := h.s.record(ulid(1)); r == nil || r.Status != StatusPending || r.Prelive {
		t.Errorf("prod stub changed: %+v", r)
	}
}

func TestEnvGoliveBeforeEnvMismatch(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	closeStagingWrites(h)
	purge(h, map[string]any{"apply": true, "expect_records": 0}, http.StatusOK)
	h.pol.SetPrincipals(allPrincipals...)
	h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"), http.StatusForbidden, "env_golive")
	h.expectError(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusForbidden, "env_golive")
	h.expectError(resolverStaging, http.MethodDelete, "/v1/articles/"+ulid(2), nil, http.StatusForbidden, "env_golive")
}

func TestConcurrentPurgeAppliesOnce(t *testing.T) {
	h := newHarness(t)
	closeStagingWrites(h)
	h.s.writeMu.Lock()
	codes := make(chan int, 2)
	for range 2 {
		go func() {
			codes <- h.do(goliveAdmin, http.MethodPost, "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": 0}).Code
		}()
	}
	time.Sleep(100 * time.Millisecond)
	h.s.writeMu.Unlock()
	got := []int{<-codes, <-codes}
	slices.Sort(got)
	if got[0] != http.StatusOK || got[1] != http.StatusGone {
		t.Errorf("concurrent applies answered %v", got)
	}
}

func TestRestorePendingRecheckedInLock(t *testing.T) {
	h := newHarness(t)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan map[string]any)
	go func() { done <- inFlight(h, synthProd, ulid(1), articleBody("Rust async runtimes")) }()
	waitFor(t, func() bool { return h.emb.embedded("Rust async runtimes") })
	h.pol.SetGoliveAt(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC))
	close(block)
	if out := <-done; out["code"] != "restore_pending" || out["http_status"] != http.StatusServiceUnavailable {
		t.Errorf("write released after golive_at appeared = %v", out)
	}
	if _, ok := h.raw(recordKey(ulid(1))); ok {
		t.Error("the write landed")
	}
}
