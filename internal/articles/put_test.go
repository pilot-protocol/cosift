package articles

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestStubFillKeepsIdentity(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	stub := article(h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated))
	if stub["version"] != 0.0 || stub["status"] != "pending" {
		t.Fatalf("stub = %v", stub)
	}
	if got := h.call(wiki, http.MethodGet, "/v1/articles/by-slug/rust-async-runtimes", nil, http.StatusOK); got["status"] != "pending" || len(got) != 6 {
		t.Errorf("stub projection = %v", got)
	}
	h.expectError(wiki, http.MethodGet, "/v1/articles/"+id, nil, http.StatusNotFound, "not_found")
	h.clock.advance(time.Hour)
	out := h.put(synthProd, id, with(articleBody("Rust async runtimes"), "expected_version", 0, "topic_ids", []string{topic(2)}), http.StatusOK)
	filled := article(out)
	if out["result"] != "filled" || filled["id"] != id || filled["slug"] != stub["slug"] || filled["created_at"] != stub["created_at"] || filled["version"] != 1.0 {
		t.Fatalf("fill = %v (stub %v)", out, stub)
	}
	if filled["updated_at"] == stub["created_at"] {
		t.Error("updated_at not moved by the fill")
	}
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if full["title"] != "Rust async runtimes" || !slices.Equal(toStrings(full["topic_ids"]), []string{topic(1), topic(2)}) {
		t.Errorf("filled record = %v", full)
	}
	if h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A different lead. It is new."), http.StatusOK)["result"] != "updated" {
		t.Error("second PUT was not an update")
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestConditionalFills(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	h.call(resolverProd, http.MethodDelete, "/v1/articles/"+id, nil, http.StatusOK)
	out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+id, with(articleBody("Rust async runtimes"), "expected_version", 0), http.StatusConflict, "version_conflict")
	if _, ok := out["current_version"]; ok {
		t.Errorf("absent record reported a version: %v", out)
	}
	if _, ok := h.raw(recordKey(id)); ok {
		t.Fatal("conditional fill created the record")
	}
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), with(articleBody("Go memory model"), "expected_version", 3), http.StatusConflict, "version_conflict"); out["current_version"] != 0.0 {
		t.Errorf("stub version conflict = %v", out)
	}
	h.put(synthProd, ulid(3), articleBody("Kafka consumer groups"), http.StatusCreated)
	h.put(synthProd, ulid(3), with(articleBody("Kafka consumer groups"), "lead", "Changed lead. Version two.", "expected_version", 1), http.StatusOK)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), with(articleBody("Kafka consumer groups"), "lead", "Third lead. Stale writer.", "expected_version", 1), http.StatusConflict, "version_conflict"); out["current_version"] != 2.0 {
		t.Errorf("stale update = %v", out)
	}
}

func TestRetriedFillIsUnchanged(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	fill := with(articleBody("Rust async runtimes"), "expected_version", 0)
	first := h.put(synthProd, id, fill, http.StatusOK)
	retry := h.put(synthProd, id, fill, http.StatusOK)
	if first["result"] != "filled" || retry["result"] != "unchanged" || article(retry)["version"] != 1.0 {
		t.Errorf("fill %v, retry %v", first, retry)
	}
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+id, with(fill, "lead", "Regenerated lead. It differs."), http.StatusConflict, "version_conflict"); out["current_version"] != 1.0 {
		t.Errorf("regenerated retry = %v", out)
	}
}

func TestEnvCheckedBeforeValidation(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(1), with(articleBody("Rust async runtimes"), "vertical", "bad"), http.StatusForbidden, "env_mismatch")
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.expectError(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("How do I configure nginx", topic(2)), http.StatusForbidden, "env_mismatch")
}

func TestIdempotentPut(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	body := with(articleBody("Rust async runtimes"), "aliases", []string{"Async executors in Rust"})
	first := h.put(synthProd, id, body, http.StatusCreated)
	again := h.put(synthProd, id, body, http.StatusOK)
	if again["result"] != "unchanged" || article(again)["version"] != 1.0 || article(again)["content_sha256"] != article(first)["content_sha256"] {
		t.Errorf("retry = %v", again)
	}
	if h.put(synthProd, id, with(body, "aliases", nil), http.StatusOK)["result"] != "unchanged" {
		t.Error("a PUT re-sending a subset of the merged aliases was not unchanged")
	}
	lower, upper := versionPrefix(id)
	if n := countRange(h, lower, upper); n != 0 {
		t.Errorf("%d versions after idempotent retries", n)
	}
}

func TestModerationLockedPut(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(1), "reject", "quality", http.StatusOK)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"), http.StatusConflict, "moderation_locked"); out["current_status"] != "rejected" {
		t.Errorf("rejected = %v", out)
	}
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.moderate(dashProd, ulid(2), "tombstone", "privacy", http.StatusOK)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), articleBody("Go memory model"), http.StatusConflict, "moderation_locked"); out["current_status"] != "tombstoned" {
		t.Errorf("tombstoned = %v", out)
	}
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(1)), http.StatusConflict, "exists")
}

func TestTightenNeverLoosen(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	body := articleBody("Rust async runtimes")
	lead := func(n int) map[string]any { return with(body, "lead", fmt.Sprintf("Lead number %d. It changes.", n)) }
	h.put(synthProd, id, body, http.StatusCreated)
	if a := article(h.put(synthProd, id, with(lead(1), "status", "held"), http.StatusOK)); a["status"] != "held" {
		t.Fatalf("tighten = %v", a)
	}
	if a := article(h.put(synthProd, id, lead(2), http.StatusOK)); a["status"] != "held" {
		t.Errorf("a PUT loosened held: %v", a)
	}
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	if a := article(h.put(synthProd, id, lead(3), http.StatusOK)); a["status"] != "published" {
		t.Errorf("approval not preserved: %v", a)
	}
	if a := article(h.put(synthProd, id, with(lead(4), "status", "held"), http.StatusOK)); a["status"] != "held" {
		t.Errorf("rebuild triaged held: %v", a)
	}
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	if out := h.put(synthProd, id, with(lead(4), "status", "held"), http.StatusOK); out["result"] != "unchanged" || article(out)["status"] != "held" {
		t.Errorf("same content requesting held: %v", out)
	}
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if m := full["moderation"].(map[string]any); m["action"] != "approve" {
		t.Errorf("moderation = %v", m)
	}
}

func TestTitleImmutable(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async executors"), http.StatusUnprocessableEntity, "invalid_field")
	if out["field"] != "title" || out["rule"] != "title_immutable" {
		t.Errorf("article = %v", out)
	}
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), articleBody("Go memory models"), http.StatusUnprocessableEntity, "invalid_field"); out["rule"] != "title_immutable" {
		t.Errorf("stub fill = %v", out)
	}
}

func TestTopicClaims(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), with(articleBody("Go memory model"), "topic_ids", []string{topic(2), topic(1)}), http.StatusConflict, "topic_claimed")
	if out["article_id"] != ulid(1) || out["claimed_status"] != "published" || out["other_env"] != nil {
		t.Errorf("same env = %v", out)
	}
	if _, ok := h.raw(recordKey(ulid(2))); ok {
		t.Error("refused create was written")
	}
	out = h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(3), with(articleBody("Go memory model"), "topic_ids", []string{topic(1)}), http.StatusConflict, "topic_claimed")
	if out["other_env"] != true || out["article_id"] != nil || out["claimed_status"] != nil {
		t.Errorf("other env = %v", out)
	}
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(4), stubBodyFor("Kafka consumer groups", topic(1)), http.StatusConflict, "topic_claimed")
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusOK)
}

func TestBlockedTitles(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)

	out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), articleBody("Rust Async Runtimes"), http.StatusConflict, "blocked")
	if out["article_id"] != ulid(1) || out["claimed_status"] != "tombstoned" {
		t.Errorf("base slug = %v", out)
	}
	if out := h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Rust async runtimes"), http.StatusConflict, "blocked"); out["other_env"] != true || out["article_id"] != nil {
		t.Errorf("other env = %v", out)
	}
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(4), stubBodyFor("Rust async runtimes", topic(4)), http.StatusConflict, "blocked")

	h.emb.set("Rust asynchronous executors", mix(1, 0, 0.8))
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(5), articleBody("Rust asynchronous executors"), http.StatusConflict, "blocked")
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(6), stubBodyFor("Rust asynchronous executors", topic(6)), http.StatusCreated)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(6), articleBody("Rust asynchronous executors"), http.StatusConflict, "blocked")

	h.moderate(dashProd, ulid(1), "erase_fingerprint", "privacy", http.StatusOK)
	h.put(synthProd, ulid(5), articleBody("Rust asynchronous executors"), http.StatusCreated)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(7), articleBody("Rust async runtimes"), http.StatusConflict, "blocked")

	h.put(synthProd, ulid(8), with(articleBody("Go memory model"), "status", "held"), http.StatusCreated)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(9), articleBody("Go memory model"), http.StatusConflict, "blocked")
}

func TestEnvMismatchBeforeConflict(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)
	h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"), http.StatusForbidden, "env_mismatch")
	h.expectError(synthStaging, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Other title here"), http.StatusForbidden, "env_mismatch")
	h.expectError(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Rust async runtimes", topic(1)), http.StatusForbidden, "env_mismatch")

	h.put(synthStaging, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.moderate(dashStaging, ulid(2), "reject", "quality", http.StatusOK)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), articleBody("Go memory model"), http.StatusForbidden, "env_mismatch")
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), with(articleBody("Go memory model"), "expected_version", 9), http.StatusForbidden, "env_mismatch")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(2)+"/status", map[string]any{"action": "hold", "by": "op"}, http.StatusForbidden, "env_mismatch")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(2)+"/status", map[string]any{"action": "approve", "by": "op"}, http.StatusForbidden, "env_mismatch")

	h.call(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(3), stubBodyFor("Kafka consumer groups", topic(3)), http.StatusCreated)
	h.expectError(resolverProd, http.MethodDelete, "/v1/articles/"+ulid(3), nil, http.StatusForbidden, "env_mismatch")
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(3), stubBodyFor("Other stub title", topic(3)), http.StatusForbidden, "env_mismatch")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(3)+"/status", map[string]any{"action": "delete_stub", "reason_code": "other", "by": "op"}, http.StatusForbidden, "env_mismatch")
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Kafka consumer groups"), http.StatusForbidden, "env_mismatch")

	if got := h.put(synthProd, ulid(4), with(articleBody("Python packaging tools"), "prelive", true), http.StatusCreated); article(got)["prelive"] != false {
		t.Errorf("prelive taken from the body: %v", got)
	}
	if got := h.put(synthStaging, ulid(5), with(articleBody("Python typing protocols"), "prelive", false), http.StatusCreated); article(got)["prelive"] != true {
		t.Errorf("staging create not prelive: %v", got)
	}
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(6), with(stubBodyFor("Stub title here", topic(6)), "prelive", false), http.StatusBadRequest, "unknown_field")
	h.expectError(dashProd, http.MethodPost, "/v1/articles/"+ulid(4)+"/status", map[string]any{"action": "hold", "by": "op", "prelive": true}, http.StatusBadRequest, "unknown_field")
}

func TestMergeCaps(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	var aliases, topics []string
	for i := range 30 {
		aliases = append(aliases, fmt.Sprintf("Alias variant %d", i))
		topics = append(topics, topic(i))
	}
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "aliases", aliases, "topic_ids", topics[:10]), http.StatusCreated)
	more := []string{"Rust async runtimes", "Extra alias one", "Extra alias two", "Extra alias three", "Extra alias four", "Extra alias five"}
	out := h.put(synthProd, id, with(articleBody("Rust async runtimes"), "aliases", append([]string{aliases[0]}, more...), "lead", "A new lead. Version two."), http.StatusOK)
	if d := out["dropped"].(map[string]any); d["aliases"] != 3.0 || d["topic_ids"] != 0.0 {
		t.Errorf("dropped = %v", d)
	}
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	got := toStrings(full["aliases"])
	if len(got) != 32 || !slices.Contains(got, "Extra alias two") || slices.Contains(got, "Extra alias three") || slices.Contains(got, "Rust async runtimes") {
		t.Errorf("aliases = %v", got)
	}
	var many []string
	for i := range 250 {
		many = append(many, topic(1000+i))
	}
	out = h.put(synthProd, id, with(articleBody("Rust async runtimes"), "topic_ids", many, "lead", "A third lead. Version three."), http.StatusOK)
	if d := out["dropped"].(map[string]any); d["topic_ids"] != 4.0 {
		t.Errorf("dropped topics = %v", d)
	}
	if _, ok := h.raw(topicKey(topic(1000 + 249))); ok {
		t.Error("a dropped topic was claimed")
	}
	if _, ok := h.raw(topicKey(topic(1000 + 245))); !ok {
		t.Error("a kept topic was not claimed")
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+id, with(articleBody("Rust async runtimes"), "aliases", append(aliases, more...)), http.StatusUnprocessableEntity, "invalid_field")
}

func TestVersionsServedAndPruned(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	for i := 2; i <= 8; i++ {
		h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", fmt.Sprintf("Lead of version %d. It changes.", i)), http.StatusOK)
	}
	out := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusOK)
	vs := out["versions"].([]any)
	if out["current"] != 8.0 || len(vs) != 5 || vs[0].(map[string]any)["version"] != 7.0 || vs[4].(map[string]any)["version"] != 3.0 {
		t.Fatalf("versions = %v", out)
	}
	if v := vs[0].(map[string]any); v["content_sha256"] == nil || v["models"] == nil || v["build"] == nil || v["quality_tier"] != "strong" {
		t.Errorf("summary = %v", v)
	}
	old := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id+"/3", nil, http.StatusOK)
	if old["version"] != 3.0 || old["lead"] != "Lead of version 3. It changes." {
		t.Errorf("version 3 = %v", old)
	}
	for _, v := range []string{"1", "2", "8", "0", "x", "03"} {
		h.expectError(dashProd, http.MethodGet, "/v1/article-versions/"+id+"/"+v, nil, http.StatusNotFound, "not_found")
	}
	h.expectError(wiki, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusForbidden, "missing_scope")
	h.expectError(dashProd, http.MethodGet, "/v1/article-versions/"+ulid(9), nil, http.StatusNotFound, "not_found")
}

func TestSlugAssignmentOnCreate(t *testing.T) {
	h := newHarness(t)
	for i, want := range []string{"rust-async-runtimes", "rust-async-runtimes-2", "rust-async-runtimes-3"} {
		if got := article(h.put(synthProd, ulid(i+1), articleBody("Rust async runtimes"), http.StatusCreated))["slug"]; got != want {
			t.Errorf("create %d slug %v, want %s", i, got, want)
		}
	}
	if got := article(h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(9), stubBodyFor("Index", topic(9)), http.StatusCreated))["slug"]; got != "index-2" {
		t.Errorf("reserved slug = %v", got)
	}
}

func TestConcurrentPuts(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	var wg sync.WaitGroup
	const n = 8
	codes := make([]int, n)
	for i := range n {
		wg.Go(func() {
			rec := h.do(synthProd, http.MethodPut, "/v1/articles/"+id, with(articleBody("Rust async runtimes"),
				"lead", fmt.Sprintf("Concurrent lead %d. It differs.", i), "aliases", []string{fmt.Sprintf("Concurrent alias %c", 'a'+i)}))
			codes[i] = rec.Code
		})
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusOK {
			t.Errorf("PUT %d: %d", i, c)
		}
	}
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)
	if full["version"] != float64(n+1) || len(toStrings(full["aliases"])) != n {
		t.Errorf("version %v aliases %v", full["version"], full["aliases"])
	}
	_, _, rows, err := decodeEmbeddings(mustRaw(t, h, embeddingKey(id)))
	if err != nil || len(rows) != 2+n {
		t.Errorf("stored rows %d, %v", len(rows), err)
	}

	for i := range n {
		wg.Go(func() {
			if rec := h.do(synthProd, http.MethodPut, "/v1/articles/"+ulid(100+i), articleBody("Go memory model")); rec.Code != http.StatusCreated {
				t.Errorf("create %d: %d", i, rec.Code)
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	h.s.view(func(ix *index) {
		for i := range n {
			if r := ix.recs[ulid(100+i)]; r != nil {
				seen[r.Slug] = true
			}
		}
	})
	if len(seen) != n || !seen["go-memory-model"] || !seen[fmt.Sprintf("go-memory-model-%d", n)] {
		t.Errorf("slugs = %v", seen)
	}
}

func countRange(h *harness, lower, upper []byte) int {
	it, err := h.ps.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		h.t.Fatal(err)
	}
	defer it.Close()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}

func mustRaw(t *testing.T, h *harness, key []byte) []byte {
	t.Helper()
	v, ok := h.raw(key)
	if !ok {
		t.Fatalf("key %q absent", key)
	}
	return v
}

func TestPutValidation(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	base := articleBody("Rust async runtimes")
	cases := []struct {
		body        map[string]any
		field, rule string
	}{
		{with(base, "status", nil), "status", "required"},
		{with(base, "status", "rejected"), "status", "enum"},
		{with(base, "title", "My startup's cloud costs"), "title", "first_second_person"},
		{with(base, "title", "!!!"), "title", "slug_empty"},
		{with(base, "lead", ""), "lead", "length"},
		{with(base, "lead", "Soft­hyphen lead."), "lead", "char_class"},
		{with(base, "body_md", "# Title\n\n## Key facts\n\n- x [1]\n"), "body_md", "h1"},
		{with(base, "body_md", strings.Repeat("word ", 400)+"[1]"), "body_md", "key_facts"},
		{with(base, "body_md", "## Key facts\n\n"+strings.Repeat("word ", 400)+"[2]"), "body_md", "citation_marker"},
		{with(base, "body_md", "## Key facts\n\n"+strings.Repeat("word ", 400)), "citations[0]", "unreferenced"},
		{with(base, "body_md", "## Key facts\n\n- short [1]\n"), "body_md", "word_count"},
		{with(base, "citations", []any{}), "citations", "required"},
		{with(base, "citations", []any{map[string]any{"n": 2, "url": "https://example.org/doc", "title": "D", "host": "example.org", "quote": "q", "content_sha": strings.Repeat("a", 64)}}), "citations[0].n", "sequence"},
		{with(base, "citations", []any{map[string]any{"n": 1, "url": "ftp://example.org/doc", "title": "D", "host": "example.org", "quote": "q", "content_sha": strings.Repeat("a", 64)}}), "citations[0].url", "url"},
		{with(base, "citations", []any{map[string]any{"n": 1, "url": "https://example.org/doc", "title": "D", "host": "Example.org", "quote": "q", "content_sha": strings.Repeat("a", 64)}}), "citations[0].host", "host_mismatch"},
		{with(base, "citations", []any{map[string]any{"n": 1, "url": "https://example.org/doc", "title": "D", "host": "example.org", "quote": "", "content_sha": strings.Repeat("a", 64)}}), "citations[0].quote", "length"},
		{with(base, "citations", []any{map[string]any{"n": 1, "url": "https://example.org/doc", "title": "D", "host": "example.org", "quote": "q", "content_sha": "xyz"}}), "citations[0].content_sha", "pattern"},
		{with(base, "citations", []any{map[string]any{"n": 1, "url": "https://example.org/missing", "title": "D", "host": "example.org", "quote": "q", "content_sha": strings.Repeat("a", 64)}}), "citations[0].url", "not_in_corpus"},
		{with(base, "quality_tier", "great"), "quality_tier", "enum"},
		{with(base, "vertical", "news"), "vertical", "enum"},
		{with(base, "sensitivity", nil), "sensitivity", "required"},
		{with(base, "sensitivity", map[string]any{"class": "gossip", "reason": ""}), "sensitivity.class", "enum"},
		{with(base, "sensitivity", map[string]any{"class": "allegation", "reason": "x"}), "status", "sensitive_must_hold"},
		{with(base, "ai_generated", false), "ai_generated", "must_be_true"},
		{with(base, "aliases", []string{"my rust project"}), "aliases[0]", "first_second_person"},
		{with(base, "topic_ids", []string{"ABC"}), "topic_ids[0]", "pattern"},
		{with(base, "cluster_ids", []string{"x1"}), "cluster_ids[0]", "pattern"},
		{with(base, "models", nil), "models", "required"},
		{with(base, "models", map[string]any{"triage": strings.Repeat("m", 101), "writer": "w"}), "models.triage", "length"},
		{with(base, "models", map[string]any{"triage": "t", "writer": "bad\u200b"}), "models.writer", "char_class"},
		{with(base, "build", nil), "build", "required"},
		{with(base, "build", map[string]any{"retrieval_params": []int{1}}), "build.retrieval_params", "object"},
		{with(base, "build", map[string]any{"retrieval_params": map[string]any{"k": strings.Repeat("x", 2100)}}), "build.retrieval_params", "length"},
	}
	for _, c := range cases {
		out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+id, c.body, http.StatusUnprocessableEntity, "invalid_field")
		if out["field"] != c.field || out["rule"] != c.rule {
			t.Errorf("%s/%s: got %v/%v", c.field, c.rule, out["field"], out["rule"])
		}
	}
	h.put(synthProd, id, with(base, "sensitivity", map[string]any{"class": "allegation", "reason": "x"}, "status", "held"), http.StatusCreated)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), with(base, "bogus", 1), http.StatusBadRequest, "unknown_field")
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), with(base, "lead", 5), http.StatusBadRequest, "invalid_body")
	owned := with(base, "title", "Go memory model", "slug", "chosen-slug", "version", 99, "promoted", false, "created_at", "1999-01-01T00:00:00Z",
		"content_sha256", "x", "moderation", map[string]any{"action": "approve"}, "schema", 7, "id", ulid(9), "prelive", true, "base_slug", "x",
		"updated_at", "x", "status_changed_at", "x", "residue", true, "reason_code", "privacy", "tombstoned_at", "x")
	if a := article(h.put(synthProd, ulid(3), owned, http.StatusCreated)); a["slug"] != "go-memory-model" || a["version"] != 1.0 || a["id"] != ulid(3) || a["prelive"] != false || a["created_at"] == "1999-01-01T00:00:00Z" {
		t.Errorf("engine-owned fields honoured: %v", a)
	}
	if full := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(3), nil, http.StatusOK); full["promoted"] != true || full["moderation"] != nil || full["schema"] != 1.0 {
		t.Errorf("stored = %v", full)
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/not-a-ulid", base, http.StatusNotFound, "not_found")
}

func TestStubRules(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	big := stubBodyFor("Rust async runtimes", topic(1))
	var topics []string
	for i := range 256 {
		topics = append(topics, topic(i))
	}
	big["topic_ids"] = topics
	big["title"] = "Rust async runtimes" + strings.Repeat(" x", 0)
	padded := strings.Replace(mustJSON(t, big), `"status"`, strings.Repeat(" ", 16<<10)+`"status"`, 1)
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+id, padded, http.StatusRequestEntityTooLarge, "body_too_large")
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+id, with(stubBodyFor("Rust async runtimes", topic(1)), "lead", "x"), http.StatusBadRequest, "unknown_field")
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+id, with(stubBodyFor("Rust async runtimes"), "topic_ids", []string{}), http.StatusUnprocessableEntity, "invalid_field")
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("How do I configure nginx", topic(1)), http.StatusUnprocessableEntity, "invalid_field")

	h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	if out := h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1), topic(2)), http.StatusOK); out["result"] != "unchanged" {
		t.Errorf("identical stub = %v", out)
	}
	if _, ok := h.raw(topicKey(topic(2))); !ok {
		t.Error("merged stub topic not claimed")
	}
	if out := h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async executors", topic(1)), http.StatusConflict, "exists"); out["current_status"] != "pending" {
		t.Errorf("different title = %v", out)
	}
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	if out := h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(3)), http.StatusConflict, "exists"); out["current_status"] != "published" {
		t.Errorf("stub over article = %v", out)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestStubTokenCannotPublish(t *testing.T) {
	h := newHarness(t)
	if out := h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"), http.StatusForbidden, "missing_scope"); out["detail"] != "requires scope articles:write" {
		t.Errorf("detail = %v", out["detail"])
	}
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Rust async runtimes", topic(1)), http.StatusForbidden, "missing_scope"); out["detail"] != "requires scope articles:stub" {
		t.Errorf("detail = %v", out["detail"])
	}
	h.expectError(synthProd, http.MethodDelete, "/v1/articles/"+ulid(1), nil, http.StatusForbidden, "missing_scope")
	h.expectError(mcpProd, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"), http.StatusForbidden, "missing_scope")
	if n := h.pol.Charges("resolver-prod") + h.pol.Charges("synth-prod") + h.pol.Charges("mcp-prod"); n != 0 {
		t.Errorf("%d charges for unauthorised writes", n)
	}
}

func TestStubDelete(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+id, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	if out := h.call(resolverProd, http.MethodDelete, "/v1/articles/"+id, nil, http.StatusOK); out["result"] != "deleted" {
		t.Errorf("delete = %v", out)
	}
	for _, k := range [][]byte{recordKey(id), slugKey("rust-async-runtimes"), topicKey(topic(1))} {
		if _, ok := h.raw(k); ok {
			t.Errorf("key %q survived", k)
		}
	}
	h.expectError(resolverProd, http.MethodDelete, "/v1/articles/"+id, nil, http.StatusNotFound, "not_found")
	if a := article(h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)); a["slug"] != "rust-async-runtimes" {
		t.Errorf("slug not released: %v", a)
	}
	h.put(synthProd, ulid(2), articleBody("Rust async runtimes"), http.StatusOK)
	if out := h.expectError(resolverProd, http.MethodDelete, "/v1/articles/"+ulid(2), nil, http.StatusConflict, "not_pending"); out["current_status"] != "published" {
		t.Errorf("not pending = %v", out)
	}
}

func TestWriteBudget(t *testing.T) {
	h := newHarness(t)
	h.pol.SetWriteBudget("synth-prod", 2, 30*time.Minute)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), with(articleBody("Rust async runtimes"), "vertical", "bad"), http.StatusUnprocessableEntity, "invalid_field")
	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	h.emb.mu.Lock()
	callsBefore := len(h.emb.calls)
	h.emb.mu.Unlock()
	rec := h.do(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Kafka consumer groups"))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), "write_budget") || rec.Header().Get("Retry-After") != "1800" {
		t.Fatalf("exhausted: %d %s %q", rec.Code, rec.Body.String(), rec.Header().Get("Retry-After"))
	}
	h.emb.mu.Lock()
	callsAfter := len(h.emb.calls)
	h.emb.mu.Unlock()
	if callsAfter != callsBefore {
		t.Error("a refused write reached the embedder")
	}
	if got := h.pol.Charges("synth-prod"); got != 3 {
		t.Errorf("charges = %d, want 3", got)
	}
	h.match(synthProd, map[string]any{"q": "x"}, http.StatusOK)
	h.call(wiki, http.MethodGet, "/v1/articles", nil, http.StatusOK)

	h.pol.SetWriteBudget("resolver-prod", 1, time.Hour)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(4), stubBodyFor("Rust async runtimes", topic(4)), http.StatusCreated)
	h.expectError(resolverProd, http.MethodDelete, "/v1/articles/"+ulid(4), nil, http.StatusTooManyRequests, "write_budget")
	if got := h.pol.Charges("resolver-prod"); got != 2 {
		t.Errorf("resolver charges = %d", got)
	}
}

func TestWritesFrozen(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(2), stubBodyFor("Go memory model", topic(2)), http.StatusCreated)
	h.pol.SetWritesFrozen(true)
	charged := h.pol.Charges("synth-prod")
	rec := h.do(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), articleBody("Kafka consumer groups"))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "writes_frozen") || rec.Header().Get("Retry-After") != "60" {
		t.Fatalf("frozen PUT: %d %s", rec.Code, rec.Body.String())
	}
	h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(4), stubBodyFor("Python packaging tools", topic(4)), http.StatusServiceUnavailable, "writes_frozen")
	h.expectError(resolverProd, http.MethodDelete, "/v1/articles/"+ulid(2), nil, http.StatusServiceUnavailable, "writes_frozen")
	if h.pol.Charges("synth-prod") != charged {
		t.Error("a frozen write was charged")
	}
	h.moderate(dashProd, ulid(1), "hold", "", http.StatusOK)
	h.moderate(dashProd, ulid(2), "delete_stub", "other", http.StatusOK)
	if out := buildMatch(h, nil, "x"); out["writes"] != "frozen" {
		t.Errorf("writes = %v", out["writes"])
	}

	h.pol.SetWritesFrozen(false)
	block := make(chan struct{})
	h.emb.setBlock(block)
	done := make(chan int)
	go func() {
		done <- h.do(synthProd, http.MethodPut, "/v1/articles/"+ulid(5), articleBody("Python typing protocols")).Code
	}()
	waitFor(t, func() bool { return h.emb.embedded("Python typing protocols") })
	h.pol.SetWritesFrozen(true)
	close(block)
	if code := <-done; code != http.StatusServiceUnavailable {
		t.Errorf("write frozen mid-request: %d", code)
	}
	if _, ok := h.raw(recordKey(ulid(5))); ok {
		t.Error("write frozen mid-request was committed")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A15: validation runs after the moderation lock and a same-hash unchanged, before version_conflict.
func TestValidationAfterIdempotency(t *testing.T) {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), with(articleBody("Rust async runtimes"), "lead", ""), http.StatusConflict, "moderation_locked")

	body := articleBody("Go memory model")
	h.put(synthProd, ulid(2), body, http.StatusCreated)
	delete(h.corpus.urls, "https://example.org/doc")
	if out := h.put(synthProd, ulid(2), body, http.StatusOK); out["result"] != "unchanged" {
		t.Errorf("same-hash retry after its citation left the corpus = %v", out)
	}
	stale := with(body, "lead", "A changed lead. Version two.", "expected_version", 7)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), stale, http.StatusUnprocessableEntity, "invalid_field"); out["rule"] != "not_in_corpus" {
		t.Errorf("changed body with a stale version = %v", out)
	}
	h.corpus.urls["https://example.org/doc"] = true
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), stale, http.StatusConflict, "version_conflict")
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(3), with(articleBody("Kafka consumer groups"), "lead", "", "expected_version", 0), http.StatusUnprocessableEntity, "invalid_field")
}

func TestPutEmbedFailure(t *testing.T) {
	h := newHarness(t)
	h.emb.fail = errors.New("embedder down")
	rec := h.do(synthProd, http.MethodPut, "/v1/articles/"+ulid(1), articleBody("Rust async runtimes"))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "embedder_unavailable") || rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("%d %s %q", rec.Code, rec.Body.String(), rec.Header().Get("Retry-After"))
	}
	if keys := rKeys(h); len(keys) != 1 {
		t.Errorf("keys written: %q", keys)
	}
	if n := h.pol.Charges("synth-prod"); n != 1 {
		t.Errorf("charges = %d", n)
	}
}

func TestStubMergeRefusesClaimedTopic(t *testing.T) {
	h := newHarness(t)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	h.put(synthProd, ulid(2), with(articleBody("Go memory model"), "topic_ids", []string{topic(2)}), http.StatusCreated)
	if out := h.expectError(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Rust async runtimes", topic(1), topic(2)), http.StatusConflict, "topic_claimed"); out["article_id"] != ulid(2) {
		t.Errorf("stub merge = %v", out)
	}
	if owner := string(mustRaw(t, h, topicKey(topic(2)))); owner != ulid(2) {
		t.Errorf("claim moved to %s", owner)
	}
}

func TestSameHashTightenSetsStatusChangedAt(t *testing.T) {
	h := newHarness(t)
	body := articleBody("Rust async runtimes")
	created := article(h.put(synthProd, ulid(1), body, http.StatusCreated))
	h.clock.advance(time.Hour)
	h.put(synthProd, ulid(1), with(body, "status", "held"), http.StatusOK)
	full := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK)
	if full["status"] != "held" || full["status_changed_at"] == created["created_at"] || full["version"] != 1.0 {
		t.Errorf("tightened record = %v", full)
	}
}
