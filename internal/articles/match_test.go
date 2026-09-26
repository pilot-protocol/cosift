package articles

import (
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServeMatchVerdicts(t *testing.T) {
	h := newHarness(t)
	if out := h.match(mcpProd, map[string]any{"q": "anything"}, http.StatusOK); out["verdict"] != "none" || out["score"] != nil {
		t.Fatalf("empty store = %v", out)
	}
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	cases := []struct {
		q       string
		vec     []float32
		verdict string
	}{
		{"q covered", mix(1, 0, 0.9), "covered"},
		{"q related", mix(1, 0, 0.76), "related"},
		{"q none", mix(1, 0, 0.5), "none"},
	}
	for _, c := range cases {
		h.emb.set(c.q, c.vec)
		out := h.match(mcpProd, map[string]any{"q": c.q}, http.StatusOK)
		if out["verdict"] != c.verdict {
			t.Errorf("%s: verdict %v", c.q, out["verdict"])
		}
		if sc, _ := out["score"].(float64); sc < 0.4 {
			t.Errorf("%s: score %v", c.q, out["score"])
		}
		a, _ := out["article"].(map[string]any)
		switch c.verdict {
		case "covered":
			if out["match"] != "embedding" || a["body_md"] == nil || a["lead"] == nil || a["topic_ids"] != nil {
				t.Errorf("covered = %v", out)
			}
		case "related":
			if len(a) != 3 || a["slug"] != "rust-async-runtimes" || a["title"] != "Rust async runtimes" {
				t.Errorf("related article = %v", a)
			}
		case "none":
			if a != nil || out["match"] != nil {
				t.Errorf("none = %v", out)
			}
		}
	}
}

func TestServeNeverMatchesIneligible(t *testing.T) {
	h := newHarness(t)
	titles := []string{"Held article title", "Rejected article title", "Tombstoned article title", "Prelive article title"}
	for i, title := range titles {
		h.emb.set(title, axis(i+1))
		h.emb.set("q "+title, mix(i+1, 0, 0.95))
	}
	h.put(synthProd, ulid(1), with(articleBody(titles[0]), "status", "held"), http.StatusCreated)
	h.put(synthProd, ulid(2), articleBody(titles[1]), http.StatusCreated)
	h.moderate(dashProd, ulid(2), "reject", "quality", http.StatusOK)
	h.put(synthProd, ulid(3), articleBody(titles[2]), http.StatusCreated)
	h.moderate(dashProd, ulid(3), "tombstone", "privacy", http.StatusOK)
	h.put(synthStaging, ulid(4), articleBody(titles[3]), http.StatusCreated)
	h.emb.set("Pending stub title", axis(5))
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(5), stubBodyFor("Pending stub title", topic(5)), http.StatusCreated)
	h.emb.set("q Pending stub title", axis(5))

	for _, title := range append(titles, "Pending stub title") {
		out := h.match(mcpProd, map[string]any{"q": "q " + title}, http.StatusOK)
		if out["verdict"] != "none" {
			t.Errorf("prod match for %q = %v", title, out)
		}
	}
	if out := h.match(mcpStaging, map[string]any{"q": "q " + titles[3]}, http.StatusOK); out["verdict"] != "covered" {
		t.Errorf("staging principal does not see a prelive published article: %v", out)
	}
}

func TestAliasOnlyUpgrades(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	h.emb.set("between", mix(1, 0, 0.76))
	h.emb.set("far", mix(1, 0, 0.5))

	if out := h.match(mcpProd, map[string]any{"q": "between", "topic_id": topic(1)}, http.StatusOK); out["verdict"] != "covered" || out["match"] != "alias" {
		t.Errorf("alias above related = %v", out)
	}
	if out := h.match(mcpProd, map[string]any{"q": "between", "topic_id": topic(9)}, http.StatusOK); out["verdict"] != "related" {
		t.Errorf("unclaimed topic = %v", out)
	}
	if out := h.match(mcpProd, map[string]any{"q": "far", "topic_id": topic(1)}, http.StatusOK); out["verdict"] != "none" {
		t.Errorf("alias hit below related made %v", out)
	}
	h.moderate(dashProd, id, "hold", "", http.StatusOK)
	h.emb.set("near", mix(1, 0, 0.95))
	if out := h.match(mcpProd, map[string]any{"q": "near", "topic_id": topic(1)}, http.StatusOK); out["verdict"] != "none" {
		t.Errorf("alias on a held article = %v", out)
	}
}

func TestMatchEmbedDeadline(t *testing.T) {
	h := newHarness(t)
	h.s.matchTimeout = 50 * time.Millisecond
	block := make(chan struct{})
	h.emb.mu.Lock()
	h.emb.ignoreCtx = true
	h.emb.mu.Unlock()
	h.emb.setBlock(block)
	time.AfterFunc(3*time.Second, func() { close(block) })
	start := time.Now()
	rec := h.do(mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "slow"})
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "embedder_unavailable") || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %v", d)
	}
}

func TestMatchValidation(t *testing.T) {
	h := newHarness(t)
	long := strings.Repeat("a", 513)
	cases := []struct {
		build bool
		body  map[string]any
		field string
	}{
		{false, map[string]any{}, "q"},
		{false, map[string]any{"q": ""}, "q"},
		{false, map[string]any{"q": long}, "q"},
		{false, map[string]any{"q": "line\nbreak"}, "q"},
		{false, map[string]any{"q": "zero​width"}, "q"},
		{false, map[string]any{"q": "x", "topic_id": "XYZ"}, "topic_id"},
		{false, map[string]any{"q": "x", "topic_ids": []string{topic(1)}}, "topic_ids"},
		{false, map[string]any{"q": "x", "self": ulid(1)}, "self"},
		{false, map[string]any{"q": "x", "reader": "short"}, "reader"},
		{false, map[string]any{"q": "x", "purpose": "other"}, "purpose"},
		{true, map[string]any{"q": "x", "purpose": "build", "topic_ids": []string{}}, "topic_ids"},
		{true, map[string]any{"q": "x", "purpose": "build", "topic_ids": []string{"nothex"}}, "topic_ids[0]"},
		{true, map[string]any{"q": "x", "purpose": "build", "topic_id": topic(1)}, "topic_id"},
		{true, map[string]any{"q": "x", "purpose": "build", "self": "bad"}, "self"},
	}
	for _, c := range cases {
		p := mcpProd
		if c.build {
			p = synthProd
		}
		out := h.expectError(p, http.MethodPost, "/v1/articles/match", c.body, http.StatusUnprocessableEntity, "invalid_field")
		if out["field"] != c.field {
			t.Errorf("%v: field %v, want %s", c.body, out["field"], c.field)
		}
	}
	if out := h.expectError(mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "x", "decay": 1}, http.StatusBadRequest, "unknown_field"); out["field"] != "decay" {
		t.Errorf("unknown field = %v", out)
	}
	if strings.Contains(h.do(mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "secret words\n"}).Body.String(), "secret") {
		t.Error("error body echoes q")
	}
}

func TestReadersCountedOnlyForCountingPrincipal(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("covered q", mix(1, 0, 0.9))
	h.emb.set("related q", mix(1, 0, 0.76))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	readerA, readerB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	h.match(mcpProd, map[string]any{"q": "covered q", "reader": readerA}, http.StatusOK)
	h.match(mcpProd, map[string]any{"q": "covered q", "reader": readerA}, http.StatusOK)
	h.match(mcpProd, map[string]any{"q": "related q", "reader": readerB}, http.StatusOK)
	h.match(mcpStaging, map[string]any{"q": "covered q", "reader": readerB}, http.StatusOK)
	h.match(mcpProd, map[string]any{"q": "covered q"}, http.StatusOK)
	h.match(synthProd, map[string]any{"q": "covered q", "reader": readerB, "purpose": "build"}, http.StatusOK)
	if got := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["readers_7d"]; got != 1.0 {
		t.Fatalf("readers_7d = %v, want 1", got)
	}
	h.match(mcpProd, map[string]any{"q": "covered q", "reader": readerB}, http.StatusOK)
	if got := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK)["readers_7d"]; got != 2.0 {
		t.Fatalf("readers_7d = %v, want 2", got)
	}
}

func TestBuildNeedsWriteOrStub(t *testing.T) {
	h := newHarness(t)
	body := map[string]any{"q": "x", "purpose": "build"}
	if out := h.expectError(mcpProd, http.MethodPost, "/v1/articles/match", body, http.StatusForbidden, "missing_scope"); out["detail"] != "requires scope articles:write" {
		t.Errorf("detail = %v", out["detail"])
	}
	h.expectError(dashProd, http.MethodPost, "/v1/articles/match", body, http.StatusForbidden, "missing_scope")
	h.match(synthProd, body, http.StatusOK)
	h.match(resolverProd, body, http.StatusOK)
}

func buildMatch(h *harness, p any, q string, extra ...any) map[string]any {
	h.t.Helper()
	body := map[string]any{"q": q, "purpose": "build"}
	for i := 0; i+1 < len(extra); i += 2 {
		body[extra[i].(string)] = extra[i+1]
	}
	pr := synthProd
	switch p {
	case "staging":
		pr = synthStaging
	case "resolver":
		pr = resolverProd
	}
	out := h.match(pr, body, http.StatusOK)
	if _, ok := out["score"]; ok {
		h.t.Errorf("build match returned a score: %v", out)
	}
	if a, _ := out["article"].(map[string]any); a != nil {
		for _, k := range []string{"lead", "body_md", "citations"} {
			if _, ok := a[k]; ok {
				h.t.Errorf("build match returned content: %v", a)
			}
		}
	}
	if _, ok := out["writes"]; !ok {
		h.t.Errorf("no writes: %v", out)
	}
	if _, ok := out["thresholds_measured"]; !ok {
		h.t.Errorf("no thresholds_measured: %v", out)
	}
	return out
}

func verdictOf(out map[string]any) (string, string) {
	a, _ := out["article"].(map[string]any)
	id, _ := a["id"].(string)
	return out["verdict"].(string), id
}

func TestBuildVerdictOrder(t *testing.T) {
	h := newHarness(t)
	// Published P claims topic 1; held H scores high against q "held near".
	h.emb.set("Published article title", axis(1))
	h.put(synthProd, ulid(1), with(articleBody("Published article title"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	h.emb.set("Held article title", axis(2))
	h.put(synthProd, ulid(2), with(articleBody("Held article title"), "status", "held", "topic_ids", []string{topic(2)}), http.StatusCreated)
	h.emb.set("Tombstoned article title", axis(3))
	h.put(synthProd, ulid(3), with(articleBody("Tombstoned article title"), "topic_ids", []string{topic(3)}), http.StatusCreated)
	h.moderate(dashProd, ulid(3), "tombstone", "privacy", http.StatusOK)

	h.emb.set("unrelated", axis(20))
	if v, id := verdictOf(buildMatch(h, nil, "unrelated", "topic_ids", []string{topic(3), topic(1)})); v != "blocked" || id != ulid(3) {
		t.Errorf("tombstoned claimant: %s %s", v, id)
	}
	if v, id := verdictOf(buildMatch(h, nil, "unrelated", "topic_ids", []string{topic(2)})); v != "blocked" || id != ulid(2) {
		t.Errorf("held claimant: %s %s", v, id)
	}
	h.emb.set("held near", mix(2, 0, 0.85))
	if v, id := verdictOf(buildMatch(h, nil, "held near")); v != "blocked" || id != ulid(2) {
		t.Errorf("held rows above covered: %s %s", v, id)
	}
	h.emb.set("held related", mix(2, 0, 0.76))
	if v, _ := verdictOf(buildMatch(h, nil, "held related")); v != "none" {
		t.Errorf("held rows below covered: %s", v)
	}
	h.emb.set("tomb reword", mix(3, 0, 0.8))
	if v, id := verdictOf(buildMatch(h, nil, "tomb reword")); v != "blocked" || id != ulid(3) {
		t.Errorf("fingerprint rewording: %s %s", v, id)
	}
	h.emb.set("tomb far", mix(3, 0, 0.6))
	if v, _ := verdictOf(buildMatch(h, nil, "tomb far")); v != "none" {
		t.Errorf("fingerprint beyond related: %s", v)
	}
	if out := buildMatch(h, nil, "Tombstoned article title"); out["verdict"] != "blocked" {
		t.Errorf("base slug of a residue: %v", out)
	}
	h.emb.set("Tombstoned Article Title!", axis(21))
	if v, id := verdictOf(buildMatch(h, nil, "Tombstoned Article Title!")); v != "blocked" || id != ulid(3) {
		t.Errorf("base slug only: %s %s", v, id)
	}
	out := buildMatch(h, nil, "unrelated", "topic_ids", []string{topic(1)})
	if v, id := verdictOf(out); v != "claimed" || id != ulid(1) || article(out)["title"] != "Published article title" || article(out)["status"] != "published" {
		t.Errorf("claimed: %v", out)
	}
	if c := out["claims"].([]any); len(c) != 1 || c[0].(map[string]any)["article_id"] != ulid(1) || c[0].(map[string]any)["status"] != "published" {
		t.Errorf("claims = %v", c)
	}
	h.emb.set("pub near", mix(1, 0, 0.9))
	if out := buildMatch(h, nil, "pub near"); out["verdict"] != "covered" || article(out)["title"] != "Published article title" || article(out)["status"] != "published" {
		t.Errorf("covered: %v", out)
	}
	h.emb.set("pub related", mix(1, 0, 0.76))
	if out := buildMatch(h, nil, "pub related"); out["verdict"] != "related" || article(out)["id"] != ulid(1) {
		t.Errorf("related: %v", out)
	}
	if out := buildMatch(h, nil, "unrelated"); out["verdict"] != "none" || out["article"] != nil || len(out["claims"].([]any)) != 0 {
		t.Errorf("none: %v", out)
	}
}

func TestBuildSelfAndStubs(t *testing.T) {
	h := newHarness(t)
	stub := ulid(1)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+stub, stubBodyFor("Rust async runtimes", topic(1)), http.StatusCreated)
	h.emb.set("rust async stuff", axis(30))
	out := buildMatch(h, "resolver", "rust async stuff", "topic_ids", []string{topic(1)})
	if v, id := verdictOf(out); v != "claimed" || id != stub || article(out)["title"] != "Rust async runtimes" || article(out)["status"] != "pending" {
		t.Errorf("stub claimant: %v", out)
	}
	if v, _ := verdictOf(buildMatch(h, "resolver", "rust async stuff", "topic_ids", []string{topic(1)}, "self", stub)); v != "none" {
		t.Errorf("self claimed: %s", v)
	}
	if out := buildMatch(h, nil, "Rust async runtimes!"); out["verdict"] != "claimed" || article(out)["id"] != stub {
		t.Errorf("same full slug: %v", out)
	}

	h.emb.set("Go memory model", axis(4))
	h.put(synthProd, ulid(2), with(articleBody("Go memory model"), "topic_ids", []string{topic(2)}), http.StatusCreated)
	h.emb.set("go memory near", mix(4, 0, 0.9))
	if v, _ := verdictOf(buildMatch(h, nil, "go memory near", "self", ulid(2))); v != "none" {
		t.Errorf("self covered: %s", v)
	}
	h.moderate(dashProd, ulid(2), "hold", "", http.StatusOK)
	if v, _ := verdictOf(buildMatch(h, nil, "go memory near", "topic_ids", []string{topic(2)}, "self", ulid(2))); v != "none" {
		t.Errorf("held self blocked: %s", v)
	}
	if v, _ := verdictOf(buildMatch(h, nil, "go memory near", "topic_ids", []string{topic(2)})); v != "blocked" {
		t.Errorf("held other not blocked: %s", v)
	}
}

func TestBuildFullSlugIgnoresCut(t *testing.T) {
	h := newHarness(t)
	a := "The quick brown fox jumps over the lazy dog while the lazy dog sleeps under a very tall tree"
	b := "The quick brown fox jumps over the lazy dog while the lazy dog sleeps under a very small tree"
	if Slugify(a) != Slugify(b) {
		t.Fatal("titles must share the cut slug")
	}
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor(a, topic(1)), http.StatusCreated)
	h.emb.set(b, axis(7))
	if v, _ := verdictOf(buildMatch(h, nil, b)); v == "claimed" {
		t.Error("titles that differ only past the cut were merged")
	}
	h.emb.set(a, axis(8))
	if v, _ := verdictOf(buildMatch(h, nil, a)); v != "claimed" {
		t.Errorf("same title: %s", v)
	}
}

func TestBuildOwnEnvironmentOnly(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Prod article title", axis(1))
	h.put(synthProd, ulid(1), with(articleBody("Prod article title"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	h.emb.set("Prod held title", axis(2))
	h.put(synthProd, ulid(2), with(articleBody("Prod held title"), "status", "held", "topic_ids", []string{topic(2)}), http.StatusCreated)
	h.emb.set("near prod", mix(1, 0, 0.9))
	out := buildMatch(h, "staging", "near prod", "topic_ids", []string{topic(1), topic(2)})
	if out["verdict"] != "none" || out["article"] != nil {
		t.Errorf("staging saw prod records: %v", out)
	}
	for _, c := range out["claims"].([]any) {
		cm := c.(map[string]any)
		if cm["other_env"] != true || cm["article_id"] != nil || cm["status"] != nil {
			t.Errorf("claim leaks another environment: %v", cm)
		}
	}
	if len(out["claims"].([]any)) != 2 {
		t.Errorf("claims = %v", out["claims"])
	}
	if v, _ := verdictOf(buildMatch(h, "staging", "Prod held title")); v == "blocked" {
		t.Error("another environment's held base slug blocked the build match")
	}
}

func TestBuildWritesAndThresholds(t *testing.T) {
	h := newHarness(t)
	if out := buildMatch(h, nil, "x"); out["writes"] != "open" || out["thresholds_measured"] != false {
		t.Errorf("default: %v", out)
	}
	h.pol.SetWriteBudget("synth-prod", 0, time.Hour)
	if out := buildMatch(h, nil, "x"); out["writes"] != "budget_exhausted" {
		t.Errorf("exhausted: %v", out)
	}
	h.pol.SetWritesFrozen(true)
	if out := buildMatch(h, nil, "x"); out["writes"] != "frozen" {
		t.Errorf("frozen: %v", out)
	}
	writeThresholds(t, h.dir+"/articles.json", `{"schema_version": 1, "theta_covered": 0.9, "theta_related": 0.8, "measured": true}`)
	if err := h.s.ReloadThresholds(); err != nil {
		t.Fatal(err)
	}
	if out := buildMatch(h, nil, "x"); out["thresholds_measured"] != true {
		t.Errorf("measured: %v", out)
	}
}

func TestReaderSetNotRecreatedAfterTombstone(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.emb.set("Rust async runtimes", axis(1))
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	release, blocked := h.clock.holdNext()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	matched := make(chan int)
	go func() {
		matched <- h.do(mcpProd, http.MethodPost, "/v1/articles/match", map[string]any{"q": "Rust async runtimes", "reader": strings.Repeat("a", 32)}).Code
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		unblock()
		t.Fatal("the match never reached its read count")
	}
	tombstoned := make(chan int)
	go func() {
		tombstoned <- h.do(dashProd, http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": "tombstone", "reason_code": "privacy", "by": "op"}).Code
	}()
	var code int
	select {
	case code = <-tombstoned:
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	if c := <-matched; c != http.StatusOK {
		t.Fatalf("match: %d", c)
	}
	if code == 0 {
		code = <-tombstoned
	}
	if code != http.StatusOK {
		t.Fatalf("tombstone: %d", code)
	}
	if f := h.call(dashProd, http.MethodGet, "/v1/articles/"+id, nil, http.StatusOK); f["readers_7d"] != 0.0 {
		t.Errorf("the residue carries a reader set: %v", f["readers_by_day"])
	}
}

// theta_related, not theta_covered, is the fingerprint threshold, from any environment.
func TestFingerprintThresholdBand(t *testing.T) {
	h := newHarness(t)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("Rust async runtime band", mix(1, 0, 0.76))
	h.put(synthProd, ulid(1), articleBody("Rust async runtimes"), http.StatusCreated)
	h.moderate(dashProd, ulid(1), "tombstone", "privacy", http.StatusOK)
	if v, id := verdictOf(buildMatch(h, nil, "Rust async runtime band")); v != "blocked" || id != ulid(1) {
		t.Errorf("build match at 0.76: %s %s", v, id)
	}
	h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(2), articleBody("Rust async runtime band"), http.StatusConflict, "blocked")

	h.emb.set("Go memory model", axis(2))
	h.emb.set("Go memory model band", mix(2, 0, 0.76))
	h.put(synthStaging, ulid(3), articleBody("Go memory model"), http.StatusCreated)
	h.moderate(dashStaging, ulid(3), "tombstone", "privacy", http.StatusOK)
	if out := h.expectError(synthProd, http.MethodPut, "/v1/articles/"+ulid(4), articleBody("Go memory model band"), http.StatusConflict, "blocked"); out["other_env"] != true || out["article_id"] != nil {
		t.Errorf("prelive fingerprint vs prod PUT = %v", out)
	}
}

func TestBuildPrecedenceAndClaimant(t *testing.T) {
	h := newHarness(t)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(1), stubBodyFor("Kafka consumer groups", topic(1)), http.StatusCreated)
	h.emb.set("Kafka partition assignment", axis(3))
	h.put(synthProd, ulid(2), articleBody("Kafka partition assignment"), http.StatusCreated)
	h.emb.set("kafka q", mix(3, 0, 0.9))
	if v, id := verdictOf(buildMatch(h, nil, "kafka q", "topic_ids", []string{topic(1)})); v != "claimed" || id != ulid(1) {
		t.Errorf("claimed before covered: %s %s", v, id)
	}

	h.clock.advance(time.Minute)
	h.put(synthProd, ulid(4), with(articleBody("Postgres streaming replication"), "topic_ids", []string{topic(12)}), http.StatusCreated)
	h.clock.advance(time.Minute)
	h.put(synthProd, ulid(3), with(articleBody("Postgres logical replication"), "topic_ids", []string{topic(10), topic(11)}), http.StatusCreated)
	if v, id := verdictOf(buildMatch(h, nil, "unrelated", "topic_ids", []string{topic(12), topic(10), topic(11)})); v != "claimed" || id != ulid(3) {
		t.Errorf("most topics: %s %s", v, id)
	}
	h.clock.advance(time.Minute)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(6), stubBodyFor("Redis cluster sharding", topic(20)), http.StatusCreated)
	h.clock.advance(time.Minute)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(5), stubBodyFor("Redis sentinel failover", topic(21)), http.StatusCreated)
	if v, id := verdictOf(buildMatch(h, nil, "unrelated", "topic_ids", []string{topic(21), topic(20)})); v != "claimed" || id != ulid(6) {
		t.Errorf("tie to the earliest created_at: %s %s", v, id)
	}

	h.emb.set("Go memory model", axis(4))
	h.put(synthProd, ulid(7), articleBody("Go memory model"), http.StatusCreated)
	h.moderate(dashProd, ulid(7), "reject", "quality", http.StatusOK)
	h.emb.set("go memory q", mix(4, 0, 0.85))
	out := buildMatch(h, nil, "go memory q")
	if v, id := verdictOf(out); v != "blocked" || id != ulid(7) {
		t.Errorf("rejected rows: %s %s", v, id)
	}
	if a := article(out); len(a) != 2 || a["status"] != "rejected" {
		t.Errorf("blocked discloses %v", a)
	}
}

func TestAliasCoversTheClaimant(t *testing.T) {
	h := newHarness(t)
	q := make([]float32, testDim)
	q[1], q[2] = 0.76, 0.65
	_ = normalize(q)
	h.emb.set("Rust async runtimes", axis(1))
	h.emb.set("Rust executor internals", q)
	h.emb.set("alias q", q)
	h.put(synthProd, ulid(1), with(articleBody("Rust async runtimes"), "topic_ids", []string{topic(1)}), http.StatusCreated)
	h.put(synthProd, ulid(2), articleBody("Rust executor internals"), http.StatusCreated)
	if out := h.match(mcpProd, map[string]any{"q": "alias q"}, http.StatusOK); article(out)["id"] != ulid(2) {
		t.Fatalf("without the topic: %v", out)
	}
	out := h.match(mcpProd, map[string]any{"q": "alias q", "topic_id": topic(1), "reader": strings.Repeat("c", 32)}, http.StatusOK)
	if out["match"] != "alias" || article(out)["id"] != ulid(1) {
		t.Errorf("alias upgrade = %v", out)
	}
	if n := h.call(dashProd, http.MethodGet, "/v1/articles/"+ulid(1), nil, http.StatusOK)["readers_7d"]; n != 1.0 {
		t.Errorf("reader on an alias covered counted %v", n)
	}
}
