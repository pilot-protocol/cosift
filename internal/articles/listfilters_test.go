package articles

import (
	"net/http"
	"net/url"
	"slices"
	"testing"
)

func slugsOf(out map[string]any) []string {
	var s []string
	for _, it := range out["items"].([]any) {
		s = append(s, it.(map[string]any)["slug"].(string))
	}
	return s
}

func wantedBoard(t *testing.T) *harness {
	h := newHarness(t)
	h.put(synthProd, ulid(1), articleBody("Alpha strong article"), http.StatusCreated)
	h.put(synthProd, ulid(2), with(articleBody("Bravo ok article"), "quality_tier", "ok"), http.StatusCreated)
	h.put(synthProd, ulid(3), with(articleBody("Charlie thin article"), "quality_tier", "thin"), http.StatusCreated)
	h.put(synthProd, ulid(4), with(articleBody("Delta held article"), "quality_tier", "thin", "status", "held"), http.StatusCreated)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(5), stubBodyFor("Echo wanted stub", topic(5)), http.StatusCreated)
	h.call(resolverProd, http.MethodPut, "/v1/articles/"+ulid(6), stubBodyFor("Foxtrot wanted stub", topic(6)), http.StatusCreated)
	h.call(resolverStaging, http.MethodPut, "/v1/articles/"+ulid(7), stubBodyFor("Golf staging stub", topic(7)), http.StatusCreated)
	return h
}

func TestListPendingForRead(t *testing.T) {
	h := wantedBoard(t)
	if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles?status=pending", nil, http.StatusOK)); !slices.Equal(got, []string{"echo-wanted-stub", "foxtrot-wanted-stub"}) {
		t.Errorf("prod pending = %v", got)
	}
	if got := slugsOf(h.call(mcpStaging, http.MethodGet, "/v1/articles?status=pending", nil, http.StatusOK)); !slices.Equal(got, []string{"echo-wanted-stub", "foxtrot-wanted-stub", "golf-staging-stub"}) {
		t.Errorf("staging pending = %v", got)
	}
	if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles", nil, http.StatusOK)); !slices.Equal(got, []string{"alpha-strong-article", "bravo-ok-article", "charlie-thin-article"}) {
		t.Errorf("default list = %v", got)
	}
	if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles?status=published", nil, http.StatusOK)); len(got) != 3 {
		t.Errorf("published = %v", got)
	}
	if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles?status=published,pending", nil, http.StatusOK)); len(got) != 5 {
		t.Errorf("published,pending = %v", got)
	}
	out := h.call(wiki, http.MethodGet, "/v1/articles?status=pending&limit=1", nil, http.StatusOK)
	stub := out["items"].([]any)[0].(map[string]any)
	if stub["status"] != "pending" || stub["version"] != 0.0 || hasKey(stub, "quality_tier") || hasKey(stub, "origin") || hasKey(stub, "contributors") {
		t.Errorf("stub item = %v", stub)
	}
	next, _ := out["next_cursor"].(string)
	if next == "" {
		t.Fatal("no cursor after the first stub")
	}
	if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles?status=pending&cursor="+url.QueryEscape(next), nil, http.StatusOK)); !slices.Equal(got, []string{"foxtrot-wanted-stub"}) {
		t.Errorf("second page = %v", got)
	}
	for _, q := range []string{"status=held", "status=pending,held", "status=rejected", "status=tombstoned", "status=bogus"} {
		h.expectError(wiki, http.MethodGet, "/v1/articles?"+q, nil, http.StatusBadRequest, "invalid_param")
	}
	if got := slugsOf(h.call(dashProd, http.MethodGet, "/v1/articles?status=pending", nil, http.StatusOK)); len(got) != 3 {
		t.Errorf("read_all pending = %v", got)
	}
}

func TestListTierFilter(t *testing.T) {
	h := wantedBoard(t)
	cases := map[string][]string{
		"tier=thin":                               {"charlie-thin-article"},
		"tier=strong":                             {"alpha-strong-article"},
		"tier=ok,thin":                            {"bravo-ok-article", "charlie-thin-article"},
		"status=published&tier=thin":              {"charlie-thin-article"},
		"status=pending&tier=thin":                nil,
		"status=published,pending&tier=strong,ok": {"alpha-strong-article", "bravo-ok-article"},
	}
	for q, want := range cases {
		if got := slugsOf(h.call(wiki, http.MethodGet, "/v1/articles?"+q, nil, http.StatusOK)); !slices.Equal(got, want) {
			t.Errorf("%s = %v, want %v", q, got, want)
		}
	}
	for _, q := range []string{"tier=gold", "tier=", "tier=thin,", "tier=THIN"} {
		h.expectError(wiki, http.MethodGet, "/v1/articles?"+q, nil, http.StatusBadRequest, "invalid_param")
	}
	if got := slugsOf(h.call(dashProd, http.MethodGet, "/v1/articles?tier=thin", nil, http.StatusOK)); !slices.Equal(got, []string{"charlie-thin-article", "delta-held-article"}) {
		t.Errorf("read_all thin = %v", got)
	}
}

func TestListCursorBoundToStatusAndTier(t *testing.T) {
	h := wantedBoard(t)
	next := func(q string) string {
		out := h.call(wiki, http.MethodGet, "/v1/articles?"+q+"&limit=1", nil, http.StatusOK)
		c, _ := out["next_cursor"].(string)
		if c == "" {
			t.Fatalf("%s: no cursor", q)
		}
		return url.QueryEscape(c)
	}
	h.call(wiki, http.MethodGet, "/v1/articles?tier=strong,ok,thin&limit=1&cursor="+next("tier=strong,ok,thin"), nil, http.StatusOK)
	h.expectError(wiki, http.MethodGet, "/v1/articles?tier=strong,ok&limit=1&cursor="+next("tier=strong,ok,thin"), nil, http.StatusBadRequest, "invalid_cursor")
	h.expectError(wiki, http.MethodGet, "/v1/articles?limit=1&cursor="+next("tier=strong,ok,thin"), nil, http.StatusBadRequest, "invalid_cursor")
	h.expectError(wiki, http.MethodGet, "/v1/articles?limit=1&cursor="+next("status=pending"), nil, http.StatusBadRequest, "invalid_cursor")
	h.expectError(wiki, http.MethodGet, "/v1/articles?status=published&limit=1&cursor="+next("status=pending"), nil, http.StatusBadRequest, "invalid_cursor")
}

func TestTombstoneAnswerListsContributors(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", "A second lead. It differs.", "expected_version", 1), http.StatusOK)
	out := h.moderate(dashProd, id, "hold", "", http.StatusOK)
	if hasKey(out, "contributors") {
		t.Errorf("hold answered with contributors: %v", out)
	}
	h.moderate(dashProd, id, "approve", "", http.StatusOK)
	out = h.moderate(dashProd, id, "tombstone", "privacy", http.StatusOK)
	if got := toStrings(out["contributors"]); !slices.Equal(got, []string{cid(1), cid(2)}) {
		t.Errorf("tombstone contributors = %v", out["contributors"])
	}
	residue := fullOf(h, id)
	if hasKey(residue, "contributors") || hasKey(residue, "origin") {
		t.Errorf("residue kept attribution: %v", residue)
	}

	h.put(synthProd, ulid(2), articleBody("Go memory model"), http.StatusCreated)
	if out := h.moderate(dashProd, ulid(2), "tombstone", "privacy", http.StatusOK); !hasKey(out, "contributors") || len(out["contributors"].([]any)) != 0 {
		t.Errorf("synth tombstone = %v", out)
	}
	h.put(synthProd, ulid(3), articleBody("Kafka consumer groups"), http.StatusCreated)
	if out := h.moderate(dashProd, ulid(3), "reject", "quality", http.StatusOK); hasKey(out, "contributors") {
		t.Errorf("reject answered with contributors: %v", out)
	}
}
