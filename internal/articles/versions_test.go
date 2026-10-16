package articles

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
)

func keptVersions(h *harness, id string) []int {
	h.t.Helper()
	out := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusOK)
	var vs []int
	for _, v := range out["versions"].([]any) {
		vs = append(vs, int(v.(map[string]any)["version"].(float64)))
	}
	return vs
}

func topRange(lo, hi int) []int {
	var out []int
	for v := hi; v >= lo; v-- {
		out = append(out, v)
	}
	return out
}

func synthRevise(h *harness, id string, n, current int) int {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", fmt.Sprintf("Synth lead %d. It differs each time.", current+i), "expected_version", current+i), http.StatusOK)
	}
	return current + n
}

func contribRevise(h *harness, id string, n, current int) int {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.contribute(id, contribBody("Rust async runtimes", cid(current+i), "lead", fmt.Sprintf("Contributed lead %d. It differs each time.", current+i), "expected_version", current+i), http.StatusOK)
	}
	return current + n
}

func TestSynthRecordKeepsFiveVersions(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	cur := synthRevise(h, id, 8, 1)
	if got := keptVersions(h, id); !slices.Equal(got, topRange(cur-5, cur-1)) {
		t.Errorf("kept %v with the record at %d", got, cur)
	}
}

func TestCommunityRecordKeepsTwentyVersions(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(0)), http.StatusCreated)
	cur := contribRevise(h, id, 25, 1)
	if got := keptVersions(h, id); !slices.Equal(got, topRange(cur-20, cur-1)) {
		t.Errorf("kept %v with the record at %d", got, cur)
	}
	for v := cur - 20; v < cur; v++ {
		h.call(dashProd, http.MethodGet, fmt.Sprintf("/v1/article-versions/%s/%d", id, v), nil, http.StatusOK)
	}
	h.expectError(dashProd, http.MethodGet, fmt.Sprintf("/v1/article-versions/%s/%d", id, cur-21), nil, http.StatusNotFound, "not_found")
}

func TestRecordTurningCommunityKeepsTwentyFromThen(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.put(synthProd, id, articleBody("Rust async runtimes"), http.StatusCreated)
	cur := synthRevise(h, id, 8, 1)
	if got := keptVersions(h, id); len(got) != 5 {
		t.Fatalf("synth kept %v", got)
	}
	cur = contribRevise(h, id, 1, cur)
	if got := keptVersions(h, id); !slices.Equal(got, topRange(cur-6, cur-1)) {
		t.Errorf("after the first contribution kept %v at %d: pruned versions came back or the new cap is not applied", got, cur)
	}
	cur = contribRevise(h, id, 30, cur)
	if got := keptVersions(h, id); !slices.Equal(got, topRange(cur-20, cur-1)) {
		t.Errorf("kept %v with the record at %d", got, cur)
	}
}

func TestSynthRebuildOfCommunityRecordKeepsTwenty(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(0)), http.StatusCreated)
	cur := contribRevise(h, id, 7, 1)
	cur = synthRevise(h, id, 10, cur)
	if got := keptVersions(h, id); !slices.Equal(got, topRange(1, cur-1)) {
		t.Errorf("kept %v with the record at %d", got, cur)
	}
}

func TestVersionRowsCarryAttribution(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	h.contribute(id, contribBody("Rust async runtimes", cid(1)), http.StatusCreated)
	h.contribute(id, contribBody("Rust async runtimes", cid(2), "lead", "A second lead. It differs.", "expected_version", 1), http.StatusOK)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "lead", "A synth lead. It differs.", "expected_version", 2), http.StatusOK)
	rows := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusOK)["versions"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %v", rows)
	}
	for i, want := range []string{cid(2), cid(1)} {
		row := rows[i].(map[string]any)
		if row["version_contributor"] != want || row["status"] != "published" || row["sensitivity_class"] != "none" || hasKey(row, "reverts_to") {
			t.Errorf("row %d = %v", i, row)
		}
	}
	if cur := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusOK)["current"]; cur != 3.0 {
		t.Errorf("current = %v", cur)
	}
}

func TestReservedContributorSlugs(t *testing.T) {
	h := newHarness(t)
	for i, title := range []string{"Style", "Terms", "Wanted", "Index"} {
		out := h.put(synthProd, ulid(i+1), articleBody(title), http.StatusCreated)
		if want := fmt.Sprintf("%s-2", []string{"style", "terms", "wanted", "index"}[i]); article(out)["slug"] != want {
			t.Errorf("%s: slug %v, want %s", title, article(out)["slug"], want)
		}
	}
}

func TestVersionRowsReportStatusAndSensitivity(t *testing.T) {
	h := newHarness(t)
	id := ulid(1)
	sensitive := map[string]any{"class": "medical_advice", "reason": "dosage"}
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held", "sensitivity", sensitive), http.StatusCreated)
	h.put(synthProd, id, with(articleBody("Rust async runtimes"), "status", "held", "lead", "A rebuilt lead. It differs.", "expected_version", 1), http.StatusOK)
	rows := h.call(dashProd, http.MethodGet, "/v1/article-versions/"+id, nil, http.StatusOK)["versions"].([]any)
	if row := rows[0].(map[string]any); row["status"] != "held" || row["sensitivity_class"] != "medical_advice" {
		t.Errorf("row = %v", row)
	}
}
