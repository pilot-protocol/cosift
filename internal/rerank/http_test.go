package rerank

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPRerankerCohereShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req httpRerankReq
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.Documents) != 3 {
			t.Errorf("docs: got %d want 3", len(req.Documents))
		}
		resp := httpRerankResp{}
		// Reorder: index 2, then 0, then 1.
		for _, idx := range []int{2, 0, 1} {
			resp.Results = append(resp.Results, struct {
				Index          int     `json:"index"`
				RelevanceScore float64 `json:"relevance_score"`
			}{Index: idx, RelevanceScore: 1.0 - float64(idx)*0.1})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	r := NewHTTPReranker(srv.URL, "", "test-model")
	cands := []Candidate{{ID: "A"}, {ID: "B"}, {ID: "C"}}
	got, err := r.Rerank(context.Background(), "q", cands)
	if err != nil {
		t.Fatalf("rerank: %v", err)
	}
	want := []string{"C", "A", "B"}
	if len(got) != 3 {
		t.Fatalf("len: got %d want 3 (%+v)", len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d]: got %q want %q (full %+v)", i, got[i], want[i], got)
		}
	}
}

func TestHTTPRerankerScoresShape(t *testing.T) {
	// Fallback shape: server returns scores[] aligned with inputs, we sort.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(httpRerankResp{Scores: []float64{0.3, 0.9, 0.5}})
	}))
	defer srv.Close()

	r := NewHTTPReranker(srv.URL, "", "")
	cands := []Candidate{{ID: "A"}, {ID: "B"}, {ID: "C"}}
	got, _ := r.Rerank(context.Background(), "q", cands)
	// Expected order by score desc: B (0.9), C (0.5), A (0.3).
	if len(got) != 3 || got[0] != "B" || got[1] != "C" || got[2] != "A" {
		t.Errorf("scores-shape order: %+v", got)
	}
}

func TestHTTPRerankerFallbackOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	r := NewHTTPReranker(srv.URL, "", "")
	cands := []Candidate{{ID: "A"}, {ID: "B"}}
	got, _ := r.Rerank(context.Background(), "q", cands)
	// On 5xx → passthrough.
	if len(got) != 2 || got[0] != "A" || got[1] != "B" {
		t.Errorf("error should fall back to passthrough; got %+v", got)
	}
}

func TestHTTPRerankerAuthHeader(t *testing.T) {
	got := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(httpRerankResp{Results: nil, Scores: []float64{1, 0}})
	}))
	defer srv.Close()

	r := NewHTTPReranker(srv.URL, "my-secret-key", "")
	_, _ = r.Rerank(context.Background(), "q", []Candidate{{ID: "A"}, {ID: "B"}})
	if got != "Bearer my-secret-key" {
		t.Errorf("auth header: got %q", got)
	}
}
