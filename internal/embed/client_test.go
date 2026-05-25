package embed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

type mockServer struct {
	calls int
}

func (m *mockServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.calls++
		b, _ := io.ReadAll(r.Body)
		var req openAIReq
		_ = json.Unmarshal(b, &req)

		// Return one deterministic vec per text — length 4 for simplicity.
		resp := openAIResp{}
		for i, t := range req.Input {
			v := make([]float32, 4)
			for j := range v {
				v[j] = float32(len(t)) + float32(i*10+j)
			}
			resp.Data = append(resp.Data, struct {
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{Embedding: v, Index: i})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

func TestOpenAIClientHappyPath(t *testing.T) {
	mock := &mockServer{}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	c := NewOpenAIClient("dummy", srv.URL, "test-model", 4)
	v, err := c.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(v) != 2 || len(v[0]) != 4 {
		t.Fatalf("shape: got %dx%d", len(v), len(v[0]))
	}
	if mock.calls != 1 {
		t.Errorf("calls: got %d want 1", mock.calls)
	}
}

func TestOpenAIClientErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":{"message":"bad key","type":"auth"}}`))
	}))
	defer srv.Close()

	c := NewOpenAIClient("dummy", srv.URL, "m", 4)
	_, err := c.Embed(context.Background(), []string{"x"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error message: got %v", err)
	}
}

func TestCachedEmbedderHits(t *testing.T) {
	mock := &mockServer{}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	inner := NewOpenAIClient("dummy", srv.URL, "m1", 4)
	cache := NewCachedEmbedder(inner, filepath.Join(t.TempDir(), "cache"))

	v1, err := cache.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if mock.calls != 1 {
		t.Fatalf("first call count: got %d want 1", mock.calls)
	}

	// Second call: all hits, zero new API calls.
	v2, err := cache.Embed(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if mock.calls != 1 {
		t.Errorf("cache miss on second call: %d calls total", mock.calls)
	}
	if v1[0][0] != v2[0][0] {
		t.Errorf("cached value differs: %v vs %v", v1[0], v2[0])
	}

	// Partial miss: one cached, one new — only 1 new API call with 1 input.
	_, _ = cache.Embed(context.Background(), []string{"hello", "fresh"})
	if mock.calls != 2 {
		t.Errorf("expected 2 calls after partial miss, got %d", mock.calls)
	}
}

func TestCachedEmbedderDifferentModelInvalidates(t *testing.T) {
	mock := &mockServer{}
	srv := httptest.NewServer(mock.handler())
	defer srv.Close()

	dir := filepath.Join(t.TempDir(), "cache")
	c1 := NewCachedEmbedder(NewOpenAIClient("k", srv.URL, "model-a", 4), dir)
	_, _ = c1.Embed(context.Background(), []string{"hello"})

	c2 := NewCachedEmbedder(NewOpenAIClient("k", srv.URL, "model-b", 4), dir)
	_, _ = c2.Embed(context.Background(), []string{"hello"})

	// Different model → different cache key → another API call.
	if mock.calls != 2 {
		t.Errorf("expected 2 calls for two models, got %d", mock.calls)
	}
}
