package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/store"
)

type localPageTransport func(*http.Request) (*http.Response, error)

func (f localPageTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestLocalIndexPersistsTextAndEmbeddings(t *testing.T) {
	emb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []string `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		data := []any{}
		for i := range body.Input {
			data = append(data, map[string]any{"index": i, "embedding": []float32{1, 2}})
		}
		json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer emb.Close()
	cfg := &config.Config{DataDir: t.TempDir(), Embeddings: config.Embeddings{URL: emb.URL, Model: "test", Dim: 2}}
	client := &http.Client{Transport: localPageTransport(func(r *http.Request) (*http.Response, error) {
		body := `<html><title>Guide</title><article>` + strings.Repeat("This is useful public scientific documentation. ", 5) + `</article></html>`
		if r.URL.Path == "/robots.txt" {
			body = "User-agent: *\nAllow: /"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/html"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}
	artifacts, err := indexLocalContributionsWithClient(context.Background(), cfg, []string{"https://example.com/guide"}, client)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 || len(artifacts[0].Chunks) == 0 || len(artifacts[0].Chunks[0].Embedding) != 2 {
		t.Fatal("missing local artifact")
	}
	db, err := store.Open(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	doc, err := db.GetDocByURL(context.Background(), "https://example.com/guide")
	if err != nil || doc == nil || doc.Text == "" {
		t.Fatal("local document was not indexed")
	}
}
