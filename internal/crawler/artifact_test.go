package crawler

import (
	"context"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type artifactReference struct{ calls int }

func (e *artifactReference) Model() string { return "model" }
func (e *artifactReference) Dim() int      { return 2 }
func (e *artifactReference) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.calls++
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{1, 2}
	}
	return out, nil
}

func TestArtifactVerificationRejectsPoisoning(t *testing.T) {
	text := strings.Repeat("Scientific documentation. ", 4)
	a := &LocalArtifact{URL: "https://example.com", Title: "Guide", Text: text, Model: "model", Chunks: []IndexedChunk{{Text: text, Embedding: []float32{1, 2}}}}
	ref := &artifactReference{}
	verified, err := VerifyArtifact(context.Background(), a, a.Title, a.Text, ref)
	if err != nil {
		t.Fatal(err)
	}
	verified.Embed(context.Background(), []string{text})
	if ref.calls != 1 {
		t.Fatal("verified uploaded vectors not reused")
	}
	verified.Embed(context.Background(), []string{"changed text"})
	if ref.calls != 2 {
		t.Fatal("changed source reused stale vectors")
	}
	a.Chunks[0].Embedding = []float32{2, 1}
	if _, err := VerifyArtifact(context.Background(), a, a.Title, a.Text, ref); err == nil {
		t.Fatal("poisoned vector accepted")
	}
	a.Chunks[0].Embedding = []float32{1, 2}
	a.Model = "wrong"
	if _, err := VerifyArtifact(context.Background(), a, a.Title, a.Text, ref); err == nil {
		t.Fatal("wrong model accepted")
	}
	a.Model = "model"
	if _, err := VerifyArtifact(context.Background(), a, a.Title, "changed", ref); err == nil {
		t.Fatal("forged text accepted")
	}
}

func TestContributionDoesNotUseBulkRemoteFetcher(t *testing.T) {
	hits := 0
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer remote.Close()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := NewWithBackend(config.Crawler{RemoteFetcherURL: remote.URL, AutoSitemap: true, FilterAdult: false}, db, index.NewBM25(db))
	_, err = c.FetchContribution(context.Background(), "http://127.0.0.1/private", nil)
	if err == nil {
		t.Fatal("private URL accepted")
	}
	safe := c.contributionCrawler
	if safe == nil || !safe.cfg.PublicOnly || !safe.cfg.FilterAdult || !safe.cfg.RespectRobots || safe.cfg.AutoSitemap || !safe.cfg.DisableLinkFollowing || safe.cfg.RemoteFetcherURL != "" {
		t.Fatal("contribution policy not isolated")
	}
	if hits != 0 {
		t.Fatal("remote fetcher was invoked")
	}
	if c.cfg.RemoteFetcherURL != remote.URL || !c.cfg.AutoSitemap {
		t.Fatal("bulk crawler configuration changed")
	}
}
