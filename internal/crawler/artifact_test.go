package crawler

import (
	"context"
	"errors"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
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
	if _, err := VerifyArtifact(context.Background(), a, a.Title, a.Text, ref); !errors.Is(err, ErrContributionRejected) {
		t.Fatalf("poisoned vector did not get permanent rejection: %v", err)
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
	_, err = c.FetchContribution(context.Background(), "http://127.0.0.1/private", nil, ApprovedContentHash("Guide", "Body"))
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

func validChunkArtifact() *LocalArtifact {
	text := strings.Repeat("Scientific documentation. ", 4)
	return &LocalArtifact{Title: "Guide", Text: text, Model: "model", Chunks: []IndexedChunk{{Text: text, Embedding: []float32{1, 2}}}}
}

func TestValidateAcceptsATextOnlyArtifact(t *testing.T) {
	a := &LocalArtifact{Title: "Guide", Text: strings.Repeat("Scientific documentation. ", 4)}
	if err := a.Validate(); err != nil {
		t.Fatalf("text-only artifact: %v", err)
	}
	for name, mut := range map[string]func(*LocalArtifact){
		"model without chunks": func(a *LocalArtifact) { a.Model = "model" },
		"chunks without model": func(a *LocalArtifact) {
			a.Chunks = []IndexedChunk{{Text: a.Text, Embedding: []float32{1}}}
		},
	} {
		b := *a
		mut(&b)
		if b.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestValidateKeepsEveryMalformedArtifactRejected(t *testing.T) {
	big := make([]float32, 4097)
	for i := range big {
		big[i] = 1
	}
	nan := float32(math.NaN())
	for name, mut := range map[string]func(*LocalArtifact){
		"title over 1000":       func(a *LocalArtifact) { a.Title = strings.Repeat("t", 1001) },
		"text under 80":         func(a *LocalArtifact) { a.Text = "short"; a.Chunks[0].Text = "short" },
		"text over 32000":       func(a *LocalArtifact) { a.Text = strings.Repeat("x", 32001); a.Chunks[0].Text = "x" },
		"model over 200":        func(a *LocalArtifact) { a.Model = strings.Repeat("m", 201) },
		"no chunks with model":  func(a *LocalArtifact) { a.Chunks = nil },
		"no model with chunks":  func(a *LocalArtifact) { a.Model = "" },
		"over 64 chunks":        func(a *LocalArtifact) { a.Chunks = slices.Repeat(a.Chunks, 65) },
		"chunk text not in doc": func(a *LocalArtifact) { a.Chunks[0].Text = "elsewhere" },
		"empty chunk text":      func(a *LocalArtifact) { a.Chunks[0].Text = "" },
		"empty embedding":       func(a *LocalArtifact) { a.Chunks[0].Embedding = nil },
		"embedding over 4096":   func(a *LocalArtifact) { a.Chunks[0].Embedding = big },
		"NaN":                   func(a *LocalArtifact) { a.Chunks[0].Embedding = []float32{nan, 1} },
		"Inf":                   func(a *LocalArtifact) { a.Chunks[0].Embedding = []float32{float32(math.Inf(1)), 1} },
		"zero embedding":        func(a *LocalArtifact) { a.Chunks[0].Embedding = []float32{0, 0} },
	} {
		a := validChunkArtifact()
		mut(a)
		if a.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := validChunkArtifact().Validate(); err != nil {
		t.Fatalf("the control artifact: %v", err)
	}
	if (*LocalArtifact)(nil).Validate() == nil {
		t.Fatal("nil accepted")
	}
}

func TestWorkVectorsAreNotBoundedByTheContributionTextCap(t *testing.T) {
	long := strings.Repeat("Scientific documentation. ", 3000)
	a := &LocalArtifact{Title: "Guide", Text: long, Model: "model", Chunks: []IndexedChunk{{Text: long[:2000], Embedding: []float32{1, 2}}}}
	if a.Validate() == nil {
		t.Fatal("the contribution path must keep its 32000-byte text bound")
	}
	ref := &artifactReference{}
	verified, err := VerifyWorkVectors(context.Background(), a, ref)
	if err != nil {
		t.Fatal(err)
	}
	verified.Embed(context.Background(), []string{long[:2000]})
	if ref.calls != 1 {
		t.Fatal("verified vectors were not reused")
	}
}

func TestWorkVectorsAreRefusedNotRepaired(t *testing.T) {
	ref := &artifactReference{}
	for name, mut := range map[string]func(*LocalArtifact){
		"wrong model":     func(a *LocalArtifact) { a.Model = "other" },
		"wrong dimension": func(a *LocalArtifact) { a.Chunks[0].Embedding = []float32{1, 2, 3} },
		"poisoned vector": func(a *LocalArtifact) { a.Chunks[0].Embedding = []float32{2, 1} },
		"malformed":       func(a *LocalArtifact) { a.Chunks = nil },
	} {
		a := validChunkArtifact()
		mut(a)
		if _, err := VerifyWorkVectors(context.Background(), a, ref); !errors.Is(err, ErrContributionRejected) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := VerifyWorkVectors(context.Background(), validChunkArtifact(), nil); !errors.Is(err, ErrContributionRejected) {
		t.Errorf("no reference embedder: %v", err)
	}
}
