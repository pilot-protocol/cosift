package articles

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

type instantEmbedder struct{ dim int }

func (e instantEmbedder) Model() string { return "bench" }
func (e instantEmbedder) Dim() int      { return e.dim }
func (e instantEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = randomUnit(rand.New(rand.NewPCG(uint64(i), 1)), e.dim)
	}
	return out, nil
}

func randomUnit(r *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(r.NormFloat64())
	}
	_ = normalize(v)
	return v
}

// benchStore holds n published articles of four rows each, like a title, a lead and two aliases.
func benchStore(b *testing.B, n, dim int) *Store {
	db, err := pebble.Open(b.TempDir(), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	s, err := Open(Options{DB: db, Embedder: instantEmbedder{dim}, Policy: v1.NewFakePolicy(allPrincipals...), ThresholdsPath: b.TempDir() + "/none.json", Logf: func(string, ...any) {}})
	if err != nil {
		b.Fatal(err)
	}
	r := rand.New(rand.NewPCG(7, 7))
	for i := range n {
		rec := &Record{Schema: 1, ID: ulid(i), Slug: fmt.Sprintf("article-%d", i), BaseSlug: fmt.Sprintf("article-%d", i), Title: fmt.Sprintf("Article %d", i), Status: StatusPublished, CreatedAt: "2026-09-29T10:00:00Z"}
		s.idx.putRecord(rec)
		s.idx.rows[rec.ID] = []row{{kind: kindTitle, vec: randomUnit(r, dim)}, {kind: kindLead, vec: randomUnit(r, dim)},
			{kind: kindAlias, alias: 0, vec: randomUnit(r, dim)}, {kind: kindAlias, alias: 1, vec: randomUnit(r, dim)}}
	}
	s.rd.SetReady()
	return s
}

func BenchmarkMatch(b *testing.B) {
	const dim = 768
	for _, n := range []int{100, 1000, 10000} {
		s := benchStore(b, n, dim)
		q := randomUnit(rand.New(rand.NewPCG(1, 2)), dim)
		text := "rust async runtimes"
		body := &matchBody{Q: &text}
		b.Run(fmt.Sprintf("serve/articles=%d", n), func(b *testing.B) {
			for b.Loop() {
				s.matchServe(mcpProd, body, q)
			}
		})
		b.Run(fmt.Sprintf("build/articles=%d", n), func(b *testing.B) {
			for b.Loop() {
				s.matchBuild(synthProd, body, q)
			}
		})
		mux := http.NewServeMux()
		for _, rt := range s.Routes() {
			mux.Handle(rt.Pattern(), rt.Handler)
		}
		b.Run(fmt.Sprintf("handler/articles=%d", n), func(b *testing.B) {
			for b.Loop() {
				req := httptest.NewRequest(http.MethodPost, "/v1/articles/match", strings.NewReader(`{"q":"rust async runtimes"}`))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, v1.RequestWithPrincipal(req, mcpProd))
				if rec.Code != http.StatusOK {
					b.Fatal(rec.Code)
				}
			}
		})
	}
}
