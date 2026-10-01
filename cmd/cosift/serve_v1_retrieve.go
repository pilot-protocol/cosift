package main

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const (
	v1SearchBodyLimit   = 8 << 10
	v1ContentsBodyLimit = 16 << 10
	v1MaxQueryBytes     = 512
	v1DefaultK          = 30
	v1MaxK              = 50
	v1MaxURLs           = 20
	v1MaxURLBytes       = 2048
	v1MaxTextBytes      = 256 << 10
	v1DenseLockWait     = 100 * time.Millisecond
	v1DenseEmbedTimeout = 5 * time.Second
)

// v1RetrievalRoutes are /v1/search and /v1/contents. Neither goes through
// handleSearch or retrieveWith: no query log, decay, expansion or rerank.
func (s *pebbleHTTP) v1RetrievalRoutes() []v1.Route {
	return []v1.Route{
		{Method: http.MethodPost, Path: "/v1/search", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: v1SearchBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleV1Search)},
		{Method: http.MethodPost, Path: "/v1/contents", Scopes: []v1.Scope{v1.ScopeRetrieveRead}, BodyLimit: v1ContentsBodyLimit, Ungated: true, Handler: http.HandlerFunc(s.handleV1Contents)},
	}
}

func v1DenseUnavailable() v1.Error {
	return v1.Error{Status: http.StatusServiceUnavailable, Code: "dense_unavailable", Detail: "dense retrieval unavailable", RetryAfter: 10 * time.Second}
}

func v1QueryRule(q string) string {
	switch {
	case q == "":
		return "required"
	case len(q) > v1MaxQueryBytes:
		return "too_long"
	}
	for _, r := range q {
		if unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r) {
			return "invalid_chars"
		}
	}
	return ""
}

type v1SearchRequest struct {
	Q         *string `json:"q"`
	K         *int    `json:"k"`
	Retriever *string `json:"retriever"`
}

type v1SearchHit struct {
	Rank        int        `json:"rank"`
	URL         string     `json:"url"`
	Title       string     `json:"title"`
	Score       float64    `json:"score"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
}

type v1SearchResponse struct {
	Retriever string        `json:"retriever"`
	K         int           `json:"k"`
	Hits      []v1SearchHit `json:"hits"`
	TookMS    int64         `json:"took_ms"`
}

func (s *pebbleHTTP) handleV1Search(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req v1SearchRequest
	if !v1.Decode(w, r, &req, v1SearchBodyLimit) {
		return
	}
	if s.store == nil || s.idx == nil {
		v1.WriteError(w, v1.IndexUnavailable())
		return
	}
	var q string
	if req.Q != nil {
		q = *req.Q
	}
	if rule := v1QueryRule(q); rule != "" {
		v1.WriteError(w, v1.InvalidField("q", rule))
		return
	}
	k := v1DefaultK
	if req.K != nil {
		if k = *req.K; k < 1 || k > v1MaxK {
			v1.WriteError(w, v1.InvalidField("k", "range"))
			return
		}
	}
	retriever := "bm25"
	if req.Retriever != nil {
		retriever = *req.Retriever
	}
	var hits []index.Hit
	switch retriever {
	case "bm25":
		var err error
		if hits, err = s.idx.Search(r.Context(), q, k); err != nil {
			v1.WriteError(w, v1.InternalError())
			return
		}
	case "dense":
		var ok bool
		if hits, ok = s.v1DenseSearch(r.Context(), q, k); !ok {
			v1.WriteError(w, v1DenseUnavailable())
			return
		}
	default:
		v1.WriteError(w, v1.InvalidField("retriever", "enum"))
		return
	}
	out := make([]v1SearchHit, 0, k)
	for _, h := range hits {
		doc, err := s.store.GetDocByURL(r.Context(), h.URL)
		if err != nil || doc == nil {
			continue
		}
		hit := v1SearchHit{Rank: len(out) + 1, URL: h.URL, Title: doc.Title, Score: h.Score}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt.UTC()
			hit.PublishedAt = &t
		}
		if out = append(out, hit); len(out) == k {
			break
		}
	}
	v1.WriteJSON(w, http.StatusOK, v1SearchResponse{Retriever: retriever, K: k, Hits: out, TookMS: time.Since(start).Milliseconds()})
}

// v1DenseSearch never waits on a compaction and never falls back to BM25.
func (s *pebbleHTTP) v1DenseSearch(ctx context.Context, q string, k int) ([]index.Hit, bool) {
	g, emb := s.hnsw(), s.v1Embedder
	if g == nil || emb == nil {
		return nil, false
	}
	ectx, cancel := context.WithTimeout(ctx, v1DenseEmbedTimeout)
	vecs, err := emb.Embed(ectx, []string{q})
	cancel()
	if err != nil || len(vecs) != 1 {
		return nil, false
	}
	vhits, ok := g.TrySearch(ctx, vecs[0], k+denseFetchSlack(), v1DenseLockWait)
	if !ok {
		return nil, false
	}
	return s.applyAuthorityToDense(vhits), true
}

type v1ContentsRequest struct {
	URLs *[]string `json:"urls"`
}

type v1ContentsFound struct {
	URL         string     `json:"url"`
	Found       bool       `json:"found"`
	Title       string     `json:"title"`
	Text        string     `json:"text"`
	Lang        string     `json:"lang"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	FetchedAt   *time.Time `json:"fetched_at,omitempty"`
	Truncated   bool       `json:"truncated"`
}

type v1ContentsMissing struct {
	URL   string `json:"url"`
	Found bool   `json:"found"`
}

func (s *pebbleHTTP) handleV1Contents(w http.ResponseWriter, r *http.Request) {
	var req v1ContentsRequest
	if !v1.Decode(w, r, &req, v1ContentsBodyLimit) {
		return
	}
	if s.store == nil || s.idx == nil {
		v1.WriteError(w, v1.IndexUnavailable())
		return
	}
	if req.URLs == nil || len(*req.URLs) == 0 {
		v1.WriteError(w, v1.InvalidField("urls", "required"))
		return
	}
	urls := *req.URLs
	if len(urls) > v1MaxURLs {
		v1.WriteError(w, v1.InvalidField("urls", "too_many"))
		return
	}
	for i, u := range urls {
		if u == "" || len(u) > v1MaxURLBytes {
			v1.WriteError(w, v1.InvalidField("urls["+strconv.Itoa(i)+"]", "length"))
			return
		}
	}
	results := make([]any, 0, len(urls))
	for _, u := range urls {
		doc, err := s.store.GetDocByURL(r.Context(), u)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			v1.WriteError(w, v1.InternalError())
			return
		}
		if doc == nil {
			results = append(results, v1ContentsMissing{URL: u})
			continue
		}
		text, truncated := cutUTF8(doc.Text, v1MaxTextBytes)
		item := v1ContentsFound{URL: u, Found: true, Title: doc.Title, Text: text, Lang: doc.Lang, Truncated: truncated}
		if !doc.PublishedAt.IsZero() {
			t := doc.PublishedAt.UTC()
			item.PublishedAt = &t
		}
		if !doc.FetchedAt.IsZero() {
			t := doc.FetchedAt.UTC()
			item.FetchedAt = &t
		}
		results = append(results, item)
	}
	v1.WriteJSON(w, http.StatusOK, map[string]any{"results": results})
}

// cutUTF8 cuts s to at most n bytes on a rune boundary.
func cutUTF8(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n], true
}
