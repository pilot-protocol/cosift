package community

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/community/wiki"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

type handleProvider struct {
	*fakeShared
	handles map[string]sharedaccount.ContributorHandle
	err     error
}

func (p handleProvider) Handles(_ context.Context, cids []string) (map[string]sharedaccount.ContributorHandle, error) {
	out := map[string]sharedaccount.ContributorHandle{}
	for _, cid := range cids {
		if h, ok := p.handles[cid]; ok {
			out[cid] = h
		}
	}
	return out, p.err
}

const (
	cidTidal  = "0f1e2d3c4b5a6978"
	cidBanned = "5b8a1c4e9d207f36"
)

func wikiEngine(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/articles/by-slug/tidal-power" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "01J8ZC2Q7W4X9M3K5N6P8R0T2V", "slug": "tidal-power", "title": "Tidal power", "lead": "A lead about tides. It stands alone [1].",
			"body_md": "## Overview\n\nProse [1].\n", "citations": []any{map[string]any{"n": 1, "url": "https://docs.example.org/a", "title": "A", "host": "docs.example.org", "quote": "q"}},
			"quality_tier": "strong", "promoted": true, "vertical": "other", "status": "published", "ai_generated": true, "version": 1,
			"created_at": "2026-10-01T10:00:00Z", "updated_at": "2026-10-06T10:00:00Z", "origin": "community", "contributors": []string{cidTidal, cidBanned},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func openWiki(t *testing.T, engine string, shared sharedaccount.Provider) *Server {
	t.Helper()
	s, err := Open(Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "http://localhost:7780", Shared: shared,
		Wiki: wiki.Config{Public: true, EngineURL: engine, EngineKey: wikiTestKey, ReportMailto: "reports@example.org"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func wikiPage(s *Server, path string) string {
	r := httptest.NewRequest("GET", path, nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w.Body.String()
}

func TestWikiNamesContributorsFromTheSharedAccountStore(t *testing.T) {
	engine := wikiEngine(t)
	shared := handleProvider{fakeShared: &fakeShared{}, handles: map[string]sharedaccount.ContributorHandle{cidTidal: {Handle: "tidal-fan"}, cidBanned: {Handle: "rusty", Banned: true}}}
	body := wikiPage(openWiki(t, engine.URL, shared), "/wiki/tidal-power")
	if !strings.Contains(body, "@tidal-fan") || strings.Contains(body, "rusty") || !strings.Contains(body, "a community member") {
		t.Fatalf("attribution wrong in\n%s", body)
	}
}

func TestWikiShowsEveryContributorAsAMemberInLocalModeAndWhenTheStoreFails(t *testing.T) {
	engine := wikiEngine(t)
	for name, shared := range map[string]sharedaccount.Provider{
		"local":  nil,
		"no src": &fakeShared{},
		"error":  handleProvider{fakeShared: &fakeShared{}, err: sharedaccount.ErrUnavailable},
	} {
		body := wikiPage(openWiki(t, engine.URL, shared), "/wiki/tidal-power")
		if strings.Contains(body, "@tidal-fan") || strings.Count(body, "a community member") != 2 {
			t.Fatalf("%s: attribution wrong in\n%s", name, body)
		}
	}
}
