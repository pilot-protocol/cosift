package community

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const wikiTestKey = "csk_fedcba9876543210_ZYXWVUTSRQPONMLKJIHGFEDCBA765432ZYXWVUTSRQPONMLKJIHG"

func TestEngineArticlesReadTheRecordWithTheWikiKey(t *testing.T) {
	var path, auth string
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, auth = r.URL.Path, r.Header.Get("Authorization")
		w.Write([]byte(`{"id":"x","origin":"community","contributors":["0f1e2d3c4b5a6978"],"version":3,"body_md":"text"}`))
	}))
	defer engine.Close()
	client, err := newEngineClient(engine.URL, wikiTestKey)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := engineArticles{client}.ArticleByID(context.Background(), testRef)
	if err != nil || rec.Origin != "community" || len(rec.Contributors) != 1 || rec.Contributors[0] != "0f1e2d3c4b5a6978" || rec.Version != 3 {
		t.Fatalf("%+v %v", rec, err)
	}
	if path != "/v1/articles/"+testRef || auth != "Bearer "+wikiTestKey {
		t.Fatalf("path %q auth %q", path, auth)
	}
}

func TestEngineArticlesClassifyTheEnginesAnswers(t *testing.T) {
	status := 200
	body := `{"origin":"community","contributors":[],"version":1}`
	calls := 0
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	defer engine.Close()
	client, _ := newEngineClient(engine.URL, wikiTestKey)
	reader := engineArticles{client}
	for _, tc := range []struct {
		status int
		body   string
		gone   bool
		fails  bool
	}{{404, "{}", true, true}, {410, "{}", true, true}, {429, "{}", false, true}, {500, "{}", false, true}, {200, "not json", false, true}, {200, `{"origin":"community","contributors":[],"version":1,"body_md":"` + strings.Repeat("x", 3<<20) + `"}`, false, true}, {200, `{"origin":"synth","contributors":[],"version":1}`, false, false}} {
		status, body = tc.status, tc.body
		_, err := reader.ArticleByID(context.Background(), testRef)
		if (err != nil) != tc.fails || errors.Is(err, ErrArticleGone) != tc.gone {
			t.Errorf("%d %.20q: %v", tc.status, tc.body, err)
		}
	}
	before := calls
	if _, err := reader.ArticleByID(context.Background(), "../etc/passwd"); err == nil || calls != before {
		t.Fatal("a non-ULID id reached the engine")
	}
}
