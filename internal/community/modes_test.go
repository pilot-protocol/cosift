package community

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRequestModesUseCosiftEndpointsAndSharedGuestLimit(t *testing.T) {
	for _, mode := range []string{"search", "research", "answer"} {
		t.Run(mode, func(t *testing.T) {
			s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/"+mode || r.URL.Query().Get("q") != "question" || r.URL.Query().Get("stream") != "false" {
					t.Errorf("wrong request %s", r.URL)
				}
				if r.URL.Query().Has("retriever") || r.URL.Query().Has("rerank") || r.URL.Query().Has("expand") {
					t.Error("Cosift defaults overridden")
				}
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
					t.Error("credentials leaked")
				}
				io.WriteString(w, `{"answer":"A grounded answer [1].","sources":[{"id":1,"title":"Source","url":"https://example.com"}],"plan":["first step"]}`)
			}))
			w := request(t, s, "GET", "/api/"+mode+"?q=question", nil, nil)
			expect(t, w, 200)
			if !strings.Contains(w.Body.String(), "grounded answer") {
				t.Fatal(w.Body.String())
			}
			for _, other := range []string{"search", "research", "answer"} {
				expect(t, request(t, s, "GET", "/api/"+other+"?q=question", nil, nil), 429)
			}
		})
	}
}

func TestMissingLLMDoesNotConsumeGuestAllowance(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/search" {
			io.WriteString(w, `{"hits":[]}`)
		} else {
			w.WriteHeader(501)
		}
	}))
	expect(t, request(t, s, "GET", "/api/research?q=question", nil, nil), 503)
	expect(t, request(t, s, "GET", "/api/answer?q=question", nil, nil), 503)
	expect(t, request(t, s, "GET", "/api/search?q=question", nil, nil), 200)
}

func TestResearchHasLongerBackendDeadline(t *testing.T) {
	s := testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		io.WriteString(w, `{"answer":"done","sources":[]}`)
	}))
	s.client.Timeout = time.Millisecond
	expect(t, request(t, s, "GET", "/api/research?q=question", nil, nil), 200)
}

func TestSavedModeMigrationAndUniqueness(t *testing.T) {
	s := testServer(t, nil)
	cookie := account(t, s, "modes@example.com")
	var userID string
	s.db.QueryRow(`SELECT id FROM users`).Scan(&userID)
	_, err := s.db.Exec(`DROP TABLE saved_searches; CREATE TABLE saved_searches(id TEXT PRIMARY KEY,user_id TEXT NOT NULL,query TEXT NOT NULL,created_at INTEGER NOT NULL,UNIQUE(user_id,query));`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO saved_searches VALUES('original',?,'same question',1234)`, userID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.cfg
	s.Close()
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, mode := range []string{"search", "answer", "research"} {
		expect(t, request(t, reopened, "POST", "/api/saved", map[string]string{"query": "same question", "mode": mode}, cookie), 200)
	}
	w := request(t, reopened, "GET", "/api/saved", nil, cookie)
	expect(t, w, 200)
	var list []SavedSearch
	if json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list) != 3 {
		t.Fatal(w.Body.String())
	}
	found := false
	for _, v := range list {
		if v.ID == "original" {
			found = v.Mode == "search" && v.CreatedAt == 1234
		}
	}
	if !found {
		t.Fatal("legacy search not preserved")
	}
	expect(t, request(t, reopened, "POST", "/api/saved", map[string]string{"query": "same question", "mode": "arbitrary"}, cookie), 400)
}
