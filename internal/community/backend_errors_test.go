package community

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

func routeServer(t *testing.T, statuses map[string]*int) *Server {
	t.Helper()
	return testServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := statuses[r.URL.Path]; code != nil && *code != 200 {
			w.WriteHeader(*code)
			io.WriteString(w, `{"error":"refused"}`)
			return
		}
		switch r.URL.Path {
		case "/v1/community/moderate":
			io.WriteString(w, `{"decision":"allow","category":"safe"}`)
		case "/v1/community/enqueue":
			io.WriteString(w, `{"indexed":true,"novel":false,"content_hash":"`+strings.Repeat("a", 64)+`"}`)
		}
	}))
}

func listSubmissions(t *testing.T, s *Server, cookie *http.Cookie) []Submission {
	t.Helper()
	w := request(t, s, "GET", "/api/submissions", nil, cookie)
	expect(t, w, 200)
	var out []Submission
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestBackendAuthFailureIsLoggedAndSurfaced(t *testing.T) {
	for _, route := range []string{"/v1/community/moderate", "/v1/community/enqueue"} {
		for _, code := range []int{401, 403} {
			t.Run(route+"/"+http.StatusText(code), func(t *testing.T) {
				logs := captureLog(t)
				status := code
				s := routeServer(t, map[string]*int{route: &status})
				cookie := account(t, s, "member@example.com")
				expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
				if err := s.dispatch(context.Background()); err != nil {
					t.Fatal(err)
				}
				got := listSubmissions(t, s, cookie)
				if len(got) != 1 || got[0].Status != "config_error" || got[0].Reason == "" {
					t.Fatalf("submission not surfaced as a configuration error: %+v", got)
				}
				if !strings.Contains(logs.String(), route) || !strings.Contains(logs.String(), "HTTP "+strconv.Itoa(code)) {
					t.Fatalf("refusal not logged with route and status: %q", logs.String())
				}
				if strings.Contains(logs.String(), testBrokerKey) || strings.Contains(got[0].Reason, testBrokerKey) {
					t.Fatal("the broker key leaked")
				}
				var attempts int
				var next int64
				s.db.QueryRow(`SELECT attempts,next_attempt FROM submissions`).Scan(&attempts, &next)
				if attempts != 0 || next < time.Now().Add(30*time.Minute).Unix() {
					t.Fatalf("a configuration error must not use up attempts or retry quickly: attempts=%d next in %ds", attempts, next-time.Now().Unix())
				}
				status = 200
				s.db.Exec(`UPDATE submissions SET next_attempt=0`)
				if err := s.dispatch(context.Background()); err != nil {
					t.Fatal(err)
				}
				if got := listSubmissions(t, s, cookie); got[0].Status != "indexed" {
					t.Fatalf("a fixed configuration did not resume the submission: %+v", got)
				}
			})
		}
	}
}

func TestEveryNon200BackendResponseIsLogged(t *testing.T) {
	for _, tc := range []struct {
		route string
		code  int
	}{{"/v1/community/moderate", 500}, {"/v1/community/moderate", 429}, {"/v1/community/enqueue", 502}, {"/v1/community/enqueue", 404}} {
		logs := captureLog(t)
		code := tc.code
		s := routeServer(t, map[string]*int{tc.route: &code})
		cookie := account(t, s, "member@example.com")
		expect(t, request(t, s, "POST", "/api/submissions", map[string]any{"urls": []string{"https://example.com/guide"}}, cookie), 202)
		if err := s.dispatch(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := listSubmissions(t, s, cookie); got[0].Status != "pending" {
			t.Errorf("%s %d: status %s, want a plain retry", tc.route, tc.code, got[0].Status)
		}
		if !strings.Contains(logs.String(), tc.route) || !strings.Contains(logs.String(), "HTTP "+strconv.Itoa(tc.code)) || strings.Contains(logs.String(), testBrokerKey) {
			t.Errorf("%s %d: log %q", tc.route, tc.code, logs.String())
		}
	}
}
