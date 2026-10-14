package community

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// fairRig serves every page as a 404 so a job ends at once, and records the order pages were fetched.
func fairRig(t *testing.T) (*Server, *[]string, *func(url string)) {
	t.Helper()
	s := testServer(t, nil)
	var mu sync.Mutex
	order := []string{}
	var onFetch func(url string)
	s.pageClient = &http.Client{Transport: pageTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/robots.txt" {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(strings.NewReader("User-agent: *\nAllow: /\n")), Request: r}, nil
		}
		mu.Lock()
		order = append(order, r.URL.String())
		hook := onFetch
		mu.Unlock()
		if hook != nil {
			hook(r.URL.String())
		}
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("gone")), Request: r}, nil
	})}
	s.moderationRobots = crawler.NewRobots(s.pageClient, "Cosift-Community/1.0")
	return s, &order, &onFetch
}

func addJobs(t *testing.T, s *Server, user, host string, n int, createdFrom int64) {
	t.Helper()
	s.db.Exec(`INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,0) ON CONFLICT DO NOTHING`, user, user+"@example.com", user, "s", "")
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := tx.Exec(`INSERT INTO submissions(id,user_id,url,created_at) VALUES(?,?,?,?)`, fmt.Sprintf("%s-%04d", user, i), user, fmt.Sprintf("https://%s/%04d", host, i), createdFrom+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatchServesAnotherAccountAfterAtMostOneJobOfThePendingOnes(t *testing.T) {
	s, order, _ := fairRig(t)
	addJobs(t, s, "a", "a.example", turnsScan+200, 1000)
	addJobs(t, s, "b", "b.example", 1, 5000)
	addJobs(t, s, "c", "c.example", 1, 6000)
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := *order
	if len(got) != 20 {
		t.Fatalf("%d jobs in one dispatch, want 20", len(got))
	}
	if strings.Join(got[:3], " ") != "https://a.example/0000 https://b.example/0000 https://c.example/0000" {
		t.Fatalf("first three jobs %v", got[:3])
	}
	for i, u := range got[3:] {
		if want := fmt.Sprintf("https://a.example/%04d", i+1); u != want {
			t.Fatalf("job %d is %s, want %s: oldest first within an account", i+3, u, want)
		}
	}
}

func TestDispatchRotatesAccountsAndTakesTheOldestJobOfEach(t *testing.T) {
	s, order, _ := fairRig(t)
	addJobs(t, s, "a", "a.example", 3, 100)
	addJobs(t, s, "b", "b.example", 3, 200)
	addJobs(t, s, "c", "c.example", 3, 50)
	s.dispatch(context.Background())
	want := "https://c.example/0000 https://a.example/0000 https://b.example/0000 https://c.example/0001 https://a.example/0001 https://b.example/0001 https://c.example/0002 https://a.example/0002 https://b.example/0002"
	if got := strings.Join(*order, " "); got != want {
		t.Fatalf("order\n got %s\nwant %s", got, want)
	}
}

func TestDispatchPicksUpAnAccountThatArrivesWhileAJobIsSlow(t *testing.T) {
	s, order, onFetch := fairRig(t)
	addJobs(t, s, "a", "a.example", 50, 100)
	arrived := false
	*onFetch = func(url string) {
		if !arrived {
			arrived = true
			addJobs(t, s, "late", "late.example", 1, 9999)
		}
	}
	s.dispatch(context.Background())
	if got := *order; len(got) < 3 || got[1] != "https://late.example/0000" {
		t.Fatalf("order %v: the late account should be next after the running job", got)
	}
}

func TestDispatchSkipsJobsThatAreNotDueAndKeepsTheBatchCap(t *testing.T) {
	s, order, _ := fairRig(t)
	addJobs(t, s, "a", "a.example", 2, 100)
	addJobs(t, s, "b", "b.example", 2, 200)
	s.db.Exec(`UPDATE submissions SET next_attempt=?  WHERE user_id='a'`, s.now().Unix()+3600)
	s.dispatch(context.Background())
	if got := strings.Join(*order, " "); got != "https://b.example/0000 https://b.example/0001" {
		t.Fatalf("order %s", got)
	}
	*order = nil
	s.db.Exec(`UPDATE submissions SET next_attempt=0, status='pending'`)
	for i := 0; i < 30; i++ {
		s.db.Exec(`INSERT INTO submissions(id,user_id,url,created_at) VALUES(?,?,?,?)`, fmt.Sprintf("x-%d", i), "a", fmt.Sprintf("https://x.example/%d", i), 1)
	}
	s.dispatch(context.Background())
	if len(*order) != 20 {
		t.Fatalf("%d jobs in one dispatch", len(*order))
	}
}
