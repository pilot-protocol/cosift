package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

// canned robots.txt server: blocks /private and /api/, allows everything else.
// Returns a Crawl-delay of 2 seconds for the wildcard user-agent.
func robotsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("User-agent: *\nCrawl-delay: 2\nDisallow: /private\nDisallow: /api/\nAllow: /\n"))
	})
	return httptest.NewServer(mux)
}

func TestRunCheckRobotsHappy(t *testing.T) {
	srv := robotsTestServer(t)
	defer srv.Close()

	cfg := &config.Config{Crawler: config.Crawler{UserAgent: "TestBot/1.0"}}
	// One allowed, one denied — exercises both result paths.
	err := runCheckRobots(context.Background(), cfg, []string{srv.URL + "/blog/post-1", srv.URL + "/private/secret"})
	if err != nil {
		t.Fatalf("runCheckRobots: %v", err)
	}
}

func TestRunCheckRobotsNoArgs(t *testing.T) {
	cfg := &config.Config{Crawler: config.Crawler{UserAgent: "TestBot/1.0"}}
	err := runCheckRobots(context.Background(), cfg, nil)
	if err == nil {
		t.Fatalf("expected usage error with no URLs")
	}
	if !strings.Contains(err.Error(), "usage:") {
		t.Errorf("error should mention usage: %v", err)
	}
}

func TestRunCheckRobotsCustomUserAgent(t *testing.T) {
	// User-agent override via -user-agent flag. Test that flag parsing works
	// (the actual user-agent doesn't change the allow/deny outcome on this
	// fixture which uses User-agent: * wildcard, but the parse path matters).
	srv := robotsTestServer(t)
	defer srv.Close()

	cfg := &config.Config{Crawler: config.Crawler{UserAgent: "FromConfig/1.0"}}
	err := runCheckRobots(context.Background(), cfg, []string{"-user-agent", "Override/2.0", srv.URL + "/"})
	if err != nil {
		t.Fatalf("runCheckRobots: %v", err)
	}
}

func TestRunCheckRobotsUnreachableHost(t *testing.T) {
	// Unreachable host — should report robots.txt as UNREACHABLE, then
	// fall back to "[OK ]" because Robots.Allowed treats fetch failure as
	// allowed (graceful degradation). The combination tells the operator
	// "no robots policy is enforced here" — useful diagnostic.
	//
	// Use a short context timeout so test runtime doesn't wait on the full
	// HTTP client timeout (15s production-default, would be 30s of test pain
	// for two probe attempts).
	cfg := &config.Config{Crawler: config.Crawler{UserAgent: "TestBot/1.0"}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := runCheckRobots(ctx, cfg, []string{"http://nonexistent.invalid.test.local/path"})
	if err != nil {
		t.Errorf("unreachable-host case should print UNREACHABLE and continue, not fail the command: %v", err)
	}
}
