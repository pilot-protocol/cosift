package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runWorkerCLI runs `cosift contribute <args>` with COSIFT_TOKEN as given and returns stdout, stderr text and the error.
func runWorkerCLI(t *testing.T, app *fakeApp, token string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	return runWorkerCLIEnv(t, app, token, nil, args...)
}

func runWorkerCLIEnv(t *testing.T, app *fakeApp, token string, env map[string]string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	origin, _ := fixtureOrigin(t)
	previous := workerFetchClient
	workerFetchClient = func() *http.Client { return originClient(origin) }
	t.Cleanup(func() { workerFetchClient = previous })
	t.Setenv("COSIFT_TOKEN", token)
	t.Setenv("COSIFT_EMAIL", "")
	t.Setenv("COSIFT_PASSWORD", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	for k, v := range env {
		t.Setenv(k, v)
	}
	outR, outW, _ := os.Pipe()
	errR, errW, _ := os.Pipe()
	prevOut, prevErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	done := make(chan struct{})
	var outBuf, errBuf bytes.Buffer
	go func() { outBuf.ReadFrom(outR); close(done) }()
	errDone := make(chan struct{})
	go func() { errBuf.ReadFrom(errR); close(errDone) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = runContributeConfigured(ctx, nil, args)
	outW.Close()
	errW.Close()
	os.Stdout, os.Stderr = prevOut, prevErr
	<-done
	<-errDone
	if token != "" && (strings.Contains(outBuf.String(), token) || strings.Contains(errBuf.String(), token) || (err != nil && strings.Contains(err.Error(), token))) {
		t.Fatalf("the token reached the output: %q %q %v", outBuf.String(), errBuf.String(), err)
	}
	return outBuf.String(), errBuf.String(), err
}

func writeSession(t *testing.T, dir, origin, token string) string {
	t.Helper()
	path := filepath.Join(dir, "session.json")
	raw, _ := json.Marshal(communitySession{Origin: origin, Token: token, Expires: time.Now().Add(24 * time.Hour)})
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func workerApp(t *testing.T) *fakeApp {
	app := newFakeApp(t)
	app.onClaim = queue(ev, "/plain.html", "/german.html")
	app.onSubmit = func(int, map[string]any) (int, http.Header, string) { return ok(verdictWaiting) }
	return app
}

func TestContributeWorkerAuthenticatesWithTheBearerTokenAndTheCommunityHeader(t *testing.T) {
	app := workerApp(t)
	stdout, _, err := runWorkerCLI(t, app, workerTestToken, "-worker", "-server", app.srv.URL, "-max-pages", "2", "-concurrency", "2")
	if err != nil {
		t.Fatal(err)
	}
	headers := app.requestHeaders()
	if len(headers) < 3 {
		t.Fatalf("%d requests", len(headers))
	}
	for _, h := range headers {
		if h.Get("Authorization") != "Bearer "+workerTestToken || h.Get("X-Cosift-Client") != "community" {
			t.Errorf("headers: %v", h)
		}
		if _, has := h["Origin"]; has {
			t.Error("an Origin header was sent")
		}
		if _, has := h["Cookie"]; has {
			t.Error("a cookie was sent")
		}
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	var last map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil || last["event"] != "summary" || last["submitted"] != float64(2) {
		t.Fatalf("stdout: %q", stdout)
	}
}

func TestContributeWorkerReadsTheBearerTokenFromASavedSession(t *testing.T) {
	app := workerApp(t)
	path := writeSession(t, t.TempDir(), app.srv.URL, workerTestToken)
	if _, _, err := runWorkerCLI(t, app, "", "-worker", "-server", app.srv.URL, "-session-file", path, "-max-pages", "2", "-concurrency", "2"); err != nil {
		t.Fatal(err)
	}
	for _, h := range app.requestHeaders() {
		if h.Get("Authorization") != "Bearer "+workerTestToken {
			t.Fatalf("headers: %v", h)
		}
		if _, has := h["Cookie"]; has {
			t.Fatal("a cookie was sent")
		}
	}
	if len(app.submitted()) != 2 {
		t.Fatalf("%d submissions", len(app.submitted()))
	}
}

func TestContributeWorkerUsesTheInstalledSessionWithoutAnyFlag(t *testing.T) {
	app := workerApp(t)
	origin, _ := fixtureOrigin(t)
	previous := workerFetchClient
	workerFetchClient = func() *http.Client { return originClient(origin) }
	t.Cleanup(func() { workerFetchClient = previous })
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "cosift"), 0o700); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(communitySession{Origin: app.srv.URL, Token: workerTestToken, Expires: time.Now().Add(time.Hour)})
	if err := os.WriteFile(filepath.Join(dir, "cosift", "community-session.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", dir)
	for _, k := range []string{"COSIFT_TOKEN", "COSIFT_EMAIL", "COSIFT_PASSWORD", "COSIFT_SESSION_FILE"} {
		t.Setenv(k, "")
	}
	if err := runContributeConfigured(context.Background(), nil, []string{"-worker", "-max-pages", "1"}); err != nil {
		t.Fatal(err)
	}
	if len(app.submitted()) != 1 {
		t.Fatalf("%d submissions", len(app.submitted()))
	}
}

func TestContributeWorkerRefusesCookieOnlyCredentialsBeforeAnyRequest(t *testing.T) {
	app := workerApp(t)
	cookie := writeSession(t, t.TempDir(), app.srv.URL, "opaque-cookie-session-value")
	for name, tc := range map[string]struct {
		token string
		args  []string
		env   map[string]string
	}{
		"cookie session":        {"", []string{"-session-file", cookie}, nil},
		"email and password":    {"", nil, map[string]string{"COSIFT_EMAIL": "me@example.com", "COSIFT_PASSWORD": "hunter2hunter2"}},
		"email only":            {"", []string{"-email", "me@example.com"}, nil},
		"no credentials at all": {"", nil, nil},
	} {
		_, _, err := runWorkerCLIEnv(t, app, tc.token, tc.env, append([]string{"-worker", "-server", app.srv.URL, "-max-pages", "1"}, tc.args...)...)
		if err == nil || !strings.Contains(err.Error(), "COSIFT_TOKEN") || (name != "no credentials at all" && !errors.Is(err, errNeedsBearer)) {
			t.Errorf("%s: got %v", name, err)
		}
		if n := len(app.requestHeaders()); n != 0 {
			t.Errorf("%s: %d requests were made", name, n)
		}
	}
}

func TestContributeWorkerRejectsMixedOperationsAndBadBounds(t *testing.T) {
	app := workerApp(t)
	base := []string{"-worker", "-server", app.srv.URL}
	for name, extra := range map[string][]string{
		"url":               {"https://example.com/a"},
		"csv":               {"-csv", "-"},
		"request":           {"-request", "-query", "x"},
		"local":             {"-index-locally"},
		"credits":           {"-credits"},
		"guest":             {"-guest"},
		"login":             {"-login"},
		"logout":            {"-logout"},
		"no bound":          {"-max-pages", "0", "-duration", "0"},
		"negative pages":    {"-max-pages", "-1"},
		"negative duration": {"-duration", "-1s"},
		"zero concurrency":  {"-concurrency", "0"},
		"too much":          {"-concurrency", "33"},
	} {
		_, _, err := runWorkerCLI(t, app, workerTestToken, append(append([]string(nil), base...), extra...)...)
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if n := len(app.requestHeaders()); n != 0 {
		t.Fatalf("%d requests were made", n)
	}
	_, _, err := runWorkerCLI(t, app, workerTestToken, "-max-pages", "5", "-server", app.srv.URL, "https://example.com/a")
	if err == nil || !strings.Contains(err.Error(), "-worker") {
		t.Fatalf("worker flags without -worker: %v", err)
	}
}

func TestContributeWorkerReportsAnAccountRefusalWithoutTheToken(t *testing.T) {
	app := newFakeApp(t)
	app.onClaim = func(int, int) (int, http.Header, string) {
		return 403, nil, `{"error":"echo ` + workerTestToken + `","code":"banned"}`
	}
	stdout, _, err := runWorkerCLI(t, app, workerTestToken, "-worker", "-server", app.srv.URL, "-max-pages", "1")
	if err == nil || !strings.Contains(err.Error(), "suspended") {
		t.Fatalf("got %v", err)
	}
	if !strings.Contains(stdout, `"event":"summary"`) {
		t.Fatalf("no summary was printed: %q", stdout)
	}
}

func TestTheWorkerFetchClientIsPublicOnly(t *testing.T) {
	origin, hits := fixtureOrigin(t)
	_, err := newWorkerFetchClient().Get(origin.URL + "/plain.html")
	if err == nil || !strings.Contains(err.Error(), "public-only") || len(hits.seen) != 0 {
		t.Fatalf("a loopback origin was reached: %v", err)
	}
	req, _ := http.NewRequest("GET", "http://10.0.0.1/x", nil)
	if err := newWorkerFetchClient().CheckRedirect(req, nil); err == nil || !strings.Contains(err.Error(), "public-only") {
		t.Fatalf("a private redirect hop was allowed: %v", err)
	}
}
