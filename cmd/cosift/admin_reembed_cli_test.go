package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calinteodor/cosift/internal/config"
	"github.com/calinteodor/cosift/internal/server"
)

// newReembedSSEStub serves /admin/reembed with a canned event sequence.
// Captures method, auth header, and decoded request body.
func newReembedSSEStub(t *testing.T, events [][2]string) (*httptest.Server, func() (method, auth string, body *server.AdminReembedRequest)) {
	t.Helper()
	var lastMethod, lastAuth string
	var lastBody *server.AdminReembedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/reembed" {
			http.NotFound(w, r)
			return
		}
		lastMethod = r.Method
		lastAuth = r.Header.Get("Authorization")
		var req server.AdminReembedRequest
		if r.ContentLength > 0 {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		lastBody = &req
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("httptest writer not a flusher")
		}
		for _, ev := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev[0], ev[1])
			f.Flush()
		}
	}))
	t.Cleanup(srv.Close)
	return srv, func() (string, string, *server.AdminReembedRequest) {
		return lastMethod, lastAuth, lastBody
	}
}

func TestAdminReembedCLIRequiresConfirm(t *testing.T) {
	// Without -y, the CLI errors WITHOUT touching the server.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed", []string{
		"-server", "http://127.0.0.1:1",
	})
	if err == nil {
		t.Fatal("reembed without -y should error")
	}
	if !strings.Contains(err.Error(), "-y") {
		t.Errorf("error should mention -y: %v", err)
	}
	if !strings.Contains(err.Error(), "LLM spend") {
		t.Errorf("error should explain why (LLM cost): %v", err)
	}
}

func TestAdminReembedCLIDropOldMessageInGuard(t *testing.T) {
	// With -drop-old but no -y, the guard message mentions the dropping step.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed", []string{"-drop-old"})
	if err == nil {
		t.Fatal("reembed -drop-old without -y should error")
	}
	if !strings.Contains(err.Error(), "drop other-model passages") {
		t.Errorf("error should mention drop step: %v", err)
	}
}

func TestAdminReembedCLIHappyPath(t *testing.T) {
	events := [][2]string{
		{"started", `{"total_docs":3,"target_model":"text-embedding-3-small"}`},
		{"progress", `{"docs_processed":1,"passages_written":4}`},
		{"progress", `{"docs_processed":2,"passages_written":8}`},
		{"done", `{"docs_processed":3,"passages_written":12,"dropped_old":0,"took":"1.2s"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-token", "secret", "-y"}); err != nil {
			t.Fatalf("admin reembed: %v", err)
		}
	})
	method, auth, body := capture()
	if method != http.MethodPost {
		t.Errorf("want POST, got %s", method)
	}
	if auth != "Bearer secret" {
		t.Errorf("bearer not propagated: %q", auth)
	}
	if body == nil {
		t.Fatal("server got no body")
	}
	if body.DropOld {
		t.Errorf("drop_old should be false by default; got true")
	}
	// Output sequence assertions.
	for _, want := range []string{
		"[reembed started: 3 docs, target=text-embedding-3-small]",
		"[progress: 1 docs, 4 passages]",
		"[progress: 2 docs, 8 passages]",
		"Done: 3 docs reembedded, 12 passages written, 0 dropped, took 1.2s.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output: %q", want, out)
		}
	}
}

func TestAdminReembedCLIDropOldFlagPropagates(t *testing.T) {
	events := [][2]string{
		{"started", `{"total_docs":1,"target_model":"v2"}`},
		{"done", `{"docs_processed":1,"passages_written":2,"dropped_old":5,"took":"100ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y", "-drop-old"}); err != nil {
			t.Fatalf("admin reembed: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || !body.DropOld {
		t.Errorf("drop_old should propagate as true; body=%+v", body)
	}
	if !strings.Contains(out, "5 dropped") {
		t.Errorf("done should report dropped count: %q", out)
	}
}

func TestAdminReembedCLIErrorEvent(t *testing.T) {
	events := [][2]string{
		{"started", `{"total_docs":1,"target_model":"v"}`},
		{"error", `{"detail":"embed batch: rate-limited"}`},
	}
	srv, _ := newReembedSSEStub(t, events)
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed",
		[]string{"-server", srv.URL, "-y"})
	if err == nil {
		t.Fatal("error event should surface as CLI error")
	}
	if !strings.Contains(err.Error(), "rate-limited") {
		t.Errorf("error detail should be in message: %v", err)
	}
}

func TestAdminReembedCLIJSONPassthrough(t *testing.T) {
	// -json streams raw SSE bytes through stdout for piping into a custom parser.
	events := [][2]string{
		{"started", `{"total_docs":0,"target_model":"v"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"took":"1ms"}`},
	}
	srv, _ := newReembedSSEStub(t, events)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y", "-json"}); err != nil {
			t.Fatalf("admin reembed -json: %v", err)
		}
	})
	// In -json mode, output is the raw SSE wire format, NOT the human-readable lines.
	if !strings.Contains(out, "event: started") {
		t.Errorf("-json should pass through raw SSE: %q", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Errorf("-json should pass through done event: %q", out)
	}
	if strings.Contains(out, "[reembed started:") {
		t.Errorf("-json should NOT render human format: %q", out)
	}
}

func TestAdminReembedCLINon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "embedder not configured", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed",
		[]string{"-server", srv.URL, "-y"})
	if err == nil {
		t.Fatal("400 should surface as CLI error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("status should be in error: %v", err)
	}
}

func TestAdminReembedCLITokenFromEnv(t *testing.T) {
	t.Setenv("COSIFT_ADMIN_TOKEN", "env-secret")
	events := [][2]string{
		{"started", `{"total_docs":0,"target_model":"v"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"took":"1ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y"}); err != nil {
			t.Fatalf("admin reembed: %v", err)
		}
	})
	_, auth, _ := capture()
	if auth != "Bearer env-secret" {
		t.Errorf("env token not picked up: %q", auth)
	}
}

func TestAdminReembedCLISincePropagates(t *testing.T) {
	events := [][2]string{
		{"started", `{"total_docs":42,"target_model":"v"}`},
		{"done", `{"docs_processed":42,"passages_written":100,"dropped_old":0,"took":"3s"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y", "-since", "2025-01-01"}); err != nil {
			t.Fatalf("admin reembed -since: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil {
		t.Fatal("server got no body")
	}
	if body.Since != "2025-01-01" {
		t.Errorf("since not propagated; got %q", body.Since)
	}
}

func TestAdminReembedCLIInvalidSinceErrorsClientSide(t *testing.T) {
	// Bad date should error BEFORE the HTTP call (no server URL needed).
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed",
		[]string{"-server", "http://127.0.0.1:1", "-y", "-since", "yesterday"})
	if err == nil {
		t.Fatal("invalid -since should error")
	}
	if !strings.Contains(err.Error(), "-since") {
		t.Errorf("error should name the flag: %v", err)
	}
	if !strings.Contains(err.Error(), "yesterday") {
		t.Errorf("error should include the bad value: %v", err)
	}
}

func TestAdminReembedCLISinceInGuardMessage(t *testing.T) {
	// Without -y but with -since, the guard message should describe the filter
	// so operators see what they'd be re-embedding before they commit.
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed",
		[]string{"-since", "2025-01-01"})
	if err == nil {
		t.Fatal("reembed without -y should error")
	}
	if !strings.Contains(err.Error(), "published >= 2025-01-01") {
		t.Errorf("guard should describe the date filter: %v", err)
	}
	// Also include -drop-old to verify the messages compose.
	err = runAdmin(context.Background(), cfg, "reembed",
		[]string{"-since", "2025-01-01", "-drop-old"})
	if err == nil {
		t.Fatal("reembed -drop-old without -y should error")
	}
	if !strings.Contains(err.Error(), "published >= 2025-01-01") {
		t.Errorf("guard should mention since: %v", err)
	}
	if !strings.Contains(err.Error(), "drop other-model") {
		t.Errorf("guard should also mention drop step: %v", err)
	}
}

func TestAdminReembedCLISinceEmptyOmittedFromBody(t *testing.T) {
	// No -since flag → request body's Since field is empty string. omitempty
	// JSON tag will drop the field entirely, server defaults to no filter.
	events := [][2]string{
		{"started", `{"total_docs":0,"target_model":"v"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"took":"1ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y"}); err != nil {
			t.Fatalf("admin reembed: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil {
		t.Fatal("server got no body")
	}
	if body.Since != "" {
		t.Errorf("Since should be empty without -since flag; got %q", body.Since)
	}
}

func TestAdminReembedCLIDryRunFlagPropagates(t *testing.T) {
	// Iter 126: -dry-run sets DryRun=true in the request body.
	events := [][2]string{
		{"started", `{"total_docs":42,"target_model":"text-embedding-3-small"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"dry_run":true,"took":"3ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-dry-run"}); err != nil {
			t.Fatalf("admin reembed -dry-run: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil {
		t.Fatal("server got no body")
	}
	if !body.DryRun {
		t.Errorf("DryRun should be true; got false")
	}
}

func TestAdminReembedCLIDryRunRendersSummary(t *testing.T) {
	// Done event with dry_run:true → CLI prints the dry-run summary referencing
	// the started event's count (NOT the done event's zeros).
	events := [][2]string{
		{"started", `{"total_docs":4250,"target_model":"text-embedding-3-small"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"dry_run":true,"took":"3ms"}`},
	}
	srv, _ := newReembedSSEStub(t, events)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-dry-run"}); err != nil {
			t.Fatalf("admin reembed -dry-run: %v", err)
		}
	})
	// Dry-run summary references the started event's count (4250), not the
	// done event's zeros (0 docs_processed). This is the iter-126 behavior:
	// dry-run users want "how many would be processed?", not "how many were processed?".
	if !strings.Contains(out, "4250 docs would be re-embedded") {
		t.Errorf("dry-run summary should reference started count: %q", out)
	}
	if !strings.Contains(out, "text-embedding-3-small") {
		t.Errorf("dry-run summary should reference target model: %q", out)
	}
	if !strings.Contains(out, "Re-run without -dry-run") {
		t.Errorf("dry-run summary should hint at the next step: %q", out)
	}
	// Should NOT show the regular "Done: 0 docs reembedded" message (the iter-113
	// non-dry-run output would be misleading for dry-run since both have docs_processed=0).
	if strings.Contains(out, "Done: 0 docs reembedded") {
		t.Errorf("dry-run shouldn't render the non-dry-run Done line: %q", out)
	}
}

func TestAdminReembedCLIGuardMentionsDryRun(t *testing.T) {
	// Without -y AND without -dry-run, the guard error should mention BOTH
	// options (iter-126: dry-run is the new escape hatch).
	cfg := config.Default()
	err := runAdmin(context.Background(), cfg, "reembed", nil)
	if err == nil {
		t.Fatal("reembed without -y or -dry-run should error")
	}
	if !strings.Contains(err.Error(), "-y") {
		t.Errorf("error should mention -y: %v", err)
	}
	if !strings.Contains(err.Error(), "-dry-run") {
		t.Errorf("error should mention -dry-run: %v", err)
	}
}

func TestAdminReembedCLIBothFlagsDryRunWins(t *testing.T) {
	// Both -y and -dry-run set → dry-run wins (iter-111 safer-on-conflict).
	events := [][2]string{
		{"started", `{"total_docs":1,"target_model":"v"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"dry_run":true,"took":"1ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	_ = captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-y", "-dry-run"}); err != nil {
			t.Fatalf("admin reembed -y -dry-run: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || !body.DryRun {
		t.Errorf("conflicting flags: dry-run should win; body=%+v", body)
	}
}

func TestAdminReembedCLIDryRunWithSinceCompose(t *testing.T) {
	// Dry-run + since: both propagate, summary still uses iter-126 dry-run shape.
	events := [][2]string{
		{"started", `{"total_docs":1234,"target_model":"v"}`},
		{"done", `{"docs_processed":0,"passages_written":0,"dropped_old":0,"dry_run":true,"took":"5ms"}`},
	}
	srv, capture := newReembedSSEStub(t, events)
	cfg := config.Default()
	out := captureAdminStdout(t, func() {
		if err := runAdmin(context.Background(), cfg, "reembed",
			[]string{"-server", srv.URL, "-dry-run", "-since", "2025-01-01"}); err != nil {
			t.Fatalf("admin reembed dry-run + since: %v", err)
		}
	})
	_, _, body := capture()
	if body == nil || !body.DryRun || body.Since != "2025-01-01" {
		t.Errorf("dry-run + since: both should propagate; body=%+v", body)
	}
	if !strings.Contains(out, "1234 docs would be re-embedded") {
		t.Errorf("summary should reference filtered count: %q", out)
	}
}

func TestAdminReembedCLIStreamWithoutDoneSucceeds(t *testing.T) {
	// Server closes mid-stream → stderr note, no CLI error.
	events := [][2]string{
		{"started", `{"total_docs":2,"target_model":"v"}`},
		{"progress", `{"docs_processed":1,"passages_written":4}`},
	}
	srv, _ := newReembedSSEStub(t, events)
	cfg := config.Default()
	if err := runAdmin(context.Background(), cfg, "reembed",
		[]string{"-server", srv.URL, "-y"}); err != nil {
		t.Errorf("stream without done shouldn't error: %v", err)
	}
}
