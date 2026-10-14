package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
)

type workStub struct {
	path, auth, ctype string
	body              map[string]any
	status            int
	answer            string
}

func (s *workStub) serve(t *testing.T) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.path, s.auth, s.ctype = r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &s.body)
		w.WriteHeader(s.status)
		io.WriteString(w, s.answer)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func runWorkCmd(url string, args ...string) (string, error) {
	var out bytes.Buffer
	err := runWork(context.Background(), &config.Config{}, append(args, "-server", url), &out, io.Discard)
	return out.String(), err
}

func TestWorkPurgeCommandIsADryRunWithoutY(t *testing.T) {
	t.Setenv("COSIFT_ADMIN_TOKEN", "env-token")
	stub := &workStub{status: 200, answer: `{"documents_deleted":1,"documents_kept":0,"records_deleted":1,"texts_deleted":0,"workers_settled":2,"parked_until":"2026-10-27T10:00:00Z"}`}
	url := stub.serve(t)
	out, err := runWorkCmd(url, "purge", "-url", "https://a.example.org/x")
	if err != nil {
		t.Fatal(err)
	}
	if stub.path != "/admin/work-purge" || stub.auth != "Bearer env-token" || stub.ctype != "application/json" || stub.body["url"] != "https://a.example.org/x" || stub.body["apply"] != false || len(stub.body) != 2 {
		t.Fatalf("%+v", stub)
	}
	if !strings.Contains(out, "dry run, nothing changed") || !strings.Contains(out, "documents_deleted") || !strings.Contains(out, "2026-10-27T10:00:00Z") {
		t.Fatalf("%s", out)
	}
	out, err = runWorkCmd(url, "purge", "--url", "https://a.example.org/x", "-y", "-token", "flag-token")
	if err != nil || stub.body["apply"] != true || stub.auth != "Bearer flag-token" || !strings.Contains(out, "purge (applied)") {
		t.Fatalf("%+v %s %v", stub, out, err)
	}
}

func TestWorkVoidCommand(t *testing.T) {
	stub := &workStub{status: 200, answer: `{"records_changed":3,"attestations_dropped":3,"records_deleted":1,"urls_parked":2}`}
	url := stub.serve(t)
	worker := strings.Repeat("a", 32)
	out, err := runWorkCmd(url, "void", "-worker", worker, "-y", "-json")
	if err != nil || stub.path != "/admin/work-void" || stub.body["worker"] != worker || stub.body["apply"] != true {
		t.Fatalf("%+v %v", stub, err)
	}
	if strings.TrimSpace(out) != stub.answer {
		t.Fatalf("-json prints the raw answer: %s", out)
	}
	out, _ = runWorkCmd(url, "void", "-worker", worker)
	if !strings.Contains(out, "urls_parked") || stub.body["apply"] != false {
		t.Fatalf("%s", out)
	}
}

func TestWorkCommandSurfacesRefusalsAndUsageErrors(t *testing.T) {
	stub := &workStub{status: 401, answer: `{"error":"Unauthorized","status":401,"detail":"missing or invalid peer token"}`}
	url := stub.serve(t)
	if _, err := runWorkCmd(url, "purge", "-url", "https://a.example.org/x"); err == nil || !strings.Contains(err.Error(), "server 401") {
		t.Fatalf("%v", err)
	}
	for _, args := range [][]string{{}, {"nuke"}, {"purge"}, {"void"}, {"purge", "-url", "u", "extra"}, {"void", "-bogus"}} {
		var ue *usageError
		if _, err := runWorkCmd(url, args...); !errors.As(err, &ue) {
			t.Errorf("%v: %v", args, err)
		}
	}
	if _, err := runWorkCmd("http://127.0.0.1:1", "purge", "-url", "u"); err == nil {
		t.Fatal("an unreachable server is an error")
	}
}
