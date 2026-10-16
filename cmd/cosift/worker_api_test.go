package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const workerTestToken = "ck_1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type apiRecorder struct {
	mu    sync.Mutex
	calls []recordedCall
}

type recordedCall struct {
	path   string
	header http.Header
	body   map[string]any
}

func (r *apiRecorder) handler(reply func(call recordedCall) (int, http.Header, string)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		call := recordedCall{path: req.URL.Path, header: req.Header.Clone()}
		json.Unmarshal(raw, &call.body)
		r.mu.Lock()
		r.calls = append(r.calls, call)
		r.mu.Unlock()
		status, header, body := reply(call)
		for k, v := range header {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	})
}

func TestWorkAPISendsTheBearerTokenAndTheCommunityHeaderWithoutAnOrigin(t *testing.T) {
	rec := &apiRecorder{}
	srv := httptest.NewServer(rec.handler(func(recordedCall) (int, http.Header, string) {
		return 200, nil, `{"leases":[{"lease_id":"c1","url":"https://example.org/a","source":"errored","expires_at":"2026-10-20T10:10:00Z","extract_version":1}],"moderation_ceiling":false}`
	}))
	defer srv.Close()
	reply, err := newWorkAPI(srv.URL+"/", workerTestToken).claim(context.Background(), 3)
	if err != nil || len(reply.Leases) != 1 || reply.Leases[0].LeaseID != "c1" {
		t.Fatalf("%+v %v", reply, err)
	}
	c := rec.calls[0]
	if c.path != "/api/work/claim" || c.body["max"] != float64(3) {
		t.Errorf("request: %s %v", c.path, c.body)
	}
	if c.header.Get("Authorization") != "Bearer "+workerTestToken || c.header.Get("X-Cosift-Client") != "community" || c.header.Get("Content-Type") != "application/json" {
		t.Errorf("headers: %v", c.header)
	}
	if _, has := c.header["Origin"]; has {
		t.Error("an Origin header was sent")
	}
	if _, has := c.header["Cookie"]; has {
		t.Error("a cookie was sent")
	}
}

func TestWorkAPIErrorsCarryCodesAndRetryHintsAndNeverTheBodyOrToken(t *testing.T) {
	rec := &apiRecorder{}
	srv := httptest.NewServer(rec.handler(func(c recordedCall) (int, http.Header, string) {
		return 429, http.Header{"Retry-After": {"17"}}, `{"error":"echo ` + c.header.Get("Authorization") + `","code":"rate_limited","retry_after_seconds":3}`
	}))
	defer srv.Close()
	_, err := newWorkAPI(srv.URL, workerTestToken).claim(context.Background(), 1)
	var apiErr *workAPIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 || apiErr.Code != "rate_limited" || apiErr.RetryAfter != 17*time.Second {
		t.Fatalf("got %#v", err)
	}
	if strings.Contains(err.Error(), "AAAA") || strings.Contains(err.Error(), "echo") {
		t.Fatalf("error text leaks: %q", err)
	}
	for _, body := range []string{`{"code":"Bad Code!"}`, `{"code":"` + strings.Repeat("a", 60) + `"}`} {
		srv2 := httptest.NewServer(rec.handler(func(recordedCall) (int, http.Header, string) { return 500, nil, body }))
		_, err := newWorkAPI(srv2.URL, workerTestToken).claim(context.Background(), 1)
		srv2.Close()
		if !errors.As(err, &apiErr) || apiErr.Code != "" {
			t.Errorf("an unsafe code was kept: %#v", apiErr)
		}
	}
}

func TestWorkAPIDoesNotFollowRedirects(t *testing.T) {
	var hits int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	_, err := newWorkAPI(srv.URL, workerTestToken).claim(context.Background(), 1)
	var apiErr *workAPIError
	if !errors.As(err, &apiErr) || apiErr.Status != 307 || hits != 0 {
		t.Fatalf("err %v, redirect target hit %d times", err, hits)
	}
}
