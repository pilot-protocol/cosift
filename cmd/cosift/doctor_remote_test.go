package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// remoteCheckByName picks out a doctorCheck by Name. Returns nil when missing.
func remoteCheckByName(checks []doctorCheck, name string) *doctorCheck {
	for i := range checks {
		if checks[i].Name == name {
			return &checks[i]
		}
	}
	return nil
}

func TestDoctorRemoteUnreachable(t *testing.T) {
	// Server URL that won't connect → single FAIL row for /healthz, no cascade.
	checks := doctorRemoteChecks(context.Background(), "http://127.0.0.1:1", "")
	if len(checks) != 1 {
		t.Fatalf("unreachable server should emit ONE check (no cascade); got %d", len(checks))
	}
	if checks[0].Name != "remote /healthz" || checks[0].Status != "FAIL" {
		t.Errorf("expected /healthz FAIL row, got %+v", checks[0])
	}
}

func TestDoctorRemoteHealthzAndStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/stats":
			_ = json.NewEncoder(w).Encode(server.StatsResponse{
				Documents: 1234,
				Terms:     5678,
				Frontier:  store.FrontierStats{Done: 1234},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "")
	if len(checks) != 2 {
		t.Fatalf("want 2 checks (healthz + stats) without token; got %d: %+v", len(checks), checks)
	}
	if c := remoteCheckByName(checks, "remote /healthz"); c == nil || c.Status != "PASS" {
		t.Errorf("/healthz should PASS: %+v", c)
	}
	if c := remoteCheckByName(checks, "remote /stats"); c == nil || c.Status != "PASS" {
		t.Errorf("/stats should PASS: %+v", c)
	} else if !strings.Contains(c.Detail, "1234 docs") {
		t.Errorf("/stats detail should mention doc count: %q", c.Detail)
	}
}

func TestDoctorRemoteAdminTokenValid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/stats":
			_ = json.NewEncoder(w).Encode(server.StatsResponse{Documents: 1})
		case "/admin/config":
			if r.Header.Get("Authorization") != "Bearer good-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(server.AdminConfigResponse{Version: "test"})
		}
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "good-token")
	if len(checks) != 3 {
		t.Fatalf("want 3 checks (healthz + stats + admin token); got %d", len(checks))
	}
	if c := remoteCheckByName(checks, "remote admin token"); c == nil || c.Status != "PASS" {
		t.Errorf("admin token check should PASS with valid token: %+v", c)
	}
}

func TestDoctorRemoteAdminTokenInvalid(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/stats":
			_ = json.NewEncoder(w).Encode(server.StatsResponse{})
		case "/admin/config":
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "bad-token")
	c := remoteCheckByName(checks, "remote admin token")
	if c == nil || c.Status != "FAIL" {
		t.Errorf("admin token check should FAIL on 401: %+v", c)
	}
	if !strings.Contains(c.Detail, "401") {
		t.Errorf("FAIL detail should mention status: %+v", c)
	}
}

func TestDoctorRemoteNoTokenSkipsAdminCheck(t *testing.T) {
	// Empty token → /admin/config check is not performed (not a failure either).
	hits := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits[r.URL.Path] = true
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/stats":
			_ = json.NewEncoder(w).Encode(server.StatsResponse{})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "")
	if hits["/admin/config"] {
		t.Error("admin/config should NOT be hit when token is empty")
	}
	if remoteCheckByName(checks, "remote admin token") != nil {
		t.Errorf("admin token check should be absent when token empty: %+v", checks)
	}
}

func TestDoctorRemoteStatsMalformed(t *testing.T) {
	// /healthz ok but /stats returns junk → WARN, not FAIL (doctor is best-effort).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/stats":
			_, _ = w.Write([]byte("not json"))
		}
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "")
	if c := remoteCheckByName(checks, "remote /stats"); c == nil || c.Status != "WARN" {
		t.Errorf("malformed /stats should WARN not FAIL: %+v", c)
	}
}

func TestDoctorRemoteHealthzNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	checks := doctorRemoteChecks(context.Background(), srv.URL, "")
	c := remoteCheckByName(checks, "remote /healthz")
	if c == nil || c.Status != "FAIL" {
		t.Errorf("/healthz 503 should FAIL: %+v", c)
	}
	if !strings.Contains(c.Detail, "503") {
		t.Errorf("FAIL detail should mention status: %+v", c)
	}
}
