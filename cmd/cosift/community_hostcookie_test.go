package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestCommunityLoginSavesTheHostPrefixedSession(t *testing.T) {
	t.Setenv("COSIFT_EMAIL", "cli@example.com")
	t.Setenv("COSIFT_PASSWORD", "cli-test-password")
	t.Setenv("COSIFT_TOKEN", "")
	t.Setenv("COSIFT_SESSION_FILE", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/login" {
			expires := time.Now().Add(time.Hour)
			http.SetCookie(w, &http.Cookie{Name: "__Host-cosift_session", Value: "host-session", Path: "/", Expires: expires, MaxAge: 3600})
			http.SetCookie(w, &http.Cookie{Name: "cosift_session", Path: "/", MaxAge: -1})
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	file := filepath.Join(t.TempDir(), "session")
	if err := runContribute(context.Background(), []string{"-server", srv.URL, "-login", "-session-file", file}); err != nil {
		t.Fatal(err)
	}
	saved, err := readCommunitySession(file, srv.URL, false)
	if err != nil || saved.Token != "host-session" {
		t.Fatalf("saved session %+v, %v", saved, err)
	}
}

func TestCommunitySessionCookiesFollowTheOriginScheme(t *testing.T) {
	session := communitySession{Origin: "https://community.example.com", Token: "tok", Expires: time.Now().Add(time.Hour)}
	var names []string
	for _, c := range session.cookies() {
		names = append(names, c.Name)
		if c.Value != "tok" || c.Path != "/" || c.Domain != "" || !c.Secure {
			t.Errorf("cookie %+v", c)
		}
	}
	if len(names) != 2 || names[0] != "cosift_session" || names[1] != "__Host-cosift_session" {
		t.Fatalf("https cookies %v", names)
	}
	session.Origin = "http://127.0.0.1:7780"
	if got := session.cookies(); len(got) != 1 || got[0].Name != "cosift_session" || got[0].Secure {
		t.Fatalf("http cookies %+v", got)
	}
}

func TestLoginCookiePrefersTheLiveHostPrefixedOne(t *testing.T) {
	live := func(name, value string) *http.Cookie { return &http.Cookie{Name: name, Value: value, MaxAge: 60} }
	cleared := &http.Cookie{Name: "cosift_session", MaxAge: -1}
	for _, tc := range []struct {
		name    string
		cookies []*http.Cookie
		want    string
	}{
		{"prefixed with a cleared legacy", []*http.Cookie{live("__Host-cosift_session", "new"), cleared}, "new"},
		{"cleared legacy first", []*http.Cookie{cleared, live("__Host-cosift_session", "new")}, "new"},
		{"legacy only", []*http.Cookie{live("cosift_session", "old")}, "old"},
		{"both live", []*http.Cookie{live("cosift_session", "old"), live("__Host-cosift_session", "new")}, "new"},
		{"both live, prefixed first", []*http.Cookie{live("__Host-cosift_session", "new"), live("cosift_session", "old")}, "new"},
		{"unrelated", []*http.Cookie{live("other", "x")}, ""},
		{"only cleared", []*http.Cookie{cleared}, ""},
	} {
		c := loginSessionCookie(tc.cookies)
		if (c != nil) != (tc.want != "") || (c != nil && c.Value != tc.want) {
			t.Errorf("%s: got %+v want %q", tc.name, c, tc.want)
		}
	}
}
