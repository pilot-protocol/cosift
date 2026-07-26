package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/index"
)

// ---------------------------------------------------------------------------
// peer/admin token comparison
// ---------------------------------------------------------------------------

func TestPeerTokenOK(t *testing.T) {
	req := func(auth string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/admin/x", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		return r
	}
	cases := []struct {
		name string
		auth string
		want string
		ok   bool
	}{
		{"empty want accepts anything", "", "", true},
		{"empty want accepts a bogus token", "Bearer nonsense", "", true},
		{"exact match", "Bearer s3cret", "s3cret", true},
		{"wrong token", "Bearer s3cres", "s3cret", false},
		{"prefix of the real token", "Bearer s3c", "s3cret", false},
		{"real token plus suffix", "Bearer s3cretX", "s3cret", false},
		{"missing header", "", "s3cret", false},
		{"wrong scheme", "Basic s3cret", "s3cret", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := peerTokenOK(req(tc.auth), tc.want); got != tc.ok {
				t.Fatalf("peerTokenOK(%q, %q) = %v, want %v", tc.auth, tc.want, got, tc.ok)
			}
		})
	}
}

// bearerCompare matches a direct string comparison of a request-supplied
// bearer credential against the configured token. Every admin gate must route
// through peerTokenOK instead, which compares in constant time.
var bearerCompare = regexp.MustCompile(
	`(?:got != want|!= "Bearer "\+want|r\.Header\.Get\("Authorization"\) != )`)

func TestAdminGatesUseConstantTimeCompare(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	var offenders []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if bearerCompare.MatchString(line) {
				offenders = append(offenders, f+":"+strconv.Itoa(i+1)+": "+strings.TrimSpace(line))
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("token comparisons not routed through peerTokenOK:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}

// requireAdmin must still reject a wrong token and admit the right one.
func TestRequireAdminGate(t *testing.T) {
	s := &pebbleHTTP{}
	s.cluster = config.Cluster{PeerAuthToken: "topsecret"}
	called := 0
	h := s.requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusOK)
	})

	r := httptest.NewRequest(http.MethodPost, "/admin/x", nil)
	r.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d, want 401", w.Code)
	}
	if called != 0 {
		t.Fatalf("handler ran for a wrong token")
	}

	r = httptest.NewRequest(http.MethodPost, "/admin/x", nil)
	r.Header.Set("Authorization", "Bearer topsecret")
	w = httptest.NewRecorder()
	h(w, r)
	if w.Code != http.StatusOK || called != 1 {
		t.Fatalf("right token: status %d called %d, want 200/1", w.Code, called)
	}
}

// ---------------------------------------------------------------------------
// embed-backfill durability
// ---------------------------------------------------------------------------

// TestEmbedBackfillPersistsHNSW checks that vectors added by the backfill
// handler are written to the store, not just to the in-memory graph: after the
// handler returns, loading the graph back from Pebble must yield the nodes.
func TestEmbedBackfillPersistsHNSW(t *testing.T) {
	f := populatedPebbleStore(t)
	mock := openaiTestServer(t)
	srv := f.makeServer(mock)
	// Start from an empty graph so every corpus doc counts as missing.
	srv.hnsw = index.NewHNSW(f.dim)

	ctx := context.Background()
	if _, ok, err := index.LoadHNSWMeta(ctx, f.ps); err != nil {
		t.Fatalf("LoadHNSWMeta: %v", err)
	} else if ok {
		t.Fatalf("precondition: store already holds HNSW meta")
	}

	r := httptest.NewRequest(http.MethodPost, "/admin/embed-backfill",
		strings.NewReader(`{"workers":2}`))
	w := httptest.NewRecorder()
	srv.handleEmbedBackfill(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Missing      int    `json:"missing"`
		Embedded     int    `json:"embedded"`
		Persisted    bool   `json:"persisted"`
		PersistError string `json:"persist_error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (%s)", err, w.Body.String())
	}
	if resp.Embedded == 0 {
		t.Fatalf("nothing embedded: %s", w.Body.String())
	}
	if !resp.Persisted {
		t.Fatalf("handler reported persisted=false (err=%q)", resp.PersistError)
	}
	if srv.hnsw.Len() == 0 {
		t.Fatalf("in-memory graph is empty after backfill")
	}

	loaded, ok, err := index.LoadHNSW(ctx, f.ps)
	if err != nil {
		t.Fatalf("LoadHNSW: %v", err)
	}
	if !ok {
		t.Fatalf("backfilled vectors were not persisted to the store")
	}
	if loaded.Len() != srv.hnsw.Len() {
		t.Fatalf("persisted graph has %d nodes, in-memory has %d",
			loaded.Len(), srv.hnsw.Len())
	}
}
