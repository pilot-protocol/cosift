package svcauth

import (
	"net/http"
	"regexp"
	"strings"
	"testing"
)

func TestNewKeyFormat(t *testing.T) {
	re := regexp.MustCompile(`^csk_[0-9a-f]{16}_[A-Z2-7]{52}$`)
	seen := map[string]bool{}
	for range 50 {
		k, id, err := NewKey()
		if err != nil {
			t.Fatal(err)
		}
		if !re.MatchString(k) || k[4:20] != id || seen[k] {
			t.Fatalf("key %q id %q", k, id)
		}
		seen[k] = true
	}
	d := Digest([]byte("key"), "The quick brown fox jumps over the lazy dog")
	if got := FormatDigest(d); got != "hmac-sha256:f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8" {
		t.Fatalf("digest %s", got)
	}
}

func TestKeyAccepts(t *testing.T) {
	h := newHarness(t, baseConfig())
	w := h.do("GET", "/v1/articles/by-slug/rust-async-runtimes", "", bearerAuth(testKeys["community-wiki"]))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d %s", w.Code, w.Body.String())
	}
	if c := h.handlerCalls(); len(c) != 1 || c[0].p.ID != "community-wiki" || c[0].p.Kind != "key" || c[0].p.Env != "prod" {
		t.Fatalf("handler saw %+v", c)
	}
	if w := h.do("GET", "/v1/articles/stats", "", bearerAuth(testKeys["dash-staging"]), remote("[::1]:5555")); w.Code != http.StatusOK {
		t.Fatalf("::1 peer: status %d", w.Code)
	}
}

// Every way a key can fail is the uniform 401 with its own log reason.
func TestKeyRejects(t *testing.T) {
	h := newHarness(t, lenient(baseConfig()), withClientIPHeader("X-Client-IP"))
	key := testKeys["community-wiki"]
	forged := key[:len(key)-1] + map[bool]string{true: "B", false: "A"}[key[len(key)-1] == 'A']
	unknownID := "csk_ffffffffffffffff_" + key[21:]
	type keyCase struct {
		name   string
		opts   []reqOpt
		reason string
	}
	cases := []keyCase{
		{"wrong secret", []reqOpt{bearerAuth(forged)}, "bad_key"},
		{"unknown key_id", []reqOpt{bearerAuth(unknownID)}, "unknown_principal"},
		{"short secret", []reqOpt{bearerAuth(key[:len(key)-1])}, "malformed_credential"},
		{"lower-case secret", []reqOpt{bearerAuth(key[:21] + strings.ToLower(key[21:]))}, "malformed_credential"},
		{"non-loopback peer", []reqOpt{bearerAuth(key), remote("10.0.0.5:4000")}, "key_not_loopback"},
		{"public peer", []reqOpt{bearerAuth(key), remote("203.0.113.9:4000")}, "key_not_loopback"},
		{"unparseable peer", []reqOpt{bearerAuth(key), remote("localhost:4000")}, "key_not_loopback"},
	}
	for _, hdr := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "Forwarded", "X-Real-IP", "CF-Connecting-IP", "True-Client-IP", "CF-Ray", "CDN-Loop", "Via", "X-Client-IP"} {
		cases = append(cases,
			keyCase{"forwarded " + hdr, []reqOpt{bearerAuth(key), header(hdr, "127.0.0.1")}, "key_forwarded"},
			keyCase{"forwarded empty " + hdr, []reqOpt{bearerAuth(key), header(hdr, "")}, "key_forwarded"},
		)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantUnauthenticated(t, h.do("GET", "/v1/articles", "", tc.opts...))
			if got := h.lastReason(); got != tc.reason {
				t.Fatalf("reason %q, want %q", got, tc.reason)
			}
		})
	}
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
}

func TestKeysDisabledByPepper(t *testing.T) {
	for name, pepper := range map[string]string{"no pepper": "", "31 bytes": strings.Repeat("p", 31)} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, baseConfig(), withPepper(pepper))
			wantUnauthenticated(t, h.do("GET", "/v1/articles", "", bearerAuth(testKeys["community-wiki"])))
			if got := h.lastReason(); got != "keys_disabled" {
				t.Fatalf("reason %q", got)
			}
			if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))); w.Code != http.StatusOK {
				t.Fatalf("OIDC with keys disabled: status %d", w.Code)
			}
		})
	}
	h := newHarness(t, baseConfig(), withPepper(strings.Repeat("p", 32)))
	wantUnauthenticated(t, h.do("GET", "/v1/articles", "", bearerAuth(testKeys["community-wiki"])))
	if got := h.lastReason(); got != "bad_key" {
		t.Fatalf("32-byte pepper: reason %q", got)
	}
}

// Rotation: both keys of a principal work, and removing one stops it.
func TestKeyRotation(t *testing.T) {
	cfg := baseConfig()
	principalByID(cfg, "dash-prod")["keys"] = []any{keyEntry(testKeys["dash-prod"]), keyEntry(testKeys["dash-prod-2"])}
	h := newHarness(t, cfg)
	for _, k := range []string{testKeys["dash-prod"], testKeys["dash-prod-2"]} {
		if w := h.do("GET", "/v1/articles/stats", "", bearerAuth(k)); w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
	}
	principalByID(cfg, "dash-prod")["keys"] = []any{keyEntry(testKeys["dash-prod-2"])}
	writeConfig(t, h.path, cfg)
	if err := h.svc.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	wantUnauthenticated(t, h.do("GET", "/v1/articles/stats", "", bearerAuth(testKeys["dash-prod"])))
	if w := h.do("GET", "/v1/articles/stats", "", bearerAuth(testKeys["dash-prod-2"])); w.Code != http.StatusOK {
		t.Fatalf("remaining key: status %d", w.Code)
	}
	for _, c := range h.handlerCalls() {
		if c.p.ID != "dash-prod" {
			t.Fatalf("principal %s", c.p.ID)
		}
	}
}
