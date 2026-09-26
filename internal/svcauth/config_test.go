package svcauth

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

func TestParseBaseConfig(t *testing.T) {
	cfg, err := Parse(mustJSON(baseConfig()), testChecks())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != DefaultListen || cfg.FailedAuth != (FailedAuth{30, 10}) || cfg.WritesFrozen || !cfg.GoliveAt.IsZero() {
		t.Fatalf("top level %+v", cfg)
	}
	if len(cfg.Principals) != 8 {
		t.Fatalf("%d principals", len(cfg.Principals))
	}
	mcp := cfg.Principals[0]
	if mcp.Burst != 300 || !mcp.CountsReads || mcp.WritesPerDay != 0 {
		t.Fatalf("mcp-prod %+v", mcp)
	}
	wiki := cfg.Principals[5]
	if wiki.Kind != v1.KindKey || len(wiki.Keys) != 1 || wiki.Keys[0].Digest != Digest([]byte(testPepper), testKeys["community-wiki"]) {
		t.Fatalf("community-wiki %+v", wiki)
	}
}

func TestParseDefaults(t *testing.T) {
	cfg := map[string]any{"schema_version": 1, "principals": []any{
		map[string]any{"id": "synth-prod", "kind": "oidc", "sub": synthSub, "email": synthEmail, "env": "prod", "scopes": []string{"articles:write"}, "rpm": 10},
	}}
	c, err := Parse(mustJSON(cfg), testChecks())
	if err != nil {
		t.Fatal(err)
	}
	p := c.Principals[0]
	if c.FailedAuth != (FailedAuth{30, 10}) || p.Burst != 3 || p.WritesPerHour != 30 || p.WritesBurst != 10 || p.WritesPerDay != 150 {
		t.Fatalf("defaults %+v %+v", c.FailedAuth, p)
	}
	cfg["golive_at"] = "2026-10-12T16:00:00Z"
	cfg["writes_frozen"] = true
	cfg["listen"] = "[::1]:7779"
	c, err = Parse(mustJSON(cfg), testChecks())
	if err != nil {
		t.Fatal(err)
	}
	if !c.WritesFrozen || !c.GoliveAt.Equal(time.Date(2026, 10, 12, 16, 0, 0, 0, time.UTC)) || c.Listen != "[::1]:7779" {
		t.Fatalf("parsed %+v", c)
	}
}

// Each case breaks one rule of the base config; the file must be refused
// with that rule named.
func TestParseRejects(t *testing.T) {
	cases := []struct {
		name string
		edit func(c map[string]any)
		want string
	}{
		{"schema_version missing", func(c map[string]any) { delete(c, "schema_version") }, "schema_version: required"},
		{"schema_version 2", func(c map[string]any) { c["schema_version"] = 2 }, "schema_version: must be 1"},
		{"listen public", func(c map[string]any) { c["listen"] = "0.0.0.0:7779" }, "listen: host must be a loopback IP literal"},
		{"listen localhost", func(c map[string]any) { c["listen"] = "localhost:7779" }, "listen: host must be a loopback IP literal"},
		{"listen no port", func(c map[string]any) { c["listen"] = "127.0.0.1" }, "listen: must be host:port"},
		{"listen port 0", func(c map[string]any) { c["listen"] = "127.0.0.1:0" }, "listen: port must be 1-65535"},
		{"failed_auth.per_minute 0", func(c map[string]any) { c["failed_auth"] = map[string]any{"per_minute": 0} }, "failed_auth.per_minute"},
		{"failed_auth.per_minute 601", func(c map[string]any) { c["failed_auth"] = map[string]any{"per_minute": 601} }, "failed_auth.per_minute"},
		{"failed_auth.burst 101", func(c map[string]any) { c["failed_auth"] = map[string]any{"burst": 101} }, "failed_auth.burst"},
		{"golive_at not RFC 3339", func(c map[string]any) { c["golive_at"] = "2026-10-12" }, "golive_at: must be RFC 3339"},
		{"writes_frozen string", func(c map[string]any) { c["writes_frozen"] = "true" }, "wrong type"},
		{"principals missing", func(c map[string]any) { delete(c, "principals") }, "principals: required"},
		{"principals empty", func(c map[string]any) { c["principals"] = []any{} }, "principals: must hold 1-64"},
		{"principals 65", func(c map[string]any) {
			var ps []any
			for range 65 {
				ps = append(ps, principalByID(c, "mcp-prod"))
			}
			c["principals"] = ps
		}, "principals: must hold 1-64"},
		{"unknown top-level field", func(c map[string]any) { c["listen_addr"] = "x" }, `unknown field "listen_addr"`},
		{"unknown principal field", func(c map[string]any) { principalByID(c, "mcp-prod")["scope"] = []string{"articles:read"} }, `unknown field "scope"`},
		{"unknown failed_auth field", func(c map[string]any) { c["failed_auth"] = map[string]any{"perminute": 3} }, `unknown field "perminute"`},
		{"id missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "id") }, "principals[0].id: required"},
		{"id bad", func(c map[string]any) { principalByID(c, "mcp-prod")["id"] = "MCP" }, "principals[0].id: must match"},
		{"id too long", func(c map[string]any) { principalByID(c, "mcp-prod")["id"] = strings.Repeat("a", 41) }, "principals[0].id: must match"},
		{"id duplicate", func(c map[string]any) { principalByID(c, "synth-prod")["id"] = "mcp-prod" }, "principals[1].id: duplicate"},
		{"kind missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "kind") }, "principals[0].kind: required"},
		{"kind bad", func(c map[string]any) { principalByID(c, "mcp-prod")["kind"] = "user" }, "principals[0].kind: must be oidc or key"},
		{"oidc sub missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "sub") }, "principals[0].sub: required"},
		{"oidc sub short", func(c map[string]any) { principalByID(c, "mcp-prod")["sub"] = "12345" }, "principals[0].sub: must match"},
		{"oidc sub duplicate", func(c map[string]any) { principalByID(c, "synth-prod")["sub"] = mcpSub }, "principals[1].sub: duplicate"},
		{"oidc email missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "email") }, "principals[0].email: required"},
		{"oidc email upper", func(c map[string]any) {
			principalByID(c, "mcp-prod")["email"] = "MCP-prod@cosift-test.iam.gserviceaccount.com"
		}, "principals[0].email: must be"},
		{"oidc user account", func(c map[string]any) { principalByID(c, "mcp-prod")["email"] = "someone@gmail.com" }, "principals[0].email: must be"},
		{"oidc email duplicate", func(c map[string]any) { principalByID(c, "synth-prod")["email"] = mcpEmail }, "principals[1].email: duplicate"},
		{"oidc with keys", func(c map[string]any) {
			principalByID(c, "mcp-prod")["keys"] = []any{keyEntry(testKeys["golive-admin"])}
		}, "principals[0].keys: not allowed"},
		{"key with sub", func(c map[string]any) { principalByID(c, "community-wiki")["sub"] = "1234567" }, "principals[5].sub: not allowed"},
		{"key with email", func(c map[string]any) { principalByID(c, "community-wiki")["email"] = mcpEmail }, "principals[5].email: not allowed"},
		{"key keys missing", func(c map[string]any) { delete(principalByID(c, "community-wiki"), "keys") }, "principals[5].keys: required"},
		{"key keys empty", func(c map[string]any) { principalByID(c, "community-wiki")["keys"] = []any{} }, "principals[5].keys: must hold 1-2"},
		{"key keys three", func(c map[string]any) {
			principalByID(c, "community-wiki")["keys"] = []any{keyEntry(testKeys["community-wiki"]), keyEntry(testKeys["golive-admin"]), keyEntry(testKeys["dash-prod-2"])}
		}, "principals[5].keys: must hold 1-2"},
		{"key_id bad", func(c map[string]any) {
			principalByID(c, "community-wiki")["keys"] = []any{map[string]any{"key_id": "ABCDEF0123456789", "digest": "hmac-sha256:" + strings.Repeat("0", 64)}}
		}, "principals[5].keys[0].key_id: must match"},
		{"key_id duplicate across principals", func(c map[string]any) {
			principalByID(c, "dash-prod")["keys"] = []any{keyEntry(testKeys["community-wiki"])}
		}, "principals[6].keys[0].key_id: duplicate"},
		{"digest bad", func(c map[string]any) {
			principalByID(c, "community-wiki")["keys"] = []any{map[string]any{"key_id": "0123456789abcdef", "digest": "sha256:" + strings.Repeat("0", 64)}}
		}, "principals[5].keys[0].digest: must match"},
		{"created_at bad", func(c map[string]any) {
			e := keyEntry(testKeys["community-wiki"])
			e["created_at"] = "28/09/2026"
			principalByID(c, "community-wiki")["keys"] = []any{e}
		}, "principals[5].keys[0].created_at"},
		{"env missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "env") }, "principals[0].env: required"},
		{"env any", func(c map[string]any) { principalByID(c, "dash-prod")["env"] = "any" }, "principals[6].env: must be prod or staging"},
		{"scopes missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "scopes") }, "principals[0].scopes: must be non-empty"},
		{"scopes empty", func(c map[string]any) { principalByID(c, "mcp-prod")["scopes"] = []string{} }, "principals[0].scopes: must be non-empty"},
		{"scope unknown", func(c map[string]any) { principalByID(c, "mcp-prod")["scopes"] = []string{"articles:reed"} }, "principals[0].scopes: unknown scope"},
		{"scope duplicate", func(c map[string]any) {
			principalByID(c, "mcp-prod")["scopes"] = []string{"articles:read", "articles:read"}
		}, "principals[0].scopes: duplicate scope"},
		{"oidc moderate", func(c map[string]any) {
			principalByID(c, "mcp-prod")["scopes"] = []string{"articles:read", "articles:moderate"}
		}, "articles:moderate not allowed on a oidc principal"},
		{"oidc read_all", func(c map[string]any) { principalByID(c, "mcp-prod")["scopes"] = []string{"articles:read_all"} }, "not allowed on a oidc principal"},
		{"oidc admin", func(c map[string]any) { principalByID(c, "mcp-prod")["scopes"] = []string{"articles:admin"} }, "not allowed on a oidc principal"},
		{"key write", func(c map[string]any) { principalByID(c, "community-wiki")["scopes"] = []string{"articles:write"} }, "not allowed on a key principal"},
		{"key retrieve", func(c map[string]any) { principalByID(c, "community-wiki")["scopes"] = []string{"retrieve:read"} }, "not allowed on a key principal"},
		{"admin with another scope", func(c map[string]any) {
			principalByID(c, "dash-prod")["scopes"] = []string{"articles:admin", "articles:read_all"}
		}, "articles:admin must be the only scope"},
		{"admin on staging", func(c map[string]any) {
			p := principalByID(c, "dash-staging")
			p["scopes"] = []string{"articles:admin"}
		}, "articles:admin must be the only scope"},
		{"rpm missing", func(c map[string]any) { delete(principalByID(c, "mcp-prod"), "rpm") }, "principals[0].rpm: required"},
		{"rpm 0", func(c map[string]any) { principalByID(c, "mcp-prod")["rpm"] = 0 }, "principals[0].rpm: must be 1-6000"},
		{"rpm 6001", func(c map[string]any) { principalByID(c, "mcp-prod")["rpm"] = 6001 }, "principals[0].rpm: must be 1-6000"},
		{"burst over rpm", func(c map[string]any) { principalByID(c, "mcp-prod")["burst"] = 1201 }, "principals[0].burst: must be 1-rpm"},
		{"burst 0", func(c map[string]any) { principalByID(c, "mcp-prod")["burst"] = 0 }, "principals[0].burst: must be 1-rpm"},
		{"writes_per_hour on a reader", func(c map[string]any) { principalByID(c, "mcp-prod")["writes_per_hour"] = 3 }, "principals[0].writes_*"},
		{"writes_burst on a reader", func(c map[string]any) { principalByID(c, "community-wiki")["writes_burst"] = 3 }, "principals[5].writes_*"},
		{"writes_per_day on a reader", func(c map[string]any) { principalByID(c, "dash-prod")["writes_per_day"] = 3 }, "principals[6].writes_*"},
		{"writes_per_hour 601", func(c map[string]any) { principalByID(c, "synth-prod")["writes_per_hour"] = 601 }, "principals[1].writes_per_hour"},
		{"writes_burst over hour", func(c map[string]any) { principalByID(c, "synth-prod")["writes_burst"] = 31 }, "principals[1].writes_burst"},
		{"writes_per_day 5001", func(c map[string]any) { principalByID(c, "synth-prod")["writes_per_day"] = 5001 }, "principals[1].writes_per_day"},
		{"writes_per_day 0", func(c map[string]any) { principalByID(c, "synth-prod")["writes_per_day"] = 0 }, "principals[1].writes_per_day"},
		{"counts_reads on staging", func(c map[string]any) { principalByID(c, "synth-staging")["counts_reads"] = true }, "principals[2].counts_reads"},
		{"counts_reads without read", func(c map[string]any) { principalByID(c, "dash-prod")["counts_reads"] = true }, "principals[6].counts_reads"},
		{"digest equals the admin token's", func(c map[string]any) {
			principalByID(c, "dash-prod")["keys"] = []any{map[string]any{"key_id": "fedcba9876543210", "digest": FormatDigest(Digest([]byte(testPepper), adminToken))}}
		}, "principals[6].keys[0].digest: equals the digest of an existing engine token"},
		{"digest equals the peer token's", func(c map[string]any) {
			principalByID(c, "dash-staging")["keys"] = []any{map[string]any{"key_id": "fedcba9876543210", "digest": FormatDigest(Digest([]byte(testPepper), peerToken))}}
		}, "principals[7].keys[0].digest: equals the digest of an existing engine token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.edit(c)
			_, err := Parse(mustJSON(c), testChecks())
			if err == nil {
				t.Fatalf("accepted; want %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q, want %q", err, tc.want)
			}
		})
	}
}

func TestParseRejectsAmbiguousJSON(t *testing.T) {
	base := string(mustJSON(baseConfig()))
	cases := map[string]string{
		"duplicate key":        strings.Replace(base, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		"case-variant key":     strings.Replace(base, `"writes_frozen":false`, `"Writes_frozen":true`, 1),
		"kelvin-sign key":      strings.Replace(base, `"kind":"key"`, "\"\u212aind\":\"key\"", 1),
		"null value":           strings.Replace(base, `"writes_frozen":false`, `"writes_frozen":null`, 1),
		"trailing data":        base + `{}`,
		"not an object":        `[1]`,
		"invalid JSON":         `{"schema_version":`,
		"nested duplicate key": strings.Replace(base, `"rpm":1200`, `"rpm":1200,"rpm":6000`, 1),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if doc == base {
				t.Fatal("edit did not apply")
			}
			if _, err := Parse([]byte(doc), testChecks()); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// With key auth disabled the collision check cannot run and key principals
// cannot authenticate; the file is still valid.
func TestParseShortPepperIsNotAnError(t *testing.T) {
	c := baseConfig()
	principalByID(c, "dash-prod")["keys"] = []any{map[string]any{"key_id": "fedcba9876543210", "digest": FormatDigest(Digest([]byte(strings.Repeat("p", 31)), adminToken))}}
	if _, err := Parse(mustJSON(c), Checks{Pepper: []byte(strings.Repeat("p", 31)), AdminToken: adminToken}); err != nil {
		t.Fatal(err)
	}
}

func TestReadFileOwnershipAndMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service-auth.json")
	uid := uint32(os.Getuid())
	if _, err := ReadFile(path, uid); !errors.Is(err, ErrAbsent) {
		t.Fatalf("missing file: %v", err)
	}
	writeConfig(t, path, baseConfig())
	if _, err := ReadFile(path, uid); err != nil {
		t.Fatalf("0640: %v", err)
	}
	for _, mode := range []os.FileMode{0o660, 0o644, 0o641, 0o642, 0o604} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFile(path, uid); err == nil || !strings.Contains(err.Error(), "mode") {
			t.Errorf("mode %o accepted (%v)", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0o600, 0o640, 0o400} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFile(path, uid); err != nil {
			t.Errorf("mode %o refused: %v", mode, err)
		}
	}
	if _, err := ReadFile(path, uid+1); err == nil || !strings.Contains(err.Error(), "owner") {
		t.Errorf("wrong owner accepted (%v)", err)
	}
	if os.Getuid() == 0 {
		if err := os.Chown(path, 12345, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFile(path, 0); err == nil || !strings.Contains(err.Error(), "owner") {
			t.Errorf("file owned by uid 12345 accepted for uid 0 (%v)", err)
		}
	}
	if _, err := ReadFile(dir, uid); err == nil {
		t.Error("directory accepted")
	}
}

func TestKeyPrincipalEntries(t *testing.T) {
	kp := KeyPrincipal{ID: "community-wiki", Env: "prod", Scopes: []string{"articles:read"}, RPM: 1200}
	var d [32]byte
	d[0] = 0xab
	p, el, err := kp.Entries("0123456789abcdef", d, "2026-09-28")
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := "hmac-sha256:ab" + strings.Repeat("0", 62)
	if got, want := string(p), `{"id":"community-wiki","kind":"key","keys":[{"key_id":"0123456789abcdef","digest":"`+wantDigest+`","created_at":"2026-09-28"}],"env":"prod","scopes":["articles:read"],"rpm":1200}`; got != want {
		t.Fatalf("entry\n got %s\nwant %s", got, want)
	}
	if got, want := string(el), `{"key_id":"0123456789abcdef","digest":"`+wantDigest+`","created_at":"2026-09-28"}`; got != want {
		t.Fatalf("element %s", got)
	}
	var round map[string]any
	if err := json.Unmarshal(p, &round); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []KeyPrincipal{
		{ID: "Bad", Env: "prod", Scopes: []string{"articles:read"}, RPM: 1},
		{ID: "x", Env: "any", Scopes: []string{"articles:read"}, RPM: 1},
		{ID: "x", Env: "prod", Scopes: []string{"articles:write"}, RPM: 1},
		{ID: "x", Env: "prod", Scopes: []string{"articles:admin", "articles:read"}, RPM: 1},
		{ID: "x", Env: "staging", Scopes: []string{"articles:admin"}, RPM: 1},
		{ID: "x", Env: "staging", Scopes: []string{"articles:read"}, RPM: 1, CountsReads: true},
		{ID: "x", Env: "prod", Scopes: []string{"articles:read"}, RPM: 0},
		{ID: "x", Env: "prod", Scopes: []string{"articles:read"}, RPM: 4, Burst: 5},
		{ID: "x", Env: "prod", Scopes: []string{"articles:read"}, RPM: 4, Burst: -1},
	} {
		if _, _, err := bad.Entries("0123456789abcdef", d, "2026-09-28"); err == nil {
			t.Errorf("%+v accepted", bad)
		} else if strings.Contains(err.Error(), "principals[") {
			t.Errorf("error keeps the file path: %v", err)
		}
	}
}

func TestReadEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cosift.env")
	body := "# comment\n; other comment\nOPENAI_API_KEY=x\n  COSIFT_SVC_PEPPER = \"a\\\"b\\\\c\"\nOTHER='q'\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if v, ok := ReadEnvFile(path, "COSIFT_SVC_PEPPER"); !ok || v != `a"b\c` {
		t.Fatalf("got %q %v", v, ok)
	}
	if v, ok := ReadEnvFile(path, "OTHER"); !ok || v != "q" {
		t.Fatalf("got %q %v", v, ok)
	}
	if _, ok := ReadEnvFile(path, "MISSING"); ok {
		t.Fatal("found a missing key")
	}
	if _, ok := ReadEnvFile(filepath.Join(t.TempDir(), "nope"), "X"); ok {
		t.Fatal("found in a missing file")
	}
}

func TestParseListenAgainstMainAddr(t *testing.T) {
	cases := []struct {
		listen, main string
		refused      bool
	}{
		{"127.0.0.1:7777", "127.0.0.1:7777", true},
		{"127.0.0.1:7777", ":7777", true},
		{"127.0.0.1:7777", "0.0.0.0:7777", true},
		{"[::1]:7777", "[::]:7777", true},
		{"127.0.0.1:7777", "localhost:7777", true},
		{"", "127.0.0.1:7779", true},
		{"127.0.0.1:7779", "127.0.0.1:7777", false},
		{"127.0.0.1:7777", "127.0.0.2:7777", false},
		{"127.0.0.1:7777", "", false},
	}
	for _, tc := range cases {
		c := baseConfig()
		if tc.listen != "" {
			c["listen"] = tc.listen
		}
		ch := testChecks()
		ch.MainAddr = tc.main
		_, err := Parse(mustJSON(c), ch)
		if tc.refused != (err != nil) || (err != nil && err.Error() != "listen: must differ from the engine's main address") {
			t.Errorf("listen %q main %q: %v", tc.listen, tc.main, err)
		}
	}
}
