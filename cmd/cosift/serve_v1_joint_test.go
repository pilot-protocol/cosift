package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

// Tests of the listener and the real article store together.

type oidcID struct{ sub, email string }

var jointOIDC = map[string]oidcID{
	"mcp-prod":         {"300000000000000000001", "mcp-prod@cosift-test.iam.gserviceaccount.com"},
	"synth-prod":       {"300000000000000000002", "synth-prod@cosift-test.iam.gserviceaccount.com"},
	"synth-staging":    {"300000000000000000003", "synth-staging@cosift-test.iam.gserviceaccount.com"},
	"resolver-prod":    {"300000000000000000004", "resolver-prod@cosift-test.iam.gserviceaccount.com"},
	"resolver-staging": {"300000000000000000005", "resolver-staging@cosift-test.iam.gserviceaccount.com"},
}

type joint struct {
	*v1Fixture
	t    *testing.T
	emb  *fixedEmbedder
	rs   v1.Reloaders
	l    *v1Listener
	addr string
	keys map[string]string
	cfg  map[string]any
}

func jointConfig(listen string, keys map[string]string) map[string]any {
	oidc := func(id, env string, scopes []string, extra ...any) map[string]any {
		p := map[string]any{"id": id, "kind": "oidc", "sub": jointOIDC[id].sub, "email": jointOIDC[id].email, "env": env, "scopes": scopes, "rpm": 6000}
		for i := 0; i+1 < len(extra); i += 2 {
			p[extra[i].(string)] = extra[i+1]
		}
		return p
	}
	key := func(id, env string, scopes []string) map[string]any {
		k := keys[id]
		return map[string]any{"id": id, "kind": "key", "keys": []any{map[string]any{"key_id": k[4:20], "digest": svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), k))}}, "env": env, "scopes": scopes, "rpm": 6000}
	}
	return map[string]any{"schema_version": 1, "listen": listen, "principals": []any{
		oidc("mcp-prod", "prod", []string{"articles:read"}, "counts_reads", true),
		oidc("synth-prod", "prod", []string{"articles:read", "articles:write", "retrieve:read"}, "writes_per_hour", 600, "writes_burst", 600, "writes_per_day", 5000),
		oidc("synth-staging", "staging", []string{"articles:read", "articles:write", "retrieve:read"}, "writes_per_hour", 600, "writes_burst", 600, "writes_per_day", 5000),
		oidc("resolver-prod", "prod", []string{"articles:read", "articles:stub", "retrieve:read"}),
		oidc("resolver-staging", "staging", []string{"articles:read", "articles:stub", "retrieve:read"}),
		key("community-wiki", "prod", []string{"articles:read"}),
		key("dash-prod", "prod", []string{"articles:read_all", "articles:moderate"}),
		key("dash-staging", "staging", []string{"articles:read_all", "articles:moderate"}),
		key("golive-admin", "prod", []string{"articles:admin"}),
	}}
}

func jointKeys(t *testing.T) map[string]string {
	keys := map[string]string{}
	for _, id := range []string{"community-wiki", "dash-prod", "dash-staging", "golive-admin"} {
		k, _, err := svcauth.NewKey()
		if err != nil {
			t.Fatal(err)
		}
		keys[id] = k
	}
	return keys
}

// setV1Globals points the listener's files and certificate source at t's.
func setV1Globals(t *testing.T, cfgPath, articlesPath string, signer *svcauthtest.Signer) {
	oldPath, oldUID, oldSrc, oldArt := svcAuthPath, svcAuthOwnerUID, v1CertSource, articlesConfigPath
	svcAuthPath, svcAuthOwnerUID, articlesConfigPath = cfgPath, uint32(os.Getuid()), articlesPath
	v1CertSource = svcauthtest.NewSource(svcauthtest.JWKS(signer), 21600).Fetch
	t.Cleanup(func() {
		svcAuthPath, svcAuthOwnerUID, v1CertSource, articlesConfigPath = oldPath, oldUID, oldSrc, oldArt
	})
	t.Setenv("COSIFT_SVC_PEPPER", v1TestPepper)
}

// newJoint starts the real wiring (startV1) over the fixture's store with the
// real article store; wrap, when set, substitutes the opened store.
func newJoint(t *testing.T, edit func(cfg map[string]any), wrap func(articleLayer) articleLayer) *joint {
	t.Helper()
	f := newV1Fixture(t)
	j := &joint{v1Fixture: f, t: t, emb: &fixedEmbedder{dim: 32}, keys: jointKeys(t)}
	f.srv.v1Embedder = j.emb
	f.srv.hnswAt.Store(nil)
	j.addr = freeAddr(t)
	j.cfg = jointConfig(j.addr, j.keys)
	if edit != nil {
		edit(j.cfg)
	}
	f.writeConfig(j.cfg)
	setV1Globals(t, f.path, filepath.Join(t.TempDir(), "articles.json"), f.signer)
	if wrap != nil {
		real := openArticles
		t.Cleanup(func() { openArticles = real })
		openArticles = func(d articleDeps) (articleLayer, error) {
			a, err := real(d)
			if err != nil {
				return nil, err
			}
			return wrap(a), nil
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	j.l = f.srv.startV1(ctx, &config.Config{}, &j.rs, "")
	t.Cleanup(func() {
		cancel()
		j.l.stop()
		j.l.closeArticles()
	})
	if j.l.svc == nil || j.l.articles == nil || j.l.svc.Addr() == "" {
		t.Fatal("the /v1 listener or the article store did not start")
	}
	if wrap == nil {
		j.waitReady()
	}
	return j
}

func (j *joint) waitReady() {
	j.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !j.l.articles.Readiness().Ready() {
		if time.Now().After(deadline) {
			j.t.Fatal("the article store never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (j *joint) cred(id string) string {
	if k, ok := j.keys[id]; ok {
		return k
	}
	o, ok := jointOIDC[id]
	if !ok {
		j.t.Fatalf("no principal %s", id)
	}
	return j.signer.Token(svcauthtest.Claims(o.sub, o.email, time.Now()))
}

type resp struct {
	code int
	body map[string]any
	raw  string
	hdr  http.Header
}

func (r resp) errCode() string { s, _ := r.body["code"].(string); return s }

func (j *joint) do(id, method, path string, body any) resp {
	j.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://"+j.addr+path, rd)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if id != "" {
		req.Header.Set("Authorization", "Bearer "+j.cred(id))
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		j.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	out := resp{code: res.StatusCode, raw: string(raw), hdr: res.Header}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func (j *joint) want(r resp, code int, errCode string) {
	j.t.Helper()
	if r.code != code || (errCode != "" && r.errCode() != errCode) {
		j.t.Fatalf("got %d %s, want %d %s", r.code, r.raw, code, errCode)
	}
}

func (j *joint) reload() error {
	j.writeConfig(j.cfg)
	return j.rs.Reload()
}

func (j *joint) metricsText() string {
	var b bytes.Buffer
	j.l.svc.Metrics.WritePrometheus(&b)
	return b.String()
}

func (j *joint) metric(series string) string {
	for _, l := range strings.Split(j.metricsText(), "\n") {
		if v, ok := strings.CutPrefix(l, series+" "); ok {
			return v
		}
	}
	return "0"
}

func jointID(n int) string {
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	b := []byte("01J9AB2Q7W4X9M3K5N6P8R0000")
	for i := 25; i >= 22 && n > 0; i-- {
		b[i] = crockford[n%32]
		n /= 32
	}
	return string(b)
}

// jointArticle is a valid article PUT body citing a corpus document.
func jointArticle(title string) map[string]any {
	return map[string]any{
		"status":       "published",
		"title":        title,
		"lead":         "About " + title + ". It is a subject an agent can use on its own.",
		"body_md":      "## Overview\n\n" + strings.Repeat("dense factual prose ", 110) + "[1]\n\n## Key facts\n\n- A fact [1]\n",
		"citations":    []any{map[string]any{"n": 1, "url": "https://x.example/raft", "title": "Raft", "host": "x.example", "quote": "A verbatim quote.", "content_sha": strings.Repeat("a", 64)}},
		"quality_tier": "strong",
		"vertical":     "dev-docs",
		"sensitivity":  map[string]any{"class": "none", "reason": ""},
		"models":       map[string]any{"triage": "triage-model", "writer": "writer-model"},
		"build":        map[string]any{"sources_considered": 4, "sources_used": 1, "retrieval_params": map[string]any{"bm25_k": 30}, "cost_usd": 0.01, "dense_degraded": false},
	}
}

func jointStub(title string, topic string) map[string]any {
	return map[string]any{"status": "pending", "title": title, "vertical": "dev-docs", "topic_ids": []string{topic}}
}

func moderation(action string) map[string]any {
	b := map[string]any{"action": action, "by": "operator"}
	if action != "approve" && action != "hold" {
		b["reason_code"] = "quality"
	}
	return b
}

// One mux holds the store's routes with /v1/search and /v1/contents, and no
// :7777 route is reachable through it.
func TestJointRouteTable(t *testing.T) {
	j := newJoint(t, nil, nil)
	fillID := regexp.MustCompile(`\{[a-z]+\}`)
	for _, rt := range j.l.articles.Routes() {
		j.do("dash-prod", rt.Method, fillID.ReplaceAllString(rt.Path, jointID(1)), map[string]any{})
		if !strings.Contains(j.metricsText(), `cosift_v1_requests_total{principal="dash-prod",route="`+rt.Pattern()+`",`) {
			t.Errorf("%s is not mounted", rt.Pattern())
		}
	}
	if n := len(j.l.articles.Routes()); n != 11 {
		t.Fatalf("%d article routes", n)
	}
	j.want(j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "consensus"}), 200, "")
	j.want(j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{"https://x.example/raft"}}), 200, "")
	src, err := os.ReadFile("serve_setup.go")
	if err != nil {
		t.Fatal(err)
	}
	matches := muxPattern.FindAllStringSubmatch(string(src), -1)
	if len(matches) < 50 {
		t.Fatalf("found %d :7777 patterns", len(matches))
	}
	fill := regexp.MustCompile(`\{[a-z]+(\.\.\.)?\}`)
	for _, m := range matches {
		for _, id := range []string{"synth-prod", "dash-prod"} {
			r := j.do(id, m[1], fill.ReplaceAllString(m[2], "x"), map[string]any{})
			if r.code != 404 || r.errCode() != "not_found" || r.hdr.Get("Location") != "" {
				t.Errorf("%s %s reached: %d %s", m[1], m[2], r.code, r.raw)
			}
		}
	}
	if v := j.metric(`cosift_v1_requests_total{principal="-",route="unmatched",code="404"}`); v != itoa(2*len(matches)) {
		t.Fatalf("unmatched count %s, want %d", v, 2*len(matches))
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

// Per-operation scope, decided by the store after decoding.
func TestJointPerOperationScope(t *testing.T) {
	j := newJoint(t, nil, nil)
	r := j.do("resolver-prod", "PUT", "/v1/articles/"+jointID(1), jointArticle("Stub writers cannot publish"))
	j.want(r, 403, "missing_scope")
	if r.body["detail"] != "requires scope articles:write" {
		t.Fatalf("detail %v", r.body["detail"])
	}
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(2), jointStub("Writers do not write stubs", "8624225e00000001")), 403, "missing_scope")
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+jointID(3), jointStub("Resolver stub title", "8624225e00000002")), 201, "")
	j.want(j.do("mcp-prod", "POST", "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}), 403, "missing_scope")
	j.want(j.do("dash-prod", "POST", "/v1/articles/match", map[string]any{"q": "x"}), 403, "missing_scope")
	j.want(j.do("synth-prod", "POST", "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}), 200, "")
	j.want(j.do("resolver-prod", "POST", "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}), 200, "")
}

// Environment binding end to end, 403 before any 409, and env_golive after
// the go-live purge even with the scopes restored.
func TestJointEnvBinding(t *testing.T) {
	j := newJoint(t, nil, nil)
	prod, pre, stone, preStone := jointID(1), jointID(2), jointID(3), jointID(4)
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+prod, jointArticle("Prod consensus article")), 201, "")
	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+pre, jointArticle("Staging consensus article")), 201, "")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+stone, jointArticle("Prod tombstoned article")), 201, "")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+stone+"/status", moderation("tombstone")), 200, "")
	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+preStone, jointArticle("Staging tombstoned article")), 201, "")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+preStone+"/status", moderation("tombstone")), 200, "")

	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+prod, jointArticle("Prod consensus article")), 403, "env_mismatch")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+pre, jointArticle("Staging consensus article")), 403, "env_mismatch")
	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+stone, jointArticle("Prod tombstoned article")), 403, "env_mismatch")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+preStone, jointArticle("Staging tombstoned article")), 403, "env_mismatch")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+stone, jointArticle("Prod tombstoned article")), 409, "moderation_locked")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+prod+"/status", moderation("hold")), 403, "env_mismatch")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+pre+"/status", moderation("hold")), 403, "env_mismatch")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+stone+"/status", moderation("approve")), 403, "env_mismatch")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+prod+"/status", moderation("hold")), 200, "")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+pre+"/status", moderation("hold")), 200, "")

	staging := map[string][]string{}
	for _, p := range principalsOfCfg(j.cfg) {
		if p["env"] == "staging" && p["kind"] == "oidc" {
			staging[p["id"].(string)] = p["scopes"].([]string)
			p["scopes"] = []string{"articles:read", "retrieve:read"}
			for _, k := range []string{"writes_per_hour", "writes_burst", "writes_per_day"} {
				delete(p, k)
			}
		}
	}
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	dry := j.do("golive-admin", "POST", "/v1/articles/purge-prelive", map[string]any{"apply": false})
	j.want(dry, 200, "")
	records := int(dry.body["records"].(float64))
	j.want(j.do("golive-admin", "POST", "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": records}), 200, "")
	for _, p := range principalsOfCfg(j.cfg) {
		if s, ok := staging[p["id"].(string)]; ok {
			p["scopes"] = s
		}
	}
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+jointID(9), jointArticle("After go-live article")), 403, "env_golive")
	j.want(j.do("resolver-staging", "PUT", "/v1/articles/"+jointID(10), jointStub("After go-live stub", "8624225e00000009")), 403, "env_golive")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+prod+"/status", moderation("approve")), 403, "env_golive")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+jointID(11)+"/status", moderation("approve")), 403, "env_golive")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(12), jointArticle("Prod after go-live article")), 201, "")
}

func principalsOfCfg(cfg map[string]any) []map[string]any {
	var out []map[string]any
	for _, p := range cfg["principals"].([]any) {
		out = append(out, p.(map[string]any))
	}
	return out
}

func principalOfCfg(cfg map[string]any, id string) map[string]any {
	for _, p := range principalsOfCfg(cfg) {
		if p["id"] == id {
			return p
		}
	}
	panic(id)
}

// The write budget is charged once per write, after decoding and before
// validation or embedding; a frozen or undecodable write is not charged.
func TestJointWriteBudget(t *testing.T) {
	j := newJoint(t, func(cfg map[string]any) {
		p := principalOfCfg(cfg, "synth-prod")
		p["writes_per_hour"], p["writes_burst"], p["writes_per_day"] = 600, 600, 5
	}, nil)
	charged := func(want string) {
		t.Helper()
		if v := j.metric(`cosift_v1_writes_total{principal="synth-prod"}`); v != want {
			t.Fatalf("writes_total %s, want %s", v, want)
		}
	}
	stone := jointID(1)
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+stone, jointArticle("Budget tombstone article")), 201, "")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+stone+"/status", moderation("tombstone")), 200, "")
	charged("1")
	embeds := j.emb.calls()
	bad := jointArticle("Budget invalid article")
	bad["body_md"] = "## Overview\n\nshort [1]\n"
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(2), bad), 422, "invalid_field")
	charged("2")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+stone, jointArticle("Budget tombstone article")), 409, "moderation_locked")
	charged("3")
	r, _ := http.NewRequest("PUT", "http://"+j.addr+"/v1/articles/"+jointID(3), strings.NewReader(`{"status":`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+j.cred("synth-prod"))
	if res, err := http.DefaultClient.Do(r); err != nil || res.StatusCode != 400 {
		t.Fatalf("undecodable write: %v %v", err, res)
	} else {
		res.Body.Close()
	}
	charged("3")
	if n := j.emb.calls(); n != embeds {
		t.Fatalf("refused writes reached the embedder (%d calls)", n-embeds)
	}
	j.cfg["writes_frozen"] = true
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(4), jointArticle("Budget frozen article")), 503, "writes_frozen")
	charged("3")
	j.cfg["writes_frozen"] = false
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(5), jointArticle("Budget fourth article")), 201, "")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(6), jointArticle("Budget fifth article")), 201, "")
	charged("5")
	r2 := j.do("synth-prod", "PUT", "/v1/articles/"+jointID(7), jointArticle("Budget sixth article"))
	j.want(r2, 429, "write_budget")
	if r2.hdr.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	j.want(j.do("synth-prod", "GET", "/v1/articles", nil), 200, "")
	if m := j.do("synth-prod", "POST", "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}); m.body["writes"] != "budget_exhausted" {
		t.Fatalf("build match: %s", m.raw)
	}
}

func TestJointWritesFrozen(t *testing.T) {
	j := newJoint(t, nil, nil)
	id := jointID(1)
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+id, jointArticle("Frozen test article")), 201, "")
	stub := jointID(2)
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+stub, jointStub("Frozen test stub", "8624225e00000003")), 201, "")
	j.cfg["writes_frozen"] = true
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	r := j.do("synth-prod", "PUT", "/v1/articles/"+jointID(3), jointArticle("Frozen second article"))
	j.want(r, 503, "writes_frozen")
	if r.hdr.Get("Retry-After") != "60" {
		t.Fatalf("Retry-After %q", r.hdr.Get("Retry-After"))
	}
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+jointID(4), jointStub("Frozen second stub", "8624225e00000004")), 503, "writes_frozen")
	j.want(j.do("resolver-prod", "DELETE", "/v1/articles/"+stub, nil), 503, "writes_frozen")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+id+"/status", moderation("hold")), 200, "")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+stub+"/status", moderation("delete_stub")), 200, "")
	if m := j.do("synth-prod", "POST", "/v1/articles/match", map[string]any{"q": "x", "purpose": "build"}); m.body["writes"] != "frozen" {
		t.Fatalf("build match: %s", m.raw)
	}
}

// delayedRebuild holds the real store's rebuild until released.
type delayedRebuild struct {
	articleLayer
	until chan struct{}
}

func (d *delayedRebuild) Rebuild(ctx context.Context) error {
	select {
	case <-d.until:
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.articleLayer.Rebuild(ctx)
}

// Before the store's rebuild every article route is 503 index_unavailable,
// while search and contents serve.
func TestJointReadiness(t *testing.T) {
	until := make(chan struct{})
	j := newJoint(t, nil, func(a articleLayer) articleLayer { return &delayedRebuild{articleLayer: a, until: until} })
	for _, rt := range j.l.articles.Routes() {
		path := regexp.MustCompile(`\{[a-z]+\}`).ReplaceAllString(rt.Path, "x")
		for _, id := range []string{"dash-prod", "synth-prod", "golive-admin"} {
			r := j.do(id, rt.Method, path, map[string]any{})
			if r.code != 503 || r.errCode() != "index_unavailable" || r.hdr.Get("Retry-After") != "5" {
				t.Fatalf("%s as %s before the rebuild: %d %s", rt.Pattern(), id, r.code, r.raw)
			}
		}
	}
	j.want(j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "consensus"}), 200, "")
	j.want(j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{"https://x.example/raft"}}), 200, "")
	wantUnauthenticated := j.do("", "GET", "/v1/articles/stats", nil)
	j.want(wantUnauthenticated, 401, "unauthenticated")
	close(until)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 25 {
				req, _ := http.NewRequest("GET", "http://"+j.addr+"/v1/articles/stats", nil)
				req.Header.Set("Authorization", "Bearer "+j.keys["dash-prod"])
				if res, err := http.DefaultClient.Do(req); err == nil {
					res.Body.Close()
				}
			}
		})
	}
	wg.Wait()
	j.waitReady()
	j.want(j.do("dash-prod", "GET", "/v1/articles/stats", nil), 200, "")
}

// A config with golive_at over a store without the go-live flag serves
// nothing that needs read, write or stub; moderation keeps working.
func TestJointRestorePending(t *testing.T) {
	j := newJoint(t, func(cfg map[string]any) { cfg["golive_at"] = "2026-10-12T16:00:00Z" }, nil)
	j.want(j.do("mcp-prod", "POST", "/v1/articles/match", map[string]any{"q": "x"}), 503, "restore_pending")
	j.want(j.do("community-wiki", "GET", "/v1/articles", nil), 503, "restore_pending")
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+jointID(1), jointArticle("Restore pending article")), 503, "restore_pending")
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+jointID(2), jointStub("Restore pending stub", "8624225e00000005")), 503, "restore_pending")
	j.want(j.do("dash-prod", "GET", "/v1/articles/stats", nil), 200, "")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+jointID(3)+"/status", moderation("hold")), 404, "not_found")
	j.want(j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "consensus"}), 200, "")
}

// One SIGHUP reloads both files independently: a bad articles.json keeps its
// values while service-auth.json still reloads.
func TestJointReloadsBothFiles(t *testing.T) {
	logs := captureDefaultLog(t)
	j := newJoint(t, nil, nil)
	thresholds := func() (float64, float64) {
		r := j.do("dash-prod", "GET", "/v1/articles/stats", nil)
		th, _ := r.body["thresholds"].(map[string]any)
		c, _ := th["covered"].(float64)
		rel, _ := th["related"].(float64)
		return c, rel
	}
	writeArticles := func(body string) {
		if err := os.WriteFile(articlesConfigPath, []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(articlesConfigPath, 0o640); err != nil {
			t.Fatal(err)
		}
	}
	wantC, wantR := 0.78, 0.74
	if os.Getuid() == 0 {
		writeArticles(`{"schema_version":1,"theta_covered":0.9,"theta_related":0.8,"measured":true}`)
		if err := j.rs.Reload(); err != nil {
			t.Fatal(err)
		}
		wantC, wantR = 0.9, 0.8
	}
	if c, r := thresholds(); c != wantC || r != wantR {
		t.Fatalf("thresholds %v %v, want %v %v", c, r, wantC, wantR)
	}
	writeArticles(`{"schema_version":1,"theta_covered":0.5,"theta_related":0.6}`)
	principalOfCfg(j.cfg, "community-wiki")["scopes"] = []string{"articles:read_all"}
	err := j.reload()
	if err == nil || !strings.Contains(err.Error(), "articles:") || strings.Contains(err.Error(), "service-auth:") {
		t.Fatalf("reload error %v", err)
	}
	if c, r := thresholds(); c != wantC || r != wantR {
		t.Fatalf("a bad articles.json changed the thresholds to %v %v", c, r)
	}
	j.want(j.do("community-wiki", "GET", "/v1/articles/stats", nil), 200, "")
	wantOK := "0"
	if os.Getuid() == 0 {
		wantOK = "1"
	}
	if ok, bad := j.metric(`cosift_v1_config_reloads_total{result="ok"}`), j.metric(`cosift_v1_config_reloads_total{result="error"}`); ok != wantOK || bad != "1" {
		t.Fatalf("reloads ok %s error %s, want %s and 1: one outcome per SIGHUP", ok, bad, wantOK)
	}
	if n := strings.Count(logs.String(), "articles: ERROR reload of "+articlesConfigPath+" refused ("); n != 1 {
		t.Fatalf("%d ERROR lines for articles.json:\n%s", n, logs.String())
	}
}

// A write authorised under the old config and still embedding when a reload
// removes the scope is refused in the store lock; nothing is written.
func TestJointConfigSwapDuringWrite(t *testing.T) {
	j := newJoint(t, nil, nil)
	title := "Swap during write article"
	entered, release := j.emb.holdOn(title)
	id := jointID(1)
	done := make(chan resp, 1)
	go func() {
		req, _ := http.NewRequest("PUT", "http://"+j.addr+"/v1/articles/"+id, bytes.NewReader(mustMarshal(jointArticle(title))))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+j.cred("synth-prod"))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- resp{code: -1}
			return
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		out := resp{code: res.StatusCode, raw: string(raw)}
		_ = json.Unmarshal(raw, &out.body)
		done <- out
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the PUT never reached the embedder")
	}
	p := principalOfCfg(j.cfg, "synth-prod")
	p["scopes"] = []string{"articles:read", "retrieve:read"}
	for _, k := range []string{"writes_per_hour", "writes_burst", "writes_per_day"} {
		delete(p, k)
	}
	reloaded := make(chan error, 1)
	go func() { reloaded <- j.reload() }()
	select {
	case err := <-reloaded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reload waited on an embedding write")
	}
	close(release)
	r := <-done
	j.want(r, 403, "missing_scope")
	j.want(j.do("dash-prod", "GET", "/v1/articles/"+id, nil), 404, "not_found")

	lock := j.l.articles.WriteLocker()
	lock.Lock()
	go func() { reloaded <- j.reload() }()
	select {
	case <-reloaded:
		lock.Unlock()
		t.Fatal("a reload swapped the config while the store lock was held")
	case <-time.After(200 * time.Millisecond):
	}
	lock.Unlock()
	if err := <-reloaded; err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Through runPebbleServe: a match, a PUT and a search leave the embedding
// cache empty, and the read counters flushed at shutdown, before Pebble
// closes, survive a restart.
func TestJointEmbedCacheAndShutdownFlush(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatal(err)
	}
	f.Close()
	embedSrv := fakeEmbeddingServer(t, f.dim)
	signer := svcauthtest.NewSigner("kid-1")
	keys := jointKeys(t)
	v1Addr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "service-auth.json")
	if err := os.WriteFile(cfgPath, mustMarshal(jointConfig(v1Addr, keys)), 0o640); err != nil {
		t.Fatal(err)
	}
	setV1Globals(t, cfgPath, filepath.Join(t.TempDir(), "articles.json"), signer)
	t.Setenv("COSIFT_LOAD_HNSW", "true")
	cacheDir := t.TempDir()
	j := &joint{v1Fixture: &v1Fixture{signer: signer}, t: t, keys: keys, addr: v1Addr}

	serve := func(body func(mainAddr string)) {
		mainAddr := freeAddr(t)
		cfg := &config.Config{Server: config.Server{Addr: mainAddr}, Embeddings: config.Embeddings{URL: embedSrv.URL, Model: "m", Dim: f.dim, CacheDir: cacheDir}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runPebbleServe(ctx, cfg, []string{"-dir", f.dir, "-addr", mainAddr}) }()
		deadline := time.Now().Add(30 * time.Second)
		for {
			req, _ := http.NewRequest("GET", "http://"+v1Addr+"/v1/articles/stats", nil)
			req.Header.Set("Authorization", "Bearer "+keys["dash-prod"])
			if res, err := http.DefaultClient.Do(req); err == nil {
				res.Body.Close()
				if res.StatusCode == 200 {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("the store never served")
			}
			time.Sleep(20 * time.Millisecond)
		}
		body(mainAddr)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(40 * time.Second):
			t.Fatal("runPebbleServe did not return")
		}
	}
	files := func() int {
		e, _ := os.ReadDir(cacheDir)
		return len(e)
	}
	denseReady := func() {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			if s := j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "raft", "retriever": "dense"}); s.code == 200 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("dense search never served")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	title := "Raft consensus explained"
	id := jointID(1)
	serve(func(string) {
		j.want(j.do("synth-prod", "PUT", "/v1/articles/"+id, jointArticle(title)), 201, "")
		m := j.do("mcp-prod", "POST", "/v1/articles/match", map[string]any{"q": title, "reader": strings.Repeat("ab", 16)})
		if m.code != 200 || m.body["verdict"] != "covered" {
			t.Fatalf("match: %d %s", m.code, m.raw)
		}
		denseReady()
		if n := files(); n != 0 {
			t.Fatalf("the /v1 match, PUT and search left %d cache files", n)
		}
	})
	serve(func(mainAddr string) {
		l := j.do("community-wiki", "GET", "/v1/articles?order=readers_7d", nil)
		items, _ := l.body["items"].([]any)
		if len(items) != 1 || items[0].(map[string]any)["readers_7d"] != 1.0 {
			t.Fatalf("read counters after a restart: %s", l.raw)
		}
		denseReady()
		res, err := http.Get("http://" + mainAddr + "/search?q=raft+consensus&retriever=dense")
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if files() == 0 {
			t.Fatal("/search wrote no cache file either; the assertion is vacuous")
		}
	})
}

// fakeEmbeddingServer answers the OpenAI embeddings shape with deterministicVec.
func fakeEmbeddingServer(t *testing.T, dim int) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		data := make([]map[string]any, len(req.Input))
		for i, in := range req.Input {
			data[i] = map[string]any{"index": i, "embedding": deterministicVec(in, dim)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// captureLog tees the default logger into a buffer for the test.
func captureDefaultLog(t *testing.T) *lockedBuffer {
	buf := &lockedBuffer{}
	prev := log.Writer()
	log.SetOutput(io.MultiWriter(prev, buf))
	t.Cleanup(func() { log.SetOutput(prev) })
	return buf
}

// serveProcess runs runPebbleServe in the test process and waits for :7777.
func serveProcess(t *testing.T, cfg *config.Config, dir string) (mainAddr string, stop func() error) {
	t.Helper()
	mainAddr = freeAddr(t)
	cfg.Server.Addr = mainAddr
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runPebbleServe(ctx, cfg, []string{"-dir", dir, "-addr", mainAddr}) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if res, err := http.Get("http://" + mainAddr + "/healthz"); err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				break
			}
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("runPebbleServe returned before serving: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal(":7777 never answered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(40 * time.Second):
				err = io.ErrNoProgress
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return mainAddr, stop
}

// A service-auth.json whose listen is the main address is refused, :7777
// serves, and a taken :7777 port fails the engine before /v1 is ever bound.
func TestServeListenCollision(t *testing.T) {
	logs := captureDefaultLog(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), svcauthtest.NewSigner("k"))
	mainAddr := freeAddr(t)
	if err := os.WriteFile(cfgPath, mustMarshal(jointConfig(mainAddr, jointKeys(t))), 0o640); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runPebbleServe(ctx, &config.Config{Server: config.Server{Addr: mainAddr}}, []string{"-dir", filepath.Join(dir, "pebble"), "-addr", mainAddr})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := http.Get("http://" + mainAddr + "/healthz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 && strings.Contains(res.Header.Get("Content-Type"), "json") {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("the engine exited: %v\n%s", err, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf(":7777 never answered:\n%s", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "listen: must differ from the engine's main address") || strings.Contains(logs.String(), "svcauth: /v1 listening on") {
		t.Fatalf("log:\n%s", logs.String())
	}
	cancel()
	<-done

	logs2 := captureDefaultLog(t)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	v1Addr := freeAddr(t)
	if err := os.WriteFile(cfgPath, mustMarshal(jointConfig(v1Addr, jointKeys(t))), 0o640); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = runPebbleServe(context.Background(), &config.Config{}, []string{"-dir", filepath.Join(dir, "pebble2"), "-addr", taken.Addr().String()})
	if err == nil || time.Since(start) > 10*time.Second {
		t.Fatalf("a taken :7777 port: %v after %s", err, time.Since(start))
	}
	if strings.Contains(logs2.String(), "svcauth: /v1 listening on") {
		t.Fatalf("/v1 started although :7777 could not bind:\n%s", logs2.String())
	}
}

// A /v1 address that cannot be bound leaves /v1 down with an ERROR and :7777
// serving.
func TestServeV1BindFailure(t *testing.T) {
	logs := captureDefaultLog(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), svcauthtest.NewSigner("k"))
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := os.WriteFile(cfgPath, mustMarshal(jointConfig(held.Addr().String(), jointKeys(t))), 0o640); err != nil {
		t.Fatal(err)
	}
	mainAddr, stop := serveProcess(t, &config.Config{}, filepath.Join(dir, "pebble"))
	if !strings.Contains(logs.String(), "svcauth: ERROR cannot listen on "+held.Addr().String()+" — /v1 listener disabled") {
		t.Fatalf("log:\n%s", logs.String())
	}
	res, err := http.Get("http://" + mainAddr + "/healthz")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf(":7777: %v %v", err, res)
	}
	res.Body.Close()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

// slowShape holds the store's rebuild inside its first embedder call.
type slowShape struct {
	inner embed.Embedder
	gate  chan struct{}
	once  sync.Once
}

func (s *slowShape) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return s.inner.Embed(ctx, texts)
}
func (s *slowShape) Model() string { return s.inner.Model() }
func (s *slowShape) Dim() int {
	s.once.Do(func() { <-s.gate })
	return s.inner.Dim()
}

// While the article rebuild is slow, :7777 answers, the article routes answer
// index_unavailable, search serves, and a real SIGHUP through the engine's
// reloaders reloads both files; /metrics carries the /v1 series.
func TestServeSlowRebuildAndSIGHUP(t *testing.T) {
	logs := captureDefaultLog(t)
	f := populatedPebbleStore(t)
	f.Close()
	embedSrv := fakeEmbeddingServer(t, f.dim)
	signer := svcauthtest.NewSigner("kid-1")
	keys := jointKeys(t)
	v1Addr := freeAddr(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	jcfg := jointConfig(v1Addr, keys)
	if err := os.WriteFile(cfgPath, mustMarshal(jcfg), 0o640); err != nil {
		t.Fatal(err)
	}
	articlesPath := filepath.Join(dir, "articles.json")
	setV1Globals(t, cfgPath, articlesPath, signer)
	gate := make(chan struct{})
	real := openArticles
	t.Cleanup(func() { openArticles = real })
	openArticles = func(d articleDeps) (articleLayer, error) {
		d.embedder = &slowShape{inner: d.embedder, gate: gate}
		return real(d)
	}
	oldRS := sighupReloaders
	sighupReloaders = &v1.Reloaders{}
	t.Cleanup(func() { sighupReloaders = oldRS })
	stopHUP := handleSIGHUP(sighupReloaders)
	defer stopHUP()

	start := time.Now()
	mainAddr, stop := serveProcess(t, &config.Config{Embeddings: config.Embeddings{URL: embedSrv.URL, Model: "m", Dim: f.dim}}, f.dir)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf(":7777 took %s behind the rebuild", d)
	}
	j := &joint{v1Fixture: &v1Fixture{signer: signer}, t: t, keys: keys, addr: v1Addr}
	j.want(j.do("dash-prod", "GET", "/v1/articles/stats", nil), 503, "index_unavailable")
	j.want(j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "raft consensus"}), 200, "")
	j.want(j.do("community-wiki", "GET", "/v1/articles", nil), 503, "index_unavailable")

	principalOfCfg(jcfg, "community-wiki")["scopes"] = []string{"articles:read_all"}
	principalOfCfg(jcfg, "synth-prod")["scopes"] = []string{"articles:read", "articles:write"}
	if err := os.WriteFile(cfgPath, mustMarshal(jcfg), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(articlesPath, []byte(`{"schema_version":1,"theta_covered":0.5,"theta_related":0.6}`), 0o640); err != nil {
		t.Fatal(err)
	}
	metrics := func() string {
		res, err := http.Get("http://" + mainAddr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitReloads := func(ok, bad string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			m := metrics()
			if strings.Contains(m, `cosift_v1_config_reloads_total{result="ok"} `+ok+"\n") && strings.Contains(m, `cosift_v1_config_reloads_total{result="error"} `+bad+"\n") {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("reloads, want ok %s error %s:\n%s", ok, bad, m)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitReloads("0", "1")
	if !strings.Contains(logs.String(), "articles: ERROR reload of "+articlesPath+" refused (") {
		t.Fatalf("no ERROR line for articles.json:\n%s", logs.String())
	}
	j.want(j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "raft consensus"}), 403, "missing_scope")
	if err := os.Remove(articlesPath); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitReloads("1", "1")
	close(gate)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if r := j.do("community-wiki", "GET", "/v1/articles/stats", nil); r.code == 200 {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("after the rebuild: %d %s", r.code, r.raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if m := metrics(); !strings.Contains(m, `cosift_v1_requests_total{principal="community-wiki",route="GET /v1/articles/stats",code="200"}`) {
		t.Fatalf("/metrics lacks the /v1 series:\n%s", m)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

// A tombstone and a purge conversion embed the stored title when its row is
// missing or of another model; through runPebbleServe those embeds, like a
// match and a PUT, leave the embedding cache empty.
func TestJointTakedownEmbedsStayUncached(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var mu sync.Mutex
	var batches [][]string
	failLeads := false
	embedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		batches = append(batches, req.Input)
		fail := failLeads
		mu.Unlock()
		for _, in := range req.Input {
			if fail && strings.HasPrefix(in, "About ") {
				http.Error(w, "down", http.StatusInternalServerError)
				return
			}
		}
		data := make([]map[string]any, len(req.Input))
		for i, in := range req.Input {
			data[i] = map[string]any{"index": i, "embedding": deterministicVec(in, f.dim)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer embedSrv.Close()
	embeddedAlone := func(text string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range batches {
			if len(b) == 1 && b[0] == text {
				return true
			}
		}
		return false
	}
	signer := svcauthtest.NewSigner("kid-1")
	keys := jointKeys(t)
	v1Addr := freeAddr(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	jcfg := jointConfig(v1Addr, keys)
	if err := os.WriteFile(cfgPath, mustMarshal(jcfg), 0o640); err != nil {
		t.Fatal(err)
	}
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), signer)
	t.Setenv("COSIFT_LOAD_HNSW", "true")
	cacheDir := t.TempDir()
	files := func() int {
		e, _ := os.ReadDir(cacheDir)
		return len(e)
	}
	j := &joint{v1Fixture: &v1Fixture{signer: signer}, t: t, keys: keys, addr: v1Addr}
	ready := func() {
		deadline := time.Now().Add(30 * time.Second)
		for {
			req, _ := http.NewRequest("GET", "http://"+v1Addr+"/v1/articles/stats", nil)
			req.Header.Set("Authorization", "Bearer "+keys["dash-prod"])
			if res, err := http.DefaultClient.Do(req); err == nil {
				res.Body.Close()
				if res.StatusCode == 200 {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatal("the store never served")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	embeds := func(model string) config.Embeddings {
		return config.Embeddings{URL: embedSrv.URL, Model: model, Dim: f.dim, CacheDir: cacheDir}
	}

	stub, prod, pre := jointID(1), jointID(2), jointID(3)
	_, stop := serveProcess(t, &config.Config{Embeddings: embeds("model-a")}, f.dir)
	ready()
	j.want(j.do("resolver-prod", "PUT", "/v1/articles/"+stub, jointStub("Planted stub title", "8624225e00000011")), 201, "")
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+stub+"/status", moderation("tombstone")), 200, "")
	if !embeddedAlone("Planted stub title") {
		t.Fatal("the stub tombstone embedded no fingerprint title")
	}
	j.want(j.do("synth-prod", "PUT", "/v1/articles/"+prod, jointArticle("Prod takedown title")), 201, "")
	j.want(j.do("synth-staging", "PUT", "/v1/articles/"+pre, jointArticle("Prelive rejected title")), 201, "")
	j.want(j.do("dash-staging", "POST", "/v1/articles/"+pre+"/status", moderation("reject")), 200, "")
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	for _, p := range principalsOfCfg(jcfg) {
		if p["env"] == "staging" && p["kind"] == "oidc" {
			p["scopes"] = []string{"articles:read", "retrieve:read"}
			for _, k := range []string{"writes_per_hour", "writes_burst", "writes_per_day"} {
				delete(p, k)
			}
		}
	}
	if err := os.WriteFile(cfgPath, mustMarshal(jcfg), 0o640); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	failLeads = true
	mu.Unlock()
	mainAddr, stop := serveProcess(t, &config.Config{Embeddings: embeds("model-b")}, f.dir)
	ready()
	j.want(j.do("dash-prod", "POST", "/v1/articles/"+prod+"/status", moderation("tombstone")), 200, "")
	if !embeddedAlone("Prod takedown title") {
		t.Fatal("the tombstone of an old-model article did not re-embed its title")
	}
	dry := j.do("golive-admin", "POST", "/v1/articles/purge-prelive", map[string]any{"apply": false})
	j.want(dry, 200, "")
	j.want(j.do("golive-admin", "POST", "/v1/articles/purge-prelive", map[string]any{"apply": true, "expect_records": int(dry.body["records"].(float64))}), 200, "")
	if !embeddedAlone("Prelive rejected title") {
		t.Fatal("the purge conversion did not re-embed the rejected title")
	}
	if n := files(); n != 0 {
		t.Fatalf("the takedown embeds left %d cache files", n)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if s := j.do("synth-prod", "POST", "/v1/search", map[string]any{"q": "raft", "retriever": "dense"}); s.code == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dense search never served")
		}
		time.Sleep(20 * time.Millisecond)
	}
	res, err := http.Get("http://" + mainAddr + "/search?q=raft+leader&retriever=dense")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if files() == 0 {
		t.Fatal("/search wrote no cache file either; the assertion is vacuous")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}
