package wiki_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/pilot-protocol/cosift/internal/articles"
	"github.com/pilot-protocol/cosift/internal/community"
	"github.com/pilot-protocol/cosift/internal/community/wiki"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
)

// The real /v1 listener and article store, in process, seeded through the real routes.

const stackPepper = "wiki-stack-test-pepper-0123456789abcdef"

type oidcID struct{ sub, email string }

var stackOIDC = map[string]oidcID{
	"synth-prod":    {"300000000000000000002", "synth-prod@cosift-test.iam.gserviceaccount.com"},
	"synth-staging": {"300000000000000000003", "synth-staging@cosift-test.iam.gserviceaccount.com"},
	"resolver-prod": {"300000000000000000004", "resolver-prod@cosift-test.iam.gserviceaccount.com"},
}

// hashEmbedder gives each text a fixed unit vector derived from its hash.
type hashEmbedder struct{}

func (hashEmbedder) Model() string { return "wiki-test-embed" }
func (hashEmbedder) Dim() int      { return 32 }
func (hashEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := make([]float32, 32)
		var norm float64
		for j := range v {
			sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", j, t)))
			v[j] = float32(int32(binary.BigEndian.Uint32(sum[:4]))) / math.MaxInt32
			norm += float64(v[j]) * float64(v[j])
		}
		for j := range v {
			v[j] /= float32(math.Sqrt(norm))
		}
		out[i] = v
	}
	return out, nil
}

type corpus struct {
	mu   sync.Mutex
	urls map[string]bool
}

func (c *corpus) HasDocument(_ context.Context, url string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.urls[url], nil
}

type stack struct {
	t      testing.TB
	addr   string
	keys   map[string]string
	signer *svcauthtest.Signer
	corpus *corpus
}

func freeAddr(t testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newStack(t testing.TB) *stack {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(filepath.Join(dir, "pebble"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := &stack{t: t, addr: freeAddr(t), keys: map[string]string{}, signer: svcauthtest.NewSigner("wiki-test"), corpus: &corpus{urls: map[string]bool{}}}
	pol := svcauth.NewPolicy(nil, nil)
	st, err := articles.Open(articles.Options{DB: db, Embedder: hashEmbedder{}, Policy: pol, Corpus: s.corpus,
		ThresholdsPath: filepath.Join(dir, "articles.json"), Logf: func(string, ...any) {}})
	if err != nil {
		t.Fatal(err)
	}
	key := func(id, env string, scopes ...string) map[string]any {
		k, _, err := svcauth.NewKey()
		if err != nil {
			t.Fatal(err)
		}
		s.keys[id] = k
		return map[string]any{"id": id, "kind": "key", "env": env, "scopes": scopes, "rpm": 6000,
			"keys": []any{map[string]any{"key_id": k[4:20], "digest": svcauth.FormatDigest(svcauth.Digest([]byte(stackPepper), k))}}}
	}
	oidc := func(id, env string, scopes ...string) map[string]any {
		return map[string]any{"id": id, "kind": "oidc", "sub": stackOIDC[id].sub, "email": stackOIDC[id].email, "env": env, "scopes": scopes,
			"rpm": 6000, "writes_per_hour": 600, "writes_burst": 600, "writes_per_day": 5000}
	}
	cfg := map[string]any{"schema_version": 1, "listen": s.addr, "principals": []any{
		oidc("synth-prod", "prod", "articles:read", "articles:write", "retrieve:read"),
		oidc("synth-staging", "staging", "articles:read", "articles:write", "retrieve:read"),
		oidc("resolver-prod", "prod", "articles:read", "articles:stub", "retrieve:read"),
		key("dash-prod", "prod", "articles:read_all", "articles:moderate"),
		key("community-wiki", "prod", "articles:read"),
	}}
	b, _ := json.Marshal(cfg)
	path := filepath.Join(dir, "service-auth.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	svc, err := svcauth.New(svcauth.Options{Path: path, OwnerUID: uint32(os.Getuid()), Pepper: []byte(stackPepper),
		Readiness: st.Readiness(), Policy: pol, CertSource: svcauthtest.NewSource(svcauthtest.JWKS(s.signer), 21600).Fetch})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Mount(st.Routes()); err != nil {
		t.Fatal(err)
	}
	pol.SetSwapLocker(st.WriteLocker())
	ctx, cancel := context.WithCancel(context.Background())
	svc.Start(ctx)
	rebuilt := make(chan error, 1)
	go func() { rebuilt <- st.Rebuild(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = svc.Shutdown(context.Background())
		<-rebuilt
		_ = st.Close()
		_ = db.Close()
	})
	deadline := time.Now().Add(10 * time.Second)
	for !st.Readiness().Ready() || svc.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("the /v1 listener or the article store never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s
}

func (s *stack) cred(id string) string {
	if k, ok := s.keys[id]; ok {
		return k
	}
	o := stackOIDC[id]
	return s.signer.Token(svcauthtest.Claims(o.sub, o.email, time.Now()))
}

func (s *stack) do(id, method, path string, body any) (int, string) {
	s.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, "http://"+s.addr+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.cred(id))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(out)
}

func (s *stack) must(id, method, path string, body any) {
	s.t.Helper()
	if code, out := s.do(id, method, path, body); code != 200 && code != 201 {
		s.t.Fatalf("%s %s as %s: %d %s", method, path, id, code, out)
	}
}

// put writes f through the article PUT as writer.
func (s *stack) put(writer string, f fixture) {
	s.t.Helper()
	var cites []any
	for i, c := range f.cites {
		s.corpus.mu.Lock()
		s.corpus.urls[c.url] = true
		s.corpus.mu.Unlock()
		host := strings.SplitN(strings.TrimPrefix(c.url, "https://"), "/", 2)[0]
		cites = append(cites, map[string]any{"n": i + 1, "url": c.url, "title": c.title, "host": host, "quote": c.quote, "content_sha": strings.Repeat(fmt.Sprintf("%x", i+1), 64)})
	}
	status := "published"
	if f.held {
		status = "held"
	}
	s.must(writer, http.MethodPut, "/v1/articles/"+f.id, map[string]any{
		"status": status, "title": f.title, "lead": f.lead, "body_md": f.body, "citations": cites,
		"quality_tier": f.tier, "vertical": f.vertical, "sensitivity": map[string]any{"class": "none", "reason": ""},
		"models": map[string]any{"triage": "triage-test-model", "writer": "writer-test-model"},
		"build":  map[string]any{"sources_considered": 6, "sources_used": len(cites), "retrieval_params": map[string]any{"k": 30}, "cost_usd": 0.02, "dense_degraded": false},
	})
}

func (s *stack) stubPut(f fixture) {
	s.t.Helper()
	s.must("resolver-prod", http.MethodPut, "/v1/articles/"+f.id, map[string]any{"status": "pending", "title": f.title, "vertical": f.vertical, "topic_ids": []string{"8624225ee5e2b9f9"}})
}

func (s *stack) moderate(id, action string) {
	s.t.Helper()
	s.must("dash-prod", http.MethodPost, "/v1/articles/"+id+"/status", map[string]any{"action": action, "by": "operator", "reason_code": "quality"})
}

// seed writes every fixture and applies its moderation.
func (s *stack) seed() {
	s.t.Helper()
	for _, f := range fixtures(s.t) {
		switch f.kind {
		case "stub":
			s.stubPut(f)
		case "other-env":
			s.put("synth-staging", f)
		default:
			s.put("synth-prod", f)
		}
		switch f.kind {
		case "tombstoned":
			s.moderate(f.id, "tombstone")
		case "rejected":
			s.moderate(f.id, "reject")
		}
	}
}

// pages opens the full community app over the stack with the pages key.
func (s *stack) pages(t testing.TB, public bool, now func() time.Time, edit ...func(*community.Config)) *community.Server {
	t.Helper()
	cfg := community.Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: "https://cosift.example", AdminToken: "wiki-test-admin",
		Wiki: wiki.Config{Public: public, EngineURL: "http://" + s.addr, EngineKey: s.keys["community-wiki"], ReportMailto: "reports@example.org", Now: now, Logf: t.Logf}}
	for _, e := range edit {
		e(&cfg)
	}
	srv, err := community.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}
