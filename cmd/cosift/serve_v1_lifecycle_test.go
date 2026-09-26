package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

var (
	cosiftBinOnce sync.Once
	cosiftBin     string
	cosiftBinErr  error
)

func buildCosift(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the binary")
	}
	cosiftBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cosift-v1-bin-")
		if err != nil {
			cosiftBinErr = err
			return
		}
		cosiftBin = filepath.Join(dir, "cosift")
		out, err := exec.Command("go", "build", "-trimpath", "-o", cosiftBin, ".").CombinedOutput()
		if err != nil {
			cosiftBinErr = fmt.Errorf("%v: %s", err, out)
		}
	})
	if cosiftBinErr != nil {
		t.Fatal(cosiftBinErr)
	}
	return cosiftBin
}

type lineWatcher struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	lines chan string
}

func watch(r io.Reader) *lineWatcher {
	w := &lineWatcher{lines: make(chan string, 256)}
	go func() {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			w.mu.Lock()
			w.buf.WriteString(sc.Text() + "\n")
			w.mu.Unlock()
			select {
			case w.lines <- sc.Text():
			default:
			}
		}
	}()
	return w
}

func (w *lineWatcher) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// seen waits up to d for a line containing substr.
func (w *lineWatcher) seen(substr string, d time.Duration) bool {
	deadline := time.After(d)
	for {
		select {
		case l := <-w.lines:
			if strings.Contains(l, substr) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// A SIGHUP during a slow start does not kill the engine. The config
// is a FIFO, so the process blocks reading it, before any HNSW load.
func TestSIGHUPDuringSlowStart(t *testing.T) {
	bin := buildCosift(t)
	dir := t.TempDir()
	fifo := filepath.Join(dir, "cosift.json")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	addr := freeAddr(t)
	cmd := exec.Command(bin, "-config", fifo, "pebble-serve", "-dir", filepath.Join(dir, "pebble"), "-addr", addr)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	logs := watch(stderr)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
	})
	logs.seen("svcauth: SIGHUP reload enabled", 5*time.Second)
	alive := func(stage string) {
		t.Helper()
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("%s: exited (%v)\n%s", stage, err, logs.String())
		case <-time.After(500 * time.Millisecond):
		}
	}
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	alive("SIGHUP while blocked on the config")

	w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteString("{}"); err != nil {
		t.Fatal(err)
	}
	w.Close()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("never served:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	alive("SIGHUP while serving")
	if !strings.Contains(logs.String(), "svcauth: SIGHUP reload enabled") {
		t.Fatalf("no startup line:\n%s", logs.String())
	}
	if _, err := os.Stat(svcauth.DefaultPath); errors.Is(err, os.ErrNotExist) && !strings.Contains(logs.String(), "svcauth: "+svcauth.DefaultPath+" absent — /v1 listener disabled") {
		t.Fatalf("no absent WARN:\n%s", logs.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		exited <- err
		if err != nil {
			t.Fatalf("exit after SIGTERM: %v\n%s", err, logs.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no exit after SIGTERM")
	}
}

type fakeArticleLayer struct {
	deps     articleDeps
	lock     sync.Mutex
	entered  chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	events   []string
	addr     string
	storeErr error
}

func (a *fakeArticleLayer) event(e string) {
	a.mu.Lock()
	a.events = append(a.events, e)
	a.mu.Unlock()
}

func (a *fakeArticleLayer) Routes() []v1.Route {
	return []v1.Route{{Method: "GET", Path: "/v1/articles", Scopes: []v1.Scope{v1.ScopeArticlesRead}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(a.entered)
		<-a.release
		a.event("handler done")
		v1.WriteJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	})}}
}

func (a *fakeArticleLayer) WriteLocker() sync.Locker { return &a.lock }

func (a *fakeArticleLayer) Rebuild(context.Context) error {
	a.event("rebuild")
	a.deps.readiness.SetReady()
	return nil
}

func (a *fakeArticleLayer) ReloadConfig() error { return nil }

func (a *fakeArticleLayer) Close() error {
	a.event("close")
	if c, err := net.DialTimeout("tcp", a.addr, 200*time.Millisecond); err == nil {
		c.Close()
		a.event("listener still open")
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				a.storeErr = fmt.Errorf("panic: %v", p)
			}
		}()
		_, _, a.storeErr = a.deps.store.CorpusStats(context.Background())
	}()
	return nil
}

// Shutdown stops :7779 with :7777 and drains its handlers,
// then closes the article store, then Pebble.
func TestV1ShutdownOrder(t *testing.T) {
	dir := t.TempDir()
	key, keyID, err := svcauth.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	v1Addr := freeAddr(t)
	cfgPath := filepath.Join(dir, "service-auth.json")
	b, _ := json.Marshal(map[string]any{"schema_version": 1, "listen": v1Addr, "principals": []any{
		map[string]any{"id": "community-wiki", "kind": "key", "keys": []any{map[string]any{"key_id": keyID, "digest": svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), key))}}, "env": "prod", "scopes": []string{"articles:read"}, "rpm": 60},
	}})
	if err := os.WriteFile(cfgPath, b, 0o640); err != nil {
		t.Fatal(err)
	}
	a := &fakeArticleLayer{entered: make(chan struct{}), release: make(chan struct{}), addr: v1Addr}
	oldPath, oldUID, oldSrc, oldOpen := svcAuthPath, svcAuthOwnerUID, v1CertSource, openArticles
	svcAuthPath, svcAuthOwnerUID = cfgPath, uint32(os.Getuid())
	v1CertSource = svcauthtest.NewSource(svcauthtest.JWKS(svcauthtest.NewSigner("k")), 21600).Fetch
	openArticles = func(d articleDeps) (articleLayer, error) {
		if d.store == nil || d.policy == nil || d.readiness == nil {
			return nil, errors.New("missing dependency")
		}
		a.deps = d
		return a, nil
	}
	t.Cleanup(func() { svcAuthPath, svcAuthOwnerUID, v1CertSource, openArticles = oldPath, oldUID, oldSrc, oldOpen })
	t.Setenv("COSIFT_SVC_PEPPER", v1TestPepper)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mainAddr := freeAddr(t)
	done := make(chan error, 1)
	go func() {
		done <- runPebbleServe(ctx, &config.Config{Server: config.Server{Addr: mainAddr}}, []string{"-dir", filepath.Join(dir, "pebble"), "-addr", mainAddr})
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := http.Get("http://" + v1Addr + "/v1/nope")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 404 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("/v1 never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	codes := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest("GET", "http://"+v1Addr+"/v1/articles", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			codes <- -1
			return
		}
		resp.Body.Close()
		codes <- resp.StatusCode
	}()
	select {
	case <-a.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("request never reached the article handler")
	}
	cancel()
	time.Sleep(300 * time.Millisecond)
	a.mu.Lock()
	early := strings.Join(a.events, ",")
	a.mu.Unlock()
	if strings.Contains(early, "close") {
		t.Fatalf("the article store closed with a handler in flight: %s", early)
	}
	close(a.release)
	if code := <-codes; code != http.StatusOK {
		t.Fatalf("in-flight request: %d", code)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("runPebbleServe did not return")
	}
	if got := strings.Join(a.events, ","); got != "rebuild,handler done,close" {
		t.Fatalf("events %s", got)
	}
	if a.storeErr != nil {
		t.Fatalf("Pebble was closed before the article store: %v", a.storeErr)
	}
}

func TestSvcKeyNew(t *testing.T) {
	pepperFile := filepath.Join(t.TempDir(), "cosift.env")
	if err := os.WriteFile(pepperFile, []byte("# env\nCOSIFT_SVC_PEPPER="+v1TestPepper+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COSIFT_SVC_PEPPER", "")
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	var out, errOut bytes.Buffer
	err := runSvcKey([]string{"new", "--id", "community-wiki", "--scopes", "articles:read", "--env", "prod", "--rpm", "1200", "--pepper-file", pepperFile}, &out, &errOut, now)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 2 || !regexp.MustCompile(`^csk_[0-9a-f]{16}_[A-Z2-7]{52}$`).MatchString(lines[0]) {
		t.Fatalf("stdout %q", out.String())
	}
	key := lines[0]
	want := fmt.Sprintf(`{"id":"community-wiki","kind":"key","keys":[{"key_id":"%s","digest":"%s","created_at":"2026-09-28"}],"env":"prod","scopes":["articles:read"],"rpm":1200}`, key[4:20], svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), key)))
	if lines[1] != want {
		t.Fatalf("entry\n got %s\nwant %s", lines[1], want)
	}
	if strings.Contains(errOut.String(), key) || !strings.Contains(errOut.String(), "svc-key: store the key now (it is not recoverable), add the entry to /etc/cosift/service-auth.json,\nsvc-key: run `cosift svc-auth check`, then `systemctl reload cosift-serve`.\n") {
		t.Fatalf("stderr %q", errOut.String())
	}
	doc := `{"schema_version":1,"principals":[` + lines[1] + `]}`
	if _, err := svcauth.Parse([]byte(doc), svcauth.Checks{Pepper: []byte(v1TestPepper)}); err != nil {
		t.Fatalf("the entry does not load: %v", err)
	}

	out.Reset()
	t.Setenv("COSIFT_SVC_PEPPER", v1TestPepper+"-env")
	if err := runSvcKey([]string{"new", "--id", "dash-prod", "--scopes", "articles:read_all,articles:moderate", "--env", "prod", "--key-only", "--pepper-file", pepperFile}, &out, &errOut, now); err != nil {
		t.Fatal(err)
	}
	lines = strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	var el map[string]string
	if err := json.Unmarshal([]byte(lines[1]), &el); err != nil || len(el) != 3 || el["digest"] != svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper+"-env"), lines[0])) {
		t.Fatalf("key-only element %s (%v): the environment pepper must win", lines[1], err)
	}

	var ue *usageError
	var ee *exitError
	for _, args := range [][]string{
		{},
		{"old"},
		{"new"},
		{"new", "--id", "x", "--scopes", "articles:read"},
		{"new", "--id", "x", "--scopes", "articles:write", "--env", "prod"},
		{"new", "--id", "x", "--scopes", "articles:read", "--env", "any"},
		{"new", "--id", "X", "--scopes", "articles:read", "--env", "prod"},
		{"new", "--id", "x", "--scopes", "articles:admin,articles:read", "--env", "prod"},
		{"new", "--id", "x", "--scopes", "articles:read", "--env", "staging", "--counts-reads"},
		{"new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "--rpm", "4", "--burst", "5"},
		{"new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "extra"},
		{"new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "--bogus"},
	} {
		out.Reset()
		if err := runSvcKey(args, &out, &errOut, now); !errors.As(err, &ue) || out.Len() != 0 {
			t.Errorf("%v: %v, stdout %q", args, err, out.String())
		}
	}
	t.Setenv("COSIFT_SVC_PEPPER", "")
	for _, pepper := range []string{"", strings.Repeat("p", 31)} {
		if err := os.WriteFile(pepperFile, []byte("COSIFT_SVC_PEPPER="+pepper+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		out.Reset()
		err := runSvcKey([]string{"new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "--pepper-file", pepperFile}, &out, &errOut, now)
		if !errors.As(err, &ee) || ee.code != 3 || out.Len() != 0 {
			t.Errorf("pepper %d bytes: %v", len(pepper), err)
		}
	}
}

func TestSvcAuthCheck(t *testing.T) {
	oldUID := svcAuthOwnerUID
	svcAuthOwnerUID = uint32(os.Getuid())
	t.Cleanup(func() { svcAuthOwnerUID = oldUID })
	dir := t.TempDir()
	file := filepath.Join(dir, "service-auth.json")
	pepperFile := filepath.Join(dir, "cosift.env")
	if err := os.WriteFile(pepperFile, []byte("COSIFT_SVC_PEPPER="+v1TestPepper+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("COSIFT_SVC_PEPPER", "")
	key, keyID, _ := svcauth.NewKey()
	write := func(digest string, mode os.FileMode) {
		b, _ := json.Marshal(map[string]any{"schema_version": 1, "principals": []any{
			map[string]any{"id": "synth-prod", "kind": "oidc", "sub": v1SynthSub, "email": v1SynthEmail, "env": "prod", "scopes": []string{"articles:read", "articles:write"}, "rpm": 300},
			map[string]any{"id": "dash-prod", "kind": "key", "keys": []any{map[string]any{"key_id": keyID, "digest": digest}}, "env": "prod", "scopes": []string{"articles:read_all", "articles:moderate"}, "rpm": 120},
		}})
		if err := os.WriteFile(file, b, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(file, mode); err != nil {
			t.Fatal(err)
		}
	}
	good := svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), key))
	cfg := &config.Config{Server: config.Server{AdminToken: "admin-secret"}, Cluster: config.Cluster{PeerAuthToken: "peer-secret"}}
	args := []string{"check", "--file", file, "--pepper-file", pepperFile}
	var out, errOut bytes.Buffer
	write(good, 0o640)
	if err := runSvcAuth(cfg, args, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "synth-prod oidc prod articles:read,articles:write\ndash-prod key prod articles:read_all,articles:moderate\n" {
		t.Fatalf("stdout %q", got)
	}
	if strings.Contains(out.String()+errOut.String(), good) || errOut.Len() != 0 {
		t.Fatalf("output holds a digest or a warning: %q %q", out.String(), errOut.String())
	}
	for name, setup := range map[string]func(){
		"collides with the admin token": func() { write(svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), "admin-secret")), 0o640) },
		"collides with the peer token":  func() { write(svcauth.FormatDigest(svcauth.Digest([]byte(v1TestPepper), "peer-secret")), 0o640) },
		"bad mode":                      func() { write(good, 0o644) },
		"bad digest":                    func() { write("hmac-sha256:xyz", 0o640) },
		"absent":                        func() { _ = os.Remove(file) },
	} {
		setup()
		out.Reset()
		err := runSvcAuth(cfg, args, &out, &errOut)
		var ue *usageError
		if err == nil || errors.As(err, &ue) || out.Len() != 0 {
			t.Errorf("%s: %v, stdout %q", name, err, out.String())
		}
	}
	write(good, 0o640)
	errOut.Reset()
	if err := runSvcAuth(cfg, []string{"check", "--file", file, "--pepper-file", filepath.Join(dir, "none")}, &out, &errOut); err != nil {
		t.Fatalf("no pepper is not a refusal: %v", err)
	}
	if !strings.Contains(errOut.String(), "svc-auth: WARN COSIFT_SVC_PEPPER absent") {
		t.Fatalf("stderr %q", errOut.String())
	}
	var ue *usageError
	if err := runSvcAuth(cfg, []string{"verify"}, &out, &errOut); !errors.As(err, &ue) {
		t.Fatalf("usage: %v", err)
	}
}

// The binary maps the CLIs' outcomes to their exit codes.
func TestSvcCLIExitCodes(t *testing.T) {
	bin := buildCosift(t)
	dir := t.TempDir()
	run := func(env []string, args ...string) (int, string, string) {
		cmd := exec.Command(bin, args...)
		cmd.Dir = dir
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}, env...)
		var out, errOut bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errOut
		err := cmd.Run()
		code := 0
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		return code, out.String(), errOut.String()
	}
	none := filepath.Join(dir, "none.env")
	if code, _, _ := run(nil, "svc-key", "new", "--id", "x"); code != 2 {
		t.Errorf("usage: exit %d", code)
	}
	if code, _, _ := run(nil, "svc-key", "new", "--id", "x", "--scopes", "retrieve:read", "--env", "prod", "--pepper-file", none); code != 2 {
		t.Errorf("validation: exit %d", code)
	}
	if code, out, _ := run(nil, "svc-key", "new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "--pepper-file", none); code != 3 || out != "" {
		t.Errorf("no pepper: exit %d stdout %q", code, out)
	}
	code, out, errOut := run([]string{"COSIFT_SVC_PEPPER=" + v1TestPepper}, "svc-key", "new", "--id", "x", "--scopes", "articles:read", "--env", "prod", "--pepper-file", none)
	if code != 0 || strings.Count(out, "\n") != 2 || !strings.HasPrefix(out, "csk_") || strings.Contains(errOut, "csk_") {
		t.Errorf("success: exit %d stdout %q stderr %q", code, out, errOut)
	}
	if code, _, _ := run(nil, "svc-auth", "check", "--file", filepath.Join(dir, "missing.json"), "--pepper-file", none); code != 1 {
		t.Errorf("check refused: exit %d", code)
	}
}

// /v1/search embeds with the inner client, so the on-disk embedding
// cache that /search fills gains no file for a /v1 query.
func TestV1DenseUsesUncachedEmbedder(t *testing.T) {
	f := populatedPebbleStore(t)
	if err := f.hnsw.Persist(context.Background(), f.ps); err != nil {
		t.Fatal(err)
	}
	f.Close()
	var mu sync.Mutex
	var embedded []string
	embedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		embedded = append(embedded, req.Input...)
		mu.Unlock()
		data := make([]map[string]any, len(req.Input))
		for i, in := range req.Input {
			data[i] = map[string]any{"index": i, "embedding": deterministicVec(in, f.dim)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	defer embedSrv.Close()

	signer := svcauthtest.NewSigner("kid-1")
	v1Addr := freeAddr(t)
	cfgPath := filepath.Join(t.TempDir(), "service-auth.json")
	b, _ := json.Marshal(map[string]any{"schema_version": 1, "listen": v1Addr, "principals": []any{
		map[string]any{"id": "synth-prod", "kind": "oidc", "sub": v1SynthSub, "email": v1SynthEmail, "env": "prod", "scopes": []string{"retrieve:read"}, "rpm": 6000},
	}})
	if err := os.WriteFile(cfgPath, b, 0o640); err != nil {
		t.Fatal(err)
	}
	oldPath, oldUID, oldSrc := svcAuthPath, svcAuthOwnerUID, v1CertSource
	svcAuthPath, svcAuthOwnerUID = cfgPath, uint32(os.Getuid())
	v1CertSource = svcauthtest.NewSource(svcauthtest.JWKS(signer), 21600).Fetch
	t.Cleanup(func() { svcAuthPath, svcAuthOwnerUID, v1CertSource = oldPath, oldUID, oldSrc })
	t.Setenv("COSIFT_LOAD_HNSW", "true")
	cacheDir := t.TempDir()
	mainAddr := freeAddr(t)
	cfg := &config.Config{Server: config.Server{Addr: mainAddr}, Embeddings: config.Embeddings{URL: embedSrv.URL, Model: "m", Dim: f.dim, CacheDir: cacheDir}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runPebbleServe(ctx, cfg, []string{"-dir", f.dir, "-addr", mainAddr}) }()
	defer func() {
		cancel()
		<-done
	}()

	query := "raft leader election uncached"
	dense := func() (int, string) {
		req, _ := http.NewRequest("POST", "http://"+v1Addr+"/v1/search", strings.NewReader(`{"q":"`+query+`","retriever":"dense","k":3}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+signer.Token(svcauthtest.Claims(v1SynthSub, v1SynthEmail, time.Now())))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, body := dense()
		if code == 200 {
			if !strings.Contains(body, `"retriever":"dense"`) || !strings.Contains(body, `"url":"https://x.example/`) {
				t.Fatalf("dense response %s", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dense never served: %d %s", code, body)
		}
		time.Sleep(50 * time.Millisecond)
	}
	files := func() int {
		entries, _ := os.ReadDir(cacheDir)
		return len(entries)
	}
	mu.Lock()
	sawQuery := slices.Contains(embedded, query)
	mu.Unlock()
	if !sawQuery {
		t.Fatalf("the /v1 query never reached the embedder: %q", embedded)
	}
	if n := files(); n != 0 {
		t.Fatalf("/v1/search left %d files in the embedding cache", n)
	}
	resp, err := http.Get("http://" + mainAddr + "/search?q=" + strings.ReplaceAll(query, " ", "+") + "&retriever=dense")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if files() == 0 {
		t.Fatal("/search wrote no cache file either; the assertion above is vacuous")
	}
}
