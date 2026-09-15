package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/server"
)

func mustResolver(t *testing.T, cidrs ...string) *server.ClientIPResolver {
	t.Helper()
	r, err := server.NewClientIPResolver(cidrs)
	if err != nil {
		t.Fatalf("NewClientIPResolver(%v): %v", cidrs, err)
	}
	return r
}

// hitRateLimit runs one request through the global rate-limit middleware and
// reports the status plus the problem "detail" string (which carries the key
// the limiter used).
func hitRateLimit(t *testing.T, s *pebbleHTTP, remoteAddr, xff string) (int, string) {
	t.Helper()
	h := s.rateLimit(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	req.RemoteAddr = remoteAddr
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	var body struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body.Detail
}

func mustResolverWithHeader(t *testing.T, header string, cidrs ...string) *server.ClientIPResolver {
	t.Helper()
	r, err := server.NewClientIPResolverWithHeader(cidrs, header)
	if err != nil {
		t.Fatalf("NewClientIPResolverWithHeader(%v, %q): %v", cidrs, header, err)
	}
	return r
}

func oneTokenLimiter() *rateLimiter {
	return &rateLimiter{rpm: 0.0001, burst: 1, whitelist: map[string]bool{}}
}

// T0.4: behind a trusted proxy the bucket keys on the XFF client, so two
// different clients arriving over the same proxy connection do not share it.
func TestRateLimitKeysOnXFFFromTrustedProxy(t *testing.T) {
	s := &pebbleHTTP{rl: oneTokenLimiter(), ipResolver: mustResolver(t, "127.0.0.0/8")}
	if code, detail := hitRateLimit(t, s, "127.0.0.1:41000", "1.1.1.1"); code != http.StatusOK {
		t.Fatalf("first client: code=%d detail=%q", code, detail)
	}
	if code, detail := hitRateLimit(t, s, "127.0.0.1:41001", "2.2.2.2"); code != http.StatusOK {
		t.Fatalf("second client shares the proxy's bucket: code=%d detail=%q", code, detail)
	}
	code, detail := hitRateLimit(t, s, "127.0.0.1:41002", "1.1.1.1")
	if code != http.StatusTooManyRequests {
		t.Fatalf("repeat of first client: code=%d, want 429", code)
	}
	if !strings.Contains(detail, "ip=1.1.1.1") {
		t.Errorf("detail = %q, want the XFF client as the key", detail)
	}
}

// The spoof case: an untrusted direct peer may not pick its own bucket.
func TestRateLimitIgnoresXFFFromUntrustedPeer(t *testing.T) {
	s := &pebbleHTTP{rl: oneTokenLimiter(), ipResolver: mustResolver(t, "127.0.0.0/8")}
	if code, _ := hitRateLimit(t, s, "9.9.9.9:2000", "1.1.1.1"); code != http.StatusOK {
		t.Fatalf("first request should pass")
	}
	code, detail := hitRateLimit(t, s, "9.9.9.9:2001", "2.2.2.2")
	if code != http.StatusTooManyRequests {
		t.Fatalf("spoofed XFF bought a fresh bucket: code=%d", code)
	}
	if !strings.Contains(detail, "ip=9.9.9.9") {
		t.Errorf("detail = %q, want the direct peer as the key", detail)
	}
}

func TestRateLimitMultiHopXFFSkipsTrustedHops(t *testing.T) {
	s := &pebbleHTTP{rl: oneTokenLimiter(), ipResolver: mustResolver(t, "127.0.0.0/8", "104.28.0.0/16")}
	if code, _ := hitRateLimit(t, s, "127.0.0.1:3000", "5.6.7.8, 104.28.216.88"); code != http.StatusOK {
		t.Fatalf("first request should pass")
	}
	code, detail := hitRateLimit(t, s, "127.0.0.1:3001", "5.6.7.8, 104.28.216.99")
	if code != http.StatusTooManyRequests {
		t.Fatalf("code=%d, want 429 — same client behind two trusted edge hops", code)
	}
	if !strings.Contains(detail, "ip=5.6.7.8") {
		t.Errorf("detail = %q, want the leftmost untrusted hop as the key", detail)
	}
}

func TestResolveClientIPWithoutResolverStripsPort(t *testing.T) {
	s := &pebbleHTTP{}
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	req.RemoteAddr = "8.8.8.8:1234"
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	if got := s.resolveClientIP(req); got != "8.8.8.8" {
		t.Errorf("got %q, want 8.8.8.8 (no trusted proxies configured)", got)
	}
}

// T0.3: /feedback keyed on the leftmost XFF hop let any caller mint a bucket.
func TestFeedbackRateLimitIgnoresSpoofedLeftmostHop(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fb-*.jsonl")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer f.Close()
	s := &pebbleHTTP{fbFile: f, fbRL: oneTokenLimiter()}

	post := func(xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/feedback", strings.NewReader(`{"query_id":"q1","rating":1}`))
		req.RemoteAddr = "9.9.9.9:5000"
		req.Header.Set("X-Forwarded-For", xff)
		rec := httptest.NewRecorder()
		s.handleFeedback(rec, req)
		return rec.Code
	}
	if code := post("1.1.1.1"); code != http.StatusOK {
		t.Fatalf("first feedback: code=%d", code)
	}
	if code := post("2.2.2.2"); code != http.StatusTooManyRequests {
		t.Fatalf("code=%d, want 429 — a spoofed XFF must not buy a fresh bucket", code)
	}
}

func lastQueryLogRec(t *testing.T, path string) queryLogRec {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open qlog: %v", err)
	}
	defer f.Close()
	var last string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			last = line
		}
	}
	if last == "" {
		t.Fatalf("query log %s is empty", path)
	}
	var rec queryLogRec
	if err := json.Unmarshal([]byte(last), &rec); err != nil {
		t.Fatalf("unmarshal %q: %v", last, err)
	}
	return rec
}

func TestQueryLogRecordsResolvedClientIP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "qlog.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	s := &pebbleHTTP{qlogFile: f, ipResolver: mustResolver(t, "127.0.0.0/8")}
	h := s.qlog(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/query?q=x", nil)
	req.RemoteAddr = "127.0.0.1:7000"
	req.Header.Set("X-Forwarded-For", "5.6.7.8, 9.9.9.9")
	h(httptest.NewRecorder(), req)
	if got := lastQueryLogRec(t, path).Caller; got != "9.9.9.9" {
		t.Errorf("caller = %q, want 9.9.9.9 (rightmost untrusted hop)", got)
	}

	direct := httptest.NewRequest(http.MethodGet, "/query?q=y", nil)
	direct.RemoteAddr = "8.8.8.8:1234"
	h(httptest.NewRecorder(), direct)
	if got := lastQueryLogRec(t, path).Caller; got != "8.8.8.8" {
		t.Errorf("caller = %q, want 8.8.8.8 (direct peer, port stripped)", got)
	}
}

func TestPebbleServeRejectsMalformedTrustedProxies(t *testing.T) {
	cfg := config.Default()
	cfg.Server.TrustedProxies = []string{"10.0.0.0/8", "not-a-cidr"}
	dir := filepath.Join(t.TempDir(), "pebble")
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runPebbleServe(ctx, cfg, []string{"-dir", dir, "-addr", addr}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected startup to fail on a malformed trusted_proxies CIDR")
		}
		if !strings.Contains(err.Error(), "trusted_proxies") {
			t.Errorf("err = %v, want it to name trusted_proxies", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPebbleServe kept serving with a malformed trusted_proxies CIDR")
	}
}

func TestLoopbackWhitelistFlagsLoopbackEntries(t *testing.T) {
	rl := &rateLimiter{whitelist: parseIPWhitelist("104.28.216.88, 127.0.0.1 ,::1, ")}
	got := rl.loopbackWhitelist()
	if len(got) != 2 {
		t.Fatalf("loopbackWhitelist() = %v, want the two loopback entries", got)
	}
	if rl := (&rateLimiter{whitelist: parseIPWhitelist("104.28.216.88")}); len(rl.loopbackWhitelist()) != 0 {
		t.Errorf("non-loopback whitelist flagged: %v", rl.loopbackWhitelist())
	}
}

// --- two-tier limiter, through the real mux ---

// serveWith launches pebble-serve against an empty store on a loopback port
// and returns its base URL.
func serveWith(t *testing.T, cfg *config.Config) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble")
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg.Server.Addr = addr
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runPebbleServe(ctx, cfg, []string{"-dir", dir, "-addr", addr}) }()
	if !waitForPort(addr, 8*time.Second) {
		cancel()
		t.Fatalf("server didn't come up on %s", addr)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Logf("server shutdown took >5s")
		}
	})
	return "http://" + addr
}

// serveForRateLimit is the configured shape: requests below arrive over
// loopback, so trusting 127.0.0.0/8 is what makes X-Forwarded-For the bucket
// key.
func serveForRateLimit(t *testing.T) string {
	t.Helper()
	return serveWith(t, &config.Config{Server: config.Server{TrustedProxies: []string{"127.0.0.0/8"}}})
}

// serveProdShaped is the live unit's topology: Caddy reverse-proxying to a
// loopback listener with server.trusted_proxies deliberately NOT set. Every
// request therefore arrives from a loopback peer carrying X-Forwarded-For.
func serveProdShaped(t *testing.T) string {
	t.Helper()
	return serveWith(t, &config.Config{})
}

// prodLimiterEnv is the live unit's rate-limit environment.
func prodLimiterEnv(t *testing.T) {
	t.Helper()
	t.Setenv("COSIFT_RATELIMIT_RPM", "240")
	t.Setenv("COSIFT_RATELIMIT_BURST", "80")
	t.Setenv("COSIFT_RATELIMIT_WHITELIST", "104.28.216.88,127.0.0.1")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_WHITELIST", "")
}

// getAs issues a GET presenting itself as client via X-Forwarded-For, the way
// Caddy presents a real request to the loopback listener.
func getAs(t *testing.T, client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	if client != "" {
		req.Header.Set("X-Forwarded-For", client)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var problem struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(b, &problem)
	return resp.StatusCode, problem.Detail
}

func postAs(t *testing.T, client, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if client != "" {
		req.Header.Set("X-Forwarded-For", client)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var problem struct {
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(b, &problem)
	return resp.StatusCode, problem.Detail
}

// firstThrottle returns the detail of the first 429 within n attempts.
func firstThrottle(t *testing.T, client, url string, n int) (bool, string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if code, d := getAs(t, client, url); code == http.StatusTooManyRequests {
			return true, d
		}
	}
	return false, ""
}

// The LLM tier must trip while the global bucket still has thousands of tokens.
func TestLLMRateLimitTripsBeforeGlobalBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "6000")
	t.Setenv("COSIFT_RATELIMIT_BURST", "5000")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "2")
	base := serveForRateLimit(t)

	blocked, detail := firstThrottle(t, "203.0.113.7", base+"/answer?q=hello", 6)
	if !blocked {
		t.Fatal("/answer never returned 429 with an LLM burst of 2")
	}
	if !strings.Contains(detail, "llm rate limit exceeded") {
		t.Errorf("detail = %q, want the LLM tier (not the global bucket) to have tripped", detail)
	}
	if code, d := getAs(t, "203.0.113.7", base+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d (%s), want 200 — the global bucket still has budget", code, d)
	}
}

// Mirror of the above: with the global bucket TIGHTER than the LLM tier, an
// LLM route must still 429 from the global bucket. Pins that lwrap keeps the
// global limiter and that the LLM tier is the outer of the two.
func TestGlobalBucketStillGatesLLMRoutes(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_BURST", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "6000")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "5000")
	base := serveForRateLimit(t)

	blocked, detail := firstThrottle(t, "203.0.113.8", base+"/answer?q=hello", 6)
	if !blocked {
		t.Fatal("/answer never returned 429 — the global bucket is not applied to the LLM routes")
	}
	if !strings.Contains(detail, "rate limit exceeded for ip=203.0.113.8") {
		t.Errorf("detail = %q, want the global bucket's message keyed on the XFF client", detail)
	}
	if strings.Contains(detail, "llm") {
		t.Errorf("detail = %q, want the GLOBAL tier — the LLM tier has 5000 tokens", detail)
	}
}

// With BOTH tiers exhausted, the LLM tier's message must be the one returned —
// that is the only observable difference between the two nesting orders.
func TestLLMTierIsOuterOfTheTwo(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_BURST", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	blocked, detail := firstThrottle(t, "203.0.113.11", base+"/answer?q=hello", 4)
	if !blocked {
		t.Fatal("/answer never returned 429 with both tiers at burst 1")
	}
	if !strings.Contains(detail, "llm rate limit exceeded") {
		t.Errorf("detail = %q, want the LLM tier to report first — it must be the outer wrapper", detail)
	}
}

// A missing COSIFT_RATELIMIT_RPM disables the global bucket; it must not
// disable the LLM tier.
func TestLLMRateLimitActiveWithGlobalRateLimitUnset(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_BURST", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	if blocked, _ := firstThrottle(t, "203.0.113.9", base+"/research?q=hello", 5); !blocked {
		t.Fatal("/research never returned 429 — the LLM tier is off when COSIFT_RATELIMIT_RPM is unset")
	}
	for i := 0; i < 5; i++ {
		if code, d := getAs(t, "203.0.113.9", base+"/healthz"); code != http.StatusOK {
			t.Fatalf("/healthz = %d (%s), want 200 — no global limiter is configured", code, d)
		}
	}
}

// Two clients behind the same proxy must not share the LLM bucket.
func TestLLMTierKeysOnResolvedClientIP(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	for i := 0; i < 6; i++ {
		client := fmt.Sprintf("203.0.113.%d", 100+i)
		if code, d := getAs(t, client, base+"/answer?q=hello"); code == http.StatusTooManyRequests {
			t.Fatalf("client %s got 429 (%s) on its first request — the LLM bucket is shared", client, d)
		}
	}
	if blocked, _ := firstThrottle(t, "203.0.113.100", base+"/answer?q=hello", 3); !blocked {
		t.Fatal("a repeat client never hit its own LLM bucket")
	}
}

// Caddy's 1 Hz active health probe carries no XFF and must never be throttled:
// a 429 on /healthz makes the proxy declare the upstream down.
func TestHealthzIsNeverRateLimited(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_BURST", "1")
	base := serveForRateLimit(t)

	// Drain the global bucket for the proxy-less peer, then probe as Caddy does.
	for i := 0; i < 5; i++ {
		getAs(t, "", base+"/stats")
	}
	for i := 0; i < 20; i++ {
		if code, d := getAs(t, "", base+"/healthz"); code != http.StatusOK {
			t.Fatalf("/healthz probe %d = %d (%s), want 200", i, code, d)
		}
	}
	// And for a probe that does arrive through the proxy with a drained bucket.
	for i := 0; i < 5; i++ {
		getAs(t, "203.0.113.40", base+"/stats")
	}
	if code, d := getAs(t, "203.0.113.40", base+"/stats"); code != http.StatusTooManyRequests {
		t.Fatalf("/stats = %d (%s), want the client's global bucket to be drained first", code, d)
	}
	for i := 0; i < 5; i++ {
		if code, d := getAs(t, "203.0.113.40", base+"/healthz"); code != http.StatusOK {
			t.Fatalf("proxied /healthz probe %d = %d (%s), want 200 — /healthz must not sit behind a limiter", i, code, d)
		}
	}
}

// /admin/eval-quick fans 10 syntheses out of one unauthenticated request, so it
// must pass the LLM tier too.
func TestAdminEvalQuickUsesLLMTier(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	if blocked, _ := firstThrottle(t, "203.0.113.20", base+"/admin/eval-quick", 5); !blocked {
		t.Fatal("/admin/eval-quick never returned 429 — it bypasses the LLM tier")
	}
}

// /search reaches the same chat model via ?rerank / ?expand, on GET and via the
// POST body, so both must pass the LLM tier while keyword-only /search does not.
func TestSearchLLMParamsUseLLMTier(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	for i := 0; i < 8; i++ {
		if code, d := getAs(t, "203.0.113.30", base+"/search?q=plain"); code == http.StatusTooManyRequests {
			t.Fatalf("keyword-only /search was charged to the LLM tier: %d (%s)", code, d)
		}
	}
	if blocked, d := firstThrottle(t, "203.0.113.31", base+"/search?q=x&rerank=true", 5); !blocked {
		t.Fatal("GET /search?rerank=true never returned 429 — it bypasses the LLM tier")
	} else if !strings.Contains(d, "llm rate limit exceeded") {
		t.Errorf("detail = %q, want the LLM tier", d)
	}
	throttled := false
	for i := 0; i < 5 && !throttled; i++ {
		if code, _ := postAs(t, "203.0.113.32", base+"/search", `{"q":"x","expand":"hyde"}`); code == http.StatusTooManyRequests {
			throttled = true
		}
	}
	if !throttled {
		t.Fatal(`POST /search {"expand":"hyde"} never returned 429 — the body opt-in bypasses the LLM tier`)
	}
}

// The prod-shaped regression: loopback peer, real clients in XFF, and
// trusted_proxies NOT yet configured. Two properties at once — distinct
// clients must not collapse into one bucket, AND each must still have a bucket.
// Asserting only the first is what let the fail-open exemption ship.
func TestNoTrustedProxiesDoesNotCollapseClientsIntoOneBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveProdShaped(t)

	for i := 0; i < 20; i++ {
		client := fmt.Sprintf("198.51.100.%d", i+1)
		if code, d := getAs(t, client, base+"/query?q=hello"); code == http.StatusTooManyRequests {
			t.Fatalf("request %d from %s got 429 (%s) — unset trusted_proxies collapsed every client into the loopback bucket", i, client, d)
		}
	}
	blocked, detail := firstThrottle(t, "198.51.100.1", base+"/query?q=hello", 3)
	if !blocked {
		t.Fatal("a repeat client was never throttled — unset trusted_proxies exempts every forwarded client from every limiter")
	}
	if !strings.Contains(detail, "ip=198.51.100.1") {
		t.Errorf("detail = %q, want the leftmost forwarded hop as the key", detail)
	}
}

// A throttled request must still leave a query-log row, keyed on the real
// client — otherwise throttling reads as a drop in demand.
func TestQueryLogRecordsThrottledRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	path := filepath.Join(t.TempDir(), "qlog.jsonl")
	t.Setenv("COSIFT_QUERY_LOG", path)
	t.Setenv("COSIFT_RATELIMIT_RPM", "")
	t.Setenv("COSIFT_RATELIMIT_LLM_RPM", "1")
	t.Setenv("COSIFT_RATELIMIT_LLM_BURST", "1")
	base := serveForRateLimit(t)

	if blocked, _ := firstThrottle(t, "203.0.113.50", base+"/answer?q=hello", 4); !blocked {
		t.Fatal("/answer never returned 429")
	}
	rec := lastQueryLogRec(t, path)
	if rec.Status != http.StatusTooManyRequests {
		t.Errorf("last query-log status = %d, want 429 — throttled requests are unlogged", rec.Status)
	}
	if rec.Caller != "203.0.113.50" {
		t.Errorf("caller = %q, want 203.0.113.50", rec.Caller)
	}
}

// --- bucket keying, eviction, and the on-box exemption ---

func TestBucketKeyCollapsesIPv6To64(t *testing.T) {
	rl := &rateLimiter{rpm: 0.0001, burst: 1, whitelist: map[string]bool{}}
	if !rl.allow("2001:db8:1234:5678::1") {
		t.Fatal("first request from the /64 should pass")
	}
	for i := 2; i <= 8; i++ {
		addr := fmt.Sprintf("2001:db8:1234:5678::%d", i)
		if rl.allow(addr) {
			t.Fatalf("%s got a fresh bucket — host-bit rotation inside one /64 defeats the limiter", addr)
		}
	}
	if !rl.allow("2001:db8:1234:9999::1") {
		t.Error("a different /64 must get its own bucket")
	}
	if got := bucketKey("1.2.3.4"); got != "1.2.3.4" {
		t.Errorf("bucketKey(IPv4) = %q, want it unchanged", got)
	}
}

func TestSweepIdleDropsRefilledBuckets(t *testing.T) {
	rl := &rateLimiter{rpm: 60, burst: 1, whitelist: map[string]bool{}}
	for i := 0; i < 50; i++ {
		rl.allow(fmt.Sprintf("198.51.100.%d", i))
	}
	if n := rl.sweepIdle(time.Now(), time.Minute); n != 0 {
		t.Fatalf("swept %d fresh buckets, want 0", n)
	}
	if n := rl.sweepIdle(time.Now().Add(2*time.Minute), time.Minute); n != 50 {
		t.Fatalf("swept %d idle buckets, want 50 — the bucket map never shrinks", n)
	}
	live := 0
	rl.buckets.Range(func(any, any) bool { live++; return true })
	if live != 0 {
		t.Errorf("%d buckets still resident after the sweep", live)
	}
}

func TestOnBoxExemptionOnlyWithoutAForwardedChain(t *testing.T) {
	s := &pebbleHTTP{ipResolver: mustResolver(t, "127.0.0.0/8")}
	bare := httptest.NewRequest(http.MethodGet, "/query", nil)
	bare.RemoteAddr = "127.0.0.1:5000"
	if !s.limiterExempt(bare, "127.0.0.1") {
		t.Error("an on-box request with no forwarded chain should be exempt")
	}
	spoof := httptest.NewRequest(http.MethodGet, "/query", nil)
	spoof.RemoteAddr = "127.0.0.1:5001"
	spoof.Header.Set("X-Forwarded-For", "127.0.0.1")
	if s.limiterExempt(spoof, "127.0.0.1") {
		t.Error(`"X-Forwarded-For: 127.0.0.1" bought an exemption`)
	}
	hdr := &pebbleHTTP{ipResolver: mustResolver(t, "127.0.0.0/8"), clientIPHeader: "CF-Connecting-IP"}
	cf := httptest.NewRequest(http.MethodGet, "/query", nil)
	cf.RemoteAddr = "127.0.0.1:5002"
	cf.Header.Set("CF-Connecting-IP", "127.0.0.1")
	if hdr.limiterExempt(cf, "127.0.0.1") {
		t.Error("a spoofed client-IP header bought an exemption")
	}
}

func TestCountKeepsThrottledOutOfTheLatencySeries(t *testing.T) {
	s := &pebbleHTTP{rl: &rateLimiter{rpm: 0.0001, burst: 1, whitelist: map[string]bool{}}}
	h := s.count(s.rateLimit(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "/answer", nil)
		req.RemoteAddr = "9.9.9.9:1234"
		h(httptest.NewRecorder(), req)
	}
	v, ok := s.requestCounts.Load("/answer")
	if !ok {
		t.Fatal("no metrics recorded for /answer")
	}
	m := v.(*endpointMetrics)
	if got := m.throttled.Load(); got != 3 {
		t.Errorf("throttled = %d, want 3", got)
	}
	if got := m.sumNanos.Load(); got >= int64(4*time.Millisecond) {
		t.Errorf("sumNanos = %d — 429s were folded into the latency series", got)
	}
}

// Client-IP header wins over the X-Forwarded-For walk, which the edge's own
// (multi-tenant) address ranges would otherwise let a client forge.
func TestClientIPHeaderBeatsForgedXFF(t *testing.T) {
	s := &pebbleHTTP{
		rl:             oneTokenLimiter(),
		ipResolver:     mustResolverWithHeader(t, "CF-Connecting-IP", "127.0.0.0/8"),
		clientIPHeader: "CF-Connecting-IP",
	}
	hit := func(forged string) (int, string) {
		h := s.rateLimit(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		req := httptest.NewRequest(http.MethodGet, "/query", nil)
		req.RemoteAddr = "127.0.0.1:41000"
		req.Header.Set("X-Forwarded-For", forged)
		req.Header.Set("CF-Connecting-IP", "198.51.100.5")
		rec := httptest.NewRecorder()
		h(rec, req)
		var body struct {
			Detail string `json:"detail"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body.Detail
	}
	if code, d := hit("203.0.113.1"); code != http.StatusOK {
		t.Fatalf("first request: %d (%s)", code, d)
	}
	code, detail := hit("203.0.113.2")
	if code != http.StatusTooManyRequests {
		t.Fatalf("code = %d, want 429 — a rotating forged XFF minted a fresh bucket", code)
	}
	if !strings.Contains(detail, "ip=198.51.100.5") {
		t.Errorf("detail = %q, want the client-IP header value as the key", detail)
	}
}

// A loopback whitelist entry makes a bucket inert behind a local reverse
// proxy; the WARN must name every limiter that carries one, not just rl.
func TestLoopbackWhitelistWarnCoversEveryLimiter(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	t.Setenv("COSIFT_RATELIMIT_RPM", "240")
	t.Setenv("COSIFT_RATELIMIT_WHITELIST", "127.0.0.1")
	t.Setenv("COSIFT_RATELIMIT_LLM_WHITELIST", "127.0.0.1")
	var buf bytes.Buffer
	prev := log.Writer()
	log.SetOutput(io.MultiWriter(prev, &buf))
	t.Cleanup(func() { log.SetOutput(prev) })

	serveForRateLimit(t)

	out := buf.String()
	for _, name := range []string{"global", "llm"} {
		want := "WARN " + name + " rate-limit whitelist contains loopback"
		if !strings.Contains(out, want) {
			t.Errorf("startup log missing %q\n--- log ---\n%s", want, out)
		}
	}
}

// The /feedback bucket must not collapse either: before trusted_proxies is
// configured, every submission behind the proxy keys to 127.0.0.1. As above,
// "no collapse" is only half the property — each client still needs a bucket,
// and a genuinely on-box submitter still needs the exemption.
func TestFeedbackNotCollapsedWithoutTrustedProxies(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fb-*.jsonl")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	defer f.Close()
	s := &pebbleHTTP{fbFile: f, fbRL: oneTokenLimiter()}
	post := func(xff string) int {
		req := httptest.NewRequest(http.MethodPost, "/feedback", strings.NewReader(`{"query_id":"q1","rating":1}`))
		req.RemoteAddr = "127.0.0.1:5000"
		if xff != "" {
			req.Header.Set("X-Forwarded-For", xff)
		}
		rec := httptest.NewRecorder()
		s.handleFeedback(rec, req)
		return rec.Code
	}

	for i := 0; i < 8; i++ {
		if code := post(fmt.Sprintf("198.51.100.%d", i+1)); code != http.StatusOK {
			t.Fatalf("client %d got %d — every client behind the proxy shares one /feedback bucket", i, code)
		}
	}
	if code := post("198.51.100.1"); code != http.StatusTooManyRequests {
		t.Fatalf("a repeat client got %d, want 429 — a forwarded client behind an unconfigured proxy has no budget at all", code)
	}
	for i := 0; i < 8; i++ {
		if code := post(""); code != http.StatusOK {
			t.Fatalf("on-box submission %d got %d, want 200 — the on-box exemption is gone", i, code)
		}
	}
}

// --- the production topology, end to end ---
//
// Everything below runs the live unit's shape: loopback listener, Caddy in
// front, server.trusted_proxies NOT set. The invariant these pin is that this
// branch is at least as restrictive as v0.2.5 on every route, with or without
// the deferred trusted_proxies step.

// The measured regression. On v0.2.5 this loop was 5×200 / 55×429, because
// feedback.go keyed on the leftmost X-Forwarded-For hop. It must not become
// 60×200.
func TestProdTopologyThrottlesAForwardedFeedbackClient(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	t.Setenv("COSIFT_QUERY_LOG", filepath.Join(t.TempDir(), "qlog.jsonl"))
	base := serveProdShaped(t)

	accepted, throttled := 0, 0
	for i := 0; i < 60; i++ {
		switch code, _ := postAs(t, "203.0.113.9", base+"/feedback", `{"query_id":"q1","rating":1}`); code {
		case http.StatusTooManyRequests:
			throttled++
		case http.StatusOK:
			accepted++
		default:
			t.Fatalf("POST /feedback %d returned %d, want 200 or 429", i, code)
		}
	}
	if throttled == 0 {
		t.Fatalf("60 posts from one forwarded client: %d accepted, 0 throttled — every limiter is bypassed behind an unconfigured local proxy", accepted)
	}
	t.Logf("MEASURED feedback: 200=%d 429=%d", accepted, throttled)
	if accepted > 15 {
		t.Errorf("accepted %d of 60, want ≈ the feedback burst (5) — v0.2.5 accepted 5", accepted)
	}
}

// Harvesters, snapshot.sh, `cosift eval` and Caddy's health probe all arrive
// from loopback with no forwarded chain. They must stay unlimited.
func TestProdTopologyExemptsOnBoxCallers(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	t.Setenv("COSIFT_QUERY_LOG", filepath.Join(t.TempDir(), "qlog.jsonl"))
	base := serveProdShaped(t)

	// 120 > the global burst (80) and >> the feedback burst (5).
	for i := 0; i < 120; i++ {
		if code, d := postAs(t, "", base+"/feedback", `{"query_id":"q1","rating":1}`); code != http.StatusOK {
			t.Fatalf("on-box /feedback %d = %d (%s), want 200", i, code, d)
		}
		if code, d := getAs(t, "", base+"/answer?q=hello"); code == http.StatusTooManyRequests {
			t.Fatalf("on-box /answer %d = 429 (%s) — the on-box exemption is gone", i, d)
		}
	}
}

// Distinct forwarded clients must get distinct feedback buckets: the fix must
// not simply drop the exemption and collapse the internet into 127.0.0.1.
func TestProdTopologyGivesForwardedClientsDistinctFeedbackBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	t.Setenv("COSIFT_QUERY_LOG", filepath.Join(t.TempDir(), "qlog.jsonl"))
	base := serveProdShaped(t)

	for i := 0; i < 40; i++ {
		client := fmt.Sprintf("198.51.100.%d", i+1)
		if code, d := postAs(t, client, base+"/feedback", `{"query_id":"q1","rating":1}`); code != http.StatusOK {
			t.Fatalf("first submission from %s = %d (%s) — the clients share one bucket", client, code, d)
		}
	}
}

// The LLM tier is the expensive one and is always on. Same three properties.
func TestProdTopologyThrottlesAForwardedLLMClient(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	base := serveProdShaped(t)

	accepted, throttled, detail := 0, 0, ""
	for i := 0; i < 60; i++ {
		code, d := getAs(t, "203.0.113.9", base+"/answer?q=hello")
		if code == http.StatusTooManyRequests {
			throttled++
			detail = d
			continue
		}
		accepted++
	}
	if throttled == 0 {
		t.Fatalf("60 /answer calls from one forwarded client: %d accepted, 0 throttled — the LLM tier never fires in production", accepted)
	}
	if !strings.Contains(detail, "llm rate limit exceeded for ip=203.0.113.9") {
		t.Errorf("detail = %q, want the LLM tier keyed on the leftmost forwarded hop", detail)
	}
	t.Logf("MEASURED llm: 200=%d 429=%d", accepted, throttled)
	if accepted > 20 {
		t.Errorf("accepted %d of 60, want ≈ the LLM burst (10)", accepted)
	}
}

func TestProdTopologyGivesForwardedClientsDistinctLLMBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	base := serveProdShaped(t)

	for i := 0; i < 40; i++ {
		client := fmt.Sprintf("198.51.100.%d", i+1)
		if code, d := getAs(t, client, base+"/answer?q=hello"); code == http.StatusTooManyRequests {
			t.Fatalf("first /answer from %s = 429 (%s) — the clients share one LLM bucket", client, d)
		}
	}
}

// peerTokenOK accepts anything when cluster.peer_auth_token is empty, which it
// is in production — so /admin/eval-quick fans 10 chat calls out of one
// anonymous request. The LLM tier is the only thing standing in front of it.
func TestProdTopologyThrottlesUnauthenticatedEvalQuick(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	prodLimiterEnv(t)
	base := serveProdShaped(t)

	blocked, detail := firstThrottle(t, "203.0.113.21", base+"/admin/eval-quick", 40)
	if !blocked {
		t.Fatal("/admin/eval-quick never returned 429 — unauthenticated, it is an unlimited LLM amplifier in production")
	}
	if !strings.Contains(detail, "llm rate limit exceeded for ip=203.0.113.21") {
		t.Errorf("detail = %q, want the LLM tier keyed on the forwarded hop", detail)
	}
}

// Query-log caller attribution must follow the same key, or every prod row
// reads 127.0.0.1 and the demand loop is blind.
func TestProdTopologyQueryLogCallerIsTheForwardedHop(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	path := filepath.Join(t.TempDir(), "qlog.jsonl")
	t.Setenv("COSIFT_QUERY_LOG", path)
	prodLimiterEnv(t)
	base := serveProdShaped(t)

	if code, d := getAs(t, "203.0.113.60", base+"/query?q=hello"); code == http.StatusTooManyRequests {
		t.Fatalf("first /query = 429 (%s)", d)
	}
	if got := lastQueryLogRec(t, path).Caller; got != "203.0.113.60" {
		t.Errorf("caller = %q, want 203.0.113.60 — attribution collapsed to the proxy", got)
	}
}

// --- unit-level keying ---

func TestResolveClientIPWithoutTrustedProxies(t *testing.T) {
	s := &pebbleHTTP{}
	for _, tc := range []struct {
		name       string
		remoteAddr string
		xff        string
		wantKey    string
		wantExempt bool
	}{
		{"on-box, no chain", "127.0.0.1:5000", "", "127.0.0.1", true},
		{"loopback proxy forwards a client", "127.0.0.1:5000", "203.0.113.9", "203.0.113.9", false},
		{"loopback proxy, multi-hop", "127.0.0.1:5000", "203.0.113.9, 104.28.216.88", "203.0.113.9", false},
		{"loopback proxy, client claims loopback", "127.0.0.1:5000", "127.0.0.1", "127.0.0.1", false},
		{"loopback proxy, unparseable chain", "127.0.0.1:5000", " ", "127.0.0.1", false},
		{"direct public peer may not forge", "8.8.8.8:1234", "1.1.1.1", "8.8.8.8", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/query", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			got := s.resolveClientIP(req)
			if got != tc.wantKey {
				t.Errorf("resolveClientIP = %q, want %q", got, tc.wantKey)
			}
			if ex := s.limiterExempt(req, got); ex != tc.wantExempt {
				t.Errorf("limiterExempt(%q) = %v, want %v", got, ex, tc.wantExempt)
			}
		})
	}
}

// COSIFT_RATELIMIT_WHITELIST names addresses the operator trusts. Behind an
// unconfigured proxy the key is client-supplied, so it must not match that list
// — otherwise "X-Forwarded-For: 127.0.0.1" is a one-header limiter bypass.
func TestForwardedHopCannotClaimTheWhitelist(t *testing.T) {
	for _, claim := range []string{"127.0.0.1", "104.28.216.88"} {
		t.Run(claim, func(t *testing.T) {
			s := &pebbleHTTP{rl: &rateLimiter{rpm: 0.0001, burst: 1, whitelist: parseIPWhitelist("104.28.216.88,127.0.0.1")}}
			if code, d := hitRateLimit(t, s, "127.0.0.1:5000", claim); code != http.StatusOK {
				t.Fatalf("first request: %d (%s)", code, d)
			}
			if code, _ := hitRateLimit(t, s, "127.0.0.1:5001", claim); code != http.StatusTooManyRequests {
				t.Fatalf("code = %d, want 429 — %q claimed a whitelist entry it cannot prove", code, claim)
			}
		})
	}
	// A hop a trusted proxy vouched for still gets the whitelist.
	s := &pebbleHTTP{
		rl:         &rateLimiter{rpm: 0.0001, burst: 1, whitelist: parseIPWhitelist("104.28.216.88")},
		ipResolver: mustResolver(t, "127.0.0.0/8"),
	}
	for i := 0; i < 4; i++ {
		if code, d := hitRateLimit(t, s, "127.0.0.1:5002", "104.28.216.88"); code != http.StatusOK {
			t.Fatalf("attested whitelisted client, request %d: %d (%s)", i, code, d)
		}
	}
}
