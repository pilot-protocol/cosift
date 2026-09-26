package svcauth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
)

func runCerts(t *testing.T, h *harness) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.svc.certs.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		h.src.SetBlock(nil)
		<-done
	})
}

func refreshes(h *harness, result string) string {
	return `cosift_v1_cert_refresh_total{result="` + result + `"} `
}

func metricValue(t *testing.T, h *harness, series string) string {
	t.Helper()
	for _, l := range strings.Split(h.metrics(), "\n") {
		if v, ok := strings.CutPrefix(l, series); ok {
			return v
		}
	}
	return ""
}

// A request cancelled while a refresh runs neither
// cancels it nor causes a 503 for a concurrent valid token.
func TestCertRefreshSurvivesCancelledRequest(t *testing.T) {
	h := newHarness(t, baseConfig())
	runCerts(t, h)
	waitFor(t, "the loop's first fetch", func() bool { return h.src.Fetches() == 2 && metricValue(t, h, refreshes(h, "ok")) == "2" })

	signer2 := svcauthtest.NewSigner("kid-2")
	h.src.Set(svcauthtest.Response{Status: 200, Header: http.Header{"Cache-Control": {"max-age=21600"}}, Body: svcauthtest.JWKS(h.signer, signer2)})
	block := make(chan struct{})
	h.src.SetBlock(block)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	r := newRequest("POST", "/v1/search", `{}`, bearerAuth(signer2.Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now()))))
	w := serve(h, r.WithContext(reqCtx))
	wantUnauthenticated(t, w)
	waitFor(t, "the early refresh to start", func() bool { return h.src.Fetches() == 3 })
	cancelReq()

	start := time.Now()
	if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))); w.Code != http.StatusOK {
		t.Fatalf("valid token during the refresh: status %d", w.Code)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("valid token waited %s on the refresh", d)
	}
	close(block)
	waitFor(t, "the refresh to land", func() bool { return h.svc.certs.HasKid("kid-2") })
	if errs := h.src.CtxErrs(); errs[len(errs)-1] != nil {
		t.Fatalf("the fetch context ended: %v", errs[len(errs)-1])
	}
	if v := metricValue(t, h, refreshes(h, "error")); v != "0" {
		t.Fatalf("refresh errors %s", v)
	}
}

// A failed refresh keeps the last good copy in service,
// for up to 24 h past its expiry.
func TestCertFailedRefreshKeepsCopy(t *testing.T) {
	h := newHarness(t, baseConfig())
	for _, bad := range []svcauthtest.Response{
		{Status: 500, Body: svcauthtest.JWKS(h.signer)},
		{Status: 200, Body: []byte("<html>challenge</html>")},
		{Status: 200, Body: []byte(`{"certs":[]}`)},
		{Status: 200, Body: []byte(`{"keys":[]}`)},
		{Status: 200, Body: []byte(`{"keys":{"kid":"x"}}`)},
		{Status: 200, Body: append([]byte(`{"keys":[{"kid":"kid-1"}],"pad":"`), append(bytes.Repeat([]byte("x"), 256<<10), '"', '}')...)},
		{Status: 302, Header: http.Header{"Location": {"https://example.com"}}},
		{Err: errors.New("dial tcp: no route")},
	} {
		h.src.Set(bad)
		if h.svc.certs.refresh(context.Background()) {
			t.Fatalf("refresh accepted %d %.40q", bad.Status, bad.Body)
		}
		if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))); w.Code != http.StatusOK {
			t.Fatalf("after a failed refresh: status %d", w.Code)
		}
	}
	if v := metricValue(t, h, refreshes(h, "error")); v != "8" {
		t.Fatalf("refresh errors %q", v)
	}
	h.clock.Advance(6*time.Hour + 23*time.Hour)
	if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))); w.Code != http.StatusOK {
		t.Fatalf("23 h past expiry: status %d", w.Code)
	}
	h.clock.Advance(time.Hour + time.Second)
	w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail)))
	if w.Code != http.StatusServiceUnavailable || decodeErr(t, w).Code != "auth_unavailable" || w.Header().Get("Retry-After") != "10" {
		t.Fatalf("24 h past expiry: status %d %s", w.Code, w.Body.String())
	}
}

// An unknown kid triggers one early refresh, is accepted
// once it lands, and a second unknown kid within 5 min triggers none.
func TestCertUnknownKidEarlyRefresh(t *testing.T) {
	h := newHarness(t, baseConfig())
	runCerts(t, h)
	waitFor(t, "the loop's first fetch", func() bool { return metricValue(t, h, refreshes(h, "ok")) == "2" })

	signer2 := svcauthtest.NewSigner("kid-2")
	h.src.Set(svcauthtest.Response{Status: 200, Header: http.Header{"Cache-Control": {"max-age=21600"}}, Body: svcauthtest.JWKS(h.signer, signer2)})
	tok2 := signer2.Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now()))
	wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, bearerAuth(tok2)))
	waitFor(t, "the early refresh", func() bool { return h.svc.certs.HasKid("kid-2") })
	if n := h.src.Fetches(); n != 3 {
		t.Fatalf("fetches %d, want 3", n)
	}
	if w := h.do("POST", "/v1/search", `{}`, bearerAuth(tok2)); w.Code != http.StatusOK {
		t.Fatalf("rotated key after the refresh: status %d", w.Code)
	}

	signer3 := svcauthtest.NewSigner("kid-3")
	tok3 := signer3.Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now()))
	h.clock.Advance(4*time.Minute + 59*time.Second)
	wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, bearerAuth(tok3)))
	time.Sleep(100 * time.Millisecond)
	if n := h.src.Fetches(); n != 3 {
		t.Fatalf("a second unknown kid inside 5 min fetched (%d)", n)
	}
	h.clock.Advance(2 * time.Second)
	wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, bearerAuth(signer3.Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now())))))
	waitFor(t, "a refresh after 5 min", func() bool { return h.src.Fetches() == 4 })
}

// With no copy ever held, a token that passed the claim checks
// is 503 at once, while the only fetch hangs.
func TestCertNeverFilled(t *testing.T) {
	h := newHarness(t, baseConfig(), withoutCerts())
	block := make(chan struct{})
	defer close(block)
	h.src.SetBlock(block)
	runCerts(t, h)
	waitFor(t, "the first fetch", func() bool { return h.src.Fetches() == 1 })
	start := time.Now()
	w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail)))
	if d := time.Since(start); d > time.Second {
		t.Fatalf("waited %s", d)
	}
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "10" {
		t.Fatalf("status %d", w.Code)
	}
	if got := w.Body.String(); got != `{"error":"Service Unavailable","status":503,"code":"auth_unavailable","detail":"authentication temporarily unavailable"}`+"\n" {
		t.Fatalf("body %s", got)
	}
	c := svcauthtest.Claims("999999999999999999999", synthEmail, h.clock.Now())
	wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, bearerAuth(h.signer.Token(c))))
	if n := len(h.handlerCalls()); n != 0 {
		t.Fatalf("handler reached %d times", n)
	}
}

// The library is answered with max-age=0 and keeps nothing: a key removed
// from the copy stops verifying on the very next token.
func TestCertLibraryKeepsNoCopy(t *testing.T) {
	h := newHarness(t, baseConfig())
	if w := h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	signer2 := svcauthtest.NewSigner("kid-2")
	h.src.Set(svcauthtest.Response{Status: 200, Header: http.Header{"Cache-Control": {"max-age=21600"}}, Body: svcauthtest.JWKS(signer2)})
	if !h.svc.certs.refresh(context.Background()) {
		t.Fatal("refresh failed")
	}
	wantUnauthenticated(t, h.do("POST", "/v1/search", `{}`, bearerAuth(h.token(synthSub, synthEmail))))
	if w := h.do("POST", "/v1/search", `{}`, bearerAuth(signer2.Token(svcauthtest.Claims(synthSub, synthEmail, h.clock.Now())))); w.Code != http.StatusOK {
		t.Fatalf("new key: status %d", w.Code)
	}
}

func TestCertRoundTrip(t *testing.T) {
	h := newHarness(t, baseConfig())
	for _, u := range []string{"https://www.gstatic.com/iap/verify/public_key-jwk", "http://www.googleapis.com/oauth2/v3/certs", "https://www.googleapis.com/oauth2/v1/certs"} {
		req, _ := http.NewRequest("GET", u, nil)
		if _, err := h.svc.certs.RoundTrip(req); !errors.Is(err, errCertURL) {
			t.Errorf("%s: %v", u, err)
		}
	}
	req, _ := http.NewRequest("POST", CertsURL, nil)
	if _, err := h.svc.certs.RoundTrip(req); !errors.Is(err, errCertURL) {
		t.Errorf("POST: %v", err)
	}
	req, _ = http.NewRequest("GET", CertsURL, nil)
	resp, err := h.svc.certs.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || resp.Header.Get("Cache-Control") != "max-age=0" || !bytes.Equal(body, svcauthtest.JWKS(h.signer)) {
		t.Fatalf("response %d %v %s", resp.StatusCode, resp.Header, body)
	}
	empty := NewCertCache(h.src.Fetch, NewMetrics(), nil, nil)
	if _, err := empty.RoundTrip(req); !errors.Is(err, ErrNoCerts) {
		t.Fatalf("empty cache: %v", err)
	}
}

func TestCertSchedule(t *testing.T) {
	h := newHarness(t, baseConfig())
	if d := h.svc.certs.scheduled(); d != 19440*time.Second {
		t.Fatalf("scheduled %s, want 90%% of 21600 s", d)
	}
	h.src.Set(svcauthtest.Response{Status: 200, Header: http.Header{"Cache-Control": {"public, max-age=20000, must-revalidate"}, "Age": {"2000"}}, Body: svcauthtest.JWKS(h.signer)})
	h.svc.certs.refresh(context.Background())
	if d := h.svc.certs.scheduled(); d != 16200*time.Second {
		t.Fatalf("scheduled %s, want 90%% of max-age minus Age", d)
	}
	h.src.Set(svcauthtest.Response{Status: 200, Body: svcauthtest.JWKS(h.signer)})
	h.svc.certs.refresh(context.Background())
	if d := h.svc.certs.scheduled(); d != certBackoffMin {
		t.Fatalf("no max-age: scheduled %s", d)
	}
	var seq []time.Duration
	d := time.Duration(0)
	for range 8 {
		d = nextBackoff(d)
		seq = append(seq, d)
	}
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 300 * time.Second, 300 * time.Second, 300 * time.Second}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("backoff %v, want %v", seq, want)
		}
	}
	for _, tc := range []struct {
		h    http.Header
		want time.Duration
	}{
		{http.Header{"Cache-Control": {"max-age=100"}, "Age": {"30"}}, 70 * time.Second},
		{http.Header{"Cache-Control": {"max-age=100"}, "Age": {"300"}}, 0},
		{http.Header{"Cache-Control": {"max-age=x"}}, 0},
		{http.Header{"Cache-Control": {"max-age=100"}, "Age": {"x"}}, 0},
	} {
		if got := freshFor(tc.h); got != tc.want {
			t.Errorf("freshFor(%v) = %s, want %s", tc.h, got, tc.want)
		}
	}
}

func TestCertFetchIsDetachedAndBounded(t *testing.T) {
	var deadline time.Time
	var hasDeadline bool
	var ctxErr error
	src := func(ctx context.Context) (*http.Response, error) {
		deadline, hasDeadline = ctx.Deadline()
		ctxErr = ctx.Err()
		return nil, errors.New("offline")
	}
	c := NewCertCache(src, NewMetrics(), nil, nil)
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	c.refresh(parent)
	end := time.Now()
	if !hasDeadline || deadline.After(end.Add(certFetchTimeout)) || deadline.Before(start.Add(certFetchTimeout)) {
		t.Fatalf("deadline %v after start (has %v)", deadline.Sub(start), hasDeadline)
	}
	if ctxErr != nil {
		t.Fatalf("the fetch inherited the parent's cancellation: %v", ctxErr)
	}
	cl := NewCertHTTPClient()
	if cl.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the certificate client follows redirects")
	}
	if tr := cl.Transport.(*http.Transport); tr.MaxResponseHeaderBytes != 64<<10 || tr.DialContext == nil || tr.Proxy != nil {
		t.Fatalf("transport: header limit %d, guarded dialer %v, proxy %v", tr.MaxResponseHeaderBytes, tr.DialContext != nil, tr.Proxy != nil)
	}
}
