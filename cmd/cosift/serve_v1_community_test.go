package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/svcauth/svcauthtest"
)

var (
	enqueueBody  = map[string]any{"submission_id": "stable-submission-123", "url": "https://example.com/page", "approved_content_hash": strings.Repeat("a", 64)}
	moderateBody = map[string]any{"url": "https://example.com/page", "title": "A guide", "text": strings.Repeat("A plain factual paragraph about a library. ", 4), "signals": ""}
)

func TestOldCommunityRoutesAreGone(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), svcauthtest.NewSigner("k"))
	cfg := &config.Config{Cluster: config.Cluster{PeerAuthToken: "peer-secret"}, Server: config.Server{AdminToken: "admin-secret"}}
	mainAddr, _ := serveProcess(t, cfg, filepath.Join(dir, "pebble"))
	call := func(method, path, token string) int {
		req, _ := http.NewRequest(method, "http://"+mainAddr+path, strings.NewReader(`{}`))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if code := call("POST", "/admin/crawl-enqueue", ""); code != 401 {
		t.Fatalf("a live /admin route without the token: %d", code)
	}
	if code := call("POST", "/admin/crawl-enqueue", "peer-secret"); code == 401 || code == 404 {
		t.Fatalf("a live /admin route with the token: %d", code)
	}
	for _, path := range []string{"/admin/community-enqueue", "/admin/community-moderate"} {
		for _, token := range []string{"", "peer-secret", "admin-secret", "old-community-admin"} {
			if code := call("POST", path, token); code != 404 {
				t.Errorf("%s with token %q: %d, want 404", path, token, code)
			}
		}
	}
}

func v1Call(t *testing.T, h http.HandlerFunc, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/community/x", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

// Each row of the enqueue contract: its status, its code, and the /v1 error envelope.
func TestCommunityEnqueueErrors(t *testing.T) {
	var fetchErr error
	s := &pebbleHTTP{crawlCommunityFetch: func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		return crawler.ContributionReceipt{Indexed: true}, fetchErr
	}}
	hash := strings.Repeat("a", 64)
	call := func(body string) *httptest.ResponseRecorder { return v1Call(t, s.handleCommunityEnqueue, body) }
	if w := call(`{}`); w.Code != 503 || v1ErrCode(t, w).Code != "community_unavailable" {
		t.Fatalf("not ready: %d %s", w.Code, w.Body)
	}
	s.crawlCommunityReady.Store(true)
	cases := []struct {
		name, body string
		err        error
		status     int
		code       string
	}{
		{"not an object", `[1]`, nil, 400, "invalid_body"},
		{"not json", `x`, nil, 400, "invalid_body"},
		{"no hash", `{"url":"https://example.com/p"}`, nil, 422, "invalid_field"},
		{"short hash", `{"url":"https://example.com/p","approved_content_hash":"abc"}`, nil, 422, "invalid_field"},
		{"bad id", `{"submission_id":"short","url":"https://example.com/p","approved_content_hash":"` + hash + `"}`, nil, 400, "invalid_submission_id"},
		{"id with a space", `{"submission_id":"stable submission 123","url":"https://example.com/p","approved_content_hash":"` + hash + `"}`, nil, 400, "invalid_submission_id"},
		{"local url", `{"url":"http://localhost/p","approved_content_hash":"` + hash + `"}`, nil, 400, "invalid_url"},
		{"conflict", `{"url":"https://example.com/p","approved_content_hash":"` + hash + `"}`, errCommunityReceiptConflict, 409, "submission_conflict"},
		{"rejected", `{"url":"https://example.com/p","approved_content_hash":"` + hash + `"}`, crawler.ErrContributionRejected, 422, "contribution_rejected"},
		{"fetch failure", `{"url":"https://example.com/p","approved_content_hash":"` + hash + `"}`, context.DeadlineExceeded, 502, "fetch_failed"},
	}
	for _, tc := range cases {
		fetchErr = tc.err
		w := call(tc.body)
		e := v1ErrCode(t, w)
		if w.Code != tc.status || e.Code != tc.code || e.Status != tc.status {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}
	if e := v1ErrCode(t, call(`{"url":"https://example.com/p"}`)); e.Field != "approved_content_hash" || e.Rule != "required" {
		t.Errorf("missing hash: %+v", e)
	}
	if e := v1ErrCode(t, call(`{"url":"https://example.com/p","approved_content_hash":"abc"}`)); e.Rule != "pattern" {
		t.Errorf("short hash: %+v", e)
	}
	fetchErr = nil
	w := call(`{"url":"https://example.com/p","approved_content_hash":"` + hash + `","unknown_member":{"a":1},"artifact":{"model":"m","extra":true}}`)
	if w.Code != 200 {
		t.Fatalf("an unknown member must be ignored: %d %s", w.Code, w.Body)
	}
	if w := v1Call(t, s.handleCommunityEnqueue, `{"url":"https://example.com/p","approved_content_hash":"`+hash+`"}`); !strings.Contains(w.Body.String(), `"indexed":true`) {
		t.Fatalf("receipt: %s", w.Body)
	}
}

// The moderate success body has exactly decision and category; every error is a /v1 error.
func TestCommunityModerateContract(t *testing.T) {
	chat := &communitySafetyChat{reply: `{"decision":"reject","category":"spam"}`, t: t}
	s := &pebbleHTTP{chat: chat}
	doc := func(mut func(m map[string]any)) string {
		m := map[string]any{}
		for k, v := range moderateBody {
			m[k] = v
		}
		if mut != nil {
			mut(m)
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
	w := v1Call(t, s.handleCommunityModerate, doc(nil))
	var got map[string]string
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil || len(got) != 2 || got["decision"] != "reject" || got["category"] != "spam" {
		t.Fatalf("verdict: %d %s", w.Code, w.Body)
	}
	cases := []struct {
		name, body string
		status     int
		code, f    string
	}{
		{"not an object", `[1]`, 400, "invalid_body", ""},
		{"unknown member", doc(func(m map[string]any) { m["extra"] = 1 }), 400, "unknown_field", "extra"},
		{"trailing data", doc(nil) + ` {}`, 400, "invalid_body", ""},
		{"text too short", doc(func(m map[string]any) { m["text"] = "short" }), 400, "invalid_document", "text"},
		{"text too long", doc(func(m map[string]any) { m["text"] = strings.Repeat("x", 32001) }), 400, "invalid_document", "text"},
		{"title too long", doc(func(m map[string]any) { m["title"] = strings.Repeat("x", 1001) }), 400, "invalid_document", "title"},
		{"signals too long", doc(func(m map[string]any) { m["signals"] = strings.Repeat("x", 4001) }), 400, "invalid_document", "signals"},
		{"local url", doc(func(m map[string]any) { m["url"] = "http://localhost/x" }), 400, "invalid_url", ""},
	}
	for _, tc := range cases {
		w := v1Call(t, s.handleCommunityModerate, tc.body)
		e := v1ErrCode(t, w)
		if w.Code != tc.status || e.Code != tc.code || e.Field != tc.f {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}
	s.chat = nil
	if w := v1Call(t, s.handleCommunityModerate, doc(nil)); w.Code != 503 || v1ErrCode(t, w).Code != "moderation_unavailable" || strings.Contains(w.Body.String(), "ceiling") {
		t.Fatalf("no model: %d %s", w.Code, w.Body)
	}
	for _, reply := range []string{`not json`, `{"decision":"allow","category":"malware"}`} {
		s.chat = &communitySafetyChat{reply: reply, t: t}
		if w := v1Call(t, s.handleCommunityModerate, doc(nil)); w.Code != 503 || v1ErrCode(t, w).Code != "moderation_unavailable" {
			t.Fatalf("reply %q: %d %s", reply, w.Code, w.Body)
		}
	}
}

type slowChat struct {
	communitySafetyChat
	d time.Duration
}

func (c *slowChat) Chat(ctx context.Context, msgs []embed.ChatMsg) (string, error) {
	time.Sleep(c.d)
	return c.communitySafetyChat.Chat(ctx, msgs)
}

// Both calls run longer than the listener's write timeout and still answer: each handler lifts the deadline.
func TestCommunityRoutesOutliveTheWriteTimeout(t *testing.T) {
	old := v1WriteTimeout
	v1WriteTimeout = 300 * time.Millisecond
	t.Cleanup(func() { v1WriteTimeout = old })
	j := newJoint(t, nil, nil)
	j.srv.chat = &slowChat{communitySafetyChat{reply: `{"decision":"allow","category":"safe"}`, t: t, calls: 0}, 800 * time.Millisecond}
	j.srv.crawlCommunityFetch = func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		time.Sleep(800 * time.Millisecond)
		return crawler.ContributionReceipt{Indexed: true}, nil
	}
	j.srv.crawlCommunityReady.Store(true)
	j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 200, "")
	j.want(j.do("community-broker", "POST", "/v1/community/enqueue", enqueueBody), 200, "")
}

func TestCommunityCapsFromEnv(t *testing.T) {
	for _, tc := range []struct {
		enq, mod string
		want     communityCaps
		bad      string
	}{
		{"", "", communityCaps{30, 30}, ""},
		{"10", "1", communityCaps{10, 1}, ""},
		{"30", "30", communityCaps{30, 30}, ""},
		{"31", "", communityCaps{}, "COSIFT_V1_ENQUEUE_PER_MIN"},
		{"", "31", communityCaps{}, "COSIFT_V1_MODERATE_PER_MIN"},
		{"0", "", communityCaps{}, "COSIFT_V1_ENQUEUE_PER_MIN"},
		{"", "-1", communityCaps{}, "COSIFT_V1_MODERATE_PER_MIN"},
		{"many", "", communityCaps{}, "COSIFT_V1_ENQUEUE_PER_MIN"},
		{"", "1.5", communityCaps{}, "COSIFT_V1_MODERATE_PER_MIN"},
	} {
		t.Setenv("COSIFT_V1_ENQUEUE_PER_MIN", tc.enq)
		t.Setenv("COSIFT_V1_MODERATE_PER_MIN", tc.mod)
		got, err := communityCapsFromEnv()
		if tc.bad != "" {
			if err == nil || !strings.Contains(err.Error(), tc.bad) {
				t.Errorf("%q %q: %v, want an error naming %s", tc.enq, tc.mod, err, tc.bad)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%q %q: %+v %v, want %+v", tc.enq, tc.mod, got, err, tc.want)
		}
	}
}

// A cap above the maximum stops the engine before it opens anything.
func TestServeRefusesACommunityCapAboveTheMaximum(t *testing.T) {
	t.Setenv("COSIFT_V1_MODERATE_PER_MIN", "31")
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runPebbleServe(ctx, &config.Config{}, []string{"-dir", filepath.Join(dir, "pebble"), "-addr", freeAddr(t)})
	if err == nil || !strings.Contains(err.Error(), "COSIFT_V1_MODERATE_PER_MIN") {
		t.Fatalf("started: %v", err)
	}
}

// Each route stops at 30 a minute for the broker principal, with a Retry-After, independently of the other.
func TestCommunityRoutesStopAtTheirCap(t *testing.T) {
	j, _ := communityJoint(t)
	for _, rt := range communityRoutes {
		ok, limited := 0, 0
		for range 40 {
			r := j.do("community-broker", "POST", rt.path, rt.body)
			switch r.code {
			case 200:
				ok++
			case 429:
				limited++
				if r.errCode() != "rate_limited" || r.hdr.Get("Retry-After") == "" {
					t.Fatalf("%s: %d %s %v", rt.name, r.code, r.raw, r.hdr)
				}
			default:
				t.Fatalf("%s: %d %s", rt.name, r.code, r.raw)
			}
		}
		if ok < 30 || ok > 31 || ok+limited != 40 {
			t.Errorf("%s: %d served, %d limited; want 30 served", rt.name, ok, limited)
		}
	}
}

// The environment's caps reach the routes of a running engine.
func TestServeHonoursTheCommunityCapEnv(t *testing.T) {
	t.Setenv("COSIFT_V1_MODERATE_PER_MIN", "3")
	t.Setenv("COSIFT_V1_ENQUEUE_PER_MIN", "5")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "service-auth.json")
	signer := svcauthtest.NewSigner("k")
	setV1Globals(t, cfgPath, filepath.Join(dir, "articles.json"), signer)
	keys := jointKeys(t)
	v1Addr := freeAddr(t)
	if err := os.WriteFile(cfgPath, mustMarshal(jointConfig(v1Addr, keys)), 0o640); err != nil {
		t.Fatal(err)
	}
	serveProcess(t, &config.Config{}, filepath.Join(dir, "pebble"))
	j := &joint{v1Fixture: &v1Fixture{signer: signer}, t: t, keys: keys, addr: v1Addr}
	for range 3 {
		j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 503, "moderation_unavailable")
	}
	j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 429, "rate_limited")
	for range 5 {
		j.want(j.do("community-broker", "POST", "/v1/community/enqueue", enqueueBody), 503, "community_unavailable")
	}
	j.want(j.do("community-broker", "POST", "/v1/community/enqueue", enqueueBody), 429, "rate_limited")
}
