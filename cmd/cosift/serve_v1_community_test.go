package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
