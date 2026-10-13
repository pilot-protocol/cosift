package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

var communityRoutes = []struct {
	name, path string
	body       map[string]any
}{
	{"enqueue", "/v1/community/enqueue", enqueueBody},
	{"moderate", "/v1/community/moderate", moderateBody},
}

// communityJoint is a listener with the engine's community handlers wired to fakes.
func communityJoint(t *testing.T) (*joint, *lockedBuffer) {
	t.Helper()
	logs := captureDefaultLog(t)
	j := newJoint(t, func(cfg map[string]any) { cfg["failed_auth"] = map[string]any{"per_minute": 600, "burst": 100} }, nil)
	j.srv.crawlCommunityFetch = func(context.Context, string, *crawler.LocalArtifact, string) (crawler.ContributionReceipt, error) {
		return crawler.ContributionReceipt{Indexed: true, Novel: true, ContentHash: strings.Repeat("e", 64), ContentIndexComplete: true}, nil
	}
	j.srv.crawlCommunityReady.Store(true)
	j.srv.chat = &communitySafetyChat{reply: `{"decision":"allow","category":"safe"}`, t: t}
	return j, logs
}

// bearer sends body to path with the given Authorization value and extra headers.
func (j *joint) bearer(path, auth string, body any, hdr ...string) resp {
	j.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "http://"+j.addr+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
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

// The rejection matrix on the real listener, route by route.
func TestCommunityRoutesRejectionMatrix(t *testing.T) {
	j, logs := communityJoint(t)
	for _, rt := range communityRoutes {
		t.Run(rt.name, func(t *testing.T) {
			j.want(j.do("", "POST", rt.path, rt.body), 401, "unauthenticated")
			for _, id := range []string{"community-wiki", "dash-prod", "dash-staging", "golive-admin", "synth-prod", "mcp-prod", "resolver-prod"} {
				r := j.do(id, "POST", rt.path, rt.body)
				j.want(r, 403, "missing_scope")
				if r.body["detail"] != "requires scope community:intake" {
					t.Errorf("%s: detail %v", id, r.body["detail"])
				}
			}
			for _, tok := range []string{"peer-secret", "admin-secret", "ck_1_" + strings.Repeat("a", 40), "old-community-admin", j.keys["community-broker"] + "x"} {
				j.want(j.bearer(rt.path, "Bearer "+tok, rt.body), 401, "unauthenticated")
			}
			j.want(j.bearer(rt.path, "Bearer "+j.keys["community-broker"], rt.body, "X-Forwarded-For", "203.0.113.9"), 401, "unauthenticated")
			j.want(j.bearer(rt.path, j.keys["community-broker"], rt.body), 401, "unauthenticated")
			req := httptest.NewRequest("POST", rt.path, strings.NewReader("{}"))
			req.RemoteAddr = "203.0.113.9:4000"
			req.Header.Set("Authorization", "Bearer "+j.keys["community-broker"])
			w := httptest.NewRecorder()
			j.l.svc.ServeHTTP(w, req)
			if w.Code != 401 {
				t.Errorf("a broker key from a non-loopback peer: %d", w.Code)
			}
			j.want(j.do("community-broker", "POST", rt.path, rt.body), 200, "")
		})
	}
	for _, line := range []string{
		`route="POST /v1/community/enqueue" status=401`, `route="POST /v1/community/enqueue" status=403`,
		`route="POST /v1/community/moderate" status=401`, `route="POST /v1/community/moderate" status=403`,
		`reason=key_forwarded`, `reason=key_not_loopback`,
	} {
		if !strings.Contains(logs.String(), line) {
			t.Errorf("no log line with %s", line)
		}
	}
	for _, secret := range []string{j.keys["community-broker"], j.keys["community-wiki"], "old-community-admin", "peer-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("the log holds a credential")
		}
	}
}

// The broker key reaches the community routes and nothing else; no OIDC
// principal can be configured with its scopes.
func TestCommunityScopesAreBrokerOnly(t *testing.T) {
	j, _ := communityJoint(t)
	id := jointID(1)
	for _, rt := range []struct{ method, path string }{
		{"GET", "/v1/articles"}, {"GET", "/v1/articles/" + id}, {"GET", "/v1/articles/by-slug/x"}, {"GET", "/v1/articles/stats"},
		{"POST", "/v1/articles/match"}, {"PUT", "/v1/articles/" + id}, {"DELETE", "/v1/articles/" + id},
		{"POST", "/v1/articles/" + id + "/status"}, {"POST", "/v1/articles/purge-prelive"},
		{"GET", "/v1/article-versions/" + id}, {"GET", "/v1/article-versions/" + id + "/1"},
		{"POST", "/v1/search"}, {"POST", "/v1/contents"},
	} {
		j.want(j.do("community-broker", rt.method, rt.path, map[string]any{}), 403, "missing_scope")
	}
	for _, scopes := range [][]string{{"articles:read", "community:intake"}, {"articles:read", "work:broker"}} {
		principalOfCfg(j.cfg, "mcp-prod")["scopes"] = scopes
		if err := j.reload(); err == nil {
			t.Fatalf("an oidc principal with %v was accepted", scopes)
		}
		j.want(j.do("mcp-prod", "POST", "/v1/community/moderate", moderateBody), 403, "missing_scope")
	}
}

// The routes do not need the article index, so a restart does not make them answer index_unavailable.
func TestCommunityRoutesServeBeforeTheIndexIsReady(t *testing.T) {
	until := make(chan struct{})
	defer close(until)
	j := newJoint(t, nil, func(a articleLayer) articleLayer { return &delayedRebuild{articleLayer: a, until: until} })
	j.srv.crawlCommunityReady.Store(true)
	j.srv.chat = &communitySafetyChat{reply: `{"decision":"allow","category":"safe"}`, t: t}
	j.want(j.do("dash-prod", "GET", "/v1/articles/stats", nil), 503, "index_unavailable")
	j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 200, "")
	j.want(j.do("community-broker", "POST", "/v1/community/enqueue", enqueueBody), 503, "community_unavailable")
}
