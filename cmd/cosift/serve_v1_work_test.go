package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

var workRoutes = []struct {
	name, method, path string
	body               map[string]any
}{
	{"claim", "POST", "/v1/work/claim", map[string]any{"worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32)}},
	{"submit", "POST", "/v1/work/submit", map[string]any{"lease_id": strings.Repeat("c", 32), "worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "http_status": 503}},
	{"push", "POST", "/v1/work/push", map[string]any{"urls": []string{"https://blocked.example.org/page"}}},
	{"settled", "GET", "/v1/work/settled?after=0", nil},
}

// workJoint is the listener with the work service wired to the real store and crawler.
func workJoint(t *testing.T) (*joint, *lockedBuffer) {
	t.Helper()
	j, logs := communityJoint(t)
	settings, err := config.Work{}.Settings()
	if err != nil {
		t.Fatal(err)
	}
	settings.HostSpacingMS = 0
	j.srv.work = &workHooks{settings: settings, enabled: true, frontier: true}
	if err := j.srv.initWork(j.ps); err != nil {
		t.Fatal(err)
	}
	ccfg := config.Default().Crawler
	ccfg.BlockPrivateNetworks = false
	ccfg.ExcludeDomains = []string{"excluded.example.org"}
	j.srv.work.crawler.Store(crawler.NewWithBackend(ccfg, j.ps, j.srv.idx))
	return j, logs
}

func TestWorkRoutesRejectionMatrix(t *testing.T) {
	j, logs := workJoint(t)
	for _, rt := range workRoutes {
		t.Run(rt.name, func(t *testing.T) {
			j.want(j.do("", rt.method, rt.path, rt.body), 401, "unauthenticated")
			for _, id := range []string{"community-wiki", "dash-prod", "dash-staging", "golive-admin", "synth-prod", "mcp-prod", "resolver-prod"} {
				r := j.do(id, rt.method, rt.path, rt.body)
				j.want(r, 403, "missing_scope")
				if r.body["detail"] != "requires scope work:broker" {
					t.Errorf("%s: detail %v", id, r.body["detail"])
				}
			}
			for _, tok := range []string{"peer-secret", "admin-secret", "ck_1_" + strings.Repeat("a", 40), "old-community-admin", j.keys["community-broker"] + "x"} {
				req, _ := http.NewRequest(rt.method, "http://"+j.addr+rt.path, nil)
				req.Header.Set("Authorization", "Bearer "+tok)
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				if res.StatusCode != 401 {
					t.Errorf("token %.8s: %d", tok, res.StatusCode)
				}
			}
			req, _ := http.NewRequest(rt.method, "http://"+j.addr+rt.path, nil)
			req.Header.Set("Authorization", "Bearer "+j.keys["community-broker"])
			req.Header.Set("X-Forwarded-For", "203.0.113.9")
			if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != 401 {
				t.Errorf("a forwarded broker key: %v %v", res, err)
			}
			req = httptest.NewRequest(rt.method, rt.path, nil)
			req.RemoteAddr = "203.0.113.9:4000"
			req.Header.Set("Authorization", "Bearer "+j.keys["community-broker"])
			w := httptest.NewRecorder()
			j.l.svc.ServeHTTP(w, req)
			if w.Code != 401 {
				t.Errorf("a broker key from a non-loopback peer: %d", w.Code)
			}
			if r := j.do("community-broker", rt.method, rt.path, rt.body); r.code == 401 || r.code == 403 || r.code == 404 {
				t.Errorf("the broker key was refused: %d %s", r.code, r.raw)
			}
		})
	}
	for _, line := range []string{`route="POST /v1/work/claim" status=401`, `route="POST /v1/work/claim" status=403`, `route="GET /v1/work/settled" status=403`} {
		if !strings.Contains(logs.String(), line) {
			t.Errorf("no log line with %s", line)
		}
	}
	for _, secret := range []string{j.keys["community-broker"], "old-community-admin", "peer-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Error("the log holds a credential")
		}
	}
}

func TestAnIntakeOnlyKeyCannotReachTheWorkRoutes(t *testing.T) {
	j, _ := workJoint(t)
	principalOfCfg(j.cfg, "community-broker")["scopes"] = []string{"community:intake"}
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	for _, rt := range workRoutes {
		j.want(j.do("community-broker", rt.method, rt.path, rt.body), 403, "missing_scope")
	}
	j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 200, "")
	principalOfCfg(j.cfg, "community-broker")["scopes"] = []string{"work:broker"}
	if err := j.reload(); err != nil {
		t.Fatal(err)
	}
	j.want(j.do("community-broker", "POST", "/v1/community/moderate", moderateBody), 403, "missing_scope")
	j.want(j.do("community-broker", "GET", "/v1/work/settled", nil), 200, "")
}

func TestWorkRoutesAnswerWorkDisabledUntilSwitchedOn(t *testing.T) {
	j, _ := workJoint(t)
	j.srv.work.enabled = false
	for _, rt := range workRoutes {
		r := j.do("community-broker", rt.method, rt.path, rt.body)
		j.want(r, 503, "work_disabled")
	}
	j.srv.work.enabled = true
	j.srv.work.crawler.Store(nil)
	for _, rt := range workRoutes {
		r := j.do("community-broker", rt.method, rt.path, rt.body)
		j.want(r, 503, "index_unavailable")
		if r.hdr.Get("Retry-After") == "" {
			t.Errorf("%s: no Retry-After", rt.name)
		}
	}
	j.srv.work = nil
	j.want(j.do("community-broker", "POST", "/v1/work/claim", workRoutes[0].body), 503, "work_disabled")
}

func TestWorkRoutesServeBeforeTheIndexIsReady(t *testing.T) {
	until := make(chan struct{})
	defer close(until)
	j := newJoint(t, nil, func(a articleLayer) articleLayer { return &delayedRebuild{articleLayer: a, until: until} })
	settings, _ := config.Work{}.Settings()
	j.srv.work = &workHooks{settings: settings, enabled: true}
	if err := j.srv.initWork(j.ps); err != nil {
		t.Fatal(err)
	}
	ccfg := config.Default().Crawler
	j.srv.work.crawler.Store(crawler.NewWithBackend(ccfg, j.ps, j.srv.idx))
	j.want(j.do("dash-prod", "GET", "/v1/articles/stats", nil), 503, "index_unavailable")
	j.want(j.do("community-broker", "GET", "/v1/work/settled", nil), 200, "")
}

func TestWorkRequestsAreStrictAndCapped(t *testing.T) {
	j, _ := workJoint(t)
	post := func(path string, body any) resp { return j.do("community-broker", "POST", path, body) }
	j.want(post("/v1/work/claim", map[string]any{"worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "extra": 1}), 400, "unknown_field")
	j.want(post("/v1/work/claim", map[string]any{"worker": strings.Repeat("a", v1WorkClaimBodyLimit)}), 413, "body_too_large")
	j.want(post("/v1/work/push", map[string]any{"urls": []string{strings.Repeat("a", v1WorkPushBodyLimit)}}), 413, "body_too_large")
	j.want(post("/v1/work/submit", map[string]any{"text": strings.Repeat("a", v1WorkSubmitBodyLimit)}), 413, "body_too_large")
	for _, path := range []string{"/v1/work/claim", "/v1/work/submit", "/v1/work/push"} {
		req, _ := http.NewRequest("POST", "http://"+j.addr+path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+j.keys["community-broker"])
		req.Header.Set("Content-Type", "text/plain")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 415 {
			t.Errorf("%s with text/plain: %d", path, res.StatusCode)
		}
	}
	rule := func(r resp, field, rule string) {
		t.Helper()
		j.want(r, 422, "invalid_field")
		if r.body["field"] != field || r.body["rule"] != rule {
			t.Fatalf("%s", r.raw)
		}
	}
	rule(post("/v1/work/claim", map[string]any{"worker": "x", "net": strings.Repeat("b", 32)}), "worker", "pattern")
	rule(post("/v1/work/claim", map[string]any{"worker": strings.Repeat("a", 32), "net": "x"}), "net", "pattern")
	rule(post("/v1/work/claim", map[string]any{"worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "max": 0}), "max", "range")
	rule(post("/v1/work/claim", map[string]any{"worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "max": 33}), "max", "range")
	rule(post("/v1/work/push", map[string]any{}), "urls", "required")
	rule(post("/v1/work/push", map[string]any{"urls": []string{}}), "urls", "required")
	rule(post("/v1/work/push", map[string]any{"urls": make([]string, 101)}), "urls", "too_many")
	for _, q := range []string{"after=x", "after=-1", "limit=0", "limit=201", "limit=x"} {
		r := j.do("community-broker", "GET", "/v1/work/settled?"+q, nil)
		j.want(r, 422, "invalid_field")
	}
	var ok struct {
		Rejected []struct {
			Index int    `json:"index"`
			Rule  string `json:"rule"`
		} `json:"rejected"`
		Accepted   int `json:"accepted"`
		Duplicates int `json:"duplicates"`
	}
	if _, err := j.ps.UpsertDocument(context.Background(), &store.Document{URL: "https://stored.example.org/doc", Title: "T", Text: "body text of the page", FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	r := post("/v1/work/push", map[string]any{"urls": []string{"https://ok.example.org/p", "http://127.0.0.1/x", "https://www.pornhub.com/view", "https://excluded.example.org/p", "https://stored.example.org/doc"}})
	j.want(r, 200, "")
	if err := json.Unmarshal([]byte(r.raw), &ok); err != nil || ok.Accepted != 1 || ok.Duplicates != 1 || len(ok.Rejected) != 3 ||
		ok.Rejected[0].Rule != "url" || ok.Rejected[1].Rule != "url" || ok.Rejected[2].Rule != "policy" || ok.Rejected[2].Index != 3 {
		t.Fatalf("%s", r.raw)
	}
}

func TestTheLeaseRowCompletesEvenWhenTheSamePageIsAlreadyStored(t *testing.T) {
	j, _ := workJoint(t)
	ctx := context.Background()
	const pageURL = "https://blocked.example.org/page"
	text := distinctProse(40)
	if err := j.ps.PushFrontier(ctx, pageURL, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := j.ps.FailFrontier(ctx, pageURL, "http 403"); err != nil {
		t.Fatal(err)
	}
	j.srv.work.svc.Feed(ctx)
	sum := sha256.Sum256([]byte(text))
	if _, err := j.ps.UpsertDocument(ctx, &store.Document{URL: pageURL, Title: "Tidal power", Text: text, Source: "crawl", ContentSHA: sum[:], FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for i, p := range [][2]string{{strings.Repeat("a", 32), strings.Repeat("1", 32)}, {strings.Repeat("b", 32), strings.Repeat("2", 32)}} {
		r := j.do("community-broker", "POST", "/v1/work/claim", map[string]any{"worker": p[0], "net": p[1]})
		j.want(r, 200, "")
		l := r.body["leases"].([]any)[0].(map[string]any)
		j.want(j.do("community-broker", "POST", "/v1/work/submit", submitBodyFor(l["lease_id"].(string), p[0], p[1], pageURL, text)), 200, "")
		_ = i
	}
	j.srv.work.svc.Sweep(ctx)
	if stats, _ := j.ps.GetFrontierStats(ctx); stats.Errored != 0 || stats.Done != 1 {
		t.Fatalf("the row stays errored although its page is indexed: %+v", stats)
	}
	if doc, err := j.ps.GetDocByURL(ctx, pageURL); err != nil || doc.Source != "crawl" {
		t.Fatalf("a repeat of stored content rewrote the document: %+v %v", doc, err)
	}
	evs := j.do("community-broker", "GET", "/v1/work/settled", nil).body["events"].([]any)
	if len(evs) != 1 || evs[0].(map[string]any)["verdict"] != "verified" || evs[0].(map[string]any)["content_novel"] != false {
		t.Fatalf("%v", evs)
	}
}

func TestSubmitWithAStaleExtractionVersionCarriesTheEnginesVersion(t *testing.T) {
	j, _ := workJoint(t)
	text := strings.Repeat("Tidal turbines turn slowly on the seabed. ", 4)
	r := j.do("community-broker", "POST", "/v1/work/submit", map[string]any{
		"lease_id": strings.Repeat("c", 32), "worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "final_url": "https://x.example.org/p",
		"http_status": 200, "title": "t", "text": text, "lang": "en", "content_hash": strings.Repeat("0", 64), "simhash": strings.Repeat("0", 16), "extract_version": 2,
	})
	j.want(r, 409, "extract_version_mismatch")
	if r.body["extract_version"] != float64(crawler.ExtractVersion) {
		t.Fatalf("%s", r.raw)
	}
	r = j.do("community-broker", "POST", "/v1/work/submit", map[string]any{
		"lease_id": strings.Repeat("c", 32), "worker": strings.Repeat("a", 32), "net": strings.Repeat("b", 32), "http_status": 503,
	})
	j.want(r, 409, "lease_expired")
	if _, ok := r.body["extract_version"]; ok {
		t.Fatal("an unrelated error carries the version")
	}
	for _, leak := range []string{strings.Repeat("c", 32), strings.Repeat("a", 32)} {
		if strings.Contains(r.raw, leak) {
			t.Fatalf("an error body echoes a request value: %s", r.raw)
		}
	}
}

func hexSHA(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// submitBodyFor is the attestation a correct CLI sends.
func submitBodyFor(leaseID, worker, net, finalURL, text string) map[string]any {
	return submitBodyAs(leaseID, worker, net, finalURL, "Tidal power", text)
}

func submitBodyAs(leaseID, worker, net, finalURL, title, text string) map[string]any {
	sum, _ := crawler.Simhash(text)
	return map[string]any{"lease_id": leaseID, "worker": worker, "net": net, "final_url": finalURL, "http_status": 200, "title": title, "text": text,
		"lang": "en", "content_hash": hexSHA(text), "simhash": crawler.SimhashHex(sum), "extract_version": crawler.ExtractVersion}
}

func TestAWorkPageIsLeasedVerifiedModeratedIndexedAndSettled(t *testing.T) {
	j, _ := workJoint(t)
	ctx := context.Background()
	const pageURL = "https://blocked.example.org/page"
	if err := j.ps.PushFrontier(ctx, pageURL, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := j.ps.FailFrontier(ctx, pageURL, "http 403"); err != nil {
		t.Fatal(err)
	}
	if err := j.ps.PushFrontier(ctx, "https://queued.example.org/page", 0, 1); err != nil {
		t.Fatal(err)
	}
	before, _ := j.ps.GetFrontierStats(ctx)
	j.srv.work.svc.Feed(ctx)
	text := strings.Repeat("Tidal turbines convert the kinetic energy of moving water into electricity, turning slowly on the seabed with each tide. ", 3)
	workerA, netA, workerB, netB := strings.Repeat("a", 32), strings.Repeat("1", 32), strings.Repeat("b", 32), strings.Repeat("2", 32)
	claim := func(worker, net string) map[string]any {
		r := j.do("community-broker", "POST", "/v1/work/claim", map[string]any{"worker": worker, "net": net, "max": 2})
		j.want(r, 200, "")
		return r.body
	}
	var leases [2]map[string]any
	for i, p := range [][2]string{{workerA, netA}, {workerB, netB}} {
		body := claim(p[0], p[1])
		ls := body["leases"].([]any)
		if len(ls) != 2 {
			t.Fatalf("leases: %s", body)
		}
		for _, l := range ls {
			if l.(map[string]any)["url"] == pageURL {
				leases[i] = l.(map[string]any)
			}
		}
		if leases[i]["source"] != "errored" || leases[i]["extract_version"] != float64(1) || len(leases[i]["lease_id"].(string)) != 32 {
			t.Fatalf("%v", leases[i])
		}
	}
	if after, _ := j.ps.GetFrontierStats(ctx); after != before {
		t.Fatalf("a claim changed the frontier: %+v -> %+v", before, after)
	}
	first := j.do("community-broker", "POST", "/v1/work/submit", submitBodyFor(leases[0]["lease_id"].(string), workerA, netA, pageURL, text))
	j.want(first, 200, "")
	if first.body["verdict"] != "awaiting_quorum" || first.body["indexed"] != false || len(first.body["agreeing"].([]any)) != 0 || first.body["window_expires_at"] == nil {
		t.Fatalf("%s", first.raw)
	}
	second := j.do("community-broker", "POST", "/v1/work/submit", submitBodyFor(leases[1]["lease_id"].(string), workerB, netB, pageURL, text))
	j.want(second, 200, "")
	if second.body["verdict"] != "verified" || fmt.Sprint(second.body["agreeing"]) != fmt.Sprint([]any{workerA, workerB}) {
		t.Fatalf("%s", second.raw)
	}
	j.srv.work.svc.Sweep(ctx)
	settled := j.do("community-broker", "GET", "/v1/work/settled?after=0&limit=10", nil)
	j.want(settled, 200, "")
	evs := settled.body["events"].([]any)
	if len(evs) != 1 {
		t.Fatalf("%s", settled.raw)
	}
	ev := evs[0].(map[string]any)
	sum := sha256.Sum256([]byte(pageURL))
	if ev["verdict"] != "verified" || ev["content_novel"] != false || ev["url_sha"] != hex.EncodeToString(sum[:]) || fmt.Sprint(ev["workers"]) != fmt.Sprint([]any{workerA, workerB}) || settled.body["next"] != float64(1) || settled.body["last_seq"] != float64(1) {
		t.Fatalf("%s", settled.raw)
	}
	contents := j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{pageURL}})
	j.want(contents, 200, "")
	item := contents.body["results"].([]any)[0].(map[string]any)
	if item["found"] != true || item["source"] != "attested" || item["verified_by"] != "quorum" || item["text"] != strings.Join(strings.Fields(text), " ") {
		t.Fatalf("%s", contents.raw)
	}
	if after, _ := j.ps.GetFrontierStats(ctx); after.Errored != 0 || after.Done != 1 || after.Queued != before.Queued {
		t.Fatalf("the lease row completes with the document: %+v", after)
	}
	late := j.do("community-broker", "POST", "/v1/work/submit", submitBodyFor(leases[0]["lease_id"].(string), workerA, netA, pageURL, text))
	j.want(late, 200, "")
	if late.body["verdict"] != "verified" || late.body["indexed"] != true {
		t.Fatalf("a repeat repeats its answer: %s", late.raw)
	}
	body, err := j.srv.buildStatsBody(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stats struct {
		Work map[string]any `json:"work"`
	}
	if err := json.Unmarshal(body, &stats); err != nil || stats.Work["enabled"] != true || stats.Work["verified"] != float64(1) || stats.Work["settled_seq"] != float64(1) {
		t.Fatalf("%v %s", err, body)
	}
	var metrics bytes.Buffer
	j.srv.writeWorkMetrics(&metrics)
	if !strings.Contains(metrics.String(), `cosift_work_settled_total{verdict="verified"} 1`) {
		t.Fatalf("%s", metrics.String())
	}
}

type tamperSide struct{ Title, Text string }

type tamperCase struct {
	ID                  string
	Anchor, Attestation tamperSide
	Expect              struct {
		Agree      bool   `json:"agree"`
		AgreedText string `json:"agreed_text"`
	}
}

// tamperedPairs are the work_agreed_text cases, by id.
func tamperedPairs(t *testing.T) map[string]tamperCase {
	t.Helper()
	raw, err := os.ReadFile("../../internal/work/testdata/work_agreed_text.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct{ Cases []tamperCase }
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out := map[string]tamperCase{}
	for _, c := range v.Cases {
		out[c.ID] = c
	}
	return out
}

func TestTamperedTextOnAnErroredPageNeverReachesTheIndex(t *testing.T) {
	cases := tamperedPairs(t)
	for _, id := range []string{"injected-paragraph", "entity-swapped-inside-one-part", "long-page-entity-swap", "different-title"} {
		t.Run(id, func(t *testing.T) {
			c := cases[id]
			j, _ := workJoint(t)
			ctx := context.Background()
			const pageURL = "https://blocked.example.org/page"
			if err := j.ps.PushFrontier(ctx, pageURL, 0, 1); err != nil {
				t.Fatal(err)
			}
			if err := j.ps.FailFrontier(ctx, pageURL, "http 403"); err != nil {
				t.Fatal(err)
			}
			j.srv.work.svc.Feed(ctx)
			var last resp
			for i, side := range []tamperSide{c.Anchor, c.Attestation} {
				worker, net := strings.Repeat(string(rune('a'+i)), 32), strings.Repeat(string(rune('1'+i)), 32)
				cl := j.do("community-broker", "POST", "/v1/work/claim", map[string]any{"worker": worker, "net": net, "max": 1})
				j.want(cl, 200, "")
				l := cl.body["leases"].([]any)[0].(map[string]any)
				last = j.do("community-broker", "POST", "/v1/work/submit", submitBodyAs(l["lease_id"].(string), worker, net, pageURL, side.Title, side.Text))
				j.want(last, 200, "")
			}
			j.srv.work.svc.Sweep(ctx)
			item := j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{pageURL}}).body["results"].([]any)[0].(map[string]any)
			if !c.Expect.Agree {
				if last.body["verdict"] != "awaiting_quorum" || item["found"] == true {
					t.Fatalf("the title decides: %s %v", last.raw, item)
				}
				return
			}
			if last.body["verdict"] != "verified" || item["found"] != true || item["text"] != c.Expect.AgreedText || item["title"] != c.Attestation.Title {
				t.Fatalf("%s\n%v", last.raw, item)
			}
		})
	}
}

func TestAFrontierOrPushedQuorumIndexesNothingAndLeavesTheRowQueued(t *testing.T) {
	for _, class := range []string{"frontier", "pushed"} {
		t.Run(class, func(t *testing.T) {
			j, _ := workJoint(t)
			ctx := context.Background()
			const pageURL = "https://queued.example.org/page"
			if class == "frontier" {
				if err := j.ps.PushFrontier(ctx, pageURL, 0, 1); err != nil {
					t.Fatal(err)
				}
				j.srv.work.svc.Feed(ctx)
			} else {
				j.want(j.do("community-broker", "POST", "/v1/work/push", map[string]any{"urls": []string{pageURL}}), 200, "")
			}
			before, _ := j.ps.GetFrontierStats(ctx)
			text := strings.Repeat("Tidal turbines convert the kinetic energy of moving water into electricity, turning slowly on the seabed with each tide. ", 3)
			var last resp
			for _, p := range [][2]string{{strings.Repeat("a", 32), strings.Repeat("1", 32)}, {strings.Repeat("b", 32), strings.Repeat("2", 32)}} {
				c := j.do("community-broker", "POST", "/v1/work/claim", map[string]any{"worker": p[0], "net": p[1]})
				j.want(c, 200, "")
				l := c.body["leases"].([]any)[0].(map[string]any)
				if l["source"] != class || l["url"] != pageURL {
					t.Fatalf("%v", l)
				}
				last = j.do("community-broker", "POST", "/v1/work/submit", submitBodyFor(l["lease_id"].(string), p[0], p[1], pageURL, text))
				j.want(last, 200, "")
			}
			if last.body["verdict"] != "awaiting_quorum" || last.body["indexed"] != false || len(last.body["agreeing"].([]any)) != 0 {
				t.Fatalf("a quorum on a %s page is not a verification: %s", class, last.raw)
			}
			if after, _ := j.ps.GetFrontierStats(ctx); after != before {
				t.Fatalf("the frontier row moved without a server fetch: %+v -> %+v", before, after)
			}
			item := j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{pageURL}}).body["results"].([]any)[0].(map[string]any)
			if item["found"] == true {
				t.Fatalf("the quorum's text was indexed: %v", item)
			}
			if evs := j.do("community-broker", "GET", "/v1/work/settled", nil).body["events"].([]any); len(evs) != 0 {
				t.Fatalf("a settled event before the server fetch: %v", evs)
			}
			if st := j.srv.work.svc.Stats(); st.Awaiting != 1 || st.Verified != 0 {
				t.Fatalf("%+v", st)
			}
		})
	}
}

func TestTheWorkBlockIsAbsentWhileWorkIsOff(t *testing.T) {
	j, _ := workJoint(t)
	j.srv.work.enabled = false
	body, err := j.srv.buildStatsBody(context.Background())
	if err != nil || strings.Contains(string(body), `"work"`) {
		t.Fatalf("%v %s", err, body)
	}
	var metrics bytes.Buffer
	j.srv.writeWorkMetrics(&metrics)
	if metrics.Len() != 0 {
		t.Fatal("work series while off")
	}
}

func TestWorkRoutesRegisterOnTheListenerTable(t *testing.T) {
	f := newV1Fixture(t)
	routes := f.srv.v1CommunityRoutes()
	seen := map[string]bool{}
	for _, rt := range routes {
		seen[rt.Pattern()] = true
		if strings.HasPrefix(rt.Path, "/v1/work/") && (!rt.Ungated || len(rt.Scopes) != 1 || rt.Scopes[0] != "work:broker") {
			t.Errorf("%s: %+v", rt.Pattern(), rt)
		}
	}
	for _, p := range []string{"POST /v1/work/claim", "POST /v1/work/submit", "POST /v1/work/push", "GET /v1/work/settled", "POST /v1/community/enqueue", "POST /v1/community/moderate"} {
		if !seen[p] {
			t.Errorf("%s is not registered", p)
		}
	}
}

func rawPost(t *testing.T, h http.HandlerFunc, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/work/submit", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h(w, r)
	return w
}

func FuzzWorkSubmitDecoder(f *testing.F) {
	text := strings.Repeat("Tidal turbines turn slowly on the seabed. ", 4)
	valid, _ := json.Marshal(submitBodyFor(strings.Repeat("c", 32), strings.Repeat("a", 32), strings.Repeat("b", 32), "https://x.example.org/p", text))
	f.Add(valid)
	f.Add([]byte(`{"lease_id":"x","http_status":503}`))
	f.Add([]byte(`{"artifact":{"model":"m","chunks":[{"text":"a","embedding":[1e999]}]}}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`{"text":"\ud800","title":null,"extract_version":1.5}`))
	f.Add([]byte("{\"lease_id\":\"" + strings.Repeat("\xff", 40) + "\"}"))
	ps, err := store.OpenPebble(f.TempDir())
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { ps.Close() })
	s := &pebbleHTTP{store: ps, idx: index.NewPebbleBM25(ps)}
	settings, _ := config.Work{}.Settings()
	s.work = &workHooks{settings: settings, enabled: true}
	if err := s.initWork(ps); err != nil {
		f.Fatal(err)
	}
	s.work.crawler.Store(crawler.NewWithBackend(config.Default().Crawler, ps, s.idx))
	f.Fuzz(func(t *testing.T, body []byte) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/v1/work/submit", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		s.handleWorkSubmit(w, r)
		if w.Code >= 500 {
			t.Fatalf("status %d for %q", w.Code, body)
		}
		var out map[string]any
		if json.Unmarshal(w.Body.Bytes(), &out) != nil {
			t.Fatalf("not JSON: %q", w.Body.String())
		}
	})
}
