package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/community"
	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	e2eUID = "0123456789abcdef"
	e2eCID = "0f1e2d3c4b5a6978"
)

// e2eAccounts is a shared-account provider that knows one token and one joined contributor.
type e2eAccounts struct{}

func (e2eAccounts) Namespace() string { return "test/staging" }
func (e2eAccounts) Verify(_ context.Context, token string) (sharedaccount.Identity, error) {
	if token == workerTestToken {
		return sharedaccount.Identity{UID: e2eUID, Email: "worker@example.com"}, nil
	}
	return sharedaccount.Identity{}, sharedaccount.ErrUnauthorized
}
func (e2eAccounts) Start(context.Context, string) (sharedaccount.Challenge, error) {
	return sharedaccount.Challenge{}, sharedaccount.ErrUnavailable
}
func (e2eAccounts) Finish(context.Context, string, string) (sharedaccount.Issued, error) {
	return sharedaccount.Issued{}, sharedaccount.ErrUnavailable
}
func (e2eAccounts) Revoke(context.Context, string) error { return nil }
func (e2eAccounts) Call(context.Context, string, string, map[string]any) (map[string]any, error) {
	return nil, sharedaccount.ErrUnavailable
}
func (e2eAccounts) JoinState(context.Context, string) (sharedaccount.JoinState, error) {
	return sharedaccount.JoinState{CID: e2eCID, TermsVersion: 1}, nil
}
func (e2eAccounts) AddCounters(context.Context, string, map[string]int64) error { return nil }
func (e2eAccounts) RaiseAlert(context.Context, sharedaccount.Alert) error       { return nil }

func openApp(t *testing.T, engineURL, brokerKey string, work bool) *httptest.Server {
	t.Helper()
	app, err := community.Open(community.Config{
		DataDir: t.TempDir(), Backend: "http://127.0.0.1:1", PublicURL: "http://localhost:7780", Shared: e2eAccounts{},
		Work: work, BrokerKey: brokerKey, EngineURL: engineURL, Environment: "prod",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { app.Close() })
	srv := httptest.NewServer(app)
	t.Cleanup(srv.Close)
	return srv
}

func TestWorkerEndToEndThroughTheCommunityAppToTheEngine(t *testing.T) {
	j, _ := workJoint(t)
	ctx := context.Background()
	moved, blocked := "http://pages.example.com/moved", "http://pages.example.com/blocked"
	for _, u := range []string{moved, blocked} {
		if err := j.ps.PushFrontier(ctx, u, 0, 1); err != nil {
			t.Fatal(err)
		}
		if err := j.ps.FailFrontier(ctx, u, "http 403"); err != nil {
			t.Fatal(err)
		}
	}
	j.srv.work.svc.Feed(ctx)

	origin, _ := fixtureOrigin(t)
	page, err := fetchAndExtract(ctx, originClient(origin), nil, moved, 0)
	if err != nil {
		t.Fatal(err)
	}
	workerB, netB := strings.Repeat("b", 32), strings.Repeat("2", 32)
	claim := j.do("community-broker", "POST", "/v1/work/claim", map[string]any{"worker": workerB, "net": netB, "max": 2})
	j.want(claim, 200, "")
	var movedLease string
	for _, l := range claim.body["leases"].([]any) {
		if l.(map[string]any)["url"] == moved {
			movedLease = l.(map[string]any)["lease_id"].(string)
		}
	}
	if movedLease == "" {
		t.Fatalf("no lease for the redirecting page: %s", claim.raw)
	}
	j.want(j.do("community-broker", "POST", "/v1/work/submit", submitBodyAs(movedLease, workerB, netB, page.FinalURL, page.Title, page.Text)), 200, "")

	app := openApp(t, "http://"+j.addr, j.keys["community-broker"], true)
	stdout, stderr, err := runWorkerCLI(t, app2fake(app), workerTestToken, "-worker", "-server", app.URL, "-max-pages", "2", "-concurrency", "2")
	if err != nil {
		t.Fatalf("%v\n%s\n%s", err, stdout, stderr)
	}
	var submitted, reported, summary map[string]any
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("stdout line %q", line)
		}
		switch e["event"] {
		case "submitted":
			submitted = e
		case "reported":
			reported = e
		case "summary":
			summary = e
		}
	}
	if submitted["verdict"] != "verified" || submitted["agreeing_count"] != float64(2) {
		t.Fatalf("submitted: %v\n%s", submitted, stdout)
	}
	if reported["http_status"] != float64(403) {
		t.Fatalf("reported: %v\n%s", reported, stdout)
	}
	if summary["claimed"] != float64(2) || summary["submitted"] != float64(1) || summary["reported"] != float64(1) || summary["stop_reason"] != "max_pages" {
		t.Fatalf("summary: %v", summary)
	}

	j.srv.work.svc.Sweep(ctx)
	settled := j.do("community-broker", "GET", "/v1/work/settled?after=0&limit=10", nil)
	j.want(settled, 200, "")
	events := settled.body["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["verdict"] != "verified" || len(events[0].(map[string]any)["workers"].([]any)) != 2 {
		t.Fatalf("settled: %s", settled.raw)
	}
	contents := j.do("synth-prod", "POST", "/v1/contents", map[string]any{"urls": []string{moved}})
	j.want(contents, 200, "")
	item := contents.body["results"].([]any)[0].(map[string]any)
	if item["found"] != true || item["source"] != "attested" || item["verified_by"] != "quorum" || item["text"] != strings.Join(strings.Fields(page.Text), " ") {
		t.Fatalf("contents: %s", contents.raw)
	}
}

func TestWorkerAgainstAnAppWithWorkSwitchedOffStopsWithAClearMessage(t *testing.T) {
	j, _ := workJoint(t)
	app := openApp(t, "http://"+j.addr, j.keys["community-broker"], false)
	stdout, _, err := runWorkerCLI(t, app2fake(app), workerTestToken, "-worker", "-server", app.URL, "-max-pages", "1")
	if err == nil || !strings.Contains(err.Error(), "does not offer worker mode") || !strings.Contains(stdout, `"event":"summary"`) {
		t.Fatalf("%v %q", err, stdout)
	}
}

// app2fake wraps a real app server in the fakeApp shape runWorkerCLI expects.
func app2fake(srv *httptest.Server) *fakeApp {
	return &fakeApp{srv: srv}
}
