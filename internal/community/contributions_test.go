package community

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

const (
	contribPublicURL = "https://cosift.example"
	articleTidal     = "01J9A3Q8B2C3D4E5F6G7H8J9K0"
	articleGone      = "01J9A3Q8B2C3D4E5F6G7H8J9K1"
	contribBase      = "01J9A3Q7W4X9M3K5N6P8R0T2V"
)

type contribProvider struct {
	*fakeShared
	mu        sync.Mutex
	member    sharedaccount.Member
	memberErr error
	recs      []sharedaccount.ContributionRecord
	recsErr   error
	limits    []int
	members   int
	byUID     map[string]sharedaccount.Member
	uids      []string
	cids      []string
}

func (p *contribProvider) Member(_ context.Context, uid string) (sharedaccount.Member, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.members++
	p.uids = append(p.uids, uid)
	if p.byUID != nil {
		m, ok := p.byUID[uid]
		if !ok {
			return sharedaccount.Member{}, sharedaccount.ErrNotFound
		}
		return m, p.memberErr
	}
	return p.member, p.memberErr
}

func (p *contribProvider) Contributions(_ context.Context, cid string, limit int) ([]sharedaccount.ContributionRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.limits = append(p.limits, limit)
	p.cids = append(p.cids, cid)
	return p.recs, p.recsErr
}

func joinedProvider() *contribProvider {
	return &contribProvider{fakeShared: &fakeShared{}, member: sharedaccount.Member{CID: "0f1e2d3c4b5a6978", Handle: "tidal-fan",
		Fetch: map[string]int64{"claimed": 12, "submitted": 10, "verified": 8, "mismatch": 0, "rejected": 1, "credits": 16}}}
}

func contribServer(t *testing.T, p *contribProvider, arts ArticleReader) *Server {
	t.Helper()
	s, err := Open(Config{DataDir: t.TempDir(), Backend: "http://127.0.0.1:9", PublicURL: contribPublicURL, Shared: p, Articles: arts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func contribArticles() *fakeArticles {
	return &fakeArticles{rec: map[string]ArticleRecord{articleTidal: {Slug: "tidal-power", Origin: "community", Version: 1}}, err: map[string]error{}}
}

func getContributions(s *Server) *httptest.ResponseRecorder {
	return bearerRequest(s, "/api/contributions", sharedTestToken)
}

func goldenJSON(t *testing.T, name string, body []byte) {
	t.Helper()
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, bytes.TrimSpace(body), "", "  "); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}
	pretty.WriteByte('\n')
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(want, pretty.Bytes()) {
		t.Fatalf("%s differs (UPDATE_GOLDEN=1 rewrites it):\n%s", path, pretty.String())
	}
}

func contribID(n int) string { return contribBase + string(rune('0'+n)) }

func sampleRecords() []sharedaccount.ContributionRecord {
	at := time.Date(2026, 10, 14, 9, 12, 31, 0, time.UTC)
	return []sharedaccount.ContributionRecord{
		{ID: contribID(8), Kind: "new", Status: "queued", StatusAt: at.Add(8 * time.Minute), Title: "Queued subject"},
		{ID: contribID(7), Kind: "new", Status: "won", StatusAt: at.Add(7 * time.Minute), Title: "Gone article", ArticleID: articleGone, Version: 2, Credits: 0},
		{ID: contribID(6), Kind: "new", Status: "taken_down", StatusAt: at.Add(6 * time.Minute), ArticleID: articleTidal, Version: 1, Credits: 1000},
		{ID: contribID(5), Kind: "revision", Status: "failed", StatusAt: at.Add(5 * time.Minute), Title: "Failed subject"},
		{ID: contribID(4), Kind: "new", Status: "needs_sources", StatusAt: at.Add(4 * time.Minute), Title: "Waiting subject", URLs: []string{"https://docs.example.org/a"}, RecheckAt: at.Add(10 * time.Hour)},
		{ID: contribID(3), Kind: "revision", Status: "lost", StatusAt: at.Add(3 * time.Minute), Title: "Lost subject", LostReason: "not_better"},
		{ID: contribID(2), Kind: "revision", Status: "refused", StatusAt: at.Add(2 * time.Minute), Title: "Rust async runtimes", Refusals: []sharedaccount.RefusalCode{{Code: "promotional", Field: "body_md"}}},
		{ID: contribID(1), Kind: "new", Status: "published", StatusAt: at, Title: "Tidal power", ArticleID: articleTidal, Version: 1, Credits: 1500},
	}
}

func seedLedger(t *testing.T, s *Server, rows [][]any) {
	t.Helper()
	var userID string
	if err := s.db.QueryRow(`SELECT user_id FROM shared_identities`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if _, err := s.db.Exec(`INSERT INTO credit_ledger(id,user_id,delta,reason,created_at) VALUES(?,?,?,?,?)`, r[0], userID, r[1], r[2], r[3]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestContributionsGoldenForAJoinedContributor(t *testing.T) {
	p := joinedProvider()
	p.recs = sampleRecords()
	s := contribServer(t, p, contribArticles())
	getContributions(s)
	seedLedger(t, s, [][]any{
		{"award:art:" + contribID(1), 1500, "contribution_award", 1791969160},
		{"award:roy:2026-10-13:" + articleTidal + ":0f1e2d3c4b5a6978", 3, "royalty", 1791969170},
		{"award:claw:" + articleGone + "_" + articleTidal + ":0f1e2d3c4b5a6978", -200, "clawback", 1791969180},
		{"fetch:abc:user", 2, "fetch_verified", 1791969190},
		{"contribution:hash", 10, "verified_contribution", 1791969150},
		{"meter:u:request-id-0001", -2, "article_read", 1791969200},
		{"monthly-free:u:2026-10", 1000, "monthly_free", 1791969100},
	})
	w := getContributions(s)
	expect(t, w, 200)
	goldenJSON(t, "contributions-joined.json", w.Body.Bytes())
	if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("headers %v", w.Header())
	}
}

func TestContributionsGoldenForANonMemberAndForLocalMode(t *testing.T) {
	p := joinedProvider()
	p.memberErr = sharedaccount.ErrNotFound
	s := contribServer(t, p, contribArticles())
	w := getContributions(s)
	expect(t, w, 200)
	goldenJSON(t, "contributions-non-member.json", w.Body.Bytes())
	if len(p.limits) != 0 {
		t.Fatal("a non-member's contributions were read")
	}
	local := testServer(t, nil)
	w = request(t, local, "GET", "/api/contributions", nil, account(t, local, "local@example.com"))
	expect(t, w, 200)
	goldenJSON(t, "contributions-local.json", w.Body.Bytes())
	if w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("cache %q", w.Header().Get("Cache-Control"))
	}
}

func TestContributionsNeedAnAccountAndAnUnavailableStoreIsA503(t *testing.T) {
	p := joinedProvider()
	s := contribServer(t, p, contribArticles())
	if w := bearerRequest(s, "/api/contributions", ""); w.Code != 401 {
		t.Fatalf("anonymous: %d", w.Code)
	}
	p.memberErr = sharedaccount.ErrUnavailable
	w := getContributions(s)
	if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"contributions_unavailable"`) || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("member error: %d %s %q", w.Code, w.Body, w.Header().Get("Cache-Control"))
	}
	p.memberErr, p.recsErr = nil, errors.New("down")
	if w = getContributions(s); w.Code != 503 || strings.Contains(w.Body.String(), "tidal-fan") {
		t.Fatalf("contributions error: %d %s", w.Code, w.Body)
	}
}

func TestContributionsAskForFiftyOneAndShowFifty(t *testing.T) {
	p := joinedProvider()
	for i := 0; i < 51; i++ {
		p.recs = append(p.recs, sharedaccount.ContributionRecord{ID: fmt.Sprintf("01J9A3Q7W4X9M3K5N6P8R0T2%02d", i), Kind: "new", Status: "queued", StatusAt: time.Unix(1791969100+int64(i), 0).UTC(), Title: "T"})
	}
	var out struct {
		Contributions []map[string]any `json:"contributions"`
		More          bool             `json:"more"`
	}
	w := getContributions(contribServer(t, p, contribArticles()))
	json.Unmarshal(w.Body.Bytes(), &out)
	if len(p.limits) != 1 || p.limits[0] != 51 || len(out.Contributions) != 50 || !out.More {
		t.Fatalf("limits %v, %d items, more %v", p.limits, len(out.Contributions), out.More)
	}
	p.recs = p.recs[:50]
	json.Unmarshal(getContributions(contribServer(t, p, contribArticles())).Body.Bytes(), &out)
	if out.More {
		t.Fatal("fifty items are not more")
	}
}
