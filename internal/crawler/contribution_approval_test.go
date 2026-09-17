package crawler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pilot-protocol/cosift/internal/store"
)

func TestContributionApprovalRejectsChangedContentBeforeIndex(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			body := sampleHTML
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Header.Get("If-None-Match") != "" {
					t.Error("approved fetch used conditional cache")
				}
				w.Header().Set("Content-Type", "text/html")
				w.Header().Set("ETag", "approved-page")
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			db := newStoreT(t)
			cfg := testCrawlerCfg()
			cfg.RespectRobots = false
			cfg.PerHostDelayMs = 0
			cfg.DisableLinkFollowing = true
			emb := &stubEmbedder{dim: 8}
			c := New(cfg, db).WithEmbedder(emb)
			// Local transport fixture only; production constructs a guarded client.
			c.contributionOnce.Do(func() { c.contributionCrawler = c; c.contributionSlots = make(chan struct{}, 2) })
			parsed, err := Parse([]byte(sampleHTML), srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			approved := ApprovedContentHash(parsed.Title, parsed.Text)
			if existing {
				if _, err := c.FetchContribution(context.Background(), srv.URL, nil, approved); err != nil {
					t.Fatal(err)
				}
			}
			embBefore, requestsBefore := emb.calls, requests
			// Fresh-cache settings must not bypass the exact-content check.
			t.Setenv("COSIFT_REFETCH_AFTER_HOURS", "999")
			body = strings.ReplaceAll(sampleHTML, "Go programming", "credential harvesting and malicious spam")
			if _, err := c.FetchContribution(context.Background(), srv.URL, nil, approved); !errors.Is(err, ErrContributionRejected) {
				t.Fatalf("changed page accepted: %v", err)
			}
			if requests != requestsBefore+1 || emb.calls != embBefore {
				t.Fatal("approval bypassed fetch or embedded rejected content")
			}
			doc, err := db.GetDocByURL(context.Background(), srv.URL)
			if existing {
				if err != nil || doc == nil || doc.Text != parsed.Text {
					t.Fatal("rejected content replaced the indexed document")
				}
			} else if err == nil && doc != nil {
				t.Fatal("rejected content entered the index")
			}
		})
	}
}

func TestContributionApprovalRequiresHashAndFreshBody(t *testing.T) {
	db := newStoreT(t)
	cfg := testCrawlerCfg()
	cfg.RespectRobots = false
	c := New(cfg, db)
	for _, value := range []string{"", "bad", strings.Repeat("A", 64)} {
		if _, err := c.FetchContribution(context.Background(), "https://example.com", nil, value); !errors.Is(err, ErrContributionRejected) {
			t.Fatalf("invalid approval accepted: %q %v", value, err)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotModified) }))
	defer srv.Close()
	ctx := context.WithValue(context.Background(), approvedContentKey{}, ApprovedContentHash("title", "text"))
	if err := c.FetchAndIndexNow(ctx, srv.URL); !errors.Is(err, ErrContributionRejected) {
		t.Fatalf("304 bypassed approval: %v", err)
	}
}

// Simulate a competing writer replacing the document after the guarded write.
// Embedding the narrow interface keeps this fixture on the normal Upsert path.
type replacingContributionStore struct {
	CrawlerStore
	replaceTitle bool
	replaced     bool
}

func (s *replacingContributionStore) UpsertDocument(ctx context.Context, doc *store.Document) (int64, error) {
	id, err := s.CrawlerStore.UpsertDocument(ctx, doc)
	if err != nil {
		return id, err
	}
	replacement := *doc
	if s.replaceTitle {
		replacement.Title = "Different unapproved title"
	} else {
		replacement.Text = "Different unapproved stored content"
	}
	replacement.ContentSHA = nil
	_, err = s.CrawlerStore.UpsertDocument(ctx, &replacement)
	s.replaced = true
	return id, err
}

func TestContributionReceiptRejectsReplacedStoredDocument(t *testing.T) {
	for _, replaceTitle := range []bool{false, true} {
		name := "text"
		if replaceTitle {
			name = "title"
		}
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.Write([]byte(sampleHTML))
			}))
			defer srv.Close()
			db := newStoreT(t)
			cfg := testCrawlerCfg()
			cfg.RespectRobots = false
			cfg.PerHostDelayMs = 0
			cfg.DisableLinkFollowing = true
			c := New(cfg, db)
			wrapped := &replacingContributionStore{CrawlerStore: db, replaceTitle: replaceTitle}
			c.store = wrapped
			// Local transport fixture only, matching the guarded-fetch tests above.
			c.contributionOnce.Do(func() { c.contributionCrawler = c; c.contributionSlots = make(chan struct{}, 2) })
			parsed, err := Parse([]byte(sampleHTML), srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := c.FetchContribution(context.Background(), srv.URL, nil, ApprovedContentHash(parsed.Title, parsed.Text))
			if !wrapped.replaced {
				t.Fatal("test did not replace the stored document")
			}
			if !errors.Is(err, ErrContributionRejected) || receipt.Indexed || receipt.Novel || receipt.ContentHash != "" {
				t.Fatalf("changed stored document produced a reward receipt: %+v %v", receipt, err)
			}
		})
	}
}
