package crawler

import (
	"context"
	"crypto/sha256"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

type capturePassages struct {
	mu sync.Mutex
	ps []*store.Passage
}

func (c *capturePassages) UpsertPassage(_ context.Context, p *store.Passage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ps = append(c.ps, p)
	return nil
}

func attestedCrawler(t *testing.T) (*Crawler, *store.PebbleStore) {
	t.Helper()
	ps, err := store.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ps.Close() })
	cfg := testCrawlerCfg()
	cfg.PerHostDelayMs = 0
	cfg.RespectRobots = false
	return NewWithBackend(cfg, ps, index.NewPebbleBM25(ps)), ps
}

const attestedText = "Tidal turbines convert the kinetic energy of moving water into electricity. A turbine sits on the seabed and turns slowly with each tide."

func TestIndexAttestedStoresAnAttestedDocument(t *testing.T) {
	ctx := context.Background()
	c, ps := attestedCrawler(t)
	if err := ps.PushFrontier(ctx, "https://example.org/old-path", 0, 1); err != nil {
		t.Fatal(err)
	}
	d := AttestedDoc{URL: "https://example.org/final", LeaseURL: "https://example.org/old-path", Title: "Tidal power", Text: attestedText, Lang: "en", VerifiedBy: "server"}
	indexed, novel, err := c.IndexAttested(ctx, d)
	if err != nil || !indexed {
		t.Fatalf("indexed %v %v", indexed, err)
	}
	if novel {
		t.Fatal("an incomplete content index must never call a page novel")
	}
	doc, err := ps.GetDocByURL(ctx, d.URL)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(attestedText))
	if doc.Source != "attested" || doc.VerifiedBy != "server" || doc.Quality != 0.5 || string(doc.ContentSHA) != string(sum[:]) || doc.Title != "Tidal power" || doc.Lang != "en" || doc.Domain != "example.org" {
		t.Fatalf("%+v", doc)
	}
	if !doc.PublishedAt.IsZero() || doc.Author != "" || doc.Image != "" || doc.Favicon != "" || doc.ETag != "" {
		t.Fatalf("an attested page carries no parse-only fields: %+v", doc)
	}
	hits, err := index.NewPebbleBM25(ps).Search(ctx, "tidal turbines", 5)
	if err != nil || len(hits) != 1 || hits[0].URL != d.URL {
		t.Fatalf("not searchable: %+v %v", hits, err)
	}
	fs, _ := ps.GetFrontierStats(ctx)
	if fs.Done != 1 || fs.Queued != 0 {
		t.Fatalf("the lease row completes with the document: %+v", fs)
	}
	if _, ok := c.completedInlineSet.Load(d.LeaseURL); ok {
		t.Fatal("the inline-completion mark leaked")
	}
}

func TestIndexedPagesSeedLinksOnlyWhenCrawled(t *testing.T) {
	ctx := context.Background()
	for _, attested := range []bool{false, true} {
		c, ps := attestedCrawler(t)
		c.cfg.MaxDepth = 3
		parsed := &ParsedDoc{Title: "T", Text: attestedText, Links: []string{"https://example.org/linked"}}
		item := store.FrontierItem{URL: "https://example.org/x"}
		if _, _, err := c.indexParsed(ctx, item, &fetchedPage{finalURL: item.URL, parsed: parsed}, indexOpts{attested: attested, verifiedBy: "server"}); err != nil {
			t.Fatal(err)
		}
		fs, _ := ps.GetFrontierStats(ctx)
		if seeded := fs.Queued > 0; seeded == attested {
			t.Errorf("attested=%v: frontier %+v", attested, fs)
		}
	}
}

func TestIndexAttestedContentNovelty(t *testing.T) {
	ctx := context.Background()
	c, ps := attestedCrawler(t)
	if _, err := ps.BackfillContentSHA(ctx, false, nil); err != nil {
		t.Fatal(err)
	}
	_, novel, err := c.IndexAttested(ctx, AttestedDoc{URL: "https://a.example.org/1", Title: "T", Text: attestedText, VerifiedBy: "server"})
	if err != nil || !novel {
		t.Fatalf("first copy of the content: novel %v %v", novel, err)
	}
	indexed, novel, err := c.IndexAttested(ctx, AttestedDoc{URL: "https://b.example.org/2", Title: "T", Text: attestedText, VerifiedBy: "quorum"})
	if err != nil || !indexed || novel {
		t.Fatalf("the same content at another URL: indexed %v novel %v %v", indexed, novel, err)
	}
	_, novel, err = c.IndexAttested(ctx, AttestedDoc{URL: "https://c.example.org/3", Title: "T", Text: attestedText + " Another sentence follows here.", VerifiedBy: "quorum"})
	if err != nil || !novel {
		t.Fatalf("different content: novel %v %v", novel, err)
	}
}

func TestIndexAttestedSameURLAndContentWritesNothing(t *testing.T) {
	ctx := context.Background()
	c, ps := attestedCrawler(t)
	d := AttestedDoc{URL: "https://example.org/page", Title: "T", Text: attestedText, VerifiedBy: "server"}
	if indexed, _, err := c.IndexAttested(ctx, d); err != nil || !indexed {
		t.Fatalf("%v %v", indexed, err)
	}
	first, _ := ps.GetDocByURL(ctx, d.URL)
	time.Sleep(10 * time.Millisecond)
	indexed, novel, err := c.IndexAttested(ctx, d)
	if err != nil || indexed || novel {
		t.Fatalf("a repeat: indexed %v novel %v %v", indexed, novel, err)
	}
	second, _ := ps.GetDocByURL(ctx, d.URL)
	if !second.FetchedAt.Equal(first.FetchedAt) || second.ID != first.ID {
		t.Fatalf("a repeat rewrote the document: %v vs %v", first.FetchedAt, second.FetchedAt)
	}
}

func TestAQuorumNeverReplacesAStoredDocument(t *testing.T) {
	ctx := context.Background()
	const stored, other = "https://example.org/stored", "https://example.org/other"
	const forged = "A forged page about something else entirely, long enough to be a document of its own."
	seed := func(c *Crawler, ps *store.PebbleStore, source, by string) {
		t.Helper()
		switch source {
		case "crawl":
			sum := sha256.Sum256([]byte(attestedText))
			if _, err := ps.UpsertDocument(ctx, &store.Document{URL: stored, Title: "Crawled", Text: attestedText, Source: "crawl", ContentSHA: sum[:], FetchedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
		default:
			if _, _, err := c.IndexAttested(ctx, AttestedDoc{URL: stored, Title: "Crawled", Text: attestedText, VerifiedBy: by}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name, source, by, lease, vouched string
		replaced                         bool
	}{
		{"a crawled document under another lease", "crawl", "", other, "quorum", false},
		{"a crawled document under its own lease", "crawl", "", stored, "quorum", false},
		{"a server-vouched document", "attested", "server", stored, "quorum", false},
		{"a quorum document under another lease", "attested", "quorum", other, "quorum", false},
		{"a quorum document under its own lease", "attested", "quorum", stored, "quorum", true},
		{"a crawled document, a server copy", "crawl", "", stored, "server", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, ps := attestedCrawler(t)
			seed(c, ps, tc.source, tc.by)
			indexed, novel, err := c.IndexAttested(ctx, AttestedDoc{URL: stored, LeaseURL: tc.lease, Title: "Forged", Text: forged, VerifiedBy: tc.vouched})
			if err != nil || indexed != tc.replaced || novel {
				t.Fatalf("indexed %v novel %v %v", indexed, novel, err)
			}
			doc, err := ps.GetDocByURL(ctx, stored)
			if err != nil {
				t.Fatal(err)
			}
			if replaced := doc.Title == "Forged"; replaced != tc.replaced {
				t.Fatalf("replaced %v, want %v: %+v", replaced, tc.replaced, doc)
			}
		})
	}
}

func TestIndexAttestedRefusesAnUnknownVerifier(t *testing.T) {
	c, ps := attestedCrawler(t)
	for _, by := range []string{"", "worker", "Server"} {
		if _, _, err := c.IndexAttested(context.Background(), AttestedDoc{URL: "https://example.org/x", Title: "T", Text: attestedText, VerifiedBy: by}); err == nil {
			t.Errorf("verified_by %q accepted", by)
		}
	}
	if _, err := ps.GetDocByURL(context.Background(), "https://example.org/x"); err == nil {
		t.Fatal("a refused page was stored")
	}
}

func TestIndexAttestedReusesVerifiedVectorsAndDropsBadOnes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		model     string
		embedding []float32
		reused    bool
	}{
		{"verified vectors are reused", "model", []float32{1, 2.005}, true},
		{"a model mismatch drops them", "other", []float32{1, 2.005}, false},
		{"a poisoned vector drops them", "model", []float32{2, 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, ps := attestedCrawler(t)
			pw := &capturePassages{}
			c = c.WithEmbedder(&artifactReference{}).WithPassageWriter(pw)
			art := &LocalArtifact{Model: tc.model, Chunks: []IndexedChunk{{Text: "T\n\n" + attestedText, Embedding: tc.embedding}}}
			indexed, _, err := c.IndexAttested(ctx, AttestedDoc{URL: "https://example.org/v", Title: "T", Text: attestedText, Artifact: art, VerifiedBy: "quorum"})
			if err != nil || !indexed {
				t.Fatalf("a bad vector must never fail the page: %v %v", indexed, err)
			}
			if _, err := ps.GetDocByURL(ctx, "https://example.org/v"); err != nil {
				t.Fatal(err)
			}
			if len(pw.ps) != 1 {
				t.Fatalf("%d passages written", len(pw.ps))
			}
			if reused := pw.ps[0].Embedding[1] == 2.005; reused != tc.reused {
				t.Errorf("passage vector %v, reused=%v want %v", pw.ps[0].Embedding, reused, tc.reused)
			}
		})
	}
}

func TestFetchCopyParsesWithoutIndexing(t *testing.T) {
	ctx := context.Background()
	var gotCond string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCond = r.Header.Get("If-None-Match")
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()
	c, ps := attestedCrawler(t)
	if _, err := ps.UpsertDocument(ctx, &store.Document{URL: srv.URL, Title: "old", Text: "old", ETag: `"v1"`, ContentSHA: []byte("x"), FetchedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	page, err := c.FetchCopy(ctx, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if page.Title != "Test Page" || !strings.Contains(page.Text, "Go programming") || page.FinalURL != srv.URL {
		t.Fatalf("%+v", page)
	}
	if gotCond != "" {
		t.Fatalf("the copy must be an unconditional GET, sent If-None-Match %q", gotCond)
	}
	if doc, _ := ps.GetDocByURL(ctx, srv.URL); doc.Title != "old" {
		t.Fatalf("FetchCopy wrote the store: %+v", doc)
	}
}

func TestFetchCopyFailsWhenTheServerRefuses(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) }))
	defer srv.Close()
	c, _ := attestedCrawler(t)
	if _, err := c.FetchCopy(ctx, srv.URL); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("%v", err)
	}
	c.cfg.ExcludeDomains = []string{"127.0.0.1"}
	var logs strings.Builder
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	if _, err := c.FetchCopy(ctx, srv.URL); !errors.Is(err, ErrCopySkipped) {
		t.Fatalf("a URL the domain policy drops has no copy: %v", err)
	}
	if strings.Contains(logs.String(), "127.0.0.1") {
		t.Fatalf("a refused copy must not log the URL: %s", logs.String())
	}
}

func TestAllowedURLAppliesTheDomainPolicyAndTheAdultFilter(t *testing.T) {
	c, _ := attestedCrawler(t)
	c.cfg.ExcludeDomains = []string{"wired.com"}
	for u, want := range map[string]bool{
		"https://example.org/page":        true,
		"https://wired.com/story":         false,
		"https://www.wired.com/story":     false,
		"https://notwired.com/story":      true,
		"https://www.pornhub.com/view":    false,
		"https://example.org/ads?x=wired": true,
	} {
		if got := c.AllowedURL(u); got != want {
			t.Errorf("%s: %v", u, got)
		}
	}
	c.cfg.IncludeDomains = []string{"example.org"}
	if c.AllowedURL("https://other.net/x") || !c.AllowedURL("https://docs.example.org/x") {
		t.Error("the include list is not applied")
	}
}

func TestACrawledAndAnAttestedPageOfTheSameTextDifferOnlyInSourceAndVerifier(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(sampleHTML))
	}))
	defer srv.Close()
	c, ps := attestedCrawler(t)
	if err := c.FetchAndIndexNow(ctx, srv.URL); err != nil {
		t.Fatal(err)
	}
	crawled, err := ps.GetDocByURL(ctx, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	other := "https://attested.example.org/page"
	if indexed, _, err := c.IndexAttested(ctx, AttestedDoc{URL: other, Title: crawled.Title, Text: crawled.Text, Lang: crawled.Lang, VerifiedBy: "server"}); err != nil || !indexed {
		t.Fatalf("%v %v", indexed, err)
	}
	attested, err := ps.GetDocByURL(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if attested.Title != crawled.Title || attested.Text != crawled.Text || attested.Lang != crawled.Lang || attested.Quality != crawled.Quality ||
		string(attested.ContentSHA) != string(crawled.ContentSHA) || attested.ChangeCount != crawled.ChangeCount {
		t.Fatalf("crawled %+v attested %+v", crawled, attested)
	}
	if crawled.Source != "crawl" || attested.Source != "attested" || attested.VerifiedBy != "server" || crawled.VerifiedBy != "" {
		t.Fatalf("%q %q %q", crawled.Source, attested.Source, attested.VerifiedBy)
	}
	hits, err := index.NewPebbleBM25(ps).Search(ctx, "concurrent programming", 5)
	if err != nil || len(hits) != 2 || hits[0].Score != hits[1].Score {
		t.Fatalf("the two documents are indexed alike: %+v %v", hits, err)
	}
}
