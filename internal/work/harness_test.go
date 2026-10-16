package work

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
)

type harness struct {
	t   *testing.T
	fs  vfs.FS
	db  *pebble.DB
	s   *Service
	cfg config.WorkSettings
	f   *fakes

	cmu   sync.Mutex
	clock time.Time
	pairs int
}

// fakes are the engine services the Service leans on.
type fakes struct {
	mu         sync.Mutex
	corpus     map[string]bool
	blockedFor map[string]bool
	pages      map[string]Page
	fetchErr   map[string]error
	fetches    map[string]int
	verdict    Verdict
	modErr     error
	modelCalls int
	modCalls   int
	indexed    []crawler.AttestedDoc
	indexErr   error
	novel      bool
	rows       map[string]string
	errored    []FrontierRow
	queued     []FrontierRow
	purged     map[string]bool
	attested   map[string]bool
	crawled    map[string]bool
}

func newFakes() *fakes {
	return &fakes{corpus: map[string]bool{}, blockedFor: map[string]bool{}, pages: map[string]Page{}, fetchErr: map[string]error{},
		fetches: map[string]int{}, verdict: Verdict{Decision: "allow", Category: "safe"}, novel: true, rows: map[string]string{},
		purged: map[string]bool{}, attested: map[string]bool{}, crawled: map[string]bool{}}
}

func hexID(n int) string { return fmt.Sprintf("%032x", n) }

func fakeCanonical(raw string) (string, error) {
	if len(raw) > 2048 {
		return "", errors.New("long")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || !strings.Contains(u.Hostname(), ".") || net.ParseIP(u.Hostname()) != nil {
		return "", errors.New("not public")
	}
	if p := u.Port(); p != "" && p != "80" && p != "443" {
		return "", errors.New("port")
	}
	u.Host, u.Fragment = strings.ToLower(u.Host), ""
	return u.String(), nil
}

func newHarness(t *testing.T, edit ...func(*config.WorkSettings)) *harness {
	t.Helper()
	cfg, err := config.Work{}.Settings()
	if err != nil {
		t.Fatal(err)
	}
	cfg.HostSpacingMS = 0
	for _, e := range edit {
		e(&cfg)
	}
	h := &harness{t: t, fs: vfs.NewMem(), cfg: cfg, f: newFakes(), clock: time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC)}
	h.open()
	t.Cleanup(func() { h.db.Close() })
	return h
}

func (h *harness) open() {
	h.t.Helper()
	db, err := pebble.Open("db", &pebble.Options{FS: h.fs})
	if err != nil {
		h.t.Fatal(err)
	}
	h.db = db
	h.s = h.service(db)
}

func (h *harness) service(db *pebble.DB) *Service {
	f := h.f
	s, err := New(Options{DB: db, Config: h.cfg, Frontier: true, Now: h.now, Logf: func(string, ...any) {}, Deps: Deps{
		Canonical: fakeCanonical,
		Allowed: func(u string) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			return !f.blockedFor[hostOf(u)]
		},
		InCorpus: func(_ context.Context, u string) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.corpus[u], nil
		},
		Frontier: f,
		Fetch: func(_ context.Context, u string) (Page, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.fetches[u]++
			if err := f.fetchErr[u]; err != nil {
				return Page{}, err
			}
			p, ok := f.pages[u]
			if !ok {
				return Page{}, errors.New("http 404")
			}
			return p, nil
		},
		BotCheck: func(title, text string) bool { return strings.Contains(strings.ToLower(title), "just a moment") },
		Moderate: func(_ context.Context, doc ModerationDoc, reserve func() error) (Verdict, error) {
			f.mu.Lock()
			f.modCalls++
			verdict, err := f.verdict, f.modErr
			f.mu.Unlock()
			if rerr := reserve(); rerr != nil {
				return Verdict{}, rerr
			}
			f.mu.Lock()
			f.modelCalls++
			f.mu.Unlock()
			return verdict, err
		},
		Index: func(_ context.Context, d crawler.AttestedDoc) (bool, bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.indexErr != nil {
				return false, false, f.indexErr
			}
			f.indexed = append(f.indexed, d)
			if _, ok := f.rows[d.LeaseURL]; ok {
				f.rows[d.LeaseURL] = "done"
			}
			return true, f.novel, nil
		},
		Purge: func(_ context.Context, u string, apply bool) (int, int, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			switch {
			case f.attested[u]:
				if apply {
					f.purged[u] = true
					delete(f.attested, u)
				}
				return 1, 0, nil
			case f.crawled[u]:
				return 0, 1, nil
			}
			return 0, 0, nil
		},
	}})
	if err != nil {
		h.t.Fatal(err)
	}
	return s
}

func (f *fakes) Errored(_ context.Context, after string, scan int) ([]FrontierRow, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scanRows(f.errored, after, scan)
}

func (f *fakes) Queued(_ context.Context, after string, scan int) ([]FrontierRow, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scanRows(f.queued, after, scan)
}

func scanRows(rows []FrontierRow, after string, scan int) ([]FrontierRow, string, bool, error) {
	var out []FrontierRow
	last := after
	for _, r := range rows {
		if r.URL <= after {
			continue
		}
		if len(out) >= scan {
			return out, last, false, nil
		}
		out = append(out, r)
		last = r.URL
	}
	return out, last, true, nil
}

func (h *harness) now() time.Time {
	h.cmu.Lock()
	defer h.cmu.Unlock()
	return h.clock
}

func (h *harness) advance(d time.Duration) {
	h.cmu.Lock()
	h.clock = h.clock.Add(d)
	h.cmu.Unlock()
}

// nextPair returns a worker and a network no earlier call returned.
func (h *harness) nextPair() (string, string) {
	h.pairs++
	return hexID(h.pairs), hexID(1000 + h.pairs)
}

// push puts URLs straight in the pool as pushed entries.
func (h *harness) push(urls ...string) { h.add(ClassPushed, urls...) }

func (h *harness) add(class Class, urls ...string) {
	h.t.Helper()
	h.s.mu.Lock()
	defer h.s.mu.Unlock()
	for _, u := range urls {
		if added, _ := h.s.poolAdd(u, class, nil); !added {
			h.t.Fatalf("pool refused %s", u)
		}
	}
}

// claimFor claims one lease for a pair.
func (h *harness) claimFor(worker, net string) (LeaseGrant, bool) {
	h.t.Helper()
	res, err := h.s.Claim(worker, net, 1)
	if err != nil {
		h.t.Fatal(err)
	}
	if len(res.Leases) == 0 {
		return LeaseGrant{}, false
	}
	return res.Leases[0], true
}

func sha(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func (h *harness) record(u string) *Record {
	h.t.Helper()
	r, err := h.s.getRecord(u)
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *harness) settled() []Event {
	h.t.Helper()
	res, err := h.s.Settled(0, 200)
	if err != nil {
		h.t.Fatal(err)
	}
	return res.Events
}

func (h *harness) page(u, text string) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	h.f.pages[u] = Page{FinalURL: u, Title: "A page", Text: text, Lang: "en"}
	delete(h.f.fetchErr, u)
}

func (h *harness) failFetch(u string) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	h.f.fetchErr[u] = errors.New("http 403")
}

type simPair struct{ a, b string }

// fixture reads a named pair of pages from the crawler's simhash vectors.
func fixture(t *testing.T, name string) (string, string, int) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "crawler", "testdata", "simhash_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Cases []struct {
			Name     string `json:"name"`
			A        string `json:"a"`
			B        string `json:"b"`
			Distance int    `json:"distance"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	for _, c := range v.Cases {
		if c.Name == name {
			return c.A, c.B, c.Distance
		}
	}
	t.Fatalf("no fixture %q", name)
	return "", "", 0
}

const (
	pageURL = "https://example.org/blocked-page"
)

var pageText = strings.Repeat("Tidal turbines turn slowly on the seabed and feed a cable to the shore. ", 4)
