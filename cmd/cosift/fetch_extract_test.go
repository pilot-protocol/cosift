package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

// originClient reaches every hostname through srv, with the browser client's redirect policy.
func originClient(srv *httptest.Server) *http.Client {
	addr := srv.Listener.Addr().String()
	return crawler.PublicRedirects(&http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}})
}

const fixtureArticle = `<p>The tessellation of the plane by regular hexagons is the densest packing of equal circles, a result that
bees appear to exploit when they build comb. This paragraph is long enough to clear the minimum text size for a page.</p>
<p>A second paragraph adds more words so that the shingles of the simhash have something to work on, and so that
the boilerplate rules have a body to keep while they drop the navigation around it.</p>`

func fixtureOrigin(t *testing.T) (*httptest.Server, *headerLog) {
	t.Helper()
	pdf, err := os.ReadFile(filepath.Join("..", "..", "testdata", "crawler", "hello.pdf"))
	if err != nil {
		t.Fatal(err)
	}
	log := &headerLog{}
	mux := http.NewServeMux()
	page := func(lang, title, body string) string {
		return `<!doctype html><html lang="` + lang + `"><head><title>` + title + `</title><meta property="og:title" content="` + title + ` | Hexagons"><style>.x{color:red}</style><script>var tracking = 1;</script></head><body>
<nav><a href="/">Home</a> <a href="/about">About us</a> <a href="/login">Sign in</a></nav><header>Site banner header</header>
<main><article><h1>` + title + `</h1>` + body + `</article></main><aside>Related: ten tips you will not believe</aside>
<footer>Copyright 2026 Hexagon Corp. All rights reserved. Cookie settings.</footer></body></html>`
	}
	mux.HandleFunc("/plain.html", func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(page("en", "Hexagons", fixtureArticle)))
	})
	mux.HandleFunc("/german.html", func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		w.Write([]byte(page("de-AT", "Sechsecke", strings.ReplaceAll(fixtureArticle, "tessellation", "Parkettierung"))))
	})
	mux.HandleFunc("/moved", func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		http.Redirect(w, r, "/plain.html", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/doc.pdf", func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		w.Header().Set("Content-Type", "application/pdf")
		w.Write(pdf)
	})
	mux.HandleFunc("/blocked", func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("<html><body>Access denied</body></html>"))
	})
	mux.HandleFunc("/partial", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte(page("en", "Hexagons", fixtureArticle)))
	})
	mux.HandleFunc("/image.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("\x89PNG"))
	})
	mux.HandleFunc("/empty.html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("<html><body></body></html>"))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, log
}

type headerLog struct {
	mu   sync.Mutex
	seen []http.Header
}

func (l *headerLog) add(r *http.Request) {
	l.mu.Lock()
	l.seen = append(l.seen, r.Header.Clone())
	l.mu.Unlock()
}

func TestFetchAndExtractEqualsFetchOneOverTheFixtureCorpus(t *testing.T) {
	srv, _ := fixtureOrigin(t)
	client := originClient(srv)
	for _, path := range []string{"/plain.html", "/german.html", "/moved", "/doc.pdf"} {
		url := "http://pages.example.com" + path
		want, err := crawler.FetchOne(context.Background(), client, "reference/1.0", url, 5<<20)
		if err != nil {
			t.Fatalf("%s: FetchOne: %v", path, err)
		}
		got, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), url, 5<<20)
		if err != nil {
			t.Fatalf("%s: fetchAndExtract: %v", path, err)
		}
		if got.Text != want.Text || got.Title != want.Title || got.Lang != want.Lang || got.FinalURL != want.URL {
			t.Errorf("%s: worker {%q %q %q %q}, server {%q %q %q %q}", path, got.Text, got.Title, got.Lang, got.FinalURL, want.Text, want.Title, want.Lang, want.URL)
		}
		sum, ok := crawler.Simhash(want.Text)
		if !ok || got.Simhash != crawler.SimhashHex(sum) || got.ContentHash != crawler.ContentHash(want.Text) {
			t.Errorf("%s: hashes differ: %s %s", path, got.ContentHash, got.Simhash)
		}
		if got.Text == "" || len(got.ContentHash) != 64 || len(got.Simhash) != 16 {
			t.Errorf("%s: empty or malformed result %+v", path, got)
		}
	}
}

func TestFetchAndExtractDropsBoilerplateAndKeepsLanguageAndTheRedirectTarget(t *testing.T) {
	srv, _ := fixtureOrigin(t)
	client := originClient(srv)
	moved, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), "http://pages.example.com/moved", 0)
	if err != nil {
		t.Fatal(err)
	}
	if moved.FinalURL != "http://pages.example.com/plain.html" || moved.Lang != "en" || !strings.Contains(moved.Text, "tessellation") {
		t.Fatalf("redirect result: %+v", moved)
	}
	for _, junk := range []string{"Copyright 2026", "Sign in", "tracking", "ten tips"} {
		if strings.Contains(moved.Text, junk) {
			t.Errorf("boilerplate %q survived: %q", junk, moved.Text)
		}
	}
	de, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), "http://pages.example.com/german.html", 0)
	if err != nil || de.Lang != "de-AT" {
		t.Fatalf("lang: %+v %v", de, err)
	}
	pdf, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), "http://pages.example.com/doc.pdf", 0)
	if err != nil || !strings.Contains(pdf.Text, "tessellation") {
		t.Fatalf("pdf: %+v %v", pdf, err)
	}
}

func TestFetchAndExtractSendsTheBrowserHeaderSet(t *testing.T) {
	srv, log := fixtureOrigin(t)
	if _, err := fetchAndExtract(context.Background(), originClient(srv), crawler.BrowserHeaders(), "http://pages.example.com/plain.html", 0); err != nil {
		t.Fatal(err)
	}
	if len(log.seen) != 1 {
		t.Fatalf("%d requests", len(log.seen))
	}
	for name, want := range crawler.BrowserHeaders() {
		if got := log.seen[0][name]; len(got) != 1 || got[0] != want[0] {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}
}

func TestFetchAndExtractReportsNon200AsAStatusAndNothingElseAsOne(t *testing.T) {
	srv, _ := fixtureOrigin(t)
	client := originClient(srv)
	for path, want := range map[string]int{"/blocked": 403, "/partial": 206, "/missing": 404} {
		_, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), "http://pages.example.com"+path, 0)
		var status *fetchStatusError
		if !errors.As(err, &status) || status.Status != want {
			t.Errorf("%s: got %v, want status %d", path, err, want)
		}
	}
	for _, path := range []string{"/image.png", "/empty.html"} {
		_, err := fetchAndExtract(context.Background(), client, crawler.BrowserHeaders(), "http://pages.example.com"+path, 0)
		var status *fetchStatusError
		if err == nil || errors.As(err, &status) {
			t.Errorf("%s: want a plain extraction error, got %v", path, err)
		}
	}
}
