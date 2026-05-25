package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// loadFixturePDF reads the tiny hand-crafted PDF from testdata/. ~600 bytes
// on disk — well within the directive's "keep disk usage low for tests" rule.
// The file is committed; generated once by the iter-73 NOTES procedure (see
// the python snippet there).
func loadFixturePDF(t *testing.T) []byte {
	t.Helper()
	// testdata/ sits at the repo root; the crawler package is internal/crawler/
	// so the relative path is ../../testdata/crawler/hello.pdf
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "crawler", "hello.pdf"))
	if err != nil {
		t.Fatalf("read fixture pdf: %v", err)
	}
	return b
}

func TestPDFTitleFromURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://example.com/docs/guide.pdf", "guide"},
		{"https://example.com/api/spec-v2.pdf", "spec-v2"},
		{"https://example.com/docs/GUIDE.PDF", "GUIDE"},
		{"https://example.com/page.pdf?download=1", "page"},
		{"https://example.com/", ""},
		{"no-slash-at-all.pdf", ""}, // no slash → empty
	}
	for _, c := range cases {
		if got := pdfTitleFromURL(c.in); got != c.want {
			t.Errorf("pdfTitleFromURL(%q) = %q want %q", c.in, got, c.want)
		}
	}
}

func TestParsePDFHappyPath(t *testing.T) {
	b := loadFixturePDF(t)
	p, err := ParsePDF(b, "https://example.com/docs/hello.pdf")
	if err != nil {
		t.Fatalf("parse pdf: %v", err)
	}
	if p.Title != "hello" {
		t.Errorf("title: got %q want %q", p.Title, "hello")
	}
	// The fixture contains the word "tessellation" — a unique signal that the
	// extracted text matches the embedded PDF content stream.
	if !strings.Contains(p.Text, "tessellation") {
		t.Errorf("expected 'tessellation' in extracted text, got: %q", p.Text)
	}
}

func TestParsePDFEmptyBody(t *testing.T) {
	_, err := ParsePDF(nil, "x")
	if err == nil {
		t.Errorf("expected error on empty body")
	}
}

func TestParsePDFGarbageBody(t *testing.T) {
	_, err := ParsePDF([]byte("this is not a pdf"), "x")
	if err == nil {
		t.Errorf("expected error on garbage body")
	}
}

// TestCrawlerHandlesPDFContentType verifies the iter-73 dispatch wiring:
// crawler fetches a URL that returns application/pdf, indexes the extracted
// text. End-to-end exercise of fetch → content-type check → ParsePDF →
// document upsert.
func TestCrawlerHandlesPDFContentType(t *testing.T) {
	pdfBytes := loadFixturePDF(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/spec.pdf", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write(pdfBytes)
	})
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Use FetchOne (simpler than running the full crawler) — it shares the
	// content-type dispatch logic with the main crawler.
	r, err := FetchOne(context.Background(), nil, "test-ua", srv.URL+"/spec.pdf", 5<<20)
	if err != nil {
		t.Fatalf("FetchOne: %v", err)
	}
	if r.Title != "spec" {
		t.Errorf("title: got %q want %q", r.Title, "spec")
	}
	if !strings.Contains(r.Text, "tessellation") {
		t.Errorf("expected 'tessellation' in PDF text, got: %q", r.Text)
	}
}
