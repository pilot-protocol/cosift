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
// The file is committed; generated once by the NOTES procedure (see
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

// TestCrawlerHandlesPDFContentType verifies the dispatch wiring:
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

// TestParsePDFNeverPanics — The ledongthuc/pdf library uses panic()
// for non-local error flow on malformed PDFs. A single malformed PDF in the
// crawl frontier was killing the entire `cosift crawl` process mid-run after
// indexing ~2200 docs. The fix wraps ParsePDF in defer/recover; this test
// locks in the invariant that no input — random bytes, truncated headers,
// garbage that looks like a PDF header, even an HTML response misidentified
// as PDF — can ever propagate a panic out of ParsePDF.
func TestParsePDFNeverPanics(t *testing.T) {
	cases := map[string][]byte{
		"empty":             nil,
		"random_bytes":      []byte{0x00, 0x01, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF},
		"pdf_header_only":   []byte("%PDF-1.4\n"),
		"truncated_pdf":     []byte("%PDF-1.4\n1 0 obj\n<</Type /Catalog>>\nendobj"),
		"missing_endobj":    []byte("%PDF-1.4\n1 0 obj\n<</Type /Catalog>>\n%%EOF"),
		"html_as_pdf":       []byte("<!doctype html><html><body>not a pdf</body></html>"),
		"junk_after_header": []byte("%PDF-1.4\n" + strings.Repeat("xyz\n", 100)),
	}
	for name, body := range cases {
		_, err := ParsePDF(body, "https://example.com/"+name+".pdf")
		// We don't care WHAT the error is — only that we got one back, NOT a
		// panic. Pre-fix, several of these cases would panic and crash.
		if err == nil {
			t.Errorf("%s: expected an error (or nil parsed doc); panic-free is what we're locking in", name)
		}
	}
}
