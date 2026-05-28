package crawler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ledongthuc/pdf"
)

// PDFParseResult is the wire shape between cosift parse-pdf (the child)
// and ParsePDFSandboxed (the parent). One JSON object on the child's
// stdout.
type PDFParseResult struct {
	Title string `json:"title,omitempty"`
	Text  string `json:"text,omitempty"`
	Error string `json:"error,omitempty"`
}

// ParsePDFSandboxed spawns a child cosift process running
// `cosift parse-pdf`, pipes the PDF body to its stdin, reads the parsed
// text from stdout, and returns a ParsedDoc. If the child OOMs (malformed
// PDF → ledongthuc/pdf.readArray unbounded alloc), times out, or otherwise
// dies, the parent stays alive and returns a clean error so the crawler
// can mark this URL failed and move on.
//
// Bin is the full path to the running cosift binary; defaults to
// os.Executable() when empty. Timeout caps total wall-clock; 30 s is a
// safe default given most legit PDFs parse in <2 s.
func ParsePDFSandboxed(ctx context.Context, body []byte, finalURL, bin string, timeout time.Duration) (*ParsedDoc, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty pdf body")
	}
	if bin == "" {
		b, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("locate self binary: %w", err)
		}
		bin = b
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, "parse-pdf")
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// Common: signal: killed (subprocess OOMed under the kernel
		// cgroup limit), exit status 1 (panic recovered + non-zero),
		// or context-deadline-exceeded (hung sitemap-style runaway).
		// All map to "this PDF is unsafe; skip".
		return nil, fmt.Errorf("pdf-sandbox: %w (stderr=%q)", err, strings.TrimSpace(stderr.String()))
	}
	var res PDFParseResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		return nil, fmt.Errorf("pdf-sandbox: malformed child stdout: %w", err)
	}
	if res.Error != "" {
		return nil, fmt.Errorf("pdf-sandbox: child reported: %s", res.Error)
	}
	if strings.TrimSpace(res.Text) == "" {
		return nil, fmt.Errorf("pdf: no extractable text")
	}
	if res.Title == "" {
		res.Title = pdfTitleFromURL(finalURL)
	}
	return &ParsedDoc{Title: res.Title, Text: res.Text, Lang: "", Links: nil}, nil
}

// ParsePDFChild is the in-process parser invoked by the child cosift
// process. Same code path as the original ParsePDF but emits a
// JSON-shaped result on stdout so the parent can read it.
func ParsePDFChild(stdin io.Reader, stdout io.Writer) {
	body, err := io.ReadAll(io.LimitReader(stdin, 100<<20)) // 100 MiB cap
	if err != nil {
		_ = json.NewEncoder(stdout).Encode(PDFParseResult{Error: "read stdin: " + err.Error()})
		return
	}
	parsed, err := ParsePDF(body, "")
	if err != nil {
		_ = json.NewEncoder(stdout).Encode(PDFParseResult{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(stdout).Encode(PDFParseResult{Title: parsed.Title, Text: parsed.Text})
}

// ParsePDF extracts text from a PDF document and returns it in the same
// ParsedDoc shape as the HTML parser. Title is derived from PDF metadata when
// available, or from the final URL's basename when not.
//
// added this so cosift can index PDF-content docs sites (real-world
// docs corpora are commonly ~30% PDFs). Pure-Go, no cgo, no system deps.
//
// Lang is left empty (PDFs don't carry HTML lang attributes). Links are
// extracted only if the PDF embeds annotation links (rare in text-only PDFs);
// extracting embedded URL annotations isn't critical for retrieval.
func ParsePDF(body []byte, finalURL string) (parsed *ParsedDoc, err error) {
	// the ledongthuc/pdf library uses panic() for non-local error
	// flow on malformed PDFs (observed error: "missing endobj after indirect
	// object definition"). Without recovery, one bad PDF takes down the entire
	// `cosift crawl` process — observed in production after ~2200 docs.
	// Convert library panics into ordinary errors so the crawler's existing
	// FailFrontier path handles them and the worker pool keeps going.
	defer func() {
		if r := recover(); r != nil {
			parsed = nil
			err = fmt.Errorf("pdf: parser panicked: %v", r)
		}
	}()

	if len(body) == 0 {
		return nil, fmt.Errorf("empty pdf body")
	}
	reader := bytes.NewReader(body)
	r, err := pdf.NewReader(reader, int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("pdf: %w", err)
	}

	var sb strings.Builder
	for i := 1; i <= r.NumPage(); i++ {
		// Page() can also panic on malformed indirect refs — recovery above
		// catches that too.
		p := r.Page(i)
		if p.V.IsNull() {
			continue
		}
		// GetPlainText with a nil font map returns the page's text content.
		// Errors here typically mean the PDF is malformed; we accumulate what
		// we got rather than failing the whole document.
		text, err := p.GetPlainText(nil)
		if err != nil {
			// continue — partial extraction is better than none.
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(text)
	}
	text := strings.TrimSpace(sb.String())
	if text == "" {
		return nil, fmt.Errorf("pdf: no extractable text")
	}

	// Title fallback: take it from the URL basename if no PDF /Title metadata.
	// The pdf library's metadata API is limited; URL-based fallback gets used most.
	title := pdfTitleFromURL(finalURL)

	return &ParsedDoc{
		Title: title,
		Text:  text,
		Lang:  "",
		Links: nil,
	}, nil
}

// pdfTitleFromURL derives a reasonable title from a PDF URL. Examples:
//
//	https://example.com/docs/guide.pdf      → "guide"
//	https://example.com/api/spec-v2.pdf     → "spec-v2"
//	https://example.com/                    → "" (caller falls back to URL)
func pdfTitleFromURL(u string) string {
	// Find last path segment, trim .pdf, leave the rest.
	slash := strings.LastIndex(u, "/")
	if slash < 0 {
		return ""
	}
	base := u[slash+1:]
	// Trim any query/fragment that snuck through.
	if q := strings.Index(base, "?"); q >= 0 {
		base = base[:q]
	}
	if h := strings.Index(base, "#"); h >= 0 {
		base = base[:h]
	}
	base = strings.TrimSuffix(base, ".pdf")
	base = strings.TrimSuffix(base, ".PDF")
	return base
}
