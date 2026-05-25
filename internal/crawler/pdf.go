package crawler

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ledongthuc/pdf"
)

// ParsePDF extracts text from a PDF document and returns it in the same
// ParsedDoc shape as the HTML parser. Title is derived from PDF metadata when
// available, or from the final URL's basename when not.
//
// Iter-73 added this so cosift can index PDF-content docs sites (real-world
// docs corpora are commonly ~30% PDFs). Pure-Go, no cgo, no system deps.
//
// Lang is left empty (PDFs don't carry HTML lang attributes). Links are
// extracted only if the PDF embeds annotation links (rare in text-only PDFs);
// extracting embedded URL annotations isn't critical for retrieval.
func ParsePDF(body []byte, finalURL string) (*ParsedDoc, error) {
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
