package wiki

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"html/template"
	"net/http"
	"strings"
)

// StyleVersion and StyleSHA256 pin the canonical bytes of style.md; a change to the text bumps the version.
const (
	StyleVersion = 1
	StyleSHA256  = "7043a389e85487cbd6c2798fcc5bfce3cd804131d7d0f4304da81089291d6d27"

	docMaxAge = 3600
)

//go:embed style.md
var styleMD []byte

var docTmpl = parse("doc.html")

type docView struct {
	Head        head
	Title, Meta string
	Body        template.HTML
}

func styleBytesOK() bool {
	sum := sha256.Sum256(styleMD)
	return hex.EncodeToString(sum[:]) == StyleSHA256
}

// renderStyle shows style.md as a page: its first line is the title, the rest goes through the article renderer.
func (w *Wiki) renderStyle() []byte {
	title, rest, _ := strings.Cut(string(styleMD), "\n")
	title = cleanLine(strings.TrimPrefix(title, "# "))
	h := w.head(title)
	h.Canonical = w.canonical("/wiki/style")
	h.Description = "How Cosift articles are written and checked."
	return execute(docTmpl, docView{Head: h, Title: title, Body: renderBody(rest, nil, 0)})
}

func (w *Wiki) serveStyle(rw http.ResponseWriter, r *http.Request, md bool) {
	if md {
		w.write(rw, http.StatusOK, mdType, styleMD, docMaxAge)
		return
	}
	w.write(rw, http.StatusOK, htmlType, w.style, docMaxAge)
}
