package wiki

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"html/template"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// StyleVersion and StyleSHA256 pin the canonical bytes of style.md; a change to the text bumps the version.
const (
	StyleVersion = 1
	StyleSHA256  = "7043a389e85487cbd6c2798fcc5bfce3cd804131d7d0f4304da81089291d6d27"

	TermsVersion = 1

	docMaxAge = 300
)

// TermsSectionIDs are the required sections of the contributor terms, in page order.
var TermsSectionIDs = []string{"license", "originality", "credits", "worker-fetching", "takedown", "synth-attribution"}

//go:embed style.md
var styleMD []byte

//go:embed terms.md
var termsMD []byte

var (
	docTmpl      = parse("doc.html")
	termsTmpl    = parse("terms.html")
	termsHeading = regexp.MustCompile(`^## (.+) \{#([a-z][a-z-]*)\}$`)
)

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

type termsSection struct {
	ID, Heading string
	Body        template.HTML
	Contact     string
	Address     string
}

type termsView struct {
	Head     head
	Title    string
	Version  int
	Intro    template.HTML
	Sections []termsSection
}

// parseTerms splits terms.md into its title, introduction and sections; a section starts at a "## Heading {#id}" line.
func parseTerms(src []byte) (title, intro string, sections []termsSection, err error) {
	var introB strings.Builder
	var bodies []*strings.Builder
	for i, line := range strings.Split(string(src), "\n") {
		if i == 0 {
			title = cleanLine(strings.TrimPrefix(line, "# "))
			continue
		}
		if m := termsHeading.FindStringSubmatch(line); m != nil {
			sections = append(sections, termsSection{ID: m[2], Heading: cleanLine(m[1])})
			bodies = append(bodies, &strings.Builder{})
			continue
		}
		if len(bodies) == 0 {
			introB.WriteString(line + "\n")
		} else {
			bodies[len(bodies)-1].WriteString(line + "\n")
		}
	}
	ids := make([]string, len(sections))
	for i, sec := range sections {
		ids[i] = sec.ID
		sections[i].Body = renderBody(bodies[i].String(), nil, 0)
		if strings.TrimSpace(bodies[i].String()) == "" {
			return "", "", nil, errors.New("wiki: a section of the terms is empty")
		}
	}
	if title == "" || !slices.Equal(ids, TermsSectionIDs) {
		return "", "", nil, errors.New("wiki: the terms do not carry the required sections")
	}
	return title, introB.String(), sections, nil
}

func (w *Wiki) renderTerms() ([]byte, error) {
	title, intro, sections, err := parseTerms(termsMD)
	if err != nil {
		return nil, err
	}
	for i := range sections {
		if sections[i].ID == "takedown" {
			sections[i].Contact, sections[i].Address = "mailto:"+w.cfg.ReportMailto, w.cfg.ReportMailto
		}
	}
	h := w.head(title)
	h.Canonical = w.canonical("/wiki/terms")
	h.Description = "The terms every Cosift contributor accepts."
	return execute(termsTmpl, termsView{Head: h, Title: title, Version: TermsVersion, Intro: renderBody(intro, nil, 0), Sections: sections}), nil
}

func (w *Wiki) serveTerms(rw http.ResponseWriter, r *http.Request) {
	w.write(rw, http.StatusOK, htmlType, w.terms, docMaxAge)
}
