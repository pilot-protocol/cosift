package wiki

import (
	"bytes"
	"embed"
	"encoding/xml"
	"html/template"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed assets/*
var assetFS embed.FS

var (
	articleTmpl = parse("article.html")
	stubTmpl    = parse("stub.html")
	hubTmpl     = parse("hub.html")
	noticeTmpl  = parse("notice.html")
)

func parse(page string) *template.Template {
	return template.Must(template.New("").ParseFS(templateFS, "templates/layout.html", "templates/"+page))
}

type head struct {
	Title, Description, Canonical, Prev, Next, GSC, GA string
	OG                                                 *openGraph
	JSONLD                                             *articleLD
}

type openGraph struct{ Type, Title, Description, URL string }

type ldOrg struct {
	Type string `json:"@type"`
	Name string `json:"name"`
}

// articleLD is JSON-LD Article; html/template JSON-encodes it in the ld+json block.
type articleLD struct {
	Context       string `json:"@context"`
	Type          string `json:"@type"`
	Headline      string `json:"headline"`
	Description   string `json:"description"`
	URL           string `json:"url"`
	MainEntity    string `json:"mainEntityOfPage"`
	DatePublished string `json:"datePublished"`
	DateModified  string `json:"dateModified"`
	InLanguage    string `json:"inLanguage"`
	Publisher     ldOrg  `json:"publisher"`
}

type quoteView struct {
	N     int
	Quote string
}

// sourceView is one Sources-list entry, its quotes in first-cited order.
type sourceView struct {
	Link, Title, Host string
	Quotes            []quoteView
}

type articleView struct {
	Head                          head
	Title, Vertical, VerticalSlug string
	Tier, Updated, UpdatedISO     string
	Body                          template.HTML
	Sources                       []sourceView
	ReportHref                    string
}

type stubView struct {
	Head                                 head
	Title, Vertical, Created, CreatedISO string
}

type hubGroup struct {
	Letter string
	Items  []listing
}

type hubView struct {
	Head        head
	Heading     string
	Groups      []hubGroup
	Prev, Next  string
	Page, Pages int
}

type noticeView struct {
	Head          head
	Heading, Text string
	Link          string
}

func execute(t *template.Template, v any) []byte {
	var b bytes.Buffer
	if err := t.ExecuteTemplate(&b, "layout", v); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (w *Wiki) head(title string) head {
	return head{Title: title + " · Cosift", GSC: w.cfg.GSCVerification, GA: w.cfg.GAMeasurementID}
}

func (w *Wiki) canonical(path string) string { return w.cfg.PublicURL + path }

func displayDate(raw string) (string, string) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "", ""
	}
	t = t.UTC()
	return t.Format("Jan 2, 2006"), t.Format(time.RFC3339)
}

// summary trims the lead for the meta description.
func summary(lead string) string {
	s := cleanLine(lead)
	if utf8.RuneCountInString(s) <= 160 {
		return s
	}
	cut := string([]rune(s)[:159])
	if i := strings.LastIndexByte(cut, ' '); i > 80 {
		cut = cut[:i]
	}
	return strings.TrimRight(cut, " ,;:") + "…"
}

// reportHref is the mailto link; + becomes %20, which mail clients decode.
func (w *Wiki) reportHref(canonical string) string {
	return "mailto:" + w.cfg.ReportMailto + "?subject=" + strings.ReplaceAll(url.QueryEscape("Report: "+canonical), "+", "%20")
}

func (w *Wiki) renderArticle(rec *engineRecord) []byte {
	title := cleanLine(rec.Title)
	canonical := w.canonical("/wiki/" + rec.Slug)
	desc := summary(rec.Lead)
	links := map[string]string{}
	var sources []sourceView
	bySource := map[string]int{}
	for _, c := range rec.Citations {
		if linkable(c.URL) {
			links[c.URL] = c.URL
		}
		i, ok := bySource[c.URL]
		if !ok {
			title, host := cleanLine(c.Title), cleanLine(c.Host)
			if title == "" {
				title = host
			}
			link := ""
			if linkable(c.URL) {
				link = c.URL
			}
			i = len(sources)
			bySource[c.URL] = i
			sources = append(sources, sourceView{Link: link, Title: title, Host: host})
		}
		sources[i].Quotes = append(sources[i].Quotes, quoteView{N: c.N, Quote: cleanQuote(c.Quote)})
	}
	updated, updatedISO := displayDate(rec.UpdatedAt)
	_, createdISO := displayDate(rec.CreatedAt)
	h := w.head(title)
	h.Description, h.Canonical = desc, canonical
	h.OG = &openGraph{Type: "article", Title: title, Description: desc, URL: canonical}
	h.JSONLD = &articleLD{Context: "https://schema.org", Type: "Article", Headline: title, Description: desc,
		URL: canonical, MainEntity: canonical, DatePublished: createdISO, DateModified: updatedISO,
		InLanguage: "en", Publisher: ldOrg{Type: "Organization", Name: "Cosift"}}
	return execute(articleTmpl, articleView{
		Head: h, Title: title, Vertical: verticals[rec.Vertical], VerticalSlug: rec.Vertical,
		Tier: tiers[rec.QualityTier], Updated: updated, UpdatedISO: updatedISO,
		Body:    renderBody(rec.Lead+"\n\n"+rec.BodyMD, links, len(rec.Citations)),
		Sources: sources, ReportHref: w.reportHref(canonical),
	})
}

// renderStub uses only the stub projection's title, vertical, slug and created date.
func (w *Wiki) renderStub(rec *engineRecord) []byte {
	title := cleanLine(rec.Title)
	created, createdISO := displayDate(rec.CreatedAt)
	h := w.head(title)
	h.Canonical = w.canonical("/wiki/" + rec.Slug)
	h.Description = "Cosift is researching this topic."
	return execute(stubTmpl, stubView{Head: h, Title: title, Vertical: verticals[rec.Vertical], Created: created, CreatedISO: createdISO})
}

func (w *Wiki) renderNotice(title, text, link string) []byte {
	return execute(noticeTmpl, noticeView{Head: w.head(title), Heading: title, Text: text, Link: link})
}

func groups(items []listing) []hubGroup {
	var out []hubGroup
	for _, it := range items {
		letter := strings.ToUpper(it.Slug[:1])
		if it.Slug[0] >= '0' && it.Slug[0] <= '9' {
			letter = "0–9"
		}
		if len(out) == 0 || out[len(out)-1].Letter != letter {
			out = append(out, hubGroup{Letter: letter})
		}
		out[len(out)-1].Items = append(out[len(out)-1].Items, it)
	}
	return out
}

// hubPath is page n of a hub rooted at base: base itself for page 1.
func hubPath(base string, n int) string {
	switch {
	case n == 1:
		return base
	case base == "/wiki":
		return "/wiki/index/" + strconv.Itoa(n)
	}
	return base + "/" + strconv.Itoa(n)
}

// renderHub renders page n of items, or nil when n is past the last page.
func (w *Wiki) renderHub(heading, base string, items []listing, n int) []byte {
	pages := max(1, (len(items)+w.hubPage-1)/w.hubPage)
	if n < 1 || n > pages {
		return nil
	}
	lo, hi := (n-1)*w.hubPage, min(n*w.hubPage, len(items))
	v := hubView{Heading: heading, Groups: groups(items[lo:hi]), Page: n, Pages: pages}
	title := heading
	if n > 1 {
		title += ", page " + strconv.Itoa(n)
	}
	v.Head = w.head(title)
	v.Head.Canonical = w.canonical(hubPath(base, n))
	v.Head.Description = "Cosift articles: " + heading + "."
	if n > 1 {
		v.Prev = hubPath(base, n-1)
		v.Head.Prev = w.canonical(v.Prev)
	}
	if n < pages {
		v.Next = hubPath(base, n+1)
		v.Head.Next = w.canonical(v.Next)
	}
	return execute(hubTmpl, v)
}

func xmlText(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (w *Wiki) sitemapFiles(s *snapshot) int {
	return max(1, (len(s.sitemap)+w.sitemapPage-1)/w.sitemapPage)
}

func (w *Wiki) renderSitemapIndex(s *snapshot) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	for n := 1; n <= w.sitemapFiles(s); n++ {
		b.WriteString("<sitemap><loc>" + xmlText(w.canonical("/sitemaps/wiki-"+strconv.Itoa(n)+".xml")) + "</loc>")
		if lo := (n - 1) * w.sitemapPage; lo < len(s.sitemap) {
			b.WriteString("<lastmod>" + xmlText(s.sitemap[lo].UpdatedRaw) + "</lastmod>")
		}
		b.WriteString("</sitemap>\n")
	}
	b.WriteString("</sitemapindex>\n")
	return []byte(b.String())
}

// renderSitemap renders file n, or nil when n is past the last file.
func (w *Wiki) renderSitemap(s *snapshot, n int) []byte {
	if n < 1 || n > w.sitemapFiles(s) {
		return nil
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	lo, hi := (n-1)*w.sitemapPage, min(n*w.sitemapPage, len(s.sitemap))
	for _, it := range s.sitemap[lo:hi] {
		b.WriteString("<url><loc>" + xmlText(w.canonical("/wiki/"+it.Slug)) + "</loc><lastmod>" + xmlText(it.UpdatedRaw) + "</lastmod></url>\n")
	}
	b.WriteString("</urlset>\n")
	return []byte(b.String())
}

func (w *Wiki) renderRobots() []byte {
	return []byte("User-agent: *\nAllow: /\nDisallow: /api/\nSitemap: " + w.canonical("/sitemap.xml") + "\n")
}
