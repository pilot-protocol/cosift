package crawler

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// jsonLDFields holds the retrieval-relevant fields extracted from JSON-LD
// (<script type="application/ld+json"> Schema.org payloads). All fields are
// best-effort optional — sites use JSON-LD with widely varying completeness.
type jsonLDFields struct {
	Name          string
	Headline      string
	Description   string
	Keywords      string
	AuthorName    string
	DatePublished string // ISO 8601 string; parsed to time.Time downstream
	DateModified  string // fallback when DatePublished absent
	Image         string // URL; first non-empty wins.
}

// merge combines two extractions, preferring non-empty values from src. Used
// when a page has multiple JSON-LD blocks (article + organization + breadcrumb)
// — we take the first non-empty value for each field.
func (f *jsonLDFields) merge(src jsonLDFields) {
	if f.Name == "" {
		f.Name = src.Name
	}
	if f.Headline == "" {
		f.Headline = src.Headline
	}
	if f.Description == "" {
		f.Description = src.Description
	}
	if f.Keywords == "" {
		f.Keywords = src.Keywords
	}
	if f.AuthorName == "" {
		f.AuthorName = src.AuthorName
	}
	if f.DatePublished == "" {
		f.DatePublished = src.DatePublished
	}
	if f.DateModified == "" {
		f.DateModified = src.DateModified
	}
	if f.Image == "" {
		f.Image = src.Image
	}
}

// extractJSONLD parses a single <script type="application/ld+json"> body and
// returns the extracted retrieval-relevant fields. Tolerates the three common
// JSON-LD shapes: single object, top-level array, @graph nesting.
//
// Strings are trimmed but otherwise unchanged. Returns zero-value on parse
// failure (malformed JSON or surprising structure) — JSON-LD is best-effort.
func extractJSONLD(raw []byte) jsonLDFields {
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return jsonLDFields{}
	}
	var out jsonLDFields
	walkJSONLDValue(v, &out)
	return out
}

// walkJSONLDValue recurses through the JSON-LD value space — maps, arrays, and
// the special @graph array — pulling fields out of any object that looks like
// an Article/WebPage/BlogPosting/Product/Thing. Schema.org has 600+ types; we
// don't gate on type, instead reading the named fields from any object we find.
func walkJSONLDValue(v interface{}, out *jsonLDFields) {
	switch x := v.(type) {
	case map[string]interface{}:
		// Pull the interesting fields if present.
		if s := jsonLDString(x, "name"); s != "" && out.Name == "" {
			out.Name = s
		}
		if s := jsonLDString(x, "headline"); s != "" && out.Headline == "" {
			out.Headline = s
		}
		if s := jsonLDString(x, "description"); s != "" && out.Description == "" {
			out.Description = s
		}
		if s := jsonLDString(x, "keywords"); s != "" && out.Keywords == "" {
			out.Keywords = s
		}
		if s := jsonLDString(x, "datePublished"); s != "" && out.DatePublished == "" {
			out.DatePublished = s
		}
		if s := jsonLDString(x, "dateModified"); s != "" && out.DateModified == "" {
			out.DateModified = s
		}
		// Author: can be a Person object with a name, or just a string. Try both.
		if out.AuthorName == "" {
			if au, ok := x["author"]; ok {
				switch a := au.(type) {
				case string:
					out.AuthorName = strings.TrimSpace(a)
				case map[string]interface{}:
					if n := jsonLDString(a, "name"); n != "" {
						out.AuthorName = n
					}
				}
			}
		}
		// Image: can be a string URL, an ImageObject with `url`, or
		// an array of either. Take the first non-empty URL string.
		if out.Image == "" {
			if im, ok := x["image"]; ok {
				out.Image = jsonLDImageURL(im)
			}
		}
		// Recurse into every value — @graph, nested objects, image/publisher/etc.
		// May surface useful fields from deeply-nested types.
		for _, val := range x {
			walkJSONLDValue(val, out)
		}
	case []interface{}:
		for _, val := range x {
			walkJSONLDValue(val, out)
		}
	}
}

// isFaviconRel reports whether a <link rel="..."> value designates a favicon.
// The rel attribute can carry multiple tokens (e.g., `rel="shortcut icon"`);
// any of the recognized tokens qualifies — "icon" alone matches the common
// "shortcut icon" pattern.
func isFaviconRel(rel string) bool {
	if rel == "" {
		return false
	}
	for _, tok := range strings.Fields(rel) {
		switch tok {
		case "icon", "apple-touch-icon", "apple-touch-icon-precomposed", "mask-icon":
			return true
		}
	}
	return false
}

// jsonLDImageURL extracts the first non-empty URL from Schema.org's polymorphic
// `image` field. Shapes seen in the wild:
//   - "https://x.com/img.jpg"             (string)
//   - {"@type":"ImageObject", "url":"..."} (object with url field)
//   - ["url1", "url2"]                    (array of strings)
//   - [{"url":"u1"}, "u2"]                (mixed array — take the first valid)
//
// Returns "" when no URL is found.
func jsonLDImageURL(v interface{}) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case map[string]interface{}:
		if u := jsonLDString(x, "url"); u != "" {
			return u
		}
		// Some sites use `contentUrl` instead of `url`.
		if u := jsonLDString(x, "contentUrl"); u != "" {
			return u
		}
	case []interface{}:
		for _, item := range x {
			if u := jsonLDImageURL(item); u != "" {
				return u
			}
		}
	}
	return ""
}

func jsonLDString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case []interface{}:
		// keywords sometimes shows up as an array of strings; join.
		parts := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				if t := strings.TrimSpace(s); t != "" {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, ", ")
	}
	return ""
}

// ParsedDoc is the output of HTML parsing.
type ParsedDoc struct {
	Title string
	Text  string
	Lang  string
	Links []string
	// PublishedAt is the document's publication date, extracted from JSON-LD
	// `datePublished` (preferred) or `dateModified` (fallback). Zero-value
	// means "unknown" — search filters skip docs with no publication signal
	// rather than treating them as 1970-01-01.
	PublishedAt time.Time
	// AuthorName extracted from JSON-LD `author.name` (preferred) or `author`
	// as a bare string. Empty when absent. (parser already extracted
	// it for the indexer prefix; exposes it on ParsedDoc).
	AuthorName string
	// Image URL. Preference: og:image > JSON-LD `image` (string or
	// ImageObject.url). Empty when absent. Used for SearchHit.image and
	// "doc-card" rendering.
	Image string
	// Favicon URL extracted from <link rel="icon">, <link rel="shortcut icon">,
	// or <link rel="apple-touch-icon">. Resolved against the page's base URL,
	// so relative hrefs (e.g., `/favicon.ico`) become absolute. Empty when no
	// <link rel="…icon…"> is present — callers can fall back to
	// `<scheme>://<host>/favicon.ico` themselves if their use case allows.
	Favicon string
}

// dropTags are dropped wholesale (nav, footer, etc.).
var dropTags = map[string]bool{
	"script":   true,
	"style":    true,
	"noscript": true,
	"iframe":   true,
	"svg":      true,
	"head":     false, // we want title from head
	"nav":      true,
	"footer":   true,
	"header":   true,
	"aside":    true,
	"form":     true,
	"button":   true,
}

// boilerplateClassTokens matches whole class/id tokens indicating non-content.
//
// Token-level match — NOT substring. Substring matching collided catastrophically
// with modern feature-flag class names: e.g. Wikipedia's `<html>` element carries
// a class like `vector-feature-main-menu-pinned-disabled` which contains "menu"
// as a hyphen-bounded sub-word, but the element itself is the document root, not
// a menu. Substring "menu" would drop the entire page tree.
//
// A class attribute is space-separated tokens; this set matches the whole token.
// `<div class="nav">` matches "nav"; `<div class="navigation">` matches
// "navigation"; `<div class="vector-feature-language-in-main-menu-disabled">`
// matches NOTHING (the token is the full hyphenated string).
var boilerplateClassTokens = map[string]bool{
	"nav":           true,
	"navbar":        true,
	"navigation":    true,
	"menu":          true,
	"sidebar":       true,
	"side-bar":      true,
	"footer":        true,
	"site-footer":   true,
	"header":        true,
	"site-header":   true,
	"page-header":   true,
	"banner":        true,
	"cookie-banner": true,
	"cookie-notice": true,
	"ad":            true,
	"ads":           true,
	"advert":        true,
	"advertisement": true,
	"promo":         true,
	"comment":       true,
	"comments":      true,
	"share":         true,
	"social":        true,
	"social-share":  true,
	"breadcrumb":    true,
	"breadcrumbs":   true,
	"newsletter":    true,
	"subscribe":     true,
}

// Parse extracts main content from an HTML byte slice.
//
// Heuristic: drop known-boilerplate tags + class/id substrings, then collapse
// remaining text. Not perfect like Trafilatura but stdlib-only and good enough
// for v0. Targets a recall-leaning extractor — we'd rather keep a noisy paragraph
// than drop a real one.
//
// added meta-tag extraction: OpenGraph (og:title, og:description) and
// Twitter Card + standard <meta name="description"> are read alongside <title>.
// The richest available title is preferred; description is prepended to the
// extracted text so the indexer gets the keyword signal.
func Parse(body []byte, base string) (*ParsedDoc, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	baseURL, _ := url.Parse(base)

	out := &ParsedDoc{}
	var sb strings.Builder
	var links []string
	seenLinks := make(map[string]struct{})

	// Meta-tag candidates by priority order. Each lookup falls through to the
	// next on empty. og: > twitter: > standard <meta name="description">.
	var ogTitle, twitterTitle string
	var ogDesc, twitterDesc, metaDesc string
	var ogImage string //

	// JSON-LD aggregate. Multiple <script type="application/ld+json">
	// blocks can appear on one page (article + organization + breadcrumb is the
	// classic trio); merge fills the first non-empty value for each field.
	var ld jsonLDFields

	var walk func(n *html.Node, inBody bool)
	walk = func(n *html.Node, inBody bool) {
		if n.Type == html.ElementNode {
			name := strings.ToLower(n.Data)

			if name == "html" {
				for _, a := range n.Attr {
					if strings.EqualFold(a.Key, "lang") {
						out.Lang = a.Val
					}
				}
			}
			if name == "title" && out.Title == "" {
				out.Title = strings.TrimSpace(nodeText(n))
				return
			}
			// <link rel="icon|shortcut icon|apple-touch-icon" href="...">.
			// First non-empty wins; preference order is page-order (whatever the
			// site lists first). Resolved against baseURL so /favicon.ico becomes
			// absolute. <link> is a leaf — no children to walk.
			if name == "link" && out.Favicon == "" {
				var rel, href string
				for _, a := range n.Attr {
					switch strings.ToLower(a.Key) {
					case "rel":
						rel = strings.ToLower(strings.TrimSpace(a.Val))
					case "href":
						href = strings.TrimSpace(a.Val)
					}
				}
				if href != "" && isFaviconRel(rel) {
					if u := resolveHref(baseURL, href); u != "" {
						out.Favicon = u
					}
				}
				return
			}
			if name == "meta" {
				// Read property/name + content. Don't return — meta is leaf, no children to walk.
				var prop, nm, content string
				for _, a := range n.Attr {
					switch strings.ToLower(a.Key) {
					case "property":
						prop = strings.ToLower(a.Val)
					case "name":
						nm = strings.ToLower(a.Val)
					case "content":
						content = strings.TrimSpace(a.Val)
					}
				}
				if content == "" {
					return
				}
				switch {
				case prop == "og:title" && ogTitle == "":
					ogTitle = content
				case prop == "og:description" && ogDesc == "":
					ogDesc = content
				case prop == "og:image" && ogImage == "":
					ogImage = content //
				case nm == "twitter:title" && twitterTitle == "":
					twitterTitle = content
				case nm == "twitter:description" && twitterDesc == "":
					twitterDesc = content
				case nm == "twitter:image" && ogImage == "":
					ogImage = content // fallback when og:image absent
				case nm == "description" && metaDesc == "":
					metaDesc = content
				}
				return
			}
			if name == "body" {
				inBody = true
			}
			// JSON-LD blocks are <script type="application/ld+json">.
			// We must intercept BEFORE the dropTags["script"] = true check below.
			if name == "script" {
				isLD := false
				for _, a := range n.Attr {
					if strings.EqualFold(a.Key, "type") && strings.Contains(strings.ToLower(a.Val), "ld+json") {
						isLD = true
						break
					}
				}
				if isLD {
					body := strings.TrimSpace(nodeText(n))
					if body != "" {
						ld.merge(extractJSONLD([]byte(body)))
					}
				}
				return // either way, don't descend into the script body as text
			}
			if dropTags[name] && name != "head" {
				return
			}
			if hasBoilerplateAttr(n) {
				return
			}
			if name == "a" && inBody {
				for _, a := range n.Attr {
					if strings.EqualFold(a.Key, "href") {
						if u := resolveHref(baseURL, a.Val); u != "" {
							if _, ok := seenLinks[u]; !ok {
								seenLinks[u] = struct{}{}
								links = append(links, u)
							}
						}
					}
				}
			}
		}
		if n.Type == html.TextNode && inBody {
			t := strings.TrimSpace(n.Data)
			if t != "" {
				sb.WriteString(t)
				sb.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, inBody)
		}
	}
	walk(doc, false)

	// Title preference: og:title → jsonld.headline → twitter:title →
	// jsonld.name → bare <title>. OG is specifically the social-sharing title
	// (typically cleanest). JSON-LD headline is the article-specific title;
	// jsonld.name can sometimes be the site name rather than the article, so
	// it sits behind twitter:title. Bare <title> is the recall-leaning floor.
	switch {
	case ogTitle != "":
		out.Title = ogTitle
	case ld.Headline != "":
		out.Title = ld.Headline
	case twitterTitle != "":
		out.Title = twitterTitle
	case ld.Name != "" && out.Title == "":
		out.Title = ld.Name
	}

	// Description: pick richest available. Prepend to extracted text so the
	// indexer gets the keyword signal — description fields are author-curated
	// summaries and often carry terms not appearing in body text (e.g. a
	// product name in og:description but only "this product" in body).
	desc := ogDesc
	if desc == "" {
		desc = ld.Description
	}
	if desc == "" {
		desc = twitterDesc
	}
	if desc == "" {
		desc = metaDesc
	}

	// Build the prefix of extra text signals to prepend to body. prepended
	// just description; also prepends JSON-LD keywords and author name
	// when present. These are author-curated and surface entities not in body text.
	var prefix strings.Builder
	if desc != "" {
		prefix.WriteString(desc)
	}
	if ld.Keywords != "" {
		if prefix.Len() > 0 {
			prefix.WriteByte(' ')
		}
		prefix.WriteString(ld.Keywords)
	}
	if ld.AuthorName != "" {
		if prefix.Len() > 0 {
			prefix.WriteByte(' ')
		}
		prefix.WriteString(ld.AuthorName)
	}

	text := collapseWhitespace(sb.String())
	if prefix.Len() > 0 {
		text = prefix.String() + " " + text
	}
	out.Text = text
	out.Links = links

	// Parse publication date. Prefer datePublished; fall back to dateModified.
	// JSON-LD dates are typically ISO 8601 (RFC3339 or just YYYY-MM-DD); tolerate both.
	if t := parseJSONLDTime(ld.DatePublished); !t.IsZero() {
		out.PublishedAt = t
	} else if t := parseJSONLDTime(ld.DateModified); !t.IsZero() {
		out.PublishedAt = t
	}
	// surface the JSON-LD author.name on ParsedDoc so callers can
	// persist it. Parser already extracted into `ld.AuthorName` for its text
	// prefix at the indexer; that path is unchanged.
	out.AuthorName = ld.AuthorName
	// og:image > twitter:image (collected into the
	// same `ogImage` var above so we don't need a second priority list) >
	// JSON-LD `image`. Empty when none extracted.
	if ogImage != "" {
		out.Image = ogImage
	} else {
		out.Image = ld.Image
	}
	return out, nil
}

// parseJSONLDTime tolerates the two common formats sites use:
//   - RFC3339 full timestamp ("2024-03-15T14:32:00Z" or with offset)
//   - Date-only ("2024-03-15")
//
// Returns zero time on parse failure.
func parseJSONLDTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	// Try RFC3339 first (most complete).
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	// Date-only.
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t
	}
	return time.Time{}
}

func nodeText(n *html.Node) string {
	var sb strings.Builder
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.TextNode {
			sb.WriteString(c.Data)
		} else {
			sb.WriteString(nodeText(c))
		}
	}
	return sb.String()
}

func hasBoilerplateAttr(n *html.Node) bool {
	for _, a := range n.Attr {
		k := strings.ToLower(a.Key)
		if k != "class" && k != "id" && k != "role" {
			continue
		}
		v := strings.ToLower(strings.TrimSpace(a.Val))
		if v == "" {
			continue
		}
		// class is space-separated tokens; id is a single token; role is a
		// single token. Splitting on whitespace handles all three uniformly.
		for _, tok := range strings.Fields(v) {
			if boilerplateClassTokens[tok] {
				return true
			}
		}
		if k == "role" && (v == "navigation" || v == "banner" || v == "complementary") {
			return true
		}
	}
	return false
}

func resolveHref(base *url.URL, href string) string {
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	if base != nil {
		u = base.ResolveReference(u)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	u.Fragment = ""
	return u.String()
}

func collapseWhitespace(s string) string {
	var sb strings.Builder
	prevSpace := true
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				sb.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		sb.WriteRune(r)
		prevSpace = false
	}
	return strings.TrimSpace(sb.String())
}
