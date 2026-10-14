package wiki

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	mdType   = "text/markdown; charset=utf-8"
	textType = "text/plain; charset=utf-8"

	licenceURL  = "https://creativecommons.org/licenses/by-sa/4.0/"
	licenceLine = "This article's text is available under the Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0): <" + licenceURL + ">"

	maxMdIndent = 3
)

// asciiPunct is any ASCII punctuation or symbol: CommonMark lets a backslash escape each of them, and an escaped one always renders as itself.
func asciiPunct(r rune) bool {
	return r >= '!' && r <= '/' || r >= ':' && r <= '@' || r >= '[' && r <= '`' || r >= '{' && r <= '~'
}

func foldCR(s string) string { return strings.ReplaceAll(s, "\r", " ") }

// mdText is one text run: line breaks and whitespace runs fold to single spaces, controls drop out, and every ASCII punctuation character is backslash-escaped.
func mdText(s string) string {
	var b strings.Builder
	for _, r := range strings.Join(strings.Fields(clean(foldCR(s))), " ") {
		if asciiPunct(r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// mdSource is mdText for a run of article Markdown: a backslash escape in the source stands for its character, as on the page.
func mdSource(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && isPunct(s[i+1]) {
			i++
		}
		b.WriteByte(s[i])
	}
	return mdText(b.String())
}

// mdLine is one line of a paragraph: its indentation kept up to three spaces, the rest as mdSource; empty when nothing is left.
func mdLine(line string) string {
	n, rest := indentOf(line)
	text := mdSource(rest)
	if text == "" {
		return ""
	}
	return strings.Repeat(" ", min(n, maxMdIndent)) + text
}

// bullet reads a list marker at the start of a line: the item text, the number of an ordered marker ("" for a bullet), and whether there was a marker.
func bullet(line string) (text, num string, ok bool) {
	_, rest := indentOf(line)
	i := 0
	switch {
	case rest == "":
		return "", "", false
	case strings.IndexByte("-*+", rest[0]) >= 0:
		i = 1
	default:
		for i < len(rest) && i < 9 && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i == 0 || i == len(rest) || (rest[i] != '.' && rest[i] != ')') {
			return "", "", false
		}
		num = rest[:i]
		i++
	}
	if i < len(rest) && rest[i] != ' ' && rest[i] != '\t' {
		return "", "", false
	}
	return rest[i:], num, true
}

// mdBody returns the article text as Markdown the code builds itself: ATX headings normalised as on the page, "- " items and paragraphs, every text run escaped. Fences, quotes, tables, setext rules and code spans become plain text.
func mdBody(text string) string {
	var out []string
	open, closer := "", ""
	sawH2 := false
	brk := func() {
		if len(out) > 0 && out[len(out)-1] != "" {
			out = append(out, "")
		}
		open = ""
	}
	para := func(line string) {
		s := mdLine(line)
		if s == "" {
			brk()
			return
		}
		if open == "" {
			open = "p"
		}
		out = append(out, s)
	}
	for _, line := range strings.Split(clean(strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(text)), "\n") {
		switch {
		case closer != "":
			if closesFence(line, closer) {
				closer = ""
				brk()
				continue
			}
			para(line)
		case fence(line) != "":
			closer = fence(line)
			brk()
		case blank(line):
			brk()
		default:
			if level, t := heading(line); level > 0 {
				brk()
				if t != "" {
					mark := "###"
					if level <= 2 || !sawH2 {
						mark, sawH2 = "##", true
					}
					out = append(out, mark+" "+mdSource(t), "")
				}
				continue
			}
			if t, num, ok := bullet(line); ok && (open != "p" || num == "" || num == "1") {
				if open != "i" {
					brk()
				}
				if item := mdSource(t); item != "" {
					out = append(out, "- "+item)
					open = "i"
				} else {
					brk()
				}
				continue
			}
			para(line)
		}
	}
	return strings.TrimRight(strings.Join(out, "\n"), "\n")
}

// autolink is "<url>" for a URL worth linking: absolute http or https with a host, and only characters that RFC 3986 allows.
func autolink(raw string) (string, bool) {
	if !linkable(raw) || strings.ContainsAny(raw, "\"<>\\^`{|}") {
		return "", false
	}
	if u, err := url.Parse(raw); err != nil || u.Hostname() == "" {
		return "", false
	}
	return "<" + raw + ">", true
}

// renderMarkdown is the article as a .md page: title, status line, text, sources and the licence line.
func (w *Wiki) renderMarkdown(rec *engineRecord, contributors []string) []byte {
	var b strings.Builder
	b.WriteString("# " + mdText(rec.Title) + "\n\nAgent submitted")
	if len(contributors) > 0 {
		names := make([]string, len(contributors))
		for i, c := range contributors {
			names[i] = mdText(c)
		}
		b.WriteString(" · Contributed by " + strings.Join(names, ", "))
	}
	b.WriteString("\n\n" + mdBody(rec.Lead+"\n\n"+rec.BodyMD) + "\n")
	if len(rec.Citations) > 0 {
		b.WriteString("\n## Sources\n\n")
	}
	seen := map[string]bool{}
	for _, c := range rec.Citations {
		if seen[c.URL] {
			continue
		}
		seen[c.URL] = true
		title := c.Title
		if mdText(title) == "" {
			title = c.Host
		}
		b.WriteString("- " + mdText(title+" ("+c.Host+")"))
		if a, ok := autolink(c.URL); ok {
			b.WriteString(" " + a)
		}
		b.WriteString("\n")
		for _, d := range rec.Citations {
			if d.URL == c.URL {
				b.WriteString("  - " + mdText("["+strconv.Itoa(d.N)+"] "+cleanQuote(foldCR(d.Quote))) + "\n")
			}
		}
	}
	b.WriteString("\n" + licenceLine + "\n")
	if a, ok := autolink(w.canonical("/wiki/" + rec.Slug)); ok {
		b.WriteString("\nCanonical page: " + a + "\n")
	}
	return []byte(b.String())
}

// refuse answers with a short plain-text body for a Markdown request, or the page for an HTML one.
func (w *Wiki) refuse(rw http.ResponseWriter, status int, md bool) {
	if md {
		w.write(rw, status, textType, []byte(http.StatusText(status)+"\n"), -1)
		return
	}
	body := w.notFound
	switch status {
	case http.StatusServiceUnavailable:
		body = w.unavailable
	case http.StatusTooManyRequests:
		body = w.tooMany
	case http.StatusGone:
		body = w.gone
	}
	w.write(rw, status, htmlType, body, -1)
}
