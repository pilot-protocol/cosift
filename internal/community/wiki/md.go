package wiki

import (
	"net/http"
	"strconv"
	"strings"
)

const (
	mdType   = "text/markdown; charset=utf-8"
	textType = "text/plain; charset=utf-8"

	licenceURL  = "https://creativecommons.org/licenses/by-sa/4.0/"
	licenceLine = "This article's text is available under the [Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0)](" + licenceURL + ")."
)

// escapeInline backslash-escapes the characters that would start markup in a single line of text.
func escapeInline(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune("\\`*_[]<>|~", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// mdLine neutralises one body line outside a fence: raw HTML, inline links and the colon of a reference definition become text; escapes are kept, and so are code spans unless a backtick run of an earlier line of the paragraph is still open (loose). It reports whether a run stayed open.
func mdLine(line string, loose bool) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(line); {
		c := line[i]
		switch {
		case c == '\\' && i+1 < len(line) && isPunct(line[i+1]):
			b.WriteString(line[i : i+2])
			i += 2
		case c == '`':
			n := run(line, i, '`')
			end := -1
			if !loose {
				end = closingTicks(line, i+n, n)
			}
			switch {
			case end >= 0 && !strings.Contains(line[i+n:end], "|"):
				b.WriteString(strings.ReplaceAll(line[i:end+n], "]:", `]\:`))
				i = end + n
			default:
				if end < 0 {
					loose = true
				}
				b.WriteString(line[i : i+n])
				i += n
			}
		case c == '<':
			b.WriteString(`\<`)
			i++
		case c == ']' && i+1 < len(line) && line[i+1] == '(':
			b.WriteString(`]\(`)
			i += 2
		case c == ']' && i+1 < len(line) && line[i+1] == ':':
			b.WriteString(`]\:`)
			i += 2
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), loose
}

// setextRule is a line of only = or -, which would turn the line above into a heading.
func setextRule(line string) bool {
	n, rest := indentOf(line)
	rest = strings.TrimRight(rest, " \t")
	return n <= 3 && rest != "" && (strings.Trim(rest, "=") == "" || strings.Trim(rest, "-") == "")
}

// mdBody returns the article text as Markdown that is safe to hand to a renderer: fences are re-emitted at column 0 with their content as written and closed at the end, everything else goes through mdLine, and headings are normalised as on the page.
func mdBody(text string) string {
	var out []string
	open, indent := "", 0
	sawH2, loose := false, false
	for _, line := range strings.Split(clean(text), "\n") {
		switch {
		case open != "":
			if closesFence(line, open) {
				out = append(out, open)
				open = ""
				continue
			}
			out = append(out, trimIndent(line, indent))
		case fence(line) != "":
			open = fence(line)
			indent, loose = leadingSpaces(line), false
			_, rest := indentOf(line)
			out = append(out, rest)
		case setextRule(line):
			n, rest := indentOf(line)
			out = append(out, strings.Repeat(" ", n)+`\`+rest)
		case blank(line):
			loose = false
			out = append(out, line)
		default:
			if level, t := heading(line); level > 0 {
				if t == "" {
					continue
				}
				mark := "###"
				if level <= 2 || !sawH2 {
					mark, sawH2 = "##", true
				}
				text, _ := mdLine(t, loose)
				out = append(out, mark+" "+text)
				continue
			}
			var text string
			text, loose = mdLine(line, loose)
			out = append(out, text)
		}
	}
	if open != "" {
		out = append(out, open)
	}
	return strings.Join(out, "\n")
}

// leadingSpaces counts the spaces before the first non-space character.
func leadingSpaces(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

// trimIndent drops up to n leading spaces, as a renderer does for the content of an indented fence.
func trimIndent(line string, n int) string {
	k := min(n, leadingSpaces(line))
	return line[k:]
}

// renderMarkdown is the article as a .md page: title, status line, text, sources and the licence line.
func (w *Wiki) renderMarkdown(rec *engineRecord, contributors []string) []byte {
	var b strings.Builder
	b.WriteString("# " + escapeInline(cleanLine(rec.Title)) + "\n\nAgent submitted")
	if len(contributors) > 0 {
		var names []string
		for _, c := range contributors {
			names = append(names, escapeInline(c))
		}
		b.WriteString(" · Contributed by " + strings.Join(names, ", "))
	}
	b.WriteString("\n\n" + strings.TrimRight(mdBody(rec.Lead+"\n\n"+rec.BodyMD), "\n") + "\n")
	if len(rec.Citations) > 0 {
		b.WriteString("\n## Sources\n\n")
	}
	seen := map[string]bool{}
	for _, c := range rec.Citations {
		if seen[c.URL] {
			continue
		}
		seen[c.URL] = true
		title, host := cleanLine(c.Title), cleanLine(c.Host)
		if title == "" {
			title = host
		}
		b.WriteString("- " + escapeInline(title) + " (" + escapeInline(host) + ")")
		if linkable(c.URL) && !strings.ContainsAny(c.URL, "<>") {
			b.WriteString(" <" + c.URL + ">")
		}
		b.WriteString("\n")
		for _, d := range rec.Citations {
			if d.URL == c.URL {
				b.WriteString("  - [" + strconv.Itoa(d.N) + "] " + escapeInline(cleanQuote(d.Quote)) + "\n")
			}
		}
	}
	b.WriteString("\n---\n\n" + licenceLine + " Canonical page: <" + w.canonical("/wiki/"+rec.Slug) + ">\n")
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
