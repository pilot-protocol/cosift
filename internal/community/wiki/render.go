package wiki

import (
	"html"
	"html/template"
	"net/url"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxListDepth = 8
	maxSpan      = 1000
	maxLinkText  = 500
	maxLinkDest  = 2100
)

// clean drops Cc (except tab and newline) and Cf characters, bidi controls
// included, and turns line and paragraph separators into newlines.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case r == '\u2028' || r == '\u2029':
			return '\n'
		case unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r):
			return -1
		}
		return r
	}, s)
}

// cleanLine is clean for single-line text, with whitespace runs collapsed.
func cleanLine(s string) string { return strings.Join(strings.Fields(clean(s)), " ") }

// linkable is a citation URL the page may link to: absolute http(s), printable ASCII.
func linkable(raw string) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] <= ' ' || raw[i] > '~' {
			return false
		}
	}
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.Opaque == ""
}

// renderBody turns the article text into allow-listed HTML: h2 h3 p ul ol li
// pre code strong em a sup, with href only to a citation URL or #cite-n.
func renderBody(text string, links map[string]string, cites int) template.HTML {
	r := &renderer{links: links, cites: cites}
	r.blocks(clean(text))
	return template.HTML(r.out.String())
}

type renderer struct {
	out   strings.Builder
	links map[string]string
	cites int
	sawH2 bool
}

func (r *renderer) text(s string) { r.out.WriteString(html.EscapeString(s)) }

func blank(line string) bool { return strings.TrimSpace(line) == "" }

func indentOf(line string) (int, string) {
	n := 0
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ':
			n++
		case '\t':
			n += 4 - n%4
		default:
			return n, line[i:]
		}
	}
	return n, ""
}

// fence returns the opening fence of a fenced code line, or "".
func fence(line string) string {
	n, rest := indentOf(line)
	if n > 3 || len(rest) < 3 || (rest[0] != '`' && rest[0] != '~') {
		return ""
	}
	k := 0
	for k < len(rest) && rest[k] == rest[0] {
		k++
	}
	if k < 3 || (rest[0] == '`' && strings.Contains(rest[k:], "`")) {
		return ""
	}
	return rest[:k]
}

func closesFence(line, open string) bool {
	n, rest := indentOf(line)
	if n > 3 {
		return false
	}
	rest = strings.TrimRight(rest, " \t")
	return len(rest) >= len(open) && strings.Trim(rest, open[:1]) == ""
}

// heading returns the ATX level and the text of a heading line.
func heading(line string) (int, string) {
	n, rest := indentOf(line)
	if n > 3 {
		return 0, ""
	}
	k := 0
	for k < len(rest) && rest[k] == '#' {
		k++
	}
	if k == 0 || k > 6 || (k < len(rest) && rest[k] != ' ' && rest[k] != '\t') {
		return 0, ""
	}
	t := strings.TrimSpace(rest[k:])
	if s := strings.TrimRight(t, "#"); s == "" || strings.HasSuffix(s, " ") || strings.HasSuffix(s, "\t") {
		t = strings.TrimSpace(s)
	}
	return k, t
}

type listLine struct {
	indent, content int
	ordered         bool
	text            string
}

func listItem(line string) (listLine, bool) {
	n, rest := indentOf(line)
	k := 0
	ordered := false
	switch {
	case rest == "":
		return listLine{}, false
	case rest[0] == '-' || rest[0] == '*' || rest[0] == '+':
		k = 1
	default:
		for k < len(rest) && k < 9 && rest[k] >= '0' && rest[k] <= '9' {
			k++
		}
		if k == 0 || k >= len(rest) || (rest[k] != '.' && rest[k] != ')') {
			return listLine{}, false
		}
		k++
		ordered = true
	}
	if k < len(rest) && rest[k] != ' ' && rest[k] != '\t' {
		return listLine{}, false
	}
	return listLine{indent: n, content: n + k + 1, ordered: ordered, text: strings.TrimSpace(rest[k:])}, true
}

func (r *renderer) blocks(src string) {
	lines := strings.Split(src, "\n")
	var para []string
	flush := func() {
		if len(para) > 0 {
			r.out.WriteString("<p>")
			r.inline(strings.Join(para, "\n"), true)
			r.out.WriteString("</p>\n")
			para = nil
		}
	}
	for i := 0; i < len(lines); {
		line := lines[i]
		if blank(line) {
			flush()
			i++
			continue
		}
		if open := fence(line); open != "" {
			flush()
			i = r.code(lines, i, open)
			continue
		}
		if level, t := heading(line); level > 0 {
			flush()
			r.heading(level, t)
			i++
			continue
		}
		if _, ok := listItem(line); ok {
			flush()
			i = r.list(lines, i)
			continue
		}
		para = append(para, strings.TrimSpace(line))
		i++
	}
	flush()
}

func (r *renderer) code(lines []string, i int, open string) int {
	var body []string
	for i++; i < len(lines) && !closesFence(lines[i], open); i++ {
		body = append(body, lines[i])
	}
	r.out.WriteString("<pre><code>")
	r.text(strings.Join(body, "\n"))
	r.out.WriteString("</code></pre>\n")
	return i + 1
}

// heading emits h2 or h3: # is demoted to h2, and an h3 before any h2 is h2.
func (r *renderer) heading(level int, t string) {
	if t == "" {
		return
	}
	tag := "h3"
	if level <= 2 || !r.sawH2 {
		tag = "h2"
		r.sawH2 = true
	}
	r.out.WriteString("<" + tag + ">")
	r.inline(t, true)
	r.out.WriteString("</" + tag + ">\n")
}

func listTag(ordered bool) string {
	if ordered {
		return "ol"
	}
	return "ul"
}

// list renders consecutive list items; nesting deeper than maxListDepth is flattened.
func (r *renderer) list(lines []string, i int) int {
	type level struct {
		indent, content int
		ordered         bool
	}
	var stack []level
	var text []string
	flushItem := func() {
		if text != nil {
			r.inline(strings.Join(text, "\n"), true)
			text = nil
		}
	}
	for i < len(lines) {
		line := lines[i]
		it, ok := listItem(line)
		if !ok {
			if blank(line) {
				j := i + 1
				for j < len(lines) && blank(lines[j]) {
					j++
				}
				if j < len(lines) {
					if _, next := listItem(lines[j]); next {
						i = j
						continue
					}
				}
				break
			}
			if fence(line) != "" {
				break
			}
			if lv, _ := heading(line); lv > 0 {
				break
			}
			text = append(text, strings.TrimSpace(line))
			i++
			continue
		}
		flushItem()
		for len(stack) > 0 && it.indent < stack[len(stack)-1].indent {
			r.out.WriteString("</li></" + listTag(stack[len(stack)-1].ordered) + ">\n")
			stack = stack[:len(stack)-1]
		}
		switch top := len(stack) - 1; {
		case top < 0 || (it.indent >= stack[top].content && len(stack) < maxListDepth):
			r.out.WriteString("<" + listTag(it.ordered) + ">\n<li>")
			stack = append(stack, level{it.indent, it.content, it.ordered})
		case it.ordered != stack[top].ordered:
			r.out.WriteString("</li></" + listTag(stack[top].ordered) + ">\n<" + listTag(it.ordered) + ">\n<li>")
			stack[top].ordered = it.ordered
		default:
			r.out.WriteString("</li>\n<li>")
		}
		text = []string{it.text}
		i++
	}
	flushItem()
	for len(stack) > 0 {
		r.out.WriteString("</li></" + listTag(stack[len(stack)-1].ordered) + ">\n")
		stack = stack[:len(stack)-1]
	}
	return i
}

func isPunct(c byte) bool {
	return c < 0x80 && unicode.IsPunct(rune(c)) || c == '`' || c == '+' || c == '<' || c == '>' || c == '|' || c == '~' || c == '^' || c == '$' || c == '='
}

func run(s string, i int, c byte) int {
	n := 0
	for i+n < len(s) && s[i+n] == c {
		n++
	}
	return n
}

// closingTicks finds a backtick run of exactly n within the span window.
func closingTicks(s string, from, n int) int {
	end := min(len(s), from+maxSpan)
	for j := from; j < end; {
		k := strings.IndexByte(s[j:end], '`')
		if k < 0 {
			return -1
		}
		j += k
		m := run(s, j, '`')
		if m == n {
			return j
		}
		j += m
	}
	return -1
}

func space(c byte) bool { return c == ' ' || c == '\t' || c == '\n' }

// closeDelim finds the closer of an emphasis span opened just before from.
func closeDelim(s string, from int, d string) int {
	if from >= len(s) || space(s[from]) {
		return -1
	}
	end := min(len(s), from+maxSpan+len(d))
	for j := from + 1; j < end; j++ {
		if !strings.HasPrefix(s[j:], d) || space(s[j-1]) {
			continue
		}
		if len(d) == 1 && (s[j-1] == '*' || (j+1 < len(s) && s[j+1] == '*')) {
			continue
		}
		return j
	}
	return -1
}

// marker parses a citation marker [n] at the start of s.
func marker(s string) (n, width int, ok bool) {
	k := 1
	for k < len(s) && k <= 4 && s[k] >= '0' && s[k] <= '9' {
		k++
	}
	if k == 1 || k >= len(s) || s[k] != ']' {
		return 0, 0, false
	}
	n, err := strconv.Atoi(s[1:k])
	return n, k + 1, err == nil
}

// link parses [text](dest) at the start of s.
func link(s string) (text, dest string, width int, ok bool) {
	closeText := strings.IndexByte(s[1:min(len(s), maxLinkText+2)], ']')
	if closeText < 0 {
		return "", "", 0, false
	}
	closeText++
	if closeText+1 >= len(s) || s[closeText+1] != '(' || closeText == 1 {
		return "", "", 0, false
	}
	depth := 0
	end := min(len(s), closeText+2+maxLinkDest)
	for j := closeText + 2; j < end; j++ {
		switch s[j] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				dest = strings.TrimSpace(s[closeText+2 : j])
				if strings.HasPrefix(dest, "<") && strings.HasSuffix(dest, ">") {
					dest = dest[1 : len(dest)-1]
				} else if k := strings.IndexAny(dest, " \t\n"); k >= 0 {
					dest = dest[:k]
				}
				return s[1:closeText], dest, j + 1, true
			}
			depth--
		}
	}
	return "", "", 0, false
}

// inline renders a text run; with refs off (link text) markers and links are text.
func (r *renderer) inline(s string, refs bool) {
	var pending strings.Builder
	flush := func() {
		r.text(pending.String())
		pending.Reset()
	}
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s) && isPunct(s[i+1]):
			pending.WriteByte(s[i+1])
			i += 2
			continue
		case c == '`':
			n := run(s, i, '`')
			if end := closingTicks(s, i+n, n); end >= 0 {
				flush()
				code := strings.ReplaceAll(s[i+n:end], "\n", " ")
				if len(code) > 2 && code[0] == ' ' && code[len(code)-1] == ' ' && strings.TrimSpace(code) != "" {
					code = code[1 : len(code)-1]
				}
				r.out.WriteString("<code>")
				r.text(code)
				r.out.WriteString("</code>")
				i = end + n
				continue
			}
			pending.WriteString(s[i : i+n])
			i += n
			continue
		case c == '*':
			d := "*"
			tag := "em"
			if strings.HasPrefix(s[i:], "**") {
				d, tag = "**", "strong"
			}
			if end := closeDelim(s, i+len(d), d); end >= 0 {
				flush()
				r.out.WriteString("<" + tag + ">")
				r.inline(s[i+len(d):end], refs)
				r.out.WriteString("</" + tag + ">")
				i = end + len(d)
				continue
			}
			pending.WriteString(d)
			i += len(d)
			continue
		case c == '[' && refs:
			if n, width, ok := marker(s[i:]); ok && (i+width >= len(s) || s[i+width] != '(') {
				flush()
				if n >= 1 && n <= r.cites {
					r.out.WriteString(`<sup><a href="#cite-` + strconv.Itoa(n) + `">[` + strconv.Itoa(n) + `]</a></sup>`)
				} else {
					r.text(s[i : i+width])
				}
				i += width
				continue
			}
			if text, dest, width, ok := link(s[i:]); ok {
				flush()
				if href, ok := r.links[dest]; ok {
					r.out.WriteString(`<a href="` + html.EscapeString(href) + `" rel="nofollow noopener">`)
					r.inline(text, false)
					r.out.WriteString("</a>")
				} else {
					r.inline(text, false)
				}
				i += width
				continue
			}
		}
		pending.WriteByte(c)
		i++
	}
	flush()
}
