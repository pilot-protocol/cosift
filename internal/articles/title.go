package articles

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// CheckTitle returns the first generic-title rule s fails, or "" when it passes.
// Aliases skip the slug_empty rule.
func CheckTitle(s string, alias bool) string {
	for _, r := range s {
		if unicode.In(r, unicode.Cc, unicode.Cf, unicode.Co, unicode.Cs, unicode.Zl, unicode.Zp) ||
			(unicode.Is(unicode.Zs, r) && r != ' ') {
			return "char_class"
		}
	}
	if strings.HasPrefix(s, " ") || strings.HasSuffix(s, " ") || strings.Contains(s, "  ") {
		return "spacing"
	}
	if n := utf8.RuneCountInString(s); n < 2 || n > 120 {
		return "length"
	}
	f := fold(s)
	addr := f
	for _, at := range []string{"[at]", "(at)", "{at}", "[@]"} {
		addr = strings.ReplaceAll(addr, at, "@")
	}
	if strings.Contains(addr, "@") {
		return "address"
	}
	if strings.Contains(f, "://") || strings.Contains(f, "www.") || strings.Contains(f, "mailto:") {
		return "url"
	}
	if strings.ContainsAny(f, "<>{}[]|\\^~*=\"`") {
		return "forbidden_char"
	}
	if strings.Contains(f, "?") {
		return "question"
	}
	if rule := hostRule(f); rule != "" {
		return rule
	}
	if longestDigitRun(f) >= 6 {
		return "digit_run"
	}
	if hasHexID(f) {
		return "hex_id"
	}
	for i := 0; i+1 < len(f); i++ {
		if f[i] == '#' && isDigit(f[i+1]) {
			return "ticket_ref"
		}
	}
	if rule := personRule(norm.NFKC.String(s)); rule != "" {
		return rule
	}
	if !alias && Slugify(s) == "" {
		return "slug_empty"
	}
	return ""
}

func fold(s string) string { return asciiLower(norm.NFKC.String(s)) }

func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

func isLowerAlnum(c byte) bool { return isDigit(c) || ('a' <= c && c <= 'z') }

var (
	internalTLDs = set("internal", "local", "corp", "lan", "intranet", "localdomain")
	publicTLDs   = set("com", "net", "org", "io", "dev", "app", "ai", "co", "me", "info", "biz", "xyz", "us", "uk", "de", "fr", "eu", "ro", "ca", "au", "in", "gov", "edu", "site", "online", "tech", "cloud", "page", "blog", "sh")
	knownNames   = set("asp.net", "ado.net", "vb.net", "socket.io")
)

func set(xs ...string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func hostRule(f string) string {
	tokens := strings.FieldsFunc(f, func(r rune) bool {
		return !(r < utf8.RuneSelf && (isLowerAlnum(byte(r)) || r == '.' || r == '-'))
	})
	for _, tok := range tokens {
		tok = strings.Trim(tok, ".-")
		if tok == "" {
			continue
		}
		labels := strings.Split(tok, ".")
		full := true
		for _, l := range labels {
			if l == "" {
				full = false
			}
		}
		if !full {
			continue
		}
		last := labels[len(labels)-1]
		if len(labels) >= 2 && internalTLDs[last] {
			return "internal_host"
		}
		if len(labels) >= 3 && len(last) >= 2 && strings.Trim(last, "abcdefghijklmnopqrstuvwxyz") == "" {
			return "host_like"
		}
		if len(labels) == 2 && publicTLDs[last] && !knownNames[tok] {
			return "host_like"
		}
	}
	return ""
}

func longestDigitRun(f string) int {
	best, run := 0, 0
	for i := 0; i < len(f); i++ {
		switch c := f[i]; {
		case isDigit(c):
			run++
			best = max(best, run)
		case run > 0 && strings.IndexByte(" -._,", c) >= 0:
		default:
			run = 0
		}
	}
	return best
}

func hasHexID(f string) bool {
	n, digit, letter := 0, false, false
	for i := 0; i <= len(f); i++ {
		if i < len(f) && (isDigit(f[i]) || ('a' <= f[i] && f[i] <= 'f')) {
			n++
			digit = digit || isDigit(f[i])
			letter = letter || !isDigit(f[i])
			continue
		}
		if n >= 12 && digit && letter {
			return true
		}
		n, digit, letter = 0, false, false
	}
	return false
}

var (
	pronouns = set("me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves",
		"i'm", "i've", "i'd", "i'll", "we're", "we've", "we'd", "we'll", "you're", "you've", "you'd", "you'll")
	beforeI = set("do", "can", "should", "could", "would", "will", "shall", "may", "might", "must", "am", "did", "have")
	afterI  = set("am", "have", "need", "want", "was", "think", "feel", "got", "use", "like", "know", "can", "will", "should")
)

func personRule(s string) string {
	s = strings.ReplaceAll(s, "’", "'")
	isWord := func(c byte) bool { return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || c == '\'' || c == '.' }
	var tokens []string
	for i := 0; i < len(s); {
		if !isWord(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isWord(s[j]) {
			j++
		}
		run := s[i:j]
		i = j
		if (run == "I" || run == "i") && j < len(s) && (s[j] == '/' || s[j] == '-' || isDigit(s[j])) {
			continue
		}
		if t := strings.Trim(run, "'."); t != "" {
			tokens = append(tokens, t)
		}
	}
	for _, t := range tokens {
		if l := asciiLower(t); pronouns[l] || (l == "us" && t != "US") {
			return "first_second_person"
		}
	}
	for k, t := range tokens {
		if asciiLower(t) != "i" {
			continue
		}
		if t != "I" || k == 0 || !('A' <= tokens[k-1][0] && tokens[k-1][0] <= 'Z') {
			return "personal_phrase"
		}
	}
	for k := 0; k+1 < len(tokens); k++ {
		a, b := asciiLower(tokens[k]), asciiLower(tokens[k+1])
		if (b == "i" && beforeI[a]) || (a == "i" && afterI[b]) {
			return "personal_phrase"
		}
	}
	return ""
}

var slugLetters = strings.NewReplacer(
	"ß", "ss", "Æ", "AE", "æ", "ae", "Œ", "OE", "œ", "oe", "Ø", "O", "ø", "o", "Ł", "L", "ł", "l",
	"Đ", "D", "đ", "d", "Ð", "D", "ð", "d", "Þ", "Th", "þ", "th", "ı", "i",
)

// Slugify is the contract's slug of a title, cut to 80 bytes.
func Slugify(title string) string { return cutSlug(fullSlug(title)) }

// fullSlug is Slugify without the 80-byte cut.
func fullSlug(title string) string {
	t := slugLetters.Replace(norm.NFKD.String(title))
	t = strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Mn, r) {
			return -1
		}
		return r
	}, t)
	t = asciiLower(t)
	var b strings.Builder
	var prev rune
	for _, r := range t {
		switch {
		case r < utf8.RuneSelf && isLowerAlnum(byte(r)):
			b.WriteRune(r)
		case r == '\'' || r == '’':
		case r == '&':
			b.WriteString(" and ")
		case r == '+':
			b.WriteString(" plus ")
		case r == '#' && prev < utf8.RuneSelf && isLowerAlnum(byte(prev)):
			b.WriteString(" sharp ")
		default:
			b.WriteByte(' ')
		}
		prev = r
	}
	return strings.Trim(strings.Join(strings.Fields(b.String()), "-"), "-")
}

func cutSlug(s string) string {
	if len(s) > 80 {
		if s[80] == '-' {
			s = s[:80]
		} else {
			s = s[:80]
			if k := strings.LastIndexByte(s, '-'); k > 0 {
				s = s[:k]
			}
		}
	}
	return strings.Trim(s, "-")
}

var reservedSlugs = set("v", "index", "sitemap", "search", "api", "admin", "random", "new")

// assignSlug returns base when it is free, else the first free disambiguated form.
func assignSlug(base string, taken func(string) bool) string {
	free := func(s string) bool { return !reservedSlugs[s] && !taken(s) }
	if free(base) {
		return base
	}
	for n := 2; ; n++ {
		suffix := "-" + strconv.Itoa(n)
		cand := strings.TrimRight(base[:min(len(base), 80-len(suffix))], "-") + suffix
		if free(cand) {
			return cand
		}
	}
}
