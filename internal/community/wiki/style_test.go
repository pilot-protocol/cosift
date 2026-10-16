package wiki

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"
)

const pinnedStyleSHA = "7043a389e85487cbd6c2798fcc5bfce3cd804131d7d0f4304da81089291d6d27"

// canonicalStyle applies the pinned normalisation: no BOM, LF endings, no trailing blanks, no outer blank lines, one final newline.
func canonicalStyle(b []byte) []byte {
	s := strings.TrimPrefix(string(b), "\ufeff")
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", "\n"), "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, " \t")
	}
	return []byte(strings.Trim(strings.Join(lines, "\n"), "\n") + "\n")
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestEmbeddedStyleIsCanonicalAndPinned(t *testing.T) {
	if string(canonicalStyle(styleMD)) != string(styleMD) {
		t.Fatal("style.md is not in its canonical form")
	}
	if got := sha(styleMD); got != pinnedStyleSHA || StyleSHA256 != pinnedStyleSHA {
		t.Fatalf("style.md hashes to %s", got)
	}
}

func TestStyleBytesServedEqualThePinnedHash(t *testing.T) {
	h := newHarness(t)
	rec := h.get("/wiki/style.md")
	wantPage(t, rec, 200, public(300))
	if rec.Header().Get("Content-Type") != "text/markdown; charset=utf-8" {
		t.Fatalf("content type %q", rec.Header().Get("Content-Type"))
	}
	if got := sha(rec.Body.Bytes()); got != pinnedStyleSHA {
		t.Fatalf("the served bytes hash to %s", got)
	}
	if h.v1.count() != 0 {
		t.Fatal("the style guide asked the engine")
	}
}

func TestNewRefusesAStyleGuideThatDoesNotMatchItsHash(t *testing.T) {
	saved := styleMD
	defer func() { styleMD = saved }()
	styleMD = append(append([]byte(nil), saved...), ' ')
	if _, err := New(testConfig("http://127.0.0.1:1", newClock(), &logBuf{})); err == nil {
		t.Fatal("New accepted an altered style guide")
	}
}

var mdMarkup = regexp.MustCompile("`|\\*\\*|^#+ |^- |^[0-9]+\\. ")

func TestStylePageShowsEveryLineOfTheGuide(t *testing.T) {
	h := newHarness(t)
	rec := h.get("/wiki/style")
	wantPage(t, rec, 200, public(300))
	doc := structure(t, "style", rec.Body.String())
	title, _, _ := strings.Cut(string(styleMD), "\n")
	if got := textOf(find(doc, tag("h1"))[0]); got != strings.TrimPrefix(title, "# ") {
		t.Fatalf("h1 %q", got)
	}
	text := strings.Join(strings.Fields(textOf(find(doc, tag("main"))[0])), " ")
	fenced := false
	for _, line := range strings.Split(string(styleMD), "\n") {
		if strings.HasPrefix(line, "```") {
			fenced = !fenced
			continue
		}
		want := strings.TrimSpace(line)
		if !fenced {
			want = mdMarkup.ReplaceAllString(want, "")
		}
		want = strings.Join(strings.Fields(want), " ")
		if want != "" && !strings.Contains(text, want) {
			t.Fatalf("the page lacks the line %q", want)
		}
	}
	if h.v1.count() != 0 {
		t.Fatal("the style guide asked the engine")
	}
}

func TestReservedPageSlugsAreNeverArticles(t *testing.T) {
	for _, slug := range []string{"style", "terms", "wanted"} {
		if pageSlug(slug) {
			t.Fatalf("%q is a page slug", slug)
		}
	}
	h := newHarness(t)
	h.v1.set("style", jsonReply(200, published("style", "Style")))
	h.get("/wiki/style")
	if h.v1.slugCalls("style") != 0 {
		t.Fatal("the engine was asked for the reserved slug")
	}
}

func TestGoldenStylePage(t *testing.T) {
	checkGolden(t, "style.html", newHarness(t).get("/wiki/style").Body.String())
}
