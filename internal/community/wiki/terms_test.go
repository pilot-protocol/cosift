package wiki

import (
	"slices"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

var pinnedTermsIDs = []string{"license", "originality", "credits", "worker-fetching", "takedown", "synth-attribution"}

// termsStatements are the statements each required section must make, as phrases of the page text.
var termsStatements = map[string][]string{
	"license": {"Creative Commons Attribution-ShareAlike 4.0 International licence (CC BY-SA 4.0)", "worldwide, non-exclusive and royalty-free", "You keep your copyright",
		"does not cover the pages your article cites", `"Contributed by @handle"`, "in its .md form, with a link to the article", "the page shows the current one", `"a community member"`, "Accounts in local mode",
		"Every article page and every .md file carries a CC BY-SA 4.0 line", "You cannot withdraw a published contribution"},
	"originality": {"your own work, or your agent's output that you have the right to submit", "does not infringe anyone's rights", "no personal data, confidential information or unlawful material",
		"short extracts from the cited pages", "You do not copy substantial parts of a source", "responsible for everything your agent submits under your handle"},
	"credits": {"no cash value", "cannot be redeemed, sold or transferred", "count like purchased ones", "do not expire", "awards of the reverted versions", "all awards of that article",
		"Royalties are never reclaimed", "never takes a balance below zero", "paused or banned", "A ban blocks future contributions only", "change the amounts of future awards and royalties"},
	"worker-fetching": {"your own machine, your network connection and your IP address", "lawful and permitted by your network", "does not guarantee that any assigned page is lawful or safe to fetch",
		"keyed hash of the /24 for IPv4 or of the /48 for IPv6", "at most 7 days", "as long as backups of those records are kept", "does not keep the address",
		"second worker or by a server fetch before it is indexed", "rejected without credit", "stop the worker at any time"},
	"takedown": {"remove or correct an article, a contribution or a handle", "takedown contact address shown below and on every article page", "names the article and says why",
		"remove the article at once and decide afterwards", "erased, apart from a residue that stops it being recreated", "awards of the article are reclaimed",
		"remove any contribution for any reason", "breaks the style guide or these terms"},
	"synth-attribution": {`labelled "Agent submitted"`, "carry the CC BY-SA 4.0 line", "attributed to Cosift, not to a person", `("Contributed by @a, @b") in order of first contribution`,
		"stays attributed to Cosift", "gives the attribution shown on its page and shares adaptations under the same licence"},
}

func sectionByID(doc *html.Node, id string) *html.Node {
	s := find(doc, func(n *html.Node) bool { return n.Data == "section" && attr(n, "id") == id })
	if len(s) != 1 {
		return nil
	}
	return s[0]
}

func TestTermsPageCarriesTheSixSectionsWithTheirPinnedIDs(t *testing.T) {
	h := newHarness(t)
	rec := h.get("/wiki/terms")
	wantPage(t, rec, 200, public(docMaxAge))
	doc := structure(t, "terms", rec.Body.String())
	if !slices.Equal(TermsSectionIDs, pinnedTermsIDs) || TermsVersion != 1 {
		t.Fatalf("exported ids %v, version %d", TermsSectionIDs, TermsVersion)
	}
	var got []string
	for _, s := range find(doc, tag("section")) {
		got = append(got, attr(s, "id"))
		if len(find(s, tag("h2"))) != 1 {
			t.Fatalf("section %s has no single heading", attr(s, "id"))
		}
	}
	if !slices.Equal(got, pinnedTermsIDs) {
		t.Fatalf("sections %v", got)
	}
	toc := find(doc, func(n *html.Node) bool { return n.Data == "a" && strings.HasPrefix(attr(n, "href"), "#") })
	if len(toc) != 6 {
		t.Fatalf("%d anchors", len(toc))
	}
	for i, a := range toc {
		if attr(a, "href") != "#"+pinnedTermsIDs[i] {
			t.Fatalf("anchor %d is %q", i, attr(a, "href"))
		}
	}
	if !strings.Contains(textOf(find(doc, tag("h1"))[0]), "version 1") || h.v1.count() != 0 {
		t.Fatal("title lacks the version, or the page asked the engine")
	}
}

func TestEverySectionMakesItsRequiredStatements(t *testing.T) {
	doc := structure(t, "terms", newHarness(t).get("/wiki/terms").Body.String())
	for _, id := range pinnedTermsIDs {
		sec := sectionByID(doc, id)
		text := strings.Join(strings.Fields(textOf(sec)), " ")
		for _, phrase := range termsStatements[id] {
			if !strings.Contains(text, phrase) {
				t.Errorf("section %s lacks %q", id, phrase)
			}
		}
	}
	if len(termsStatements) != len(pinnedTermsIDs) {
		t.Fatal("a section has no statements to check")
	}
}

func TestTakedownSectionShowsTheContactAddress(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.ReportMailto = "takedown@example.org" })
	doc := structure(t, "terms", h.get("/wiki/terms").Body.String())
	links := find(sectionByID(doc, "takedown"), func(n *html.Node) bool { return n.Data == "a" })
	if len(links) != 1 || attr(links[0], "href") != "mailto:takedown@example.org" || textOf(links[0]) != "takedown@example.org" {
		t.Fatalf("contact links %v", links)
	}
	for _, id := range pinnedTermsIDs {
		if id != "takedown" && len(find(sectionByID(doc, id), tag("a"))) != 0 {
			t.Fatalf("section %s carries a link", id)
		}
	}
}

func TestTermsWithAnyOtherSectionsAreRefused(t *testing.T) {
	good := string(termsMD)
	without := func(id string) string {
		i := strings.Index(good, "{#"+id+"}")
		start := strings.LastIndex(good[:i], "\n## ")
		end := strings.Index(good[i:], "\n## ")
		if end < 0 {
			return good[:start+1]
		}
		return good[:start+1] + good[i+end+1:]
	}
	for name, src := range map[string]string{
		"missing section":    without("credits"),
		"renamed id":         strings.Replace(good, "{#worker-fetching}", "{#worker}", 1),
		"extra section":      good + "\n## Extra {#extra}\n\n- more\n",
		"duplicate":          good + "\n## Again {#license}\n\n- again\n",
		"swapped order":      strings.Replace(strings.Replace(good, "{#license}", "{#tmp}", 1), "{#originality}", "{#license}", 1),
		"empty section":      good[:strings.Index(good, "- Articles written by")],
		"no title":           strings.TrimPrefix(good, "# Cosift contributor terms (version 1)"),
		"heading without id": strings.Replace(good, " {#credits}", "", 1),
	} {
		if _, _, _, err := parseTerms([]byte(src)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	saved := termsMD
	defer func() { termsMD = saved }()
	termsMD = []byte(without("takedown"))
	if _, err := New(testConfig("http://127.0.0.1:1", newClock(), &logBuf{})); err == nil {
		t.Fatal("New accepted terms without a required section")
	}
}

func TestTermsAreNotAnArticleSlugOrAMarkdownPage(t *testing.T) {
	h := newHarness(t)
	h.v1.set("terms", jsonReply(200, published("terms", "Terms")))
	if rec := h.get("/wiki/terms.md"); rec.Code != 404 {
		t.Fatalf("terms.md answered %d", rec.Code)
	}
	if h.v1.slugCalls("terms") != 0 {
		t.Fatal("the engine was asked for the reserved slug")
	}
}

func TestGoldenTermsPage(t *testing.T) {
	checkGolden(t, "terms.html", newHarness(t).get("/wiki/terms").Body.String())
}
