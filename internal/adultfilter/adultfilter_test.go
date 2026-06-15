package adultfilter

import "testing"

// TestNoFalsePositives — every case here is a REAL false positive observed
// against the live 13M-doc corpus (or a classic substring trap). None may be
// classified adult.
func TestNoFalsePositives(t *testing.T) {
	cases := []struct {
		name, title, text, url string
	}{
		// Fragment-splitting traps (the bug class that mis-deleted live docs).
		{"kernel-3xxx", "HighPoint RocketRAID 3xxx/4xxx Adapter Driver", "SCSI driver documentation.", "https://www.kernel.org/doc/html/latest/scsi/hptiop.html"},
		{"roman-numeral-xxx", "Historia Augusta — Tyranni XXX", "The Thirty Tyrants, a Roman text.", "https://penelope.uchicago.edu/Thayer/E/Roman/Texts/Historia_Augusta/Tyranni_XXX.html"},
		{"jacques-tits", "Abel Prize Laureates 2008", "John G. Thompson and Jacques Tits.", "https://abelprize.no/abel-prize-laureates/2008"},
		{"hungarian-analizis", "Legközelebbi szomszéd analízis", "Nearest-neighbour analízis eredmény.", "https://commons.wikimedia.org/wiki/File:analizis.JPG"},
		{"symb-anal-net", "Symbol analysis networks", "Harnad symb.anal.net.searle paper.", "https://www.southampton.ac.uk/~harnad/Papers/Harnad/harnad93.symb.anal.net.searle.html"},
		{"record-id-fap", "Caltech library record", "Archived dataset.", "https://authors.library.caltech.edu/records/7fap3-5v118"},
		// Single-explicit-term topical mentions (≥2-distinct rule lets through).
		{"hrw-anal-exam", "Forced anal examinations", "Human Rights Watch report on prosecutions.", "https://www.hrw.org/report/forced-anal-examinations-homosexuality"},
		{"mentalhealth-porn", "When does porn become a problem", "A clinical overview of compulsive behaviour.", "https://www.mentalhealth.com/library/when-does-porn-become-a-problem"},
		{"pornography-law", "Pornography law in the EU", "A legal overview of regulation.", "https://law.example.org/eu"},
		{"hardcore-engineer", "Hardcore Engineer blog", "Cut your AI agent token costs.", "https://dev.to/hardcore-engineer"},
		{"sex-research", "Sex differences in cognition", "Research into the biology of sex chromosomes.", "https://journal.example.org/paper"},
		// Classic traps.
		{"analysis", "Statistical analysis of canal sediment", "A regression analysis of the data.", "https://example.com/analysis"},
		{"scunthorpe", "Scunthorpe United FC", "The match in Scunthorpe.", "https://bbc.co.uk/sport/scunthorpe"},
		{"essex-sussex", "Essex and Sussex travel", "Middlesex and Wessex regions.", "https://travel.example.org/sussex"},
		{"cum-laude", "Graduated magna cum laude", "Summa cum laude degree.", "https://uni.example.edu/news"},
		{"camera", "Best webcam for streaming", "Reviewing camera hardware.", "https://tech.example.com/webcam"},
		{"dotcam-legit", "Acme Camera Co", "Photography gear.", "https://acme.cam/store"},
		{"plain", "Intro to Go programming", "Goroutines and channels.", "https://go.example.dev/intro"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if adult, n, reason := Classify(c.title, c.text, c.url); adult {
				t.Errorf("FALSE POSITIVE: classified adult (distinctTerms=%d reason=%q)", n, reason)
			}
		})
	}
}

// TestTruePositives — clear porn must be caught, primarily via host/TLD.
func TestTruePositives(t *testing.T) {
	cases := []struct {
		name, title, text, url string
	}{
		{"host-pornhub", "Some Video", "watch now", "https://www.pornhub.com/view"},
		{"host-xvideos", "clip", "body", "https://xvideos.com/v/123"},
		{"host-porn-substr", "Gallery", "x", "https://www.thotsaporn.com/"},
		{"host-xporn", "Gallery", "x", "https://xporn.org/"},
		{"tld-xxx", "welcome", "content", "https://site.xxx/page"},
		{"tld-cfd", "members", "x", "https://hotcams.cfd/live"},
		{"tld-sbs", "members", "x", "https://freecams.sbs/live"},
		{"tld-porn", "members", "x", "https://watch.porn/clip"},
		{"lexical-two-terms", "Free MILF Creampie Videos", "watch", "https://vids.example.net/clip"},
		{"lexical-body", "Members Area", "blowjob gangbang cumshot compilation for adults", "https://example.com/members"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if adult, n, reason := Classify(c.title, c.text, c.url); !adult {
				t.Errorf("FALSE NEGATIVE: not classified adult (distinctTerms=%d reason=%q)", n, reason)
			}
		})
	}
}

// TestSingleExplicitTermDoesNotTrip — exactly one distinct explicit term,
// however many times it repeats, is never enough on its own.
func TestSingleExplicitTermDoesNotTrip(t *testing.T) {
	if IsAdult("Porn addiction research", "porn porn porn studies of porn consumption and porn habits", "https://research.example.edu/porn-study") {
		t.Error("a single distinct explicit term (repeated) must not trip the filter")
	}
}

// TestTwoDistinctTermsTrips — two different explicit terms is the lexical bar.
func TestTwoDistinctTermsTrips(t *testing.T) {
	if !IsAdult("Gallery", "creampie and gangbang scenes", "https://generic.example.net/g") {
		t.Error("two distinct explicit terms should trip the filter")
	}
}

func TestHostMatchIsConclusive(t *testing.T) {
	if adult, _, reason := Classify("", "", "https://m.redtube.com/"); !adult || reason != "host:redtube" {
		t.Errorf("host match failed: adult=%v reason=%q", adult, reason)
	}
}

// TestFragmentsNeverTokenize guards the tokenizer directly against the
// fragment bugs.
func TestFragmentsNeverTokenize(t *testing.T) {
	for _, s := range []string{"3xxx", "4xxx", "7fap3", "analízis", "xxxi"} {
		for _, tok := range tokenize(s) {
			if _, bad := explicitTerms[tok]; bad {
				t.Errorf("%q tokenized to explicit term %q", s, tok)
			}
		}
	}
}
