// Package adultfilter provides a precision-oriented classifier that flags
// pornographic / adult web pages so the crawler can refuse to index them and
// an offline sweep can purge ones already in the corpus.
//
// Design goal: HIGH PRECISION over recall. A false positive silently drops
// legitimate content from the index, which is far worse than letting a
// borderline page through. The classifier therefore relies on two narrow,
// high-confidence signals and deliberately ignores everything ambiguous:
//
//   - Host / TLD match (CONCLUSIVE): the URL host contains a known adult-site
//     brand fragment (pornhub, xvideos, "porn", …) or sits under an adult /
//     heavily-abused gTLD (.xxx, .porn, .sex, .adult, .cfd, .sbs). This is the
//     signal that reliably identifies a porn SITE.
//
//   - Lexical: a page is flagged ONLY when it contains TWO OR MORE DISTINCT
//     unambiguous explicit terms (porn, blowjob, creampie, gangbang, …). A
//     page that merely mentions "porn" or "masturbation" once — a news story,
//     a health article, an academic paper, a kernel doc — has at most one and
//     is NOT flagged. Real porn pages carry many distinct explicit terms.
//
// Two traps motivated this design (both observed against the live corpus):
//
//  1. Fragment splitting. Naive tokenisation on non-letters turned "3xxx"
//     (kernel RAID driver), Roman numeral "XXX", "7fap3" (a record ID),
//     "analízis" (Hungarian) and "symb.anal.net" into the tokens xxx / fap /
//     anal. Fixed two ways: tokens are runs of Unicode letters AND digits
//     ("3xxx" stays one token, never "xxx"), and the short ambiguous words
//     (xxx, anal, fap, tits, sex, cum, …) are NOT lexical terms at all — xxx
//     only ever matters as a TLD/host signal.
//
//  2. Topical mentions. "Pornography law", "porn-induced dysfunction",
//     "forced anal examinations" (Human Rights Watch) each contain a single
//     explicit token. The ≥2-distinct-terms rule lets all of them through.
package adultfilter

import (
	"net/url"
	"strings"
	"unicode"
)

// explicitTerms are unambiguous, multi-letter pornographic words. Each is a
// strong signal, but ONE is never enough (topical articles contain one);
// TWO DISTINCT terms is the lexical bar. Deliberately EXCLUDES every short or
// dual-sense word — sex, cum, anal, xxx, tits, fap, cock, dick, escort,
// naked, nude, adult, model, hardcore, fetish, slut, boobs — which produced
// real false positives and add little recall that host/TLD matching misses.
var explicitTerms = map[string]struct{}{
	"porn": {}, "porno": {}, "pornography": {}, "pornographic": {},
	"blowjob": {}, "blowjobs": {}, "handjob": {}, "handjobs": {},
	"footjob": {}, "rimjob": {}, "titfuck": {}, "deepthroat": {},
	"bukkake": {}, "gangbang": {}, "gangbangs": {}, "creampie": {}, "creampies": {},
	"cumshot": {}, "cumshots": {}, "facials": {}, "gloryhole": {},
	"milf": {}, "milfs": {}, "hentai": {}, "camgirl": {}, "camgirls": {},
	"camwhore": {}, "fleshlight": {}, "buttplug": {}, "fisting": {},
	"masturbation": {}, "masturbating": {}, "masturbate": {},
	"cunnilingus": {}, "fellatio": {}, "shemale": {}, "shemales": {},
	"ladyboy": {}, "nympho": {}, "cameltoe": {}, "dildo": {}, "dildos": {},
	"bbw": {}, "creampied": {}, "cocksucking": {}, "pussyfucking": {},
	"upskirt": {}, "downblouse": {}, "jerkoff": {}, "fapping": {},
	"xvideos": {}, "xhamster": {}, "xnxx": {}, "youporn": {}, "redtube": {},
	"brazzers": {}, "chaturbate": {}, "bangbros": {}, "pornstar": {}, "pornstars": {},
}

// hostTokens are adult-site name fragments. If the URL host contains any as a
// substring the page is conclusively adult. Every entry is a brand name or
// the literal "porn" — fragments with no innocent host-name sense. Short
// ambiguous fragments (sex → essex, anal → analytics, milf → milford) are
// intentionally absent: they match legitimate hosts.
var hostTokens = []string{
	"porn", "xvideos", "xhamster", "xnxx", "youporn", "redtube", "spankbang",
	"brazzers", "chaturbate", "rule34", "hentai", "fapello",
	"camsoda", "stripchat", "myfreecams", "livejasmin", "tnaflix", "eporner",
	"nhentai", "motherless", "drtuber", "tube8", "keezmovies", "bangbros",
	"naughtyamerica", "thothub", "faphouse", "spankwire",
}

// adultTLDs are gTLDs whose registrant base is overwhelmingly adult or
// adult-spam: the ICANN adult-sponsored set (.xxx/.porn/.sex/.adult) plus
// .cfd and .sbs, which the live-corpus audit showed dominated by porn/spam.
// Generic TLDs with substantial legitimate use (.cam, .tube, .sexy, .xyz)
// are intentionally excluded.
var adultTLDs = []string{".xxx", ".porn", ".sex", ".adult", ".cfd", ".sbs"}

// IsAdult reports whether the page is pornographic. Convenience wrapper over
// Classify for the boolean-only caller (the crawler).
func IsAdult(title, text, rawURL string) bool {
	adult, _, _ := Classify(title, text, rawURL)
	return adult
}

// Classify returns whether the page is adult, the number of DISTINCT explicit
// terms found (0 when the verdict came from a host/TLD match — see reason),
// and a short human-readable reason for dry-run / audit logging.
func Classify(title, text, rawURL string) (adult bool, distinctTerms int, reason string) {
	if host := hostOf(rawURL); host != "" {
		if t, ok := matchHost(host); ok {
			return true, 0, "host:" + t
		}
	}

	// Lexical: count DISTINCT explicit terms across title + body. The URL
	// path is intentionally NOT scanned — slugs and IDs are a false-positive
	// minefield (3xxx, record IDs, "anal.net") and the host signal already
	// covers the domain.
	found := map[string]struct{}{}
	collect := func(s string) {
		for _, tok := range tokenize(s) {
			if _, ok := explicitTerms[tok]; ok {
				found[tok] = struct{}{}
			}
		}
	}
	collect(title)
	body := text
	if len(body) > 200_000 {
		body = body[:200_000] // signal saturates; cap work on huge pages
	}
	collect(body)

	if len(found) >= 2 {
		return true, len(found), "lexical:" + strings.Join(sortedKeys(found), ",")
	}
	return false, len(found), ""
}

// tokenize lowercases and splits s into runs of Unicode letters OR digits.
// Keeping digits attached is what makes "3xxx" → "3xxx" (never "xxx") and
// "7fap3" → "7fap3" (never "fap"); treating í/é/… as letters keeps
// "analízis" a single token (never "anal").
func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// insertion sort — tiny maps, avoids importing sort for this alone
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func hostOf(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return strings.ToLower(rawURL) // maybe a bare host was passed
	}
	return strings.ToLower(u.Hostname())
}

// matchHost reports whether host is an adult host (brand fragment or adult
// TLD). TLDs are matched on the final label only, so a path/subdomain that
// happens to end in ".sex" elsewhere can't trip it.
func matchHost(host string) (string, bool) {
	for _, t := range hostTokens {
		if strings.Contains(host, t) {
			return t, true
		}
	}
	for _, tld := range adultTLDs {
		if strings.HasSuffix(host, tld) {
			return tld, true
		}
	}
	return "", false
}
