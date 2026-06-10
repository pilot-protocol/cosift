// Package authority scores indexed hosts on a [0, 1] authority scale.
//
// Search ranking multiplies BM25 / hybrid scores by (1 + alpha * authority)
// so high-authority hosts (Wikipedia, MDN, kernel.org, arxiv) float above
// the spam directories that dominate the long tail of the crawl. The score
// is a blend of:
//
//   - **Tranco / Majestic rank lookups** (optional CSV imports). Tranco
//     measures DNS-level popularity; Majestic measures referring-subnet
//     link diversity. Both are publicly downloadable.
//   - **TLD heuristics.** .gov / .edu / .mil / .ac.* / .nhs.uk lift the
//     score; ephemeral spam TLDs (.sbs, .cfd, .top, .xyz with subdomain
//     density) drag it down.
//   - **Subdomain-density penalty.** When 1,000+ subdomains share an
//     eTLD+1 and none are otherwise authoritative, it's a spam farm
//     (cutestat clones, lap.hu mirrors, programas-gratis directory
//     networks). Applied via SetSubdomainCount before Score.
//   - **Hand-curated whitelist** of well-known authoritative hosts so a
//     scorer with no external data still produces useful signals.
//
// The Scorer is read-mostly after construction; Score is safe to call
// from many goroutines. CSV loads happen at server startup and are
// gated by a single Load* call. No background mutations.
//
// Operators with no Tranco/Majestic CSVs available still get useful
// scoring from the embedded whitelist + TLD + subdomain-density signals.
package authority

import (
	"strings"
	"sync"

	"golang.org/x/net/publicsuffix"
)

// Scorer holds the data needed to rank a host. Zero value is unusable;
// construct via New.
type Scorer struct {
	// rank-style lookups; lower rank = more authoritative.
	tranco   map[string]int
	majestic map[string]int

	// per-host static lists.
	trusted map[string]bool

	// eTLD+1 → number of distinct subdomains observed in the corpus.
	// Populated externally (e.g. by the domain-audit job). High values
	// trigger the subdomain-farm penalty unless the eTLD+1 itself is
	// trusted.
	mu        sync.RWMutex
	subdomain map[string]int

	// Tunables, settable via env at construction:
	//   alpha — exposed for callers that want to know the multiplier
	//   used by Score; the actual multiplication is done at the call
	//   site, not here, so we just hold it for visibility.
	alpha float64
}

// New builds a Scorer pre-populated with the embedded whitelist and
// default suspect-TLD set. Call LoadTranco / LoadMajestic to add
// external rank data.
func New() *Scorer {
	t := make(map[string]bool, len(embeddedTrusted))
	for _, h := range embeddedTrusted {
		t[h] = true
	}
	return &Scorer{
		tranco:    map[string]int{},
		majestic:  map[string]int{},
		trusted:   t,
		subdomain: map[string]int{},
		alpha:     2.0,
	}
}

// Alpha is the multiplier callers should apply: final = base * (1 + alpha * Score(host)).
// Default 2.0 (top-of-list trusted host gets ~3x lift; junk stays at 1x).
func (s *Scorer) Alpha() float64 { return s.alpha }

// WithAlpha sets the recommended multiplier. Chainable.
func (s *Scorer) WithAlpha(a float64) *Scorer {
	if a >= 0 {
		s.alpha = a
	}
	return s
}

// SetSubdomainCounts replaces the eTLD+1 → host count map. Called by
// the audit job. Pass nil to clear the penalty.
func (s *Scorer) SetSubdomainCounts(counts map[string]int) {
	s.mu.Lock()
	if counts == nil {
		s.subdomain = map[string]int{}
	} else {
		s.subdomain = counts
	}
	s.mu.Unlock()
}

// Score returns a [0, 1] authority score for the host.
// Lookups are O(1) maps; safe from many goroutines.
func (s *Scorer) Score(host string) float64 {
	if s == nil || host == "" {
		return 0.5 // unknown defaults to neutral
	}
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "www.")

	score := 0.5

	// Embedded whitelist — generous bump.
	if s.trusted[host] || s.trusted[eTLD1(host)] {
		score += 0.4
	}

	// Tranco rank → log-scale boost. Top 100 → +0.3; top 1k → +0.2;
	// top 100k → +0.1; top 1M → +0.05.
	if r, ok := s.tranco[host]; ok && r > 0 {
		score += trancoBoost(r)
	} else if r, ok := s.tranco[eTLD1(host)]; ok && r > 0 {
		score += trancoBoost(r) * 0.8 // slight penalty for matching only the eTLD+1
	}

	// Majestic Trust Flow is 0-100 → up to +0.15.
	if tf, ok := s.majestic[host]; ok && tf > 0 {
		score += float64(tf) / 100.0 * 0.15
	} else if tf, ok := s.majestic[eTLD1(host)]; ok && tf > 0 {
		score += float64(tf) / 100.0 * 0.12
	}

	// TLD adjustments.
	t := tldOf(host)
	switch {
	case institutionalTLDs[t]:
		score += 0.2
	case suspectTLDs[t]:
		score -= 0.3
	}

	// .ac.* and .nhs.uk style — second-level institutional.
	if strings.HasSuffix(host, ".ac.uk") || strings.HasSuffix(host, ".ac.jp") ||
		strings.HasSuffix(host, ".nhs.uk") || strings.HasSuffix(host, ".gov.uk") {
		score += 0.15
	}

	// Subdomain-farm penalty: when the eTLD+1 has an absurd number of
	// distinct hosts, almost certainly a spam farm. Thresholds tuned
	// against the live GH200 distribution where the real spam clusters
	// (cutestat.com 831K, jiali.sbs 215K, meiru.cfd 182K, etc.) sit at
	// 100K+ while legitimate platforms (github.io, tumblr.com,
	// wordpress.com, bbc.co.uk) sit well below 10K. Below-10K gets a
	// mild penalty; below-1K gets none. Known major platforms are
	// further exempted via platformETLDs so a legit Tumblr/GitHub Pages
	// page isn't dragged down regardless of size.
	e := eTLD1(host)
	s.mu.RLock()
	subN := s.subdomain[e]
	s.mu.RUnlock()
	if !s.trusted[host] && !s.trusted[e] && !platformETLDs[e] {
		switch {
		case subN >= 100000:
			score -= 0.40 // unambiguous spam farm
		case subN >= 10000:
			score -= 0.30
		case subN >= 1000:
			score -= 0.10
		}
	}

	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}
	return score
}

// Multiplier is the convenience wrapper for callers that don't want to
// remember the formula: returned value is (1 + alpha * Score(host)),
// ready to multiply into a BM25 / hybrid score.
func (s *Scorer) Multiplier(host string) float64 {
	if s == nil {
		return 1.0
	}
	return 1.0 + s.alpha*s.Score(host)
}

// Stats is a snapshot of loaded data sizes for /stats publishing.
type Stats struct {
	TrustedHosts          int     `json:"trusted_hosts"`
	TrancoEntries         int     `json:"tranco_entries"`
	MajesticEntries       int     `json:"majestic_entries"`
	SubdomainCountEntries int     `json:"subdomain_count_entries"`
	Alpha                 float64 `json:"alpha"`
}

func (s *Scorer) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	s.mu.RLock()
	subN := len(s.subdomain)
	s.mu.RUnlock()
	return Stats{
		TrustedHosts:          len(s.trusted),
		TrancoEntries:         len(s.tranco),
		MajesticEntries:       len(s.majestic),
		SubdomainCountEntries: subN,
		Alpha:                 s.alpha,
	}
}

// trancoBoost converts a Tranco rank (1 = best, 1M = worst) into a
// monotone-decreasing additive score in [0, 0.3].
func trancoBoost(rank int) float64 {
	switch {
	case rank <= 100:
		return 0.30
	case rank <= 1_000:
		return 0.22
	case rank <= 10_000:
		return 0.16
	case rank <= 100_000:
		return 0.10
	case rank <= 1_000_000:
		return 0.05
	default:
		return 0.0
	}
}

// ETLD1 is the exported alias for eTLD1 so external callers (the
// in-server subdomain-count bootstrap, audit subcommand) can share the
// same publicsuffix-aware key with the Scorer. Without this, ad-hoc
// in-line parsers would produce different bucket keys and the lookup
// in Score would silently miss for compound suffixes (.co.uk, .com.au).
func ETLD1(host string) string { return eTLD1(host) }

// eTLD1 returns the registrable eTLD+1 (e.g. "bbc.co.uk" for
// "news.bbc.co.uk", "github.io" for "raft.github.io"). Backed by the
// IANA Public Suffix List bundled with golang.org/x/net/publicsuffix.
//
// On a parse failure (malformed host, empty input) we fall back to
// returning the input unchanged — the lookup just misses the cache,
// no panic. The previous naive "last 2 components" parser fired for
// most cases but mis-classified compound suffixes like .co.uk /
// .com.au / .ac.uk; live GH200 audit saw news.bbc.co.uk score 0.40
// instead of 0.90 because the trust-list lookup for "bbc.co.uk"
// missed (parser returned "co.uk").
func eTLD1(host string) string {
	if host == "" {
		return host
	}
	if e, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil && e != "" {
		return e
	}
	// publicsuffix returns an error for hosts that ARE themselves a
	// public suffix (e.g. "co.uk") or for IP addresses. Fall back to
	// the cheap last-two-components heuristic so the rest of the
	// scoring pipeline still gets a stable bucket key.
	parts := strings.Split(host, ".")
	if len(parts) < 2 {
		return host
	}
	return parts[len(parts)-2] + "." + parts[len(parts)-1]
}

// tldOf returns the rightmost dot-separated component, lowercased.
func tldOf(host string) string {
	i := strings.LastIndex(host, ".")
	if i < 0 || i == len(host)-1 {
		return ""
	}
	return strings.ToLower(host[i+1:])
}
