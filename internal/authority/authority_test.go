package authority

import (
	"strings"
	"testing"
)

func TestScoreEmbeddedTrusted(t *testing.T) {
	s := New()
	cases := []struct {
		host    string
		minWant float64
	}{
		{"raft.github.io", 0.85},
		{"wikipedia.org", 0.85},
		{"en.wikipedia.org", 0.85}, // inherits from etld+1
		{"docs.python.org", 0.85},
		{"kernel.org", 0.85},
		{"datatracker.ietf.org", 0.85},
	}
	for _, c := range cases {
		got := s.Score(c.host)
		if got < c.minWant {
			t.Errorf("%s: got %.2f, want ≥ %.2f", c.host, got, c.minWant)
		}
	}
}

func TestScoreSuspectTLD(t *testing.T) {
	s := New()
	cases := []string{
		"sketchy.sbs", "freeshop.cfd", "fakevpn.top",
		"random.xyz", "deal.click", "wow.icu",
	}
	for _, h := range cases {
		got := s.Score(h)
		if got >= 0.4 {
			t.Errorf("%s: got %.2f, want < 0.4", h, got)
		}
	}
}

func TestScoreInstitutional(t *testing.T) {
	s := New()
	hosts := []string{"unknown.gov", "obscure.edu", "random.mil"}
	for _, h := range hosts {
		got := s.Score(h)
		if got < 0.65 {
			t.Errorf("%s: got %.2f, want ≥ 0.65 (institutional TLD)", h, got)
		}
	}
}

func TestScoreSubdomainFarm(t *testing.T) {
	s := New()
	s.SetSubdomainCounts(map[string]int{
		"cutestat.com":   831000, // observed real GH200 number
		"jiali.sbs":      215000,
		"genericfarm.io": 12000, // legitimate-TLD farm
		"safebrand.io":   500,   // below the 1000 threshold
	})
	if got := s.Score("foo.cutestat.com"); got >= 0.25 {
		t.Errorf("real spam farm not blocked: got %.2f want < 0.25", got)
	}
	if got := s.Score("bar.jiali.sbs"); got >= 0.25 {
		t.Errorf("real spam farm not blocked: got %.2f want < 0.25", got)
	}
	if got := s.Score("api.safebrand.io"); got < 0.4 {
		t.Errorf("low-density site over-penalized: got %.2f", got)
	}
}

// TestPlatformETLDExempt locks in the GH200 false-positive fix:
// legitimate content platforms with many subdomains (GitHub Pages,
// Tumblr, WordPress, BBC sub-sites) must not be classified as spam
// farms.
func TestPlatformETLDExempt(t *testing.T) {
	s := New()
	s.SetSubdomainCounts(map[string]int{
		"github.io":     20000, // hypothetically high
		"tumblr.com":    50000,
		"wordpress.com": 100000,
		"bbc.co.uk":     500,
	})
	cases := []string{
		"clojure.github.io",
		"scala.github.io",
		"www.tumblr.com",
		"pythonwise.blogspot.com",
		"news.bbc.co.uk",
		"bandcamp.com",
		"itch.io",
	}
	for _, host := range cases {
		got := s.Score(host)
		if got < 0.40 {
			t.Errorf("legit platform %s classified as block: %.2f", host, got)
		}
	}
}

func TestSubdomainPenaltyNotAppliedToTrustedEtld1(t *testing.T) {
	s := New()
	// Many github.io subdomains, but github.com is trusted.
	s.SetSubdomainCounts(map[string]int{"github.io": 10000})
	if got := s.Score("raft.github.io"); got < 0.85 {
		t.Errorf("trusted eTLD+1 unfairly penalized: got %.2f", got)
	}
}

func TestLoadTranco(t *testing.T) {
	s := New()
	csv := `1,google.com
2,youtube.com
3,facebook.com
500,example.org
50000,niche-site.io
`
	n, err := s.LoadTranco(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("LoadTranco: %v", err)
	}
	if n != 5 {
		t.Errorf("loaded %d, want 5", n)
	}
	if got := s.Score("google.com"); got < 0.85 {
		t.Errorf("google.com (rank 1): %.2f", got)
	}
	if got := s.Score("niche-site.io"); got < 0.55 {
		t.Errorf("niche-site.io (rank 50k): %.2f want > 0.55", got)
	}
}

func TestLoadMajestic(t *testing.T) {
	s := New()
	csv := `GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,PrevGlobalRank,TrustFlow
1,1,google.com,com,500000,1000000,google.com,com,1,99
50,3,example.org,org,1000,5000,example.org,org,52,75
`
	n, err := s.LoadMajestic(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("LoadMajestic: %v", err)
	}
	if n != 2 {
		t.Errorf("loaded %d, want 2", n)
	}
	if s.majestic["google.com"] != 99 {
		t.Errorf("google.com TF: got %d, want 99", s.majestic["google.com"])
	}
}

func TestMultiplier(t *testing.T) {
	s := New().WithAlpha(2.0)
	// Trusted: 0.9 → mult 2.8
	if m := s.Multiplier("raft.github.io"); m < 2.5 {
		t.Errorf("trusted multiplier: got %.2f, want ≥ 2.5", m)
	}
	// Junk: ~0.2 → mult ~1.4
	if m := s.Multiplier("sketchy.sbs"); m > 1.5 {
		t.Errorf("junk multiplier: got %.2f, want ≤ 1.5", m)
	}
	// Nil receiver = passthrough
	var nilS *Scorer
	if m := nilS.Multiplier("anything"); m != 1.0 {
		t.Errorf("nil multiplier: got %.2f, want 1.0", m)
	}
}

func TestETLD1(t *testing.T) {
	cases := map[string]string{
		"x.y.example.com": "example.com",
		"docs.python.org": "python.org",
		"example.com":     "example.com",
		"raft.github.io":  "raft.github.io", // github.io is a public suffix → site is the eTLD+1
		"localhost":       "localhost",
		// The bug we shipped publicsuffix to fix:
		"news.bbc.co.uk":      "bbc.co.uk",
		"www.bbc.co.uk":       "bbc.co.uk",
		"docs.example.com.au": "example.com.au",
		"www.gov.uk":          "www.gov.uk", // gov.uk is the suffix; www is the registrable
		"foo.ac.jp":           "foo.ac.jp",
	}
	for in, want := range cases {
		if got := eTLD1(in); got != want {
			t.Errorf("eTLD1(%q) = %q want %q", in, got, want)
		}
	}
}

// TestBBCCoUKTrusted locks in the publicsuffix fix: news.bbc.co.uk
// must inherit the trusted boost from bbc.co.uk in the embedded list.
// Pre-fix, the naive parser returned eTLD+1=co.uk so the host lookup
// missed and the score landed at 0.40 instead of 0.90.
func TestBBCCoUKTrusted(t *testing.T) {
	s := New()
	hosts := []string{"news.bbc.co.uk", "www.bbc.co.uk", "bbc.co.uk", "bbc.com"}
	for _, h := range hosts {
		if got := s.Score(h); got < 0.85 {
			t.Errorf("%s: got %.2f, want ≥ 0.85", h, got)
		}
	}
}

func TestNilScorerSafe(t *testing.T) {
	var s *Scorer
	if got := s.Score("anything"); got != 0.5 {
		t.Errorf("nil Score: got %.2f, want 0.5 (neutral)", got)
	}
	if (s.Stats() != Stats{}) {
		t.Errorf("nil Stats not zero")
	}
}
