package crawler

import "testing"

func TestAllowedDomainMatchesOnDotBoundary(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		exclude []string
		url     string
		want    bool
	}{
		{"exact include", []string{"wired.com"}, nil, "https://wired.com/a", true},
		{"subdomain include", []string{"wired.com"}, nil, "https://www.wired.com/a", true},
		{"lookalike include", []string{"wired.com"}, nil, "https://notwired.com/a", false},
		{"lookalike prefix include", []string{"wired.com"}, nil, "https://wired.com.evil.example/a", false},
		{"include ignores URL port", []string{"wired.com"}, nil, "https://www.wired.com:8443/a", true},
		{"include is case-insensitive", []string{"Wired.COM"}, nil, "https://WWW.wired.com/a", true},
		{"include with leading dot", []string{".wired.com"}, nil, "https://wired.com/a", true},
		{"include entry port ignored", []string{"wired.com:8443"}, nil, "https://wired.com/a", true},
		{"include trailing dot host", []string{"wired.com"}, nil, "https://wired.com./a", true},
		{"include whitespace entry", []string{" wired.com "}, nil, "https://wired.com/a", true},
		{"empty include entry matches nothing", []string{""}, nil, "https://wired.com/a", false},
		{"exact exclude", nil, []string{"spam.example"}, "https://spam.example/a", false},
		{"subdomain exclude", nil, []string{"spam.example"}, "https://cdn.spam.example/a", false},
		{"exclude with explicit default port", nil, []string{"spam.example"}, "https://spam.example:443/a", false},
		{"exclude with explicit odd port", nil, []string{"spam.example"}, "http://spam.example:8080/a", false},
		{"lookalike exclude keeps legitimate host", nil, []string{"t.co"}, "https://microsoft.co/a", true},
		{"unrelated host passes exclude", nil, []string{"spam.example"}, "https://good.example/a", true},
		{"exclude wins over include", []string{"example.com"}, []string{"ads.example.com"}, "https://ads.example.com/a", false},
		{"empty exclude entry matches nothing", nil, []string{""}, "https://good.example/a", true},
		{"empty exclude entry does not match a host-less URL", nil, []string{""}, "http:///x", true},
		{"fullwidth spelling of an excluded host", []string{"example.com"}, []string{"spam.example.com"}, "http://\uff53\uff50\uff41\uff4d.example.com/a", false},
		{"ideographic full stops in an excluded host", nil, []string{"spam.example.com"}, "http://spam\u3002example\u3002com/a", false},
		{"ignorable character inside an excluded host", nil, []string{"spam.example.com"}, "http://sp\u200bam.example.com/a", false},
		{"trailing ideographic full stop on an excluded host", nil, []string{"spam.example.com"}, "http://spam.example.com\u3002/a", false},
		{"unicode exclude entry matches the punycode host", nil, []string{"b\u00fccher.example"}, "http://xn--bcher-kva.example/a", false},
		{"punycode exclude entry matches the unicode host", nil, []string{"xn--bcher-kva.example"}, "http://b\u00fccher.example/a", false},
		{"unicode include entry admits the punycode host", []string{"b\u00fccher.example"}, nil, "http://xn--bcher-kva.example/a", true},
		{"fullwidth lookalike of an included host", []string{"example.com"}, nil, "http://\uff45xample.com/a", true},
		{"unmappable host is refused", nil, nil, "http://a\u00a0b.example/a", false},
		{"unicode host stays allowed", nil, []string{"spam.example"}, "http://b\u00fccher.example/a", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testCrawlerCfg()
			cfg.IncludeDomains, cfg.ExcludeDomains = tc.include, tc.exclude
			if got := newBare(cfg).allowedDomain(tc.url); got != tc.want {
				t.Fatalf("allowedDomain(%s) include=%v exclude=%v = %v, want %v", tc.url, tc.include, tc.exclude, got, tc.want)
			}
		})
	}
}

func TestAllowedDomainDynamicListIgnoresPort(t *testing.T) {
	c := newBare(testCrawlerCfg())
	c.cfg.IncludeDomains = []string{"arxiv.org"}
	c.dynDomains.Store("example.com", struct{}{})
	for url, want := range map[string]bool{
		"https://example.com:8443/a":    true,
		"https://blog.example.com/a":    true,
		"https://attacker-example.com/": false,
	} {
		if got := c.allowedDomain(url); got != want {
			t.Errorf("allowedDomain(%s) = %v, want %v", url, got, want)
		}
	}
}
