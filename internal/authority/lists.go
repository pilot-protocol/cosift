package authority

// institutionalTLDs reliably indicate vetted institutional content.
// Most countries' .gov.* are second-level (handled separately in
// authority.go); these are the unambiguous top-level forms.
var institutionalTLDs = map[string]bool{
	"gov": true, "edu": true, "mil": true,
}

// suspectTLDs are the TLDs disproportionately associated with the
// short-lived spam directories observed in our corpus. Inclusion here
// drags the authority score by 0.3 — operators can override by
// whitelisting specific hosts that happen to live on these TLDs.
//
// Curated from Spamhaus, Surbl, and direct corpus observation
// (cutestat / heisi / fennen subdomain farms; bandwidth-throttling
// VPN-review sites that camp on .sbs / .cfd / .top).
var suspectTLDs = map[string]bool{
	"sbs": true, "cfd": true, "top": true, "xyz": true, "biz": true,
	"online": true, "site": true, "website": true, "store": true,
	"fun": true, "tk": true, "ml": true, "ga": true, "cf": true,
	"monster": true, "loan": true, "work": true, "mom": true, "bond": true,
	"click": true, "quest": true, "rest": true, "fyi": true,
	"buzz": true, "gq": true, "support": true, "lol": true, "icu": true,
}

// platformETLDs identifies major content platforms that legitimately
// have many subdomains in our corpus — GitHub Pages, Tumblr, WordPress,
// blog hosts, indie game / music platforms, BBC sub-sites. These get
// EXEMPTED from the subdomain-density penalty (a real spam-farm signal
// for unknown eTLD+1s) but don't get the trusted-list boost. They
// score from baseline 0.5 — let TLD heuristics + per-host signals
// (Tranco/Majestic when loaded) sort them out.
//
// Curated from observed false positives: pre-platforms list, the live
// audit was flagging clojure.github.io, scala.github.io, news.bbc.co.uk,
// pythonwise.blogspot.com, www.tumblr.com, bandcamp.com, itch.io,
// wordpress.com all as "block."
var platformETLDs = map[string]bool{
	"github.io":       true,
	"gitlab.io":       true,
	"netlify.app":     true,
	"vercel.app":      true,
	"pages.dev":       true,
	"web.app":         true,
	"firebaseapp.com": true,
	"appspot.com":     true,
	"herokuapp.com":   true,
	"glitch.me":       true,
	"replit.dev":      true,
	"repl.co":         true,
	"deno.dev":        true,
	"fly.dev":         true,
	"workers.dev":     true,
	"neocities.org":   true,

	"tumblr.com":      true,
	"wordpress.com":   true,
	"blogspot.com":    true,
	"substack.com":    true,
	"medium.com":      true,
	"ghost.io":        true,
	"mastodon.social": true,
	"mastodon.online": true,

	"bandcamp.com":   true,
	"itch.io":        true,
	"soundcloud.com": true,

	"fandom.com": true,
	"wikia.com":  true,
	"sched.com":  true,

	"bbc.co.uk": true,
	"bbc.com":   true,

	"reddit.com": true,
	"quora.com":  true,
	"medium.dev": true,

	"shopify.com":     true,
	"squarespace.com": true,
	"wixsite.com":     true,
	"weebly.com":      true,

	"amazonaws.com":  true,
	"cloudfront.net": true,
	"akamaihd.net":   true,
	"akamaized.net":  true,
	"fastly.net":     true,
}

// embeddedTrusted is the curated allow-list shipped in the binary so
// that a scorer with no external Tranco/Majestic data still produces
// useful signals from day one. Selected for: documentation primary
// sources (kernel.org, postgresql.org), academic gateways (arxiv,
// pubmed, aclanthology), encyclopedic content (wikipedia, mdn),
// major code hosts (github, gitlab), recognized news / standards
// bodies (ietf, w3, iana), and the top consumer platforms our
// answer-quality eval consistently surfaced.
//
// Entries are eTLD+1 form (no www.) so a subdomain still benefits
// (docs.python.org inherits from python.org). Concrete subdomains
// also listed when the eTLD+1 hosts mixed quality content
// (raft.github.io vs random *.github.io repos).
var embeddedTrusted = []string{
	// Encyclopedic
	"wikipedia.org", "wikimedia.org", "wiktionary.org",
	"plato.stanford.edu", "britannica.com",
	// Standards bodies
	"ietf.org", "iana.org", "w3.org", "rfc-editor.org", "datatracker.ietf.org",
	// Code hosts (eTLD+1 only — repo content varies wildly)
	"github.com", "gitlab.com", "bitbucket.org", "sourceforge.net",
	"raft.github.io",
	// Academic / research
	"arxiv.org", "aclanthology.org", "dblp.org", "dblp.uni-trier.de",
	"pubmed.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov", "nih.gov",
	"openalex.org", "crossref.org", "semanticscholar.org",
	// Major universities / labs (US)
	"mit.edu", "harvard.edu", "berkeley.edu", "cmu.edu", "stanford.edu",
	"princeton.edu", "yale.edu", "caltech.edu", "uchicago.edu", "columbia.edu",
	// Major universities (intl)
	"ox.ac.uk", "cam.ac.uk", "ethz.ch", "epfl.ch", "u-tokyo.ac.jp",
	// Operating-system & language docs
	"kernel.org", "linux.org", "freebsd.org",
	"python.org", "rust-lang.org", "golang.org", "ruby-lang.org",
	"php.net", "nodejs.org", "openjdk.org", "kotlinlang.org", "scala-lang.org",
	"haskell.org", "ocaml.org", "elixir-lang.org", "erlang.org",
	// Databases / infra
	"postgresql.org", "redis.io", "mongodb.com", "elastic.co", "clickhouse.com",
	"sqlite.org", "mariadb.org",
	// Cloud / kubernetes
	"kubernetes.io", "k8s.io", "cncf.io", "istio.io", "envoyproxy.io",
	"docker.com", "podman.io", "containerd.io",
	"aws.amazon.com", "cloud.google.com", "azure.microsoft.com",
	"hashicorp.com", "kubevault.com",
	// Browser / web platform
	"mozilla.org", "developer.mozilla.org", "chromium.org",
	"webkit.org", "whatwg.org",
	// Vendor docs
	"developer.apple.com", "developer.android.com", "learn.microsoft.com",
	"docs.python.org", "doc.rust-lang.org", "pkg.go.dev", "godoc.org",
	"docs.docker.com", "kubernetes.io",
	"developer.arm.com", "intel.com", "nvidia.com", "ibm.com",
	// News / journalism (mainstream)
	"nytimes.com", "washingtonpost.com", "bbc.com", "bbc.co.uk", "reuters.com",
	"theguardian.com", "ft.com", "economist.com", "nature.com", "science.org",
	"acm.org", "ieee.org",
	// Q&A and dev-community
	"stackoverflow.com", "stackexchange.com", "lobste.rs",
	// Reference / patents
	"patents.google.com", "uspto.gov", "epo.org", "wipo.int",
	// Big tech docs / blogs
	"google.com", "googleblog.com", "engineering.fb.com",
	"netflixtechblog.com", "uber.com", "stripe.com",
	// Health
	"who.int", "cdc.gov", "mayoclinic.org", "nhs.uk", "medlineplus.gov",
	// Finance / government data
	"sec.gov", "federalreserve.gov", "treasury.gov", "irs.gov", "data.gov",
	"ecb.europa.eu", "europa.eu",
	// Browsers / archives
	"archive.org", "web.archive.org",
	// Misc reputable
	"redis.antirez.com", "kernel.dk",
}
