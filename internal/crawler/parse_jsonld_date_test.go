package crawler

import (
	"strings"
	"testing"
	"time"
)

func TestParseJSONLDDatePublished(t *testing.T) {
	html := `<!doctype html>
<html><head>
<script type="application/ld+json">
{"@type":"Article","headline":"Foo","datePublished":"2024-03-15T14:32:00Z"}
</script>
</head><body>body</body></html>`
	p, err := Parse([]byte(html), "https://x/")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.PublishedAt.IsZero() {
		t.Fatalf("PublishedAt should be set from datePublished")
	}
	want, _ := time.Parse(time.RFC3339, "2024-03-15T14:32:00Z")
	if !p.PublishedAt.Equal(want) {
		t.Errorf("PublishedAt: got %v want %v", p.PublishedAt, want)
	}
}

func TestParseJSONLDDateOnlyFormat(t *testing.T) {
	// Some sites publish dates as YYYY-MM-DD without a time component.
	html := `<!doctype html><html><head>
<script type="application/ld+json">{"@type":"Article","datePublished":"2024-03-15"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.PublishedAt.IsZero() {
		t.Fatalf("date-only format should parse")
	}
	if p.PublishedAt.Format("2006-01-02") != "2024-03-15" {
		t.Errorf("got %v", p.PublishedAt)
	}
}

func TestParseJSONLDDateModifiedFallback(t *testing.T) {
	// dateModified used when datePublished absent.
	html := `<!doctype html><html><head>
<script type="application/ld+json">{"@type":"Article","dateModified":"2024-03-15"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if p.PublishedAt.IsZero() {
		t.Errorf("dateModified should fall back when datePublished absent")
	}
}

func TestParseJSONLDDateMalformedIgnored(t *testing.T) {
	html := `<!doctype html><html><head>
<script type="application/ld+json">{"@type":"Article","datePublished":"not a real date"}</script>
</head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if !p.PublishedAt.IsZero() {
		t.Errorf("malformed date should yield zero PublishedAt, got %v", p.PublishedAt)
	}
}

func TestParseJSONLDNoDate(t *testing.T) {
	// Pages without JSON-LD or with no date field — PublishedAt stays zero.
	html := `<!doctype html><html><head><title>No date here</title></head><body>body</body></html>`
	p, _ := Parse([]byte(html), "https://x/")
	if !p.PublishedAt.IsZero() {
		t.Errorf("no date present should leave PublishedAt zero, got %v", p.PublishedAt)
	}
}

func TestParseJSONLDTimeHelper(t *testing.T) {
	cases := []struct {
		in   string
		want bool // true = should parse, false = should return zero
	}{
		{"", false},
		{"2024-03-15", true},
		{"2024-03-15T14:32:00Z", true},
		{"2024-03-15T14:32:00+02:00", true},
		{"not a date", false},
		{"2024/03/15", false}, // non-ISO format not supported
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := parseJSONLDTime(c.in)
			if c.want && got.IsZero() {
				t.Errorf("%q should have parsed", c.in)
			}
			if !c.want && !got.IsZero() {
				t.Errorf("%q should NOT have parsed; got %v", c.in, got)
			}
		})
	}
	_ = strings.TrimSpace // import keeper
}
