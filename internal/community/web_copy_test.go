package community

import (
	"regexp"
	"testing"
)

func TestStaticPageDoesNotPromiseTheReward(t *testing.T) {
	html, err := assets.ReadFile("web/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if m := regexp.MustCompile(`(?i)\bearn(?:s|ing)?\b|\d+ credits for new`).Find(html); m != nil {
		t.Fatalf("index.html hard-codes a reward promise: %q", m)
	}
	js, err := assets.ReadFile("web/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if m := regexp.MustCompile(`Sign in to contribute webpages and earn credits`).Find(js); m != nil {
		t.Fatalf("app.js notices promise a reward: %q", m)
	}
}
