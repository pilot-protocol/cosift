package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/work"
)

func TestHeadAndTailKeepsTheEndsOfALongText(t *testing.T) {
	short := strings.Repeat("a", workModerationText)
	if headAndTail(short) != short {
		t.Fatal("a text at the limit is cut")
	}
	long := strings.Repeat("é", 30000) + "MIDDLE" + strings.Repeat("ü", 30000)
	got := headAndTail(long)
	if !utf8.ValidString(got) || !strings.Contains(got, workModerationJoin) || strings.Contains(got, "MIDDLE") {
		t.Fatal("not cut on character boundaries around the join")
	}
	head, tail, _ := strings.Cut(got, workModerationJoin)
	if len(head) > workModerationHead || len(tail) > workModerationTail || !strings.HasPrefix(head, "é") || !strings.HasSuffix(tail, "ü") {
		t.Fatalf("head %d tail %d", len(head), len(tail))
	}
	if len(got) > 32000 {
		t.Fatalf("%d bytes go to the model", len(got))
	}
}

type recordingChat struct {
	seen  []string
	reply string
}

func (c *recordingChat) Model() string { return "safety-test" }
func (c *recordingChat) Chat(_ context.Context, msgs []embed.ChatMsg) (string, error) {
	c.seen = append(c.seen, msgs[1].Content)
	return c.reply, nil
}

func TestWorkModerationDecidesWithoutTheModelWhenItCan(t *testing.T) {
	chat := &recordingChat{reply: `{"decision":"allow","category":"safe"}`}
	s := &pebbleHTTP{chat: chat}
	reserved := 0
	reserve := func() error { reserved++; return nil }
	filler := distinctProse(600)
	tooLong := func(middle string) work.ModerationDoc {
		return work.ModerationDoc{URL: "https://example.org/p", Title: "Tidal power", Text: filler + middle + filler}
	}
	v, err := s.workModerate(context.Background(), tooLong("creampie and gangbang scenes. "), reserve)
	if err != nil || v != (work.Verdict{Decision: "reject", Category: "adult"}) || reserved != 0 || len(chat.seen) != 0 {
		t.Fatalf("an adult page in the part the model never sees: %+v %v reserved %d", v, err, reserved)
	}
	v, err = s.workModerate(context.Background(), work.ModerationDoc{URL: "https://www.pornhub.com/view", Title: "t", Text: filler}, reserve)
	if err != nil || v.Category != "adult" || reserved != 0 {
		t.Fatalf("%+v %v", v, err)
	}
	v, err = s.workModerate(context.Background(), work.ModerationDoc{URL: "https://example.org/p", Title: "Just a moment...", Text: "Checking your browser before accessing the site."}, reserve)
	if err != nil || v != (work.Verdict{Decision: "uncertain", Category: "unverified"}) || reserved != 0 || len(chat.seen) != 0 {
		t.Fatalf("a bot-check page: %+v %v", v, err)
	}
	v, err = s.workModerate(context.Background(), work.ModerationDoc{URL: "https://example.org/p", Title: "Domain for sale", Text: "A parked page about nothing."}, reserve)
	if err != nil || v != (work.Verdict{Decision: "reject", Category: "low_quality"}) || reserved != 0 {
		t.Fatalf("a parked page: %+v %v", v, err)
	}
}

func TestWorkModerationAsksTheModelForTheHeadAndTailUnderTheCeiling(t *testing.T) {
	chat := &recordingChat{reply: `{"decision":"allow","category":"safe"}`}
	s := &pebbleHTTP{chat: chat}
	reserved := 0
	filler := distinctProse(600)
	doc := work.ModerationDoc{URL: "https://example.org/p", Title: strings.Repeat("t", 1500), Text: filler + "MIDDLE MARKER " + filler}
	v, err := s.workModerate(context.Background(), doc, func() error { reserved++; return nil })
	if err != nil || v != (work.Verdict{Decision: "allow", Category: "safe"}) || reserved != 1 || len(chat.seen) != 1 {
		t.Fatalf("%+v %v reserved %d", v, err, reserved)
	}
	if strings.Contains(chat.seen[0], "MIDDLE MARKER") || strings.Count(chat.seen[0], "t") == 0 || len(chat.seen[0]) > 40000 {
		t.Fatal("the model saw the middle of the page, or too much")
	}
	ceiling := &work.CeilingError{Retry: 5}
	_, err = s.workModerate(context.Background(), doc, func() error { return ceiling })
	var got *work.CeilingError
	if !errors.As(err, &got) || got != ceiling || len(chat.seen) != 1 {
		t.Fatalf("the ceiling must stop the model call: %v", err)
	}
	s.chat = nil
	if _, err := s.workModerate(context.Background(), doc, func() error { t.Fatal("reserved without a model"); return nil }); err == nil {
		t.Fatal("no model, no verdict")
	}
}

func TestWorkCanonicalAcceptsOnlyPublicWebURLsInTheCrawlersForm(t *testing.T) {
	for raw, want := range map[string]string{
		"https://Example.org/a?utm_source=x&b=2#frag": "https://example.org/a?b=2",
		"http://example.org":                          "http://example.org",
		"https://example.org:443/x":                   "https://example.org:443/x",
	} {
		if got, err := workCanonical(raw); err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"http://127.0.0.1/x", "http://localhost/x", "ftp://example.org/x", "https://user:pw@example.org/x", "https://example.org:8080/x",
		"https://intranet/x", "https://example.internal/x", "https://bücher.example/x", "https://www.pornhub.com/view", "https://example.org/setup.exe", strings.Repeat("a", 2049)} {
		if got, err := workCanonical(raw); err == nil {
			t.Errorf("%s accepted as %q", raw, got)
		}
	}
}

func distinctProse(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "Section %d covers topic %d with details about unit %d of the tidal array. ", i, i*7+3, i*13+5)
	}
	return b.String()
}

func TestAServerCopyThatIsNotARealPageIsUnusable(t *testing.T) {
	prose := distinctProse(30)
	for _, tc := range []struct {
		title, text string
		want        bool
	}{
		{"Just a moment...", distinctProse(5), true},
		{"Access denied", "You do not have permission to view this page.", true},
		{"Domain for sale", "This domain may be for sale.", true},
		{strings.Repeat("Keyword stuffing ", 3), strings.Repeat("buy cheap widgets online today with free shipping from our trusted store ", 40), true},
		{"Tidal power", prose, false},
	} {
		if got := workUnusableCopy(tc.title, tc.text); got != tc.want {
			t.Errorf("%q: %v", tc.title, got)
		}
	}
}

func TestLoadWorkConfigRefusesABadBlockOrFlag(t *testing.T) {
	ptr := func(n int) *int { return &n }
	for _, tc := range []struct {
		name string
		cfg  config.Work
		env  map[string]string
		want string
	}{
		{"lease ttl", config.Work{LeaseTTLS: ptr(59)}, nil, "work.lease_ttl_s"},
		{"moderation ceiling above its maximum", config.Work{WorkModerationsPerDay: ptr(1001)}, nil, "work.work_moderations_per_day"},
		{"attestation ttl above a week", config.Work{AttestationTTLH: ptr(169)}, nil, "work.attestation_ttl_h"},
		{"enabled flag", config.Work{}, map[string]string{"COSIFT_WORK_ENABLED": "yes"}, "COSIFT_WORK_ENABLED"},
		{"frontier flag", config.Work{}, map[string]string{"COSIFT_WORK_FRONTIER": "2"}, "COSIFT_WORK_FRONTIER"},
	} {
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
		_, err := loadWorkConfig(&config.Config{Work: tc.cfg})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.name, err)
		}
		for k := range tc.env {
			t.Setenv(k, "")
		}
	}
	t.Setenv("COSIFT_WORK_ENABLED", "1")
	t.Setenv("COSIFT_WORK_FRONTIER", "0")
	h, err := loadWorkConfig(&config.Config{})
	if err != nil || !h.enabled || h.frontier || h.settings.LeaseTTLS != 600 {
		t.Fatalf("%+v %v", h, err)
	}
}
