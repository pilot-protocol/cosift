package sharedaccount

import (
	"strings"
	"testing"
	"time"
)

func TestAlertDedupKeysAreTheDocumentedVectors(t *testing.T) {
	for _, c := range []struct{ kind, subject, want string }{
		{"work_moderation_ceiling", "2026-10-12", "256cd67205883c8ec3e5570b"},
		{"work_moderation_ceiling", "2026-10-12/moderate", "f3047c52a708bc48ff602eb2"},
		{"intake_ceiling", "2026-10-12", "2bf188952aa5cfcabb4fe957"},
		{"intake_failure_spike", "2026-10-12", "54320cadfd4e5908c452fe03"},
		{"outbox_stuck", "2026-10-12", "f1951c478e280d47b738ad69"},
	} {
		if got := AlertDedupKey(c.kind, c.subject); got != c.want {
			t.Errorf("%s %s: %s, want %s", c.kind, c.subject, got, c.want)
		}
	}
}

func TestLeakDetectorsAreBoundedAndSeeThroughObfuscation(t *testing.T) {
	tid := strings.Repeat("ab", 32)
	spaced := ""
	for i := 0; i < 64; i += 8 {
		spaced += tid[i:i+8] + " "
	}
	for _, c := range []struct {
		name string
		s    string
		leak bool
	}{
		{"63 hex", tid[:63], false},
		{"64 hex", tid, true},
		{"65 hex", tid + "c", true},
		{"64 digits", strings.Repeat("1", 64), true},
		{"63 digits", strings.Repeat("1", 63), false},
		{"5 digits", "code 12345", false},
		{"6 digits bare", "123456", true},
		{"7 digits", "1234567", false},
		{"two 6-digit runs", "481516 and 234235", true},
		{"address", "ops@example.com", true},
		{"plain", "example.com queue stalled for 45m", false},
		{"fullwidth at", "raised for reader＠example.com", true},
		{"small commercial at", "raised for reader﹫example.com", true},
		{"bracketed at", "raised for reader [at] example.com", true},
		{"parenthesised at", "raised for reader (at) example.com", true},
		{"tid split by a dash", tid[:32] + "-" + tid[32:], true},
		{"tid split every eight", spaced, true},
		{"tid split by newlines", strings.ReplaceAll(spaced, " ", "\n"), true},
		{"code split by a space", "code 481 516 now", true},
		{"code split by a hyphen", "code 481-516", true},
		{"code zero padded to seven", "code 0481516 now", false},
		{"code in fullwidth digits", "code ４８１５１６", true},
		{"a grouped six-digit count", "queued 250 000 urls", true},
		{"a version number", "cosift-mcp 1.2.3 on 4 instances", false},
		{"a host that spells at", "crawl errors at example.com", false},
		{"a ceiling", "global ceiling 100000 reached", true},
		{"a rephrased ceiling", "global ceiling 100k reached", false},
	} {
		if got := checkLeak("field", c.s) != nil; got != c.leak {
			t.Errorf("%s: leak = %t, want %t", c.name, got, c.leak)
		}
	}
}

func TestAnAlertIsScannedAcrossItsFieldsAndNeverEchoesTheMatch(t *testing.T) {
	tid := strings.Repeat("cd", 32)
	a := Alert{Kind: "work_moderation_ceiling", Severity: "warning", Subject: "2026-10-12", SubjectKind: "day", Title: "Work moderation ceiling reached", Detail: "Fetch-work moderation calls are halted for the rest of the UTC day.", Now: time.Now(),
		Labels: map[string]string{"first": tid[:32], "second": tid[32:]}}
	err := a.Validate()
	if err == nil {
		t.Fatal("a tid split across two labels was accepted")
	}
	if strings.Contains(err.Error(), tid[:8]) {
		t.Fatalf("the refusal echoes the match: %v", err)
	}
}

// The leak vectors of the work_moderation_ceiling alert rows, as the dash's own validator accepts and refuses them.
func TestTheWorkModerationAlertLeakVectors(t *testing.T) {
	const (
		title      = "Work moderation ceiling reached"
		fetchBody  = "Fetch-work moderation calls are halted for the rest of the UTC day."
		moderate   = "Community moderation calls are halted for the rest of the UTC day."
		labelCode  = "label ceiling looks like a one-time code"
		detailCode = "detail looks like a one-time code"
	)
	for _, c := range []struct {
		name, subject, detail string
		labels                map[string]string
		refusal               string
	}{
		{"work moderation", "2026-10-12", fetchBody, map[string]string{"environment": "prod"}, ""},
		{"work moderation, ceiling", "2026-10-12", fetchBody, map[string]string{"ceiling": "500 calls", "environment": "prod"}, ""},
		{"the moderate route's ceiling", "2026-10-12/moderate", moderate, map[string]string{"ceiling": "1000 calls", "environment": "prod"}, ""},
		{"a bare six-digit ceiling", "2026-10-12", fetchBody, map[string]string{"ceiling": "100000"}, labelCode},
		{"a cid whose digits make a run", "2026-10-12", "Contributor a481516bcdef0123 has awards pending.", nil, detailCode},
	} {
		a := Alert{Kind: "work_moderation_ceiling", Severity: "warning", Subject: c.subject, SubjectKind: "day", Title: title, Detail: c.detail, Labels: c.labels, Now: time.Now()}
		err := a.Validate()
		switch {
		case c.refusal == "" && err != nil:
			t.Errorf("%s: refused (%v)", c.name, err)
		case c.refusal != "" && (err == nil || !strings.HasSuffix(err.Error(), c.refusal)):
			t.Errorf("%s: err = %v, want the refusal %q", c.name, err, c.refusal)
		}
	}
}

func TestAnAlertOutsideTheCommunityKindsAndBoundsIsRefused(t *testing.T) {
	good := Alert{Kind: "work_moderation_ceiling", Severity: "warning", Subject: "2026-10-12", SubjectKind: "day", Title: "t", Detail: "d", Now: time.Now()}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Alert){
		"kind":         func(a *Alert) { a.Kind = "intake_ceiling" },
		"severity":     func(a *Alert) { a.Severity = "fatal" },
		"subject kind": func(a *Alert) { a.SubjectKind = "planet" },
		"no subject":   func(a *Alert) { a.Subject = "" },
		"long subject": func(a *Alert) { a.Subject = strings.Repeat("s", 254) },
		"no title":     func(a *Alert) { a.Title = "" },
		"long title":   func(a *Alert) { a.Title = strings.Repeat("t", 121) },
		"long detail":  func(a *Alert) { a.Detail = strings.Repeat("d", 2001) },
		"no time":      func(a *Alert) { a.Now = time.Time{} },
		"many labels": func(a *Alert) {
			a.Labels = map[string]string{}
			for i := 0; i < 21; i++ {
				a.Labels[strings.Repeat("k", i+1)] = "v"
			}
		},
	} {
		a := good
		mutate(&a)
		if a.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
