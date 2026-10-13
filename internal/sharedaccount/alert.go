package sharedaccount

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const (
	alertSource     = "community"
	maxAlertSubject = 253
	maxAlertTitle   = 120
	maxAlertDetail  = 2000
	maxAlertLabels  = 20
)

var (
	alertKinds    = map[string]bool{"work_moderation_ceiling": true}
	severities    = map[string]bool{"info": true, "warning": true, "critical": true}
	subjectKinds  = map[string]bool{"host": true, "account": true, "day": true, "domain": true, "global": true}
	atSpellings   = []string{"[at]", "(at)", "{at}", "[@]"}
	digitSepBytes = " -_.,"
)

// Alert is one occurrence of an alerts/{dedup_key} document; the source is always community.
type Alert struct {
	Kind, Severity, Subject, SubjectKind, Title, Detail string
	Labels                                              map[string]string
	Now                                                 time.Time
}

// AlertDedupKey is the document id, sha256(kind NUL subject) cut to 24 hex characters.
func AlertDedupKey(kind, subject string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + subject))
	return hex.EncodeToString(sum[:])[:24]
}

func (a Alert) Validate() error {
	switch {
	case !alertKinds[a.Kind]:
		return fmt.Errorf("%w: alert kind", ErrInvalid)
	case !severities[a.Severity]:
		return fmt.Errorf("%w: severity", ErrInvalid)
	case !subjectKinds[a.SubjectKind]:
		return fmt.Errorf("%w: subject_kind", ErrInvalid)
	case a.Subject == "" || len(a.Subject) > maxAlertSubject:
		return fmt.Errorf("%w: subject", ErrInvalid)
	case a.Title == "" || len(a.Title) > maxAlertTitle:
		return fmt.Errorf("%w: title", ErrInvalid)
	case len(a.Detail) > maxAlertDetail:
		return fmt.Errorf("%w: detail", ErrInvalid)
	case len(a.Labels) > maxAlertLabels:
		return fmt.Errorf("%w: labels", ErrInvalid)
	case a.Now.IsZero():
		return fmt.Errorf("%w: time", ErrInvalid)
	}
	return a.checkLeaks()
}

// checkLeaks scans each field, then every value together, so a token id split across two labels still shows.
func (a Alert) checkLeaks() error {
	type field struct{ name, value string }
	fields := []field{{"subject", a.Subject}, {"title", a.Title}, {"detail", a.Detail}}
	for _, k := range slices.Sorted(maps.Keys(a.Labels)) {
		fields = append(fields, field{"label key", k}, field{"label " + k, a.Labels[k]})
	}
	values := make([]string, 0, len(fields))
	for _, f := range fields {
		if err := checkLeak(f.name, f.value); err != nil {
			return err
		}
		if f.name != "label key" {
			values = append(values, f.value)
		}
	}
	return checkLeak("alert", strings.Join(values, " "))
}

// checkLeak refuses an address, a token id and a one-time code; the error names the field, never the match.
func checkLeak(field, s string) error {
	scan := strings.ToLower(norm.NFKC.String(s))
	for _, spelling := range atSpellings {
		scan = strings.ReplaceAll(scan, spelling, "@")
	}
	switch {
	case strings.Contains(scan, "@"):
		return fmt.Errorf("%w: %s must not contain an address", ErrInvalid, field)
	case hasHexRun(scan, 64), hasSplitTokenID(scan):
		return fmt.Errorf("%w: %s looks like a token id", ErrInvalid, field)
	case hasDigitRun(scan, 6):
		return fmt.Errorf("%w: %s looks like a one-time code", ErrInvalid, field)
	}
	return nil
}

func isHex(r rune) bool { return r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F' }

func hasHexRun(s string, n int) bool {
	run := 0
	for _, r := range s {
		if isHex(r) {
			if run++; run >= n {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}

// hasSplitTokenID bridges fragments of at least four hex characters, and needs a letter in the run so "63 hex a" and long numbers pass.
func hasSplitTokenID(s string) bool {
	const minFragment = 4
	run, letters, frag, fragLetters, found := 0, 0, 0, 0, false
	end := func(reset bool) {
		if frag >= minFragment && !reset {
			run, letters = run+frag, letters+fragLetters
		} else {
			run, letters = 0, 0
		}
		frag, fragLetters = 0, 0
		found = found || (run >= 64 && letters > 0)
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			frag++
		case isHex(r):
			frag, fragLetters = frag+1, fragLetters+1
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			end(true)
		default:
			end(false)
		}
	}
	end(false)
	return found
}

// hasDigitRun finds a maximal run of exactly n digits, digit groups joined by a separator counting as one run.
func hasDigitRun(s string, n int) bool {
	run := 0
	for i := 0; i <= len(s); i++ {
		switch {
		case i < len(s) && s[i] >= '0' && s[i] <= '9':
			run++
			continue
		case i < len(s) && strings.IndexByte(digitSepBytes, s[i]) >= 0 && run > 0 && nextIsDigit(s[i+1:]):
			continue
		}
		if run == n {
			return true
		}
		run = 0
	}
	return false
}

func nextIsDigit(s string) bool {
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(digitSepBytes, s[i]) >= 0 {
			continue
		}
		return s[i] >= '0' && s[i] <= '9'
	}
	return false
}
