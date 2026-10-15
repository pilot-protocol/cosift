package sharedaccount

import (
	"errors"
	"testing"
)

func TestResolvedVersionAcceptsOnlyABoundedPositiveVersion(t *testing.T) {
	const prefix = "projects/p/secrets/cosift-token-pepper/versions/"
	for name, tc := range map[string]struct {
		in   string
		want int
		err  error
	}{
		"plain":            {prefix + "7", 7, nil},
		"six digits":       {prefix + "999999", 999999, nil},
		"project number":   {"projects/123/secrets/cosift-token-pepper/versions/12", 12, nil},
		"alias not number": {prefix + "latest", 0, ErrUnavailable},
		"zero":             {prefix + "0", 0, ErrUnavailable},
		"leading zero":     {prefix + "07", 0, ErrUnavailable},
		"seven digits":     {prefix + "1000000", 0, ErrUnavailable},
		"negative":         {prefix + "-1", 0, ErrUnavailable},
		"no version":       {"projects/p/secrets/cosift-token-pepper", 0, ErrUnavailable},
		"empty":            {"", 0, ErrUnavailable},
		"trailing path":    {prefix + "7/extra", 0, ErrUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := resolvedVersion(tc.in)
			if got != tc.want || !errors.Is(err, tc.err) {
				t.Fatalf("resolvedVersion(%q) = %d, %v; want %d, %v", tc.in, got, err, tc.want, tc.err)
			}
		})
	}
}
