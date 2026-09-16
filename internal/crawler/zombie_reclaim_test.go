package crawler

import "testing"

func TestZombieReclaimEnabled(t *testing.T) {
	for _, tc := range []struct {
		v    string
		want bool
	}{{"", true}, {"1", true}, {"true", true}, {"0", false}, {"false", false}, {"OFF", false}} {
		t.Setenv("COSIFT_ZOMBIE_RECLAIM", tc.v)
		if got := ZombieReclaimEnabled(); got != tc.want {
			t.Errorf("%q: got %v want %v", tc.v, got, tc.want)
		}
	}
}
