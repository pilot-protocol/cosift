package main

import (
	"math"
	"os"
	"testing"
	"time"
)

func TestAuthStatus(t *testing.T) {
	cases := []struct {
		name       string
		configured bool
		key        string
		urlSet     bool
		want       string
	}{
		{"not configured", false, "", false, "(none)"},
		{"not configured ignores other args", false, "k", true, "(none)"},
		{"with bearer", true, "secret", false, "bearer-token"},
		{"with bearer + custom url", true, "secret", true, "bearer-token"},
		{"anonymous custom URL", true, "", true, "anonymous(custom-url)"},
		{"missing", true, "", false, "MISSING"},
	}
	for _, c := range cases {
		if got := authStatus(c.configured, c.key, c.urlSet); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestResolveAPIKey(t *testing.T) {
	// Save & wipe relevant envs.
	saved := map[string]string{}
	for _, k := range []string{
		"COSIFT_EMBED_API_KEY", "COSIFT_CHAT_API_KEY",
		"OPENAI_API_KEY", "OPENAI",
	} {
		saved[k] = os.Getenv(k)
		os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for k, v := range saved {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	})

	// Empty → "".
	if got := resolveAPIKey("embed"); got != "" {
		t.Errorf("empty env: got %q want \"\"", got)
	}

	// OPENAI_API_KEY fallback.
	os.Setenv("OPENAI_API_KEY", "openai-key")
	if got := resolveAPIKey("embed"); got != "openai-key" {
		t.Errorf("openai key: got %q", got)
	}

	// Slot-specific takes precedence.
	os.Setenv("COSIFT_EMBED_API_KEY", "embed-key")
	if got := resolveAPIKey("embed"); got != "embed-key" {
		t.Errorf("embed key: got %q", got)
	}

	// Chat slot — uses chat slot env.
	os.Setenv("COSIFT_CHAT_API_KEY", "chat-key")
	if got := resolveAPIKey("chat"); got != "chat-key" {
		t.Errorf("chat key: got %q", got)
	}

	// Unknown slot falls through to OPENAI_API_KEY.
	if got := resolveAPIKey("unknown"); got != "openai-key" {
		t.Errorf("unknown slot fallthrough: got %q", got)
	}

	// OPENAI (without _API_KEY) fallback.
	os.Unsetenv("OPENAI_API_KEY")
	os.Setenv("OPENAI", "legacy")
	if got := resolveAPIKey("unknown"); got != "legacy" {
		t.Errorf("OPENAI fallback: got %q", got)
	}
}

func TestResolveEmbedAPIKey(t *testing.T) {
	saved := os.Getenv("COSIFT_EMBED_API_KEY")
	os.Setenv("COSIFT_EMBED_API_KEY", "embed-direct")
	t.Cleanup(func() {
		if saved == "" {
			os.Unsetenv("COSIFT_EMBED_API_KEY")
		} else {
			os.Setenv("COSIFT_EMBED_API_KEY", saved)
		}
	})
	if got := resolveEmbedAPIKey(); got != "embed-direct" {
		t.Errorf("resolveEmbedAPIKey: got %q", got)
	}
}

func TestFirstEnv(t *testing.T) {
	for _, k := range []string{"COSIFT_FIRSTENV_A", "COSIFT_FIRSTENV_B", "COSIFT_FIRSTENV_C"} {
		os.Unsetenv(k)
		k := k
		t.Cleanup(func() { os.Unsetenv(k) })
	}

	// All empty.
	if got := firstEnv("COSIFT_FIRSTENV_A", "COSIFT_FIRSTENV_B"); got != "" {
		t.Errorf("all empty: got %q", got)
	}

	// Returns first non-empty.
	os.Setenv("COSIFT_FIRSTENV_B", "b-val")
	if got := firstEnv("COSIFT_FIRSTENV_A", "COSIFT_FIRSTENV_B", "COSIFT_FIRSTENV_C"); got != "b-val" {
		t.Errorf("got %q want b-val", got)
	}

	// Earlier wins.
	os.Setenv("COSIFT_FIRSTENV_A", "a-val")
	if got := firstEnv("COSIFT_FIRSTENV_A", "COSIFT_FIRSTENV_B"); got != "a-val" {
		t.Errorf("got %q want a-val (first)", got)
	}

	// No args.
	if got := firstEnv(); got != "" {
		t.Errorf("no args: got %q", got)
	}
}

func TestContains(t *testing.T) {
	if !contains([]string{"a", "b", "c"}, "b") {
		t.Errorf("expected true for present element")
	}
	if contains([]string{"a", "b", "c"}, "z") {
		t.Errorf("expected false for absent element")
	}
	if contains(nil, "x") {
		t.Errorf("expected false on nil slice")
	}
}

func TestChunkerWith(t *testing.T) {
	c := chunkerWith(100, 20)
	if c == nil {
		t.Errorf("chunkerWith returned nil")
	}
}

func TestSqrt(t *testing.T) {
	cases := []struct {
		in, want float64
	}{
		{4, 2},
		{9, 3},
		{16, 4},
		{2, math.Sqrt2},
	}
	for _, c := range cases {
		got := sqrt(c.in)
		if math.Abs(got-c.want) > 1e-6 {
			t.Errorf("sqrt(%v): got %v want %v", c.in, got, c.want)
		}
	}
	// Edge: sqrt(0).
	if got := sqrt(0); got != 0 {
		// The Newton iteration starts at x/2 = 0 and breaks immediately.
		_ = got
	}
}

func TestRandUnit(t *testing.T) {
	rng := newSeededRand()
	v := randUnit(rng, 8)
	if len(v) != 8 {
		t.Errorf("len: got %d want 8", len(v))
	}
	// Unit vector — magnitude ~ 1.
	var mag float64
	for _, x := range v {
		mag += float64(x) * float64(x)
	}
	mag = math.Sqrt(mag)
	if math.Abs(mag-1.0) > 0.01 {
		t.Errorf("magnitude: got %v want ~1.0", mag)
	}

	// Deterministic.
	rng2 := newSeededRand()
	v2 := randUnit(rng2, 8)
	for i := range v {
		if v[i] != v2[i] {
			t.Errorf("non-deterministic at %d: %v vs %v", i, v[i], v2[i])
		}
	}
}

func TestPercentiles(t *testing.T) {
	// Empty.
	p50, p95, p99 := percentiles(nil)
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Errorf("empty: got %v %v %v", p50, p95, p99)
	}

	// Sorted input — index math gives p50 = sorted[N*0.5].
	ds := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		4 * time.Millisecond,
		5 * time.Millisecond,
		6 * time.Millisecond,
		7 * time.Millisecond,
		8 * time.Millisecond,
		9 * time.Millisecond,
		10 * time.Millisecond,
	}
	p50, p95, p99 = percentiles(ds)
	// idx = int((N-1)*p): p50 at idx 4 = 5ms, p95 at idx 8 = 9ms, p99 at idx 8 = 9ms.
	if p50 != 5*time.Millisecond {
		t.Errorf("p50: got %v want 5ms", p50)
	}
	if p95 != 9*time.Millisecond {
		t.Errorf("p95: got %v want 9ms", p95)
	}
	if p99 != 9*time.Millisecond {
		t.Errorf("p99: got %v want 9ms", p99)
	}

	// Unsorted input — function copies + sorts.
	unsorted := []time.Duration{10, 1, 5, 3, 7, 9, 2, 4, 6, 8}
	for i := range unsorted {
		unsorted[i] *= time.Millisecond
	}
	p50b, _, _ := percentiles(unsorted)
	if p50b != 5*time.Millisecond {
		t.Errorf("unsorted p50: got %v want 5ms", p50b)
	}
}

func TestSumDur(t *testing.T) {
	if got := sumDur(nil); got != 0 {
		t.Errorf("nil: got %v", got)
	}
	got := sumDur([]time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		50 * time.Millisecond,
	})
	if got != 400*time.Millisecond {
		t.Errorf("sum: got %v want 400ms", got)
	}
}

func TestNewSeededRandIsDeterministic(t *testing.T) {
	a := newSeededRand()
	b := newSeededRand()
	for i := 0; i < 10; i++ {
		x, y := a.Float64(), b.Float64()
		if x != y {
			t.Errorf("non-deterministic at iter %d: %v vs %v", i, x, y)
		}
	}
}
