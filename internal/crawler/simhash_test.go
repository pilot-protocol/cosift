package crawler

import (
	"encoding/json"
	"os"
	"testing"
)

type simhashVectors struct {
	Cases []struct {
		Name     string `json:"name"`
		Relation string `json:"relation"`
		A        string `json:"a"`
		B        string `json:"b"`
		ASimhash string `json:"a_simhash"`
		BSimhash string `json:"b_simhash"`
		Distance int    `json:"distance"`
	} `json:"cases"`
	Unicode []struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Simhash string `json:"simhash"`
	} `json:"unicode"`
}

func loadSimhashVectors(t *testing.T) simhashVectors {
	t.Helper()
	b, err := os.ReadFile("testdata/simhash_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v simhashVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSimhashMatchesTheVectors(t *testing.T) {
	const simhashMax = 6
	v := loadSimhashVectors(t)
	if len(v.Cases) < 4 || len(v.Unicode) < 4 {
		t.Fatalf("vectors truncated: %d cases, %d unicode", len(v.Cases), len(v.Unicode))
	}
	for _, c := range v.Cases {
		a, okA := Simhash(c.A)
		b, okB := Simhash(c.B)
		if !okA || !okB {
			t.Fatalf("%s: no token", c.Name)
		}
		if SimhashHex(a) != c.ASimhash || SimhashHex(b) != c.BSimhash {
			t.Errorf("%s: simhash %s %s, want %s %s", c.Name, SimhashHex(a), SimhashHex(b), c.ASimhash, c.BSimhash)
		}
		d := SimhashDistance(a, b)
		if d != c.Distance {
			t.Errorf("%s: distance %d, want %d", c.Name, d, c.Distance)
		}
		if within := d <= simhashMax; within != (c.Relation == "within") {
			t.Errorf("%s: distance %d is not %s the bound", c.Name, d, c.Relation)
		}
	}
	for _, u := range v.Unicode {
		got, ok := Simhash(u.Text)
		if !ok || SimhashHex(got) != u.Simhash {
			t.Errorf("%s: %s %v, want %s", u.Name, SimhashHex(got), ok, u.Simhash)
		}
	}
}

func TestSimhashNeedsAToken(t *testing.T) {
	for _, s := range []string{"", "   ", ",.;!? \n\t-—…", "\xff\xfe"} {
		if _, ok := Simhash(s); ok {
			t.Errorf("%q has no token", s)
		}
	}
	if _, ok := Simhash("é"); !ok {
		t.Error("a single letter is a token")
	}
}

func TestSimhashIgnoresCaseAndSeparators(t *testing.T) {
	a, _ := Simhash("The Quick, brown fox -- jumps over the LAZY dog")
	b, _ := Simhash("the quick brown fox jumps over the lazy dog")
	if a != b {
		t.Fatalf("%s != %s", SimhashHex(a), SimhashHex(b))
	}
}

func TestSimhashHexRoundTrip(t *testing.T) {
	for _, sum := range []uint64{0, 1, 1 << 63, 0x2d1a482a4439cff0, ^uint64(0)} {
		s := SimhashHex(sum)
		if len(s) != 16 {
			t.Fatalf("%q", s)
		}
		if got, ok := ParseSimhash(s); !ok || got != sum {
			t.Errorf("%016x -> %q -> %016x %v", sum, s, got, ok)
		}
	}
	for _, bad := range []string{"", "abc", "2D1A482A4439CFF0", "2d1a482a4439cff", "2d1a482a4439cff00", "zzzzzzzzzzzzzzzz"} {
		if _, ok := ParseSimhash(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestSimhashDistance(t *testing.T) {
	if SimhashDistance(0, ^uint64(0)) != 64 || SimhashDistance(5, 5) != 0 || SimhashDistance(0b1010, 0b0110) != 2 {
		t.Fatal("hamming distance")
	}
}
