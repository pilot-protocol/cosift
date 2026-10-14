package work

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
)

type agreedSide struct {
	Title string `json:"title"`
	Lang  string `json:"lang"`
	Text  string `json:"text"`
}

type agreedCase struct {
	ID          string     `json:"id"`
	Anchor      agreedSide `json:"anchor"`
	Attestation agreedSide `json:"attestation"`
	Expect      struct {
		Simhashes    []string `json:"simhashes"`
		Distance     int      `json:"distance"`
		Fails        []string `json:"fails"`
		Agree        bool     `json:"agree"`
		Matches      [][2]int `json:"matches"`
		MatchedBytes int      `json:"matched_bytes"`
		AgreedText   string   `json:"agreed_text"`
		AgreedBytes  int      `json:"agreed_bytes"`
		Title        string   `json:"title"`
		ContentHash  string   `json:"content_hash"`
	} `json:"expect"`
}

type alignmentCase struct {
	ID      string   `json:"id"`
	A       []string `json:"a"`
	C       []string `json:"c"`
	ATotal  *int     `json:"a_total"`
	CTotal  *int     `json:"c_total"`
	Matches [][2]int `json:"matches"`
	Agreed  []string `json:"agreed"`
}

type agreedVectors struct {
	Parameters struct {
		SimhashMax       int `json:"simhash_max"`
		PartsMax         int `json:"parts_max"`
		BoundaryMinWords int `json:"boundary_min_words"`
		PartMaxWords     int `json:"part_max_words"`
		BoundaryModulus  int `json:"boundary_modulus"`
		FloorPercent     int `json:"floor_percent"`
		AgreedMinBytes   int `json:"agreed_min_bytes"`
	} `json:"parameters"`
	Splitter []struct {
		ID        string   `json:"id"`
		Text      string   `json:"text"`
		PartWords []int    `json:"part_words"`
		Parts     []string `json:"parts"`
	} `json:"splitter"`
	TitleSha []struct {
		ID    string `json:"id"`
		Title string `json:"title"`
		Sha   string `json:"sha"`
	} `json:"title_sha"`
	Alignment []alignmentCase `json:"alignment"`
	Cap       struct {
		Parts          int    `json:"parts"`
		Considered     int    `json:"considered"`
		TextBytes      int    `json:"text_bytes"`
		TextSha256     string `json:"text_sha256"`
		First          string `json:"first"`
		LastConsidered string `json:"last_considered"`
		FirstBeyond    string `json:"first_beyond"`
		Matched        int    `json:"matched"`
		MatchedBytes   int    `json:"matched_bytes"`
		AgreedBytes    int    `json:"agreed_bytes"`
		TailCopies     struct {
			TextBytes   int    `json:"text_bytes"`
			TextSha256  string `json:"text_sha256"`
			Matched     int    `json:"matched"`
			AgreedBytes int    `json:"agreed_bytes"`
		} `json:"tail_copies"`
	} `json:"cap"`
	Cases []agreedCase `json:"cases"`
}

func loadAgreed(t testing.TB) *agreedVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/work_agreed_text.json")
	if err != nil {
		t.Fatal(err)
	}
	var v agreedVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return &v
}

func (v *agreedVectors) byID(t testing.TB, id string) agreedCase {
	t.Helper()
	for _, c := range v.Cases {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no vector case %q", id)
	return agreedCase{}
}

func hexSum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hexes(hashes []uint64) []string {
	out := make([]string, len(hashes))
	for i, h := range hashes {
		out[i] = hexOf(h)
	}
	return out
}

func (p *splitText) partWords() []int {
	var out []int
	start := 0
	for _, end := range p.ends {
		out = append(out, end-start)
		start = end
	}
	return out
}

func TestThePartParametersAreTheContractsOnes(t *testing.T) {
	v := loadAgreed(t)
	got := [...]int{maxParts, boundaryMinWords, partMaxWords, boundaryModulus, floorPercent, minAgreedBytes}
	want := [...]int{v.Parameters.PartsMax, v.Parameters.BoundaryMinWords, v.Parameters.PartMaxWords, v.Parameters.BoundaryModulus, v.Parameters.FloorPercent, v.Parameters.AgreedMinBytes}
	if got != want {
		t.Fatalf("parameters %v, vectors %v", got, want)
	}
}

func TestTheSplitterAndThePartHashMatchTheVectors(t *testing.T) {
	v := loadAgreed(t)
	if len(v.Splitter) < 4 {
		t.Fatalf("vectors truncated: %d splitter texts", len(v.Splitter))
	}
	for _, c := range v.Splitter {
		t.Run(c.ID, func(t *testing.T) {
			p := splitParts(c.Text)
			if fmt.Sprint(p.partWords()) != fmt.Sprint(c.PartWords) {
				t.Errorf("part words %v, want %v", p.partWords(), c.PartWords)
			}
			if got := hexes(p.hashes); fmt.Sprint(got) != fmt.Sprint(c.Parts) {
				t.Errorf("part hashes %v, want %v", got, c.Parts)
			}
			if p.total != len(p.hashes) {
				t.Errorf("%d parts counted, %d hashed", p.total, len(p.hashes))
			}
		})
	}
}

func TestTitleShaMatchesTheVectors(t *testing.T) {
	for _, c := range loadAgreed(t).TitleSha {
		if got := titleSha(c.Title); got != c.Sha {
			t.Errorf("%s: %q hashes to %s, want %s", c.ID, c.Title, got, c.Sha)
		}
	}
}

// capText is the cap vector's text: parts of eight words, the last chosen so that the part ends there.
func capText(parts, repeatFrom int) string {
	var words []string
	for k := 0; k < parts; k++ {
		j := k
		if repeatFrom > 0 && k >= repeatFrom {
			j = k - repeatFrom
		}
		for _, c := range "abcdefg" {
			words = append(words, fmt.Sprintf("q%d%c", j, c))
		}
		for s := 0; ; s++ {
			h := fnv.New64a()
			fmt.Fprintf(h, "q%dg q%dh%d", j, j, s)
			if h.Sum64()%32 == 0 {
				words = append(words, fmt.Sprintf("q%dh%d", j, s))
				break
			}
		}
	}
	return strings.Join(words, " ")
}

func TestOnlyTheFirst2048PartsCanBeAgreed(t *testing.T) {
	c := loadAgreed(t).Cap
	text := capText(c.Parts, 0)
	if hexSum(text) != c.TextSha256 || len(text) != c.TextBytes {
		t.Fatalf("the generated text is not the vector's: %d bytes %s", len(text), hexSum(text))
	}
	p := splitParts(text)
	if len(p.hashes) != c.Considered || len(p.ends) != c.Considered || p.total != c.Parts {
		t.Fatalf("%d hashes, %d ends, %d parts counted", len(p.hashes), len(p.ends), p.total)
	}
	if hexOf(p.hashes[0]) != c.First || hexOf(p.hashes[c.Considered-1]) != c.LastConsidered {
		t.Errorf("first %s last %s", hexOf(p.hashes[0]), hexOf(p.hashes[c.Considered-1]))
	}
	words := strings.Fields(text)
	beyond := words[c.Considered*8 : (c.Considered+1)*8]
	if got := sha8(beyond); got != c.FirstBeyond {
		t.Errorf("the 2,049th part hashes to %s, want %s", got, c.FirstBeyond)
	}
	ag := p.agreeWith(p.hashes, p.total)
	if !strings.HasSuffix(ag.text, " "+gap) || strings.Count(ag.text, gap) != 1 || len(ag.text) != c.AgreedBytes || ag.matched != c.MatchedBytes {
		t.Fatalf("agreed text of %d bytes, %d gaps, M %d, want %d bytes ending in one gap, M %d", len(ag.text), strings.Count(ag.text, gap), ag.matched, c.AgreedBytes, c.MatchedBytes)
	}
	if want := strings.Join(words[:c.Considered*8], " ") + " " + gap; ag.text != want {
		t.Error("the agreed text is not the first 2,048 parts and a gap")
	}
	tail := capText(c.Parts, c.Considered)
	if hexSum(tail) != c.TailCopies.TextSha256 || len(tail) != c.TailCopies.TextBytes {
		t.Fatalf("the tail-copies text is not the vector's")
	}
	if got := splitParts(tail).agreeWith(p.hashes, p.total); got.text != ag.text || len(got.text) != c.TailCopies.AgreedBytes || got.matched != ag.matched {
		t.Errorf("parts beyond the cap whose hashes are in the anchor's list were agreed: %d bytes, M %d", len(got.text), got.matched)
	}
}

func sha8(words []string) string {
	sum := sha256.Sum256([]byte(strings.Join(words, " ")))
	return hex.EncodeToString(sum[:8])
}

func TestTheAgreementOfEveryVectorCaseMatchesByteForByte(t *testing.T) {
	v := loadAgreed(t)
	if len(v.Cases) < 30 {
		t.Fatalf("vectors truncated: %d cases", len(v.Cases))
	}
	for _, c := range v.Cases {
		t.Run(c.ID, func(t *testing.T) {
			anchor, att := splitParts(c.Anchor.Text), splitParts(c.Attestation.Text)
			if got := align(anchor.hashes, att.hashes); fmt.Sprint(got) != fmt.Sprint(matchPairs(c.Expect.Matches)) {
				t.Errorf("matches %v, want %v", got, c.Expect.Matches)
			}
			ag := att.agreeWith(anchor.hashes, anchor.total)
			if ag.text != c.Expect.AgreedText || len(ag.text) != c.Expect.AgreedBytes || ag.matched != c.Expect.MatchedBytes {
				t.Errorf("agreed text of %d bytes, M %d, want %d, %d:\n%q\nwant\n%q", len(ag.text), ag.matched, c.Expect.AgreedBytes, c.Expect.MatchedBytes, ag.text, c.Expect.AgreedText)
			}
			if c.Expect.Agree && (hexSum(ag.text) != c.Expect.ContentHash || normaliseTitle(c.Attestation.Title) != c.Expect.Title) {
				t.Errorf("content hash %s title %q, want %s %q", hexSum(ag.text), normaliseTitle(c.Attestation.Title), c.Expect.ContentHash, c.Expect.Title)
			}
		})
	}
}

func matchPairs(in [][2]int) []match {
	out := make([]match, len(in))
	for i, p := range in {
		out[i] = match{p[0], p[1]}
	}
	return out
}

func TestTheAlignmentWalkAndItsGapsMatchTheVectors(t *testing.T) {
	v := loadAgreed(t)
	if len(v.Alignment) < 15 {
		t.Fatalf("vectors truncated: %d alignment cases", len(v.Alignment))
	}
	for _, c := range v.Alignment {
		t.Run(c.ID, func(t *testing.T) {
			ms := align(c.A, c.C)
			if fmt.Sprint(ms) != fmt.Sprint(matchPairs(c.Matches)) {
				t.Errorf("matches %v, want %v", ms, c.Matches)
			}
			aTotal, cTotal := len(c.A), len(c.C)
			if c.ATotal != nil {
				aTotal = *c.ATotal
			}
			if c.CTotal != nil {
				cTotal = *c.CTotal
			}
			var agreed []string
			for _, k := range layout(ms, aTotal, cTotal) {
				if k == gapMark {
					agreed = append(agreed, gap)
				} else {
					agreed = append(agreed, c.C[k])
				}
			}
			if fmt.Sprint(agreed) != fmt.Sprint(c.Agreed) {
				t.Errorf("agreed %v, want %v", agreed, c.Agreed)
			}
		})
	}
}

func TestTheAlignmentOfTwoTextsAtTheCapFitsItsTable(t *testing.T) {
	a, c := make([]uint64, maxParts), make([]uint64, maxParts)
	for i := range a {
		a[i], c[i] = uint64(i), uint64(maxParts-1-i)
	}
	if got := align(a, c); len(got) != 1 || got[0] != (match{maxParts - 1, 0}) {
		t.Fatalf("a text and its reverse: %v", got)
	}
	if got := align(a, a); len(got) != maxParts {
		t.Fatalf("a text and itself: %d matches", len(got))
	}
}

// refWalk is the walk of WORK 6.5 written from its recursive definition of L.
func refWalk(a, c []int) []match {
	memo := map[[2]int]int{}
	var l func(i, j int) int
	l = func(i, j int) int {
		if i >= len(a) || j >= len(c) {
			return 0
		}
		if v, ok := memo[[2]int{i, j}]; ok {
			return v
		}
		v := max(l(i+1, j), l(i, j+1))
		if a[i] == c[j] {
			v = 1 + l(i+1, j+1)
		}
		memo[[2]int{i, j}] = v
		return v
	}
	var out []match
	for i, j := 0, 0; i < len(a) && j < len(c); {
		if a[i] == c[j] {
			out = append(out, match{i, j})
			i, j = i+1, j+1
		} else if l(i+1, j) >= l(i, j+1) {
			i++
		} else {
			j++
		}
	}
	return out
}

func TestTheWalkIsTheSpecifiedOrderPreservingLongestCommonSubsequenceThatMatchesEachPartOnce(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	list := func() []int {
		out := make([]int, rng.IntN(10))
		for i := range out {
			out[i] = rng.IntN(4)
		}
		return out
	}
	for n := 0; n < 4000; n++ {
		a, c := list(), list()
		got := align(a, c)
		if want := refWalk(a, c); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%v against %v: matches %v, the walk of WORK 6.5 gives %v", a, c, got, want)
		}
		for k, m := range got {
			if a[m.i] != c[m.j] || (k > 0 && (m.i <= got[k-1].i || m.j <= got[k-1].j)) {
				t.Fatalf("%v against %v: matches %v are not one in-order pairing of equal parts", a, c, got)
			}
		}
	}
	if got := align([]string{"x", "x", "y"}, []string{"x", "y"}); fmt.Sprint(got) != fmt.Sprint([]match{{0, 0}, {2, 1}}) {
		t.Errorf("a repeated anchor part against one copy: %v", got)
	}
}

func TestAnAgreedTextIsAlwaysSingleSpacedAndGapsAreTrimmed(t *testing.T) {
	text := "one two three four five six seven eight nine ten"
	p := splitParts(text)
	if got := p.agreeWith(nil, 0); got.text != "" || got.matched != 0 {
		t.Errorf("nothing in common: %+v, want no agreed text", got)
	}
	if got := p.agreeWith(p.hashes, p.total); got.text != text || got.matched != len(text)-(p.total-1) {
		t.Errorf("every part matched: %+v over %d parts", got, p.total)
	}
	spaced := splitParts("  one \t two\nthree\u00a0four five six seven eight nine ten  ")
	if got := spaced.agreeWith(p.hashes, p.total); got.text != text {
		t.Errorf("white space not collapsed: %q", got.text)
	}
}
