package work

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
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
		Simhashes   []string `json:"simhashes"`
		Distance    int      `json:"distance"`
		Fails       []string `json:"fails"`
		Agree       bool     `json:"agree"`
		AgreedText  string   `json:"agreed_text"`
		AgreedBytes int      `json:"agreed_bytes"`
		ContentHash string   `json:"content_hash"`
	} `json:"expect"`
}

type agreedVectors struct {
	Parameters struct {
		SimhashMax       int `json:"simhash_max"`
		PartsMax         int `json:"parts_max"`
		BoundaryMinWords int `json:"boundary_min_words"`
		PartMaxWords     int `json:"part_max_words"`
		BoundaryModulus  int `json:"boundary_modulus"`
		FloorPercent     int `json:"floor_percent"`
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
	Cap struct {
		Parts          int    `json:"parts"`
		Considered     int    `json:"considered"`
		TextBytes      int    `json:"text_bytes"`
		TextSha256     string `json:"text_sha256"`
		First          string `json:"first"`
		LastConsidered string `json:"last_considered"`
		FirstBeyond    string `json:"first_beyond"`
		AgreedBytes    int    `json:"agreed_bytes"`
		TailCopies     struct {
			TextBytes   int    `json:"text_bytes"`
			TextSha256  string `json:"text_sha256"`
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
	got := [...]int{maxParts, boundaryMinWords, partMaxWords, boundaryModulus, floorPercent}
	want := [...]int{v.Parameters.PartsMax, v.Parameters.BoundaryMinWords, v.Parameters.PartMaxWords, v.Parameters.BoundaryModulus, v.Parameters.FloorPercent}
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
			if p.beyond {
				t.Error("a short text has no parts beyond the cap")
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

func TestOnlyTheFirst8192PartsCanBeAgreed(t *testing.T) {
	c := loadAgreed(t).Cap
	text := capText(c.Parts, 0)
	if hexSum(text) != c.TextSha256 || len(text) != c.TextBytes {
		t.Fatalf("the generated text is not the vector's: %d bytes %s", len(text), hexSum(text))
	}
	p := splitParts(text)
	if len(p.hashes) != c.Considered || len(p.ends) != c.Considered || !p.beyond {
		t.Fatalf("%d hashes, %d ends, beyond %v", len(p.hashes), len(p.ends), p.beyond)
	}
	if hexOf(p.hashes[0]) != c.First || hexOf(p.hashes[c.Considered-1]) != c.LastConsidered {
		t.Errorf("first %s last %s", hexOf(p.hashes[0]), hexOf(p.hashes[c.Considered-1]))
	}
	words := strings.Fields(text)
	beyond := words[c.Considered*8 : (c.Considered+1)*8]
	if got := sha8(beyond); got != c.FirstBeyond {
		t.Errorf("the 8,193rd part hashes to %s, want %s", got, c.FirstBeyond)
	}
	agreed := p.agreed(partSet(p.hashes))
	if !strings.HasSuffix(agreed, " "+gap) || strings.Count(agreed, gap) != 1 || len(agreed) != c.AgreedBytes {
		t.Fatalf("agreed text of %d bytes, %d gaps, want %d bytes ending in one gap", len(agreed), strings.Count(agreed, gap), c.AgreedBytes)
	}
	if want := strings.Join(words[:c.Considered*8], " ") + " " + gap; agreed != want {
		t.Error("the agreed text is not the first 8,192 parts and a gap")
	}
	tail := capText(c.Parts, c.Considered)
	if hexSum(tail) != c.TailCopies.TextSha256 || len(tail) != c.TailCopies.TextBytes {
		t.Fatalf("the tail-copies text is not the vector's")
	}
	if got := splitParts(tail).agreed(partSet(p.hashes)); got != agreed || len(got) != c.TailCopies.AgreedBytes {
		t.Errorf("parts beyond the cap whose hashes are in the anchor's list were agreed: %d bytes", len(got))
	}
}

func sha8(words []string) string {
	sum := sha256.Sum256([]byte(strings.Join(words, " ")))
	return hex.EncodeToString(sum[:8])
}

func TestTheAgreedTextMatchesTheVectorsByteForByte(t *testing.T) {
	v := loadAgreed(t)
	if len(v.Cases) < 20 {
		t.Fatalf("vectors truncated: %d cases", len(v.Cases))
	}
	for _, c := range v.Cases {
		t.Run(c.ID, func(t *testing.T) {
			anchor := splitParts(c.Anchor.Text)
			got := splitParts(c.Attestation.Text).agreed(partSet(anchor.hashes))
			if got != c.Expect.AgreedText || len(got) != c.Expect.AgreedBytes {
				t.Errorf("agreed text of %d bytes, want %d:\n%q\nwant\n%q", len(got), c.Expect.AgreedBytes, got, c.Expect.AgreedText)
			}
			if c.Expect.Agree && hexSum(got) != c.Expect.ContentHash {
				t.Errorf("content hash %s, want %s", hexSum(got), c.Expect.ContentHash)
			}
		})
	}
}

func TestAnAgreedTextIsAlwaysSingleSpacedAndGapsAreTrimmed(t *testing.T) {
	text := "one two three four five six seven eight nine ten"
	p := splitParts(text)
	if got := p.agreed(partSet(nil)); got != gap {
		t.Errorf("no part kept: %q, want just the gap", got)
	}
	if got := p.agreed(partSet(p.hashes)); got != text {
		t.Errorf("every part kept: %q", got)
	}
	spaced := splitParts("  one \t two\nthree\u00a0four five six seven eight nine ten  ")
	if got := spaced.agreed(partSet(spaced.hashes)); got != text {
		t.Errorf("white space not collapsed: %q", got)
	}
}
