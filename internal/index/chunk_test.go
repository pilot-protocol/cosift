package index

import (
	"strings"
	"testing"
)

func TestChunkShortText(t *testing.T) {
	c := NewChunker()
	chunks := c.Chunk("only a few words here")
	if len(chunks) != 1 {
		t.Fatalf("short text: got %d chunks want 1", len(chunks))
	}
	if chunks[0].Text != "only a few words here" {
		t.Errorf("text mismatch: %q", chunks[0].Text)
	}
}

func TestChunkEmpty(t *testing.T) {
	c := NewChunker()
	if got := c.Chunk(""); got != nil {
		t.Errorf("empty input should give nil, got %+v", got)
	}
	if got := c.Chunk("   \t  \n   "); got != nil {
		t.Errorf("whitespace-only should give nil, got %+v", got)
	}
}

func TestChunkLongTextOverlap(t *testing.T) {
	// Generate a long source: 1000 words, all distinct so we can detect overlap.
	parts := make([]string, 1000)
	for i := range parts {
		parts[i] = "w" + itoa(i)
	}
	source := strings.Join(parts, " ")

	c := &Chunker{Size: 100, Overlap: 20}
	chunks := c.Chunk(source)

	if len(chunks) < 8 {
		t.Errorf("expected >=8 chunks for 1000 words at size=100, got %d", len(chunks))
	}
	// Verify byte spans reconstruct chunk text exactly.
	for i, ch := range chunks {
		got := source[ch.Offset : ch.Offset+ch.Length]
		if got != ch.Text {
			t.Errorf("chunk %d: span vs text mismatch", i)
		}
	}
	// Verify overlap: last 20 words of chunk i should appear in chunk i+1.
	for i := 0; i+1 < len(chunks); i++ {
		tail := lastNWords(chunks[i].Text, 20)
		head := firstNWords(chunks[i+1].Text, 20)
		if tail != head {
			t.Errorf("chunk %d→%d overlap mismatch (tail=%q head=%q)", i, i+1, tail, head)
		}
	}
}

func TestChunkSpansAreSortedAndNonNegative(t *testing.T) {
	source := strings.Repeat("alpha beta gamma delta epsilon ", 50)
	c := &Chunker{Size: 20, Overlap: 5}
	chunks := c.Chunk(source)
	prev := -1
	for i, ch := range chunks {
		if ch.Offset < 0 || ch.Length <= 0 {
			t.Errorf("chunk %d: bad span Offset=%d Length=%d", i, ch.Offset, ch.Length)
		}
		if ch.Offset < prev {
			t.Errorf("chunk %d: offsets not monotonic (prev=%d cur=%d)", i, prev, ch.Offset)
		}
		prev = ch.Offset
	}
}

func TestChunkOverlapBounded(t *testing.T) {
	// Overlap >= Size should be capped to Size/2.
	source := strings.Repeat("foo bar baz ", 100)
	c := &Chunker{Size: 10, Overlap: 100}
	chunks := c.Chunk(source)
	// Should still terminate and produce multiple chunks.
	if len(chunks) < 2 {
		t.Errorf("expected multiple chunks with capped overlap, got %d", len(chunks))
	}
}

// itoa is a tiny local helper to avoid pulling in fmt for tiny digits in tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func firstNWords(s string, n int) string {
	parts := strings.Fields(s)
	if n > len(parts) {
		n = len(parts)
	}
	return strings.Join(parts[:n], " ")
}

func lastNWords(s string, n int) string {
	parts := strings.Fields(s)
	if n > len(parts) {
		n = len(parts)
	}
	return strings.Join(parts[len(parts)-n:], " ")
}

// NewChunkerWith locks the override-fallback contract.
func TestNewChunkerWith(t *testing.T) {
	defaults := NewChunker()
	cases := []struct {
		name              string
		size, overlap     int
		wantSize, wantOvl int
	}{
		{"both zero → defaults", 0, 0, defaults.Size, defaults.Overlap},
		{"size only → size override, overlap default", 100, 0, 100, defaults.Overlap},
		{"overlap only → overlap override, size default", 0, 20, defaults.Size, 20},
		{"both → both override", 256, 48, 256, 48},
		{"negative size → ignored (fall through)", -5, 0, defaults.Size, defaults.Overlap},
		{"negative overlap → ignored (fall through)", 0, -5, defaults.Size, defaults.Overlap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewChunkerWith(tc.size, tc.overlap)
			if c.Size != tc.wantSize {
				t.Errorf("size: got %d want %d", c.Size, tc.wantSize)
			}
			if c.Overlap != tc.wantOvl {
				t.Errorf("overlap: got %d want %d", c.Overlap, tc.wantOvl)
			}
		})
	}
}
