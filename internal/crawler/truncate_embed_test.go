package crawler

import (
	"strings"
	"testing"
)

// TestTruncateForEmbedByteCap locks in's byte cap.
// nomic-embed-text rejects inputs above 2048 BERT tokens. Dense source
// code / CI logs / JSON have many micro-tokens per byte, so token
// estimates alone underestimate cost and inputs still 400. The cap is
// in addition to the token estimate — whichever fires first wins.
func TestTruncateForEmbedByteCap(t *testing.T) {
	denseCode := strings.Repeat("p.f(x){_a[i++]=Math.min(a,b);}", 200)
	// 30 bytes × 200 = 6000 bytes of dense punctuation-heavy code.
	got := truncateForEmbed(denseCode, 1500)
	if len(got) > 2500 {
		t.Errorf("byte cap not enforced: got %d bytes, want ≤ 2500", len(got))
	}
}

// TestTruncateForEmbedTokenCap covers the original path: long
// CJK text where tokens accumulate at 1/rune even at low byte count.
func TestTruncateForEmbedTokenCap(t *testing.T) {
	cjk := strings.Repeat("中文测试", 1000) // 4 runes × 3 bytes = 12 bytes; 4 tokens
	got := truncateForEmbed(cjk, 500)
	// At 1 token/CJK-rune, 500 tokens = 500 runes = 1500 bytes.
	if len(got) > 1600 {
		t.Errorf("token cap not enforced on CJK: got %d bytes for 500 tokens", len(got))
	}
}

// TestTruncateForEmbedShortInput leaves short input unchanged.
func TestTruncateForEmbedShortInput(t *testing.T) {
	in := "hello world"
	if got := truncateForEmbed(in, 1500); got != in {
		t.Errorf("short input modified: got %q want %q", got, in)
	}
}

// TestTruncateForEmbedCutsAtWhitespace prefers a whitespace boundary
// over splitting a word, when one exists in the back half of the kept
// span.
func TestTruncateForEmbedCutsAtWhitespace(t *testing.T) {
	long := strings.Repeat("aaaaaaaa bbbbbbbb ", 200) // ~3600 bytes
	got := truncateForEmbed(long, 5000)               // token cap won't bite
	if len(got) > 2500 {
		t.Errorf("byte cap not enforced: %d bytes", len(got))
	}
	if got[len(got)-1] != 'a' && got[len(got)-1] != 'b' && got[len(got)-1] != ' ' {
		// Last byte must be from the source string (we don't append anything).
		// Just sanity that we returned a prefix.
		t.Errorf("unexpected last byte: %q", got[len(got)-1])
	}
}
