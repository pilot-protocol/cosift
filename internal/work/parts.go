package work

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	maxParts         = 2048
	boundaryMinWords = 8
	partMaxWords     = 96
	boundaryModulus  = 32
	floorPercent     = 60
	minAgreedBytes   = 80
	gap              = "…"

	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// splitText is a text cut into content-defined parts (WORK 6.5). Only the first maxParts parts are
// hashed; total counts them all.
type splitText struct {
	words  []string
	ends   []int
	hashes []uint64
	total  int
}

func splitParts(text string) *splitText {
	p := &splitText{words: strings.Fields(text)}
	start, prev := 0, ""
	for i, w := range p.words {
		low := strings.Map(unicode.ToLower, w)
		n := i - start + 1
		if n == partMaxWords || (n >= boundaryMinWords && boundary(prev, low)) {
			p.cut(start, i+1)
			start = i + 1
		}
		prev = low
	}
	if start < len(p.words) {
		p.cut(start, len(p.words))
	}
	return p
}

func (p *splitText) cut(start, end int) {
	p.total++
	if len(p.ends) < maxParts {
		p.ends = append(p.ends, end)
		p.hashes = append(p.hashes, hashWords(p.words[start:end]))
	}
}

// part is the text of the j-th hashed part: its words joined by single spaces.
func (p *splitText) part(j int) string {
	start := 0
	if j > 0 {
		start = p.ends[j-1]
	}
	return strings.Join(p.words[start:p.ends[j]], " ")
}

// boundary is FNV-1a-64 of the two lower-cased words joined by a space, as the simhash hashes its shingles.
func boundary(prev, cur string) bool {
	h := uint64(fnvOffset64)
	for _, s := range [...]string{prev, " ", cur} {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= fnvPrime64
		}
	}
	return h%boundaryModulus == 0
}

// hashWords is the first 8 bytes of SHA-256 over the words joined by single spaces.
func hashWords(words []string) uint64 {
	sum := sha256.Sum256([]byte(strings.Join(words, " ")))
	return binary.BigEndian.Uint64(sum[:8])
}

func hexOf(h uint64) string { return fmt.Sprintf("%016x", h) }

// packParts is the W p value of an anchor: its part hashes as 8-byte big-endian values.
func packParts(hashes []uint64) []byte {
	out := make([]byte, 0, 8*len(hashes))
	for _, h := range hashes {
		out = binary.BigEndian.AppendUint64(out, h)
	}
	return out
}

func unpackParts(b []byte) ([]uint64, error) {
	if len(b)%8 != 0 {
		return nil, errors.New("work: malformed W p")
	}
	out := make([]uint64, len(b)/8)
	for i := range out {
		out[i] = binary.BigEndian.Uint64(b[8*i:])
	}
	return out, nil
}

// normaliseTitle is the title's words joined by single spaces, the only form that is compared or indexed.
func normaliseTitle(title string) string { return strings.Join(strings.Fields(title), " ") }

func titleSha(title string) string { return hexOf(hashWords(strings.Fields(title))) }

type match struct{ i, j int }

// align is the in-order walk of WORK 6.5 over the part hashes of an anchor a and a completing submission c.
func align[T comparable](a, c []T) []match {
	n, m := len(a), len(c)
	w := m + 1
	l := make([]uint16, (n+1)*w)
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == c[j] {
				l[i*w+j] = l[(i+1)*w+j+1] + 1
			} else {
				l[i*w+j] = max(l[(i+1)*w+j], l[i*w+j+1])
			}
		}
	}
	var out []match
	for i, j := 0, 0; i < n && j < m; {
		switch {
		case a[i] == c[j]:
			out = append(out, match{i, j})
			i, j = i+1, j+1
		case l[(i+1)*w+j] >= l[i*w+j+1]:
			i++
		default:
			j++
		}
	}
	return out
}

const gapMark = -1

// layout lists the completer's matched parts in order, with gapMark wherever either side has unmatched parts;
// aTotal and cTotal count the parts of each text before the cap.
func layout(ms []match, aTotal, cTotal int) []int {
	var out []int
	lastI, lastJ := -1, -1
	for _, m := range ms {
		if m.i-lastI > 1 || m.j-lastJ > 1 {
			out = append(out, gapMark)
		}
		out = append(out, m.j)
		lastI, lastJ = m.i, m.j
	}
	if len(ms) > 0 && (lastI+1 < aTotal || lastJ+1 < cTotal) {
		out = append(out, gapMark)
	}
	return out
}

// agreement is what a completing submission shares, in order, with an anchor.
type agreement struct {
	text    string // the matched parts in order, a gap for every unmatched span; empty when nothing matched
	matched int    // the bytes of the matched parts, M
}

// agreeWith aligns the text with an anchor's part hashes; anchorTotal is the anchor's part count before the cap.
func (p *splitText) agreeWith(anchor []uint64, anchorTotal int) agreement {
	var b strings.Builder
	ag := agreement{}
	for _, k := range layout(align(anchor, p.hashes), anchorTotal, p.total) {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		if k == gapMark {
			b.WriteString(gap)
			continue
		}
		part := p.part(k)
		ag.matched += len(part)
		b.WriteString(part)
	}
	ag.text = b.String()
	return ag
}

// floorMet reports whether matched bytes are at least floorPercent of length.
func floorMet(matched, length int) bool { return matched*100 >= floorPercent*length }
