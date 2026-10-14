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
	maxParts         = 8192
	boundaryMinWords = 8
	partMaxWords     = 96
	boundaryModulus  = 32
	floorPercent     = 60
	gap              = "…"

	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// splitText is a text cut into content-defined parts (WORK 6.5). Only the first maxParts parts are hashed.
type splitText struct {
	words  []string
	ends   []int
	hashes []uint64
	beyond bool
}

func splitParts(text string) *splitText {
	p := &splitText{words: strings.Fields(text)}
	start, prev := 0, ""
	for i, w := range p.words {
		low := strings.Map(unicode.ToLower, w)
		n := i - start + 1
		if n == partMaxWords || (n >= boundaryMinWords && boundary(prev, low)) {
			p.add(i + 1)
			start = i + 1
			if len(p.ends) == maxParts {
				p.beyond = start < len(p.words)
				return p
			}
		}
		prev = low
	}
	if start < len(p.words) {
		p.add(len(p.words))
	}
	return p
}

func (p *splitText) add(end int) {
	start := 0
	if n := len(p.ends); n > 0 {
		start = p.ends[n-1]
	}
	p.ends = append(p.ends, end)
	p.hashes = append(p.hashes, hashWords(p.words[start:end]))
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

func partSet(hashes []uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(hashes))
	for _, h := range hashes {
		set[h] = struct{}{}
	}
	return set
}

// agreed is the text of the parts whose hash is in keep, one gap for each run of the others.
func (p *splitText) agreed(keep map[uint64]struct{}) string {
	var b strings.Builder
	start, gapped := 0, false
	for i, end := range p.ends {
		if _, ok := keep[p.hashes[i]]; ok {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(strings.Join(p.words[start:end], " "))
			gapped = false
		} else if !gapped {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(gap)
			gapped = true
		}
		start = end
	}
	if p.beyond && !gapped {
		b.WriteByte(' ')
		b.WriteString(gap)
	}
	return b.String()
}
