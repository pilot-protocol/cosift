package work

import (
	"crypto/sha256"
	"encoding/hex"
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
	hashes []string
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

func hashWords(words []string) string {
	sum := sha256.Sum256([]byte(strings.Join(words, " ")))
	return hex.EncodeToString(sum[:8])
}

func titleSha(title string) string { return hashWords(strings.Fields(title)) }

func partSet(hashes []string) map[string]struct{} {
	set := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		set[h] = struct{}{}
	}
	return set
}

// agreed is the text of the parts whose hash is in keep, one gap for each run of the others.
func (p *splitText) agreed(keep map[string]struct{}) string {
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
