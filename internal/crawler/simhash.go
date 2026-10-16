package crawler

import (
	"encoding/hex"
	"math/bits"
	"strings"
	"unicode"
)

// ExtractVersion changes with anything that can alter the text Parse or ParsePDF return for the same
// bytes, and with the simhash below: a worker and the server agree on text only at equal versions.
const ExtractVersion = 1

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// Simhash is the 64-bit simhash of text over word shingles; ok is false when text holds no token.
func Simhash(text string) (sum uint64, ok bool) {
	tokens := simhashTokens(text)
	if len(tokens) == 0 {
		return 0, false
	}
	var counts [64]int
	add := func(h uint64) {
		for i := 0; i < 64; i++ {
			if h>>uint(i)&1 == 1 {
				counts[i]++
			} else {
				counts[i]--
			}
		}
	}
	if len(tokens) < 3 {
		for _, t := range tokens {
			add(fnv1a64(t))
		}
	} else {
		for i := 0; i+3 <= len(tokens); i++ {
			add(fnv1a64(tokens[i] + " " + tokens[i+1] + " " + tokens[i+2]))
		}
	}
	for i, c := range counts {
		if c > 0 {
			sum |= 1 << uint(i)
		}
	}
	return sum, true
}

// SimhashHex is the 16 lower-case hex digits of a simhash, big-endian.
func SimhashHex(sum uint64) string {
	var b [8]byte
	for i := range b {
		b[i] = byte(sum >> uint(56-8*i))
	}
	return hex.EncodeToString(b[:])
}

// ParseSimhash reads the 16 lower-case hex digits SimhashHex writes.
func ParseSimhash(s string) (uint64, bool) {
	if len(s) != 16 || strings.ToLower(s) != s {
		return 0, false
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return 0, false
	}
	var sum uint64
	for _, b := range raw {
		sum = sum<<8 | uint64(b)
	}
	return sum, true
}

// SimhashDistance is the Hamming distance of two simhashes.
func SimhashDistance(a, b uint64) int { return bits.OnesCount64(a ^ b) }

func simhashTokens(text string) []string {
	var tokens []string
	start := -1
	flush := func(end int) {
		if start >= 0 {
			tokens = append(tokens, strings.Map(unicode.ToLower, text[start:end]))
			start = -1
		}
	}
	for i, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		flush(i)
	}
	flush(len(text))
	return tokens
}

func fnv1a64(s string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}
