package index

import (
	"unicode"
)

// Chunk is one passage with byte offsets back into the original document text.
// Offset + Length identify the source span so we can reconstruct highlights
// without storing the chunk text in the vector index.
type Chunk struct {
	Text   string
	Offset int // byte offset of the chunk's first word in the source
	Length int // byte length of the chunk
}

// Chunker splits text into overlapping word-window passages.
//
// Why word-windows instead of true tokens: a real tokenizer (BPE) would track
// the embedding model's tokenization exactly, but adds a dep (sentencepiece/tiktoken)
// and ~5MB of vocab data. At v0 scale the chunk-boundary loss from word-windows is
// small (<1% on BEIR-style benches in published comparisons); we can swap in a
// real tokenizer later without changing the storage shape.
//
// Defaults: 320 words per chunk (~512 tokens at ~0.6 tokens/word for English)
// with 64 words overlap. Tunable per call so vertical deployments can sweep.
type Chunker struct {
	Size    int // target words per chunk
	Overlap int // shared words between consecutive chunks
}

// NewChunker returns a Chunker with reasonable defaults.
func NewChunker() *Chunker { return &Chunker{Size: 320, Overlap: 64} }

// NewChunkerWith returns a Chunker, applying size/overlap overrides only when
// the corresponding argument is strictly positive. Zero/negative inputs fall
// through to NewChunker's defaults (320/64). Single source of truth for the
// override-fallback pattern that previously lived inline at 3 callsites
// (iter 142 crawler, iter 145 server reembed, iter 145 cmd chunkerWith).
//
// Use this anywhere chunker parameters might come from config or per-host
// overrides where 0 means "leave default alone."
func NewChunkerWith(size, overlap int) *Chunker {
	c := NewChunker()
	if size > 0 {
		c.Size = size
	}
	if overlap > 0 {
		c.Overlap = overlap
	}
	return c
}

// Chunk splits the document into overlapping windows.
//
// Documents shorter than Size produce a single chunk covering the whole doc.
// Empty input → nil. The byte spans line up exactly with the source string, so
// `source[ch.Offset : ch.Offset+ch.Length]` reconstructs the chunk verbatim.
func (c *Chunker) Chunk(text string) []Chunk {
	size := c.Size
	if size <= 0 {
		size = 320
	}
	overlap := c.Overlap
	if overlap < 0 {
		overlap = 0
	}
	if overlap >= size {
		overlap = size / 2
	}

	words := splitWordsWithPos(text)
	if len(words) == 0 {
		return nil
	}
	if len(words) <= size {
		s := words[0].Start
		e := words[len(words)-1].End
		return []Chunk{{Text: text[s:e], Offset: s, Length: e - s}}
	}

	step := size - overlap
	out := make([]Chunk, 0, len(words)/step+1)
	for i := 0; i < len(words); i += step {
		end := i + size
		if end > len(words) {
			end = len(words)
		}
		s := words[i].Start
		e := words[end-1].End
		out = append(out, Chunk{Text: text[s:e], Offset: s, Length: e - s})
		if end == len(words) {
			break
		}
	}
	return out
}

type wordPos struct {
	Start, End int
}

// splitWordsWithPos returns word byte-spans without allocating intermediate strings.
func splitWordsWithPos(text string) []wordPos {
	out := make([]wordPos, 0, len(text)/6)
	inWord := false
	start := 0
	for i, r := range text {
		if unicode.IsSpace(r) {
			if inWord {
				out = append(out, wordPos{Start: start, End: i})
				inWord = false
			}
		} else if !inWord {
			inWord = true
			start = i
		}
	}
	if inWord {
		out = append(out, wordPos{Start: start, End: len(text)})
	}
	return out
}
