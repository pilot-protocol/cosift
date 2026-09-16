package crawler

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/pilot-protocol/cosift/internal/embed"
)

type IndexedChunk struct {
	Text      string    `json:"text"`
	Embedding []float32 `json:"embedding"`
}

type LocalArtifact struct {
	URL    string         `json:"url"`
	Title  string         `json:"title"`
	Text   string         `json:"text"`
	Model  string         `json:"model"`
	Chunks []IndexedChunk `json:"chunks"`
}

func (a *LocalArtifact) Validate() error {
	if a == nil || len(a.Title) > 1000 || len(a.Text) < 80 || len(a.Text) > 32000 || a.Model == "" || len(a.Model) > 200 || len(a.Chunks) == 0 || len(a.Chunks) > 64 {
		return fmt.Errorf("expected bounded text, metadata, model and 1–64 embedding chunks")
	}
	full := a.Title + "\n\n" + a.Text
	for _, ch := range a.Chunks {
		if ch.Text == "" || !strings.Contains(full, ch.Text) || len(ch.Embedding) == 0 || len(ch.Embedding) > 4096 {
			return fmt.Errorf("invalid embedding chunk")
		}
		var norm float64
		for _, x := range ch.Embedding {
			if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
				return fmt.Errorf("non-finite embedding")
			}
			norm += float64(x) * float64(x)
		}
		if norm == 0 {
			return fmt.Errorf("zero embedding")
		}
	}
	return nil
}

// Verify every uploaded vector against the configured model before reuse.
// This intentionally saves no verification compute in v1: trusting arbitrary
// client vectors would allow index poisoning and fraudulent contribution credit.
func VerifyArtifact(ctx context.Context, a *LocalArtifact, title, text string, reference embed.Embedder) (embed.Embedder, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	if a.Title != title || a.Text != text {
		return nil, fmt.Errorf("local content does not match the current webpage")
	}
	if reference == nil || a.Model != reference.Model() {
		return nil, fmt.Errorf("embedding model does not match the index")
	}
	texts := make([]string, len(a.Chunks))
	for i, ch := range a.Chunks {
		if len(ch.Embedding) != reference.Dim() {
			return nil, fmt.Errorf("embedding dimension does not match the index")
		}
		texts[i] = ch.Text
	}
	refs, err := reference.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(refs) != len(texts) {
		return nil, fmt.Errorf("embedding verifier unavailable")
	}
	verified := &artifactEmbedder{Embedder: reference, vectors: map[string][]float32{}}
	for i, ch := range a.Chunks {
		if len(refs[i]) != len(ch.Embedding) {
			return nil, fmt.Errorf("embedding verifier dimension mismatch")
		}
		var difference, norm float64
		for j, x := range ch.Embedding {
			r := float64(refs[i][j])
			d := float64(x) - r
			difference += d * d
			norm += r * r
		}
		if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) || math.IsNaN(difference) || difference/norm > 0.0001 {
			return nil, fmt.Errorf("local embedding failed verification")
		}
		verified.vectors[ch.Text] = ch.Embedding
	}
	return verified, nil
}

type artifactEmbedder struct {
	embed.Embedder
	vectors map[string][]float32
}

func (e *artifactEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		v, ok := e.vectors[text]
		if !ok {
			return e.Embedder.Embed(ctx, texts)
		}
		out[i] = v
	}
	return out, nil
}
