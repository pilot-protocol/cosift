package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/store"
)

func indexLocalContributions(ctx context.Context, cfg *config.Config, urls []string) ([]*crawler.LocalArtifact, error) {
	return indexLocalContributionsWithClient(ctx, cfg, urls, crawler.PublicHTTPClient(30*time.Second))
}

func indexLocalContributionsWithClient(ctx context.Context, cfg *config.Config, urls []string, client *http.Client) ([]*crawler.LocalArtifact, error) {
	if cfg == nil || cfg.Embeddings.Model == "" || cfg.Embeddings.Dim <= 0 || cfg.Embeddings.URL == "" {
		return nil, fmt.Errorf("configure embeddings.url, model and dim for your local embedding service")
	}
	db, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	emb := embed.NewOpenAIClient(resolveEmbedAPIKey(), cfg.Embeddings.URL, cfg.Embeddings.Model, cfg.Embeddings.Dim)
	robots := crawler.NewRobots(client, "Cosift-Community/1.0")
	var out []*crawler.LocalArtifact
	for _, raw := range urls {
		allowed, delay, err := robots.Allowed(ctx, raw)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, fmt.Errorf("robots.txt excludes %s", raw)
		}
		if delay > 0 {
			if delay > 15*time.Second {
				return nil, fmt.Errorf("crawl delay exceeds local indexing limit")
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		page, err := crawler.FetchOne(ctx, client, "Cosift-Community/1.0", raw, 2<<20)
		if err != nil {
			return nil, err
		}
		u, _ := url.Parse(raw)
		size, overlap := cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap
		if v := cfg.Crawler.PerHostChunkSize[u.Host]; v > 0 {
			size = v
		}
		if v := cfg.Crawler.PerHostChunkOverlap[u.Host]; v > 0 {
			overlap = v
		}
		chunks := index.NewChunkerWith(size, overlap).Chunk(page.Title + "\n\n" + page.Text)
		if len(page.Text) > 32000 || len(chunks) > 64 {
			return nil, fmt.Errorf("page exceeds local contribution size limit")
		}
		texts := make([]string, len(chunks))
		for i, ch := range chunks {
			texts[i] = ch.Text
		}
		vectors, err := emb.Embed(ctx, texts)
		if err != nil {
			return nil, err
		}
		if len(vectors) != len(chunks) {
			return nil, fmt.Errorf("embedding service returned wrong vector count")
		}
		a := &crawler.LocalArtifact{URL: raw, Title: page.Title, Text: page.Text, Model: emb.Model()}
		for i, ch := range chunks {
			a.Chunks = append(a.Chunks, crawler.IndexedChunk{Text: ch.Text, Embedding: vectors[i]})
		}
		if err := a.Validate(); err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(page.Text))
		id, err := db.UpsertDocument(ctx, &store.Document{URL: raw, Domain: u.Host, Title: page.Title, Text: page.Text, Lang: page.Lang, Source: "crawl", FetchedAt: time.Now(), ContentSHA: sum[:]})
		if err != nil {
			return nil, err
		}
		if err := index.NewBM25(db).IndexDocument(ctx, id, page.Title, page.Text); err != nil {
			return nil, err
		}
		for i, ch := range chunks {
			if err := db.UpsertPassage(ctx, &store.Passage{DocID: id, Offset: ch.Offset, Length: ch.Length, Model: emb.Model(), Embedding: vectors[i]}); err != nil {
				return nil, err
			}
		}
		out = append(out, a)
	}
	return out, nil
}
