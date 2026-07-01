package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/embed"
	"github.com/pilot-protocol/cosift/internal/index"
	"github.com/pilot-protocol/cosift/internal/rerank"
	"github.com/pilot-protocol/cosift/internal/server"
	"github.com/pilot-protocol/cosift/internal/store"
)

// authStatus describes how a configured capability will authenticate to its
// endpoint, for /doctor output.
func authStatus(configured bool, key string, urlSet bool) string {
	switch {
	case !configured:
		return "(none)"
	case key != "":
		return "bearer-token"
	case urlSet:
		return "anonymous(custom-url)"
	default:
		return "MISSING"
	}
}

// hnswPassageWriter bridges the crawler's PassageWriter contract to
// an in-memory index.HNSW graph. Each embedded passage looks up the doc's URL
// and Title (via *PebbleStore.GetDocByID) and is inserted into the graph; the
// graph is persisted to Pebble's 'v' family in a single Persist() call at the
// end of the crawl.
type hnswPassageWriter struct {
	ps   *store.PebbleStore
	hnsw *index.HNSW
}

func (w *hnswPassageWriter) UpsertPassage(ctx context.Context, p *store.Passage) error {
	doc, err := w.ps.GetDocByID(ctx, p.DocID)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil // doc was deleted between index and embed — skip silently
	}
	w.hnsw.AddPassage(doc.URL, doc.Title, p.Offset, p.Length, p.Embedding)
	return nil
}

// UpsertPassageBatch writes all passages in a single HNSW lock
// acquisition — meaningful when a doc has many chunks. Implements the
// optional crawler.PassageWriterBatch interface; the crawler will call
// this when present instead of looping UpsertPassage per chunk.
func (w *hnswPassageWriter) UpsertPassageBatch(ctx context.Context, ps []*store.Passage) error {
	if len(ps) == 0 {
		return nil
	}
	// All passages in a batch belong to the same DocID (the crawler hands
	// us one doc's chunks). Resolve url + title once.
	doc, err := w.ps.GetDocByID(ctx, ps[0].DocID)
	if err != nil {
		return err
	}
	if doc == nil {
		return nil
	}
	items := make([]index.PassageInput, 0, len(ps))
	for _, p := range ps {
		items = append(items, index.PassageInput{
			URL: doc.URL, Title: doc.Title, Offset: p.Offset, Length: p.Length, Vec: p.Embedding,
		})
	}
	w.hnsw.AddPassageBatch(items)
	return nil
}

// MarkURLInvalid satisfies the optional crawler.URLInvalidator interface.
// Lets the crawler reclaim zombie passages (prior generations of chunks for
// the same URL) before pushing a fresh chunk batch. Returns count zeroed.
func (w *hnswPassageWriter) MarkURLInvalid(ctx context.Context, url string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return w.hnsw.MarkURLPassagesInvalid(url), nil
}

func runServe(ctx context.Context, cfg *config.Config) error {
	s, err := store.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	srv := server.New(s)
	// Instance-wide retrieval defaults from cfg.Defaults. Per-request query
	// params still override; this is just what the handler picks up when the
	// caller doesn't specify.
	srv = srv.WithDefaults(server.Defaults{
		Retriever:         cfg.Defaults.Retriever,
		Expand:            cfg.Defaults.Expand,
		ResearchStrategy:  cfg.Defaults.ResearchStrategy,
		ResearchSynthK:    cfg.Defaults.ResearchSynthK,
		ExpandMainWeight:  cfg.Defaults.ExpandMainWeight,
		HybridDenseWeight: cfg.Defaults.HybridDenseWeight,
	})
	if cfg.Defaults.Retriever != "" || cfg.Defaults.Expand || cfg.Defaults.ResearchStrategy != "" || cfg.Defaults.ResearchSynthK > 0 || cfg.Defaults.ExpandMainWeight > 0 || cfg.Defaults.HybridDenseWeight > 0 {
		log.Printf("defaults: retriever=%q expand=%v research_strategy=%q research_synth_k=%d expand_main_weight=%v hybrid_dense_weight=%v",
			cfg.Defaults.Retriever, cfg.Defaults.Expand, cfg.Defaults.ResearchStrategy, cfg.Defaults.ResearchSynthK, cfg.Defaults.ExpandMainWeight, cfg.Defaults.HybridDenseWeight)
	}
	// thread chunker config to /admin/reembed handler so re-embed
	// produces passages with the same shape as crawl-time indexing.
	if cfg.Crawler.ChunkSize > 0 || cfg.Crawler.ChunkOverlap > 0 {
		srv = srv.WithChunker(cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
		log.Printf("chunker: size=%d overlap=%d", cfg.Crawler.ChunkSize, cfg.Crawler.ChunkOverlap)
	}
	if tok := cfg.Server.AdminToken; tok != "" {
		srv = srv.WithAdminToken(tok)
		log.Printf("/admin/* endpoints enabled (bearer-auth)")
	}
	if len(cfg.Server.TrustedProxies) > 0 {
		s2, err := srv.WithTrustedProxies(cfg.Server.TrustedProxies)
		if err != nil {
			return fmt.Errorf("trusted_proxies: %w", err)
		}
		srv = s2
		log.Printf("X-Forwarded-For trusted from %v", cfg.Server.TrustedProxies)
	}

	// Auto-wire embeddings if model is set and an API key is in env.
	if cfg.Embeddings.Model != "" {
		apiKey := os.Getenv("OPENAI_API_KEY")
		if apiKey == "" {
			apiKey = os.Getenv("OPENAI")
		}
		if apiKey == "" {
			log.Printf("warning: embeddings configured but no OPENAI_API_KEY in env; dense/hybrid disabled")
		} else {
			model := cfg.Embeddings.Model
			dim := cfg.Embeddings.Dim
			if dim == 0 {
				dim = 1536
			}
			emb := embed.NewOpenAIClient(apiKey, cfg.Embeddings.URL, model, dim)
			vi, err := index.LoadVectorIndex(ctx, s, model, dim)
			if err != nil {
				return fmt.Errorf("load vector index: %w", err)
			}
			srv = srv.WithVector(vi, emb)
			log.Printf("vector index loaded: %d passages, model=%s dim=%d", vi.Len(), model, dim)

			// Chat is opt-in via cfg.Chat.Model. Reuses the same API key.
			if cfg.Chat.Model != "" {
				chat := embed.NewOpenAIChat(apiKey, cfg.Chat.URL, cfg.Chat.Model)
				srv = srv.WithChat(chat)
				log.Printf("/answer enabled with chat model=%s", cfg.Chat.Model)
				// Auto-enable /search?expand=true via the same chat client.
				// measured +0.02 nDCG at 10k distractors for $0.004 per run.
				srv = srv.WithParaphraser(chat, 2)
				log.Printf("/search?expand=true enabled (auto-paraphrase via %s)", cfg.Chat.Model)
			}

			// Reranker: HTTP backend wins if URL is set; otherwise LLM backend
			// (only if chat is configured). Either is opt-out via cfg.Rerank.Enabled=false
			// even when configured.
			var rr rerank.Reranker
			rerankWanted := cfg.Rerank.URL != "" || cfg.Chat.Model != ""
			if !cfg.Rerank.Enabled && rerankWanted {
				// Default ON when wired; flip cfg.Rerank.Enabled false to skip.
				cfg.Rerank.Enabled = true
			}
			if cfg.Rerank.Enabled && cfg.Rerank.URL != "" {
				rerankKey := cfg.Rerank.APIKey
				if rerankKey == "" {
					rerankKey = firstEnv("COHERE_API_KEY", "VOYAGE_API_KEY")
				}
				rr = rerank.NewHTTPReranker(cfg.Rerank.URL, rerankKey, cfg.Rerank.Model)
				log.Printf("rerank enabled with http endpoint=%s model=%s", cfg.Rerank.URL, cfg.Rerank.Model)
			} else if cfg.Rerank.Enabled && cfg.Chat.Model != "" {
				rerankModel := cfg.Rerank.Model
				if rerankModel == "" {
					rerankModel = cfg.Chat.Model
				}
				rerankChat := embed.NewOpenAIChat(apiKey, cfg.Chat.URL, rerankModel)
				rr = rerank.NewLLMReranker(rerankChat)
				log.Printf("rerank enabled with LLM model=%s (fallback path; consider an HTTP reranker for cost)", rerankModel)
			}
			if rr != nil {
				srv = srv.WithReranker(rr, cfg.Rerank.CandidateK)
			}
		}
	}

	// On-demand /contents: use the crawler's FetchOne. Doesn't share the worker
	// pool's rate gate or robots cache — callers should set a sensible UA + body cap.
	srv = srv.WithFetcher(func(ctx context.Context, u string) (string, string, string, error) {
		r, err := crawler.FetchOne(ctx, nil, cfg.Crawler.UserAgent, u, cfg.Crawler.MaxBodyBytes)
		if err != nil {
			return "", "", "", err
		}
		return r.Title, r.Text, r.Lang, nil
	})

	log.Printf("cosift serving on %s", cfg.Server.Addr)
	return server.ListenAndServe(ctx, cfg.Server.Addr, srv.Handler())
}
