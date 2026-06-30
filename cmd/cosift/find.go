package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/embed"
)

// /find is the live resource-federation endpoint. Where /search and /research
// query the INDEXED corpus (great for exploration, weak for known-item lookup),
// /find federates LIVE queries to the authoritative source APIs — HuggingFace
// Hub, GitHub, and PyPI — at request time, then synthesizes a cited answer with
// real identifiers (HF model id, pip package, GitHub repo) and install commands.
//
// Rationale: a search engine over a sampled corpus can't beat hitting the source
// index for "find me the specific installable thing for platform X". This closes
// that gap by doing what a human does with WebFetch, in one call.

type findHit struct {
	Source  string `json:"source"` // huggingface | github | pypi
	Title   string `json:"title"`
	URL     string `json:"url"`
	Summary string `json:"summary"`
	Meta    string `json:"meta"` // "41 likes" | "3249 stars" | "v1.1.0"
	rank    int    // source-local popularity for ordering
}

func findHTTPGet(ctx context.Context, u string, hdr map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "cosift-find/1.0")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return b, resp.StatusCode, err
}

// federateHF queries the HF Hub search API across models, datasets, and spaces.
func federateHF(ctx context.Context, q string) []findHit {
	var out []findHit
	kinds := []struct{ api, prefix string }{
		{"models", "/"}, {"datasets", "/datasets/"}, {"spaces", "/spaces/"},
	}
	for _, k := range kinds {
		u := fmt.Sprintf("https://huggingface.co/api/%s?search=%s&limit=5&sort=likes&direction=-1",
			k.api, url.QueryEscape(q))
		b, code, err := findHTTPGet(ctx, u, nil)
		if err != nil || code != 200 {
			continue
		}
		var items []struct {
			ID        string `json:"id"`
			Likes     int    `json:"likes"`
			Downloads int    `json:"downloads"`
		}
		if json.Unmarshal(b, &items) != nil {
			continue
		}
		for _, it := range items {
			if it.ID == "" {
				continue
			}
			out = append(out, findHit{
				Source: "huggingface", Title: it.ID,
				URL:  "https://huggingface.co" + k.prefix + it.ID,
				Meta: fmt.Sprintf("%d likes, %d dl (%s)", it.Likes, it.Downloads, k.api),
				rank: it.Likes,
			})
		}
	}
	return out
}

// federateGitHub queries the GitHub repo search API (by relevance/stars).
func federateGitHub(ctx context.Context, q string) []findHit {
	u := "https://api.github.com/search/repositories?q=" + url.QueryEscape(q) + "&sort=stars&order=desc&per_page=6"
	b, code, err := findHTTPGet(ctx, u, map[string]string{"Accept": "application/vnd.github+json"})
	if err != nil || code != 200 {
		return nil
	}
	var res struct {
		Items []struct {
			FullName string `json:"full_name"`
			HTMLURL  string `json:"html_url"`
			Desc     string `json:"description"`
			Stars    int    `json:"stargazers_count"`
			Lang     string `json:"language"`
		} `json:"items"`
	}
	if json.Unmarshal(b, &res) != nil {
		return nil
	}
	var out []findHit
	for _, it := range res.Items {
		out = append(out, findHit{
			Source: "github", Title: it.FullName, URL: it.HTMLURL,
			Summary: it.Desc, Meta: fmt.Sprintf("%d stars, %s", it.Stars, it.Lang),
			rank: it.Stars,
		})
	}
	return out
}

var pypiNameRe = regexp.MustCompile(`package-snippet__name">([^<]+)<`)
var pypiDescRe = regexp.MustCompile(`package-snippet__description">([^<]*)<`)
var pypiVerRe = regexp.MustCompile(`package-snippet__version">([^<]+)<`)

// federatePyPI parses the PyPI search results page (PyPI retired its search API,
// but the HTML results page is stable and parseable). Best-effort.
func federatePyPI(ctx context.Context, q string) []findHit {
	u := "https://pypi.org/search/?q=" + url.QueryEscape(q)
	b, code, err := findHTTPGet(ctx, u, nil)
	if err != nil || code != 200 {
		return nil
	}
	html := string(b)
	names := pypiNameRe.FindAllStringSubmatch(html, 8)
	descs := pypiDescRe.FindAllStringSubmatch(html, 8)
	vers := pypiVerRe.FindAllStringSubmatch(html, 8)
	var out []findHit
	for i, n := range names {
		name := strings.TrimSpace(n[1])
		if name == "" {
			continue
		}
		h := findHit{
			Source: "pypi", Title: name,
			URL: "https://pypi.org/project/" + name + "/",
		}
		if i < len(descs) {
			h.Summary = strings.TrimSpace(descs[i][1])
		}
		if i < len(vers) {
			h.Meta = strings.TrimSpace(vers[i][1])
		}
		out = append(out, h)
	}
	return out
}

const findSynthPrompt = `You are a technical resource finder for ML/software engineering. The user wants SPECIFIC, installable resources. Using ONLY the candidate resources provided (from HuggingFace, GitHub, and PyPI), recommend the most relevant ones.
- Give EXACT identifiers: HF model/dataset id, pip package name, GitHub repo (owner/name).
- Give the install/download command where applicable: ` + "`pip install <pkg>`" + `, ` + "`hf_hub_download(...)`" + `, ` + "`git clone`" + `.
- If the user named a platform (e.g. Jetson/aarch64/JetPack), note compatibility ONLY if a source states it; otherwise say it must be verified on the resource's page.
- Cite every recommendation by its numeric id, e.g. [2]. Do not invent resources or claims not in the sources.
- Be concise and actionable: lead with the top 2-3 picks.`

const findPlanPrompt = `Convert the user's resource request into 1-4 SHORT search phrases (2-4 words each) for searching model/package/repo catalogs (HuggingFace, GitHub, PyPI). These catalogs do AND-style matching, so long natural-language queries return nothing — extract the core nouns. Each phrase should target ONE resource the user needs (e.g. a detector, an OCR backend, a classifier). Drop platform/qualifier words (jetson, aarch64, model, library). Output ONLY a JSON array of strings. Example: ["license plate detection", "license plate OCR", "vehicle classifier"]`

func (s *pebbleHTTP) handleFind(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeProblem(w, http.StatusBadRequest, "missing q parameter")
		return
	}
	start := time.Now()

	// Distill the NL request into short keyword phrases — the source search
	// APIs AND-match, so a verbose query ("YOLOv8 LPR model for Jetson aarch64")
	// returns zero, but "license plate detection" returns the right models.
	phrases := []string{q}
	if s.chat != nil {
		if raw, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
			{Role: "system", Content: findPlanPrompt},
			{Role: "user", Content: q},
		}); err == nil {
			if p := parseSubQueries(raw, q); len(p) > 0 {
				phrases = p
			}
		}
	}
	if len(phrases) > 4 {
		phrases = phrases[:4]
	}

	// Federate each phrase across all three sources in parallel; each call
	// degrades to empty on failure (e.g. GitHub rate-limit) so the others
	// still answer. Dedup by URL afterward.
	var mu sync.Mutex
	var hits []findHit
	var wg sync.WaitGroup
	for _, ph := range phrases {
		for _, fn := range []func(context.Context, string) []findHit{federateHF, federateGitHub, federatePyPI} {
			ph, fn := ph, fn
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := fn(r.Context(), ph)
				mu.Lock()
				hits = append(hits, h...)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()

	// Order: source-local popularity desc, grouped so the synth sees the
	// strongest candidates first.
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].rank > hits[j].rank })

	// Dedup by URL (keeps the highest-ranked occurrence since we sorted first),
	// then cap the pool fed to synthesis.
	seen := make(map[string]bool, len(hits))
	deduped := hits[:0]
	for _, h := range hits {
		if h.URL == "" || seen[h.URL] {
			continue
		}
		seen[h.URL] = true
		deduped = append(deduped, h)
	}
	hits = deduped
	const maxFindHits = 24
	if len(hits) > maxFindHits {
		hits = hits[:maxFindHits]
	}

	if len(hits) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{
			"query": q, "sources": []findHit{}, "answer": "",
			"note": "no resources found via HuggingFace/GitHub/PyPI federation",
			"took": time.Since(start).String(),
		})
		return
	}

	// Build the cited sources block + synthesize (when chat is configured).
	var sb strings.Builder
	for i, h := range hits {
		fmt.Fprintf(&sb, "[%d] (%s) %s — %s\n    %s%s\n", i+1, h.Source, h.Title, h.URL,
			h.Summary, func() string {
				if h.Meta != "" {
					return " (" + h.Meta + ")"
				}
				return ""
			}())
	}
	answer := ""
	if s.chat != nil {
		out, err := s.doChat(r.Context(), s.chat, []embed.ChatMsg{
			{Role: "system", Content: findSynthPrompt},
			{Role: "user", Content: "Request: " + q + "\n\nCandidate resources:\n" + sb.String()},
		})
		if err == nil {
			answer = out
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"query": q, "answer": answer, "sources": hits,
		"count": len(hits), "took": time.Since(start).String(),
	})
}
