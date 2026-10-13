package crawler

import (
	"container/list"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// robotsGroup is the rules for one or more matching User-agent lines.
type robotsGroup struct {
	agents     []string
	disallow   []string
	allow      []string
	crawlDelay time.Duration
}

// robotsRules is the parsed robots.txt for one host.
type robotsRules struct {
	groups    []robotsGroup
	fetchedAt time.Time
	// sitemaps are URLs from the `Sitemap:` directive in robots.txt.
	// Group-independent per the sitemaps.org spec (separate from per-UA groups).
	// Modern crawlers commonly auto-discover sitemap URLs this way; exposing
	// them via Robots.Sitemaps() lets operators inspect what they'd find before
	// running `cosift crawl -sitemap`.
	sitemaps []string
}

// Robots caches robots.txt per host and decides Allowed for a given URL.
//
// Implementation notes:
//   - We are lenient when robots.txt is missing or 4xx/5xx — return Allowed=true.
//     This matches most modern crawler libraries' behavior.
//   - Cache TTL: 24h per host. The cache is also bounded by entry count and
//     retained bytes, evicting the least recently used host first.
//   - We follow the REP (Robots Exclusion Protocol) draft semantics: longest UA
//     prefix wins; for path rules, Allow with a longer pattern beats Disallow.
//   - Supported directives: User-agent, Disallow, Allow, Crawl-delay, Sitemap.
//   - Pattern wildcards: '*' supported; '$' end-anchor supported.
type Robots struct {
	mu         sync.Mutex
	cache      map[string]*list.Element // value: *robotsEntry
	lru        *list.List               // front = most recently used
	bytes      int64
	maxEntries int
	maxBytes   int64
	client     *http.Client
	ua         string
	ttl        time.Duration
}

type robotsEntry struct {
	key   string
	rules *robotsRules
	size  int64
}

// Default cache bounds for NewRobots.
const (
	DefaultRobotsMaxEntries = 50_000
	DefaultRobotsMaxBytes   = 64 << 20
)

// NewRobots constructs a Robots cache with the default bounds.
func NewRobots(client *http.Client, userAgent string) *Robots {
	return NewRobotsWithLimits(client, userAgent, DefaultRobotsMaxEntries, DefaultRobotsMaxBytes)
}

// NewRobotsWithLimits constructs a Robots cache holding at most maxEntries
// hosts and about maxBytes of parsed rules; non-positive limits use the defaults.
func NewRobotsWithLimits(client *http.Client, userAgent string, maxEntries int, maxBytes int64) *Robots {
	if maxEntries <= 0 {
		maxEntries = DefaultRobotsMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = DefaultRobotsMaxBytes
	}
	return &Robots{
		cache:      make(map[string]*list.Element),
		lru:        list.New(),
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		client:     client,
		ua:         userAgent,
		ttl:        24 * time.Hour,
	}
}

// Allowed reports whether the given URL can be crawled.
// The returned Crawl-delay (if any) should be honored on top of the per-host throttle.
func (r *Robots) Allowed(ctx context.Context, rawURL string) (bool, time.Duration, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, 0, err
	}
	rules := r.rulesFor(ctx, u.Scheme+"://"+u.Host)

	path := u.Path
	if path == "" {
		path = "/"
	}
	allowed, delay := decideRobots(rules, path, r.ua)
	return allowed, delay, nil
}

// Sitemaps returns the URLs from the host's robots.txt Sitemap: directives.
// Returns nil if the host has no robots.txt or no Sitemap entries.
// modern-crawler parity (Googlebot/Bingbot auto-discover sitemaps
// from robots.txt; cosift now exposes the same data).
//
// `hostURL` only needs scheme + host (e.g., "https://example.com"). Subsequent
// calls hit the same 24h cache as Allowed().
func (r *Robots) Sitemaps(ctx context.Context, hostURL string) []string {
	u, err := url.Parse(hostURL)
	if err != nil || u.Host == "" {
		return nil
	}
	rules := r.rulesFor(ctx, u.Scheme+"://"+u.Host)

	if len(rules.sitemaps) == 0 {
		return nil
	}
	// Return a copy so callers can't mutate the cached entry.
	out := make([]string, len(rules.sitemaps))
	copy(out, rules.sitemaps)
	return out
}

// rulesFor returns the host's rules, refetching once the TTL has passed.
func (r *Robots) rulesFor(ctx context.Context, key string) *robotsRules {
	r.mu.Lock()
	if el, ok := r.cache[key]; ok {
		if e := el.Value.(*robotsEntry); time.Since(e.rules.fetchedAt) <= r.ttl {
			r.lru.MoveToFront(el)
			r.mu.Unlock()
			return e.rules
		}
	}
	r.mu.Unlock()

	rules := r.fetch(ctx, key)
	r.store(key, rules)
	return rules
}

func (r *Robots) store(key string, rules *robotsRules) {
	size := rules.size() + int64(len(key))
	r.mu.Lock()
	defer r.mu.Unlock()
	if el, ok := r.cache[key]; ok {
		e := el.Value.(*robotsEntry)
		r.bytes += size - e.size
		e.rules, e.size = rules, size
		r.lru.MoveToFront(el)
	} else {
		r.cache[key] = r.lru.PushFront(&robotsEntry{key: key, rules: rules, size: size})
		r.bytes += size
	}
	for r.lru.Len() > 1 && (r.lru.Len() > r.maxEntries || r.bytes > r.maxBytes) {
		oldest := r.lru.Back()
		e := oldest.Value.(*robotsEntry)
		r.lru.Remove(oldest)
		delete(r.cache, e.key)
		r.bytes -= e.size
	}
}

// size estimates the bytes a parsed robots.txt keeps alive.
func (rr *robotsRules) size() int64 {
	const header = 16
	n := int64(128)
	for _, g := range rr.groups {
		n += 96
		for _, vals := range [][]string{g.agents, g.disallow, g.allow} {
			for _, v := range vals {
				n += int64(len(v)) + header
			}
		}
	}
	for _, v := range rr.sitemaps {
		n += int64(len(v)) + header
	}
	return n
}

func (r *Robots) fetch(ctx context.Context, base string) *robotsRules {
	empty := &robotsRules{fetchedAt: time.Now()}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/robots.txt", http.NoBody)
	if err != nil {
		return empty
	}
	req.Header.Set("User-Agent", r.ua)
	resp, err := r.client.Do(req)
	if err != nil {
		return empty
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return empty
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 500_000))
	if err != nil {
		return empty
	}
	return parseRobots(string(body))
}

// parseRobots is exported for tests via the helper below.
func parseRobots(body string) *robotsRules {
	rules := &robotsRules{fetchedAt: time.Now()}
	var current *robotsGroup
	// "expectingUA" means the previous line was also User-agent — we should
	// group these together, not start a new group on each User-agent line.
	expectingUA := false

	for _, line := range strings.Split(body, "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			expectingUA = false
			continue
		}
		i := strings.Index(line, ":")
		if i < 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:i]))
		val := strings.TrimSpace(line[i+1:])

		switch key {
		case "user-agent":
			if !expectingUA {
				rules.groups = append(rules.groups, robotsGroup{})
				current = &rules.groups[len(rules.groups)-1]
			}
			if current != nil {
				current.agents = append(current.agents, strings.Clone(strings.ToLower(val)))
			}
			expectingUA = true
		case "disallow":
			expectingUA = false
			if current == nil {
				continue
			}
			current.disallow = append(current.disallow, strings.Clone(val))
		case "allow":
			expectingUA = false
			if current == nil {
				continue
			}
			current.allow = append(current.allow, strings.Clone(val))
		case "crawl-delay":
			expectingUA = false
			if current == nil {
				continue
			}
			// Fractional values ("Crawl-delay: 0.5") are common in the wild.
			if d, err := strconv.ParseFloat(val, 64); err == nil && d > 0 {
				current.crawlDelay = time.Duration(d * float64(time.Second))
			}
		case "sitemap":
			// Sitemap directive is group-independent per the
			// sitemaps.org spec. Attaches to rules, not to current group.
			// Modern crawlers auto-discover sitemap URLs from robots.txt.
			expectingUA = false
			if val != "" {
				rules.sitemaps = append(rules.sitemaps, strings.Clone(val))
			}
		default:
			expectingUA = false
		}
	}
	return rules
}

func decideRobots(rules *robotsRules, path, ua string) (bool, time.Duration) {
	uaLower := strings.ToLower(ua)

	// Pick the longest UA match. Fall back to "*" if nothing specific matches.
	var best *robotsGroup
	var bestLen int
	var star *robotsGroup
	for i := range rules.groups {
		g := &rules.groups[i]
		for _, a := range g.agents {
			if a == "*" {
				if star == nil {
					star = g
				}
				continue
			}
			if strings.Contains(uaLower, a) && len(a) > bestLen {
				best = g
				bestLen = len(a)
			}
		}
	}
	if best == nil {
		best = star
	}
	if best == nil {
		return true, 0
	}

	allow := longestMatch(best.allow, path)
	disallow := longestMatch(best.disallow, path)
	if disallow < 0 {
		return true, best.crawlDelay
	}
	if allow > disallow {
		return true, best.crawlDelay
	}
	return false, best.crawlDelay
}

func longestMatch(patterns []string, path string) int {
	best := -1
	for _, p := range patterns {
		if p == "" {
			continue
		}
		if patternMatches(p, path) && len(p) > best {
			best = len(p)
		}
	}
	return best
}

// patternMatches implements REP wildcard matching anchored at the path start.
// Supports `*` (zero-or-more) anywhere in the pattern, and `$` end-anchor at
// the very end. Examples:
//
//	"/admin"     prefix-matches any URL starting with /admin (incl. /admin/foo)
//	"/admin$"    matches ONLY /admin (not /admin/foo) —
//	"/*.pdf$"    matches any URL ending with .pdf —
//	"/private/*" matches /private/anything (existing behavior)
func patternMatches(pattern, path string) bool {
	// detect trailing `$` end-anchor and strip it before matching.
	endAnchored := strings.HasSuffix(pattern, "$")
	if endAnchored {
		pattern = pattern[:len(pattern)-1]
	}

	if !strings.Contains(pattern, "*") {
		if endAnchored {
			return path == pattern
		}
		return strings.HasPrefix(path, pattern)
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 0 {
		return true
	}
	// First segment must be a prefix.
	if parts[0] != "" && !strings.HasPrefix(path, parts[0]) {
		return false
	}
	idx := len(parts[0])

	// Process middle segments [1 .. len-2] with the existing "find next match"
	// strategy. The last segment is handled separately when end-anchored to
	// enforce "match must end at len(path)".
	last := len(parts) - 1
	for i := 1; i < last; i++ {
		seg := parts[i]
		if seg == "" {
			continue
		}
		j := strings.Index(path[idx:], seg)
		if j < 0 {
			return false
		}
		idx += j + len(seg)
	}
	lastSeg := parts[last]
	if endAnchored {
		// Last segment must terminate at len(path). Empty last segment means
		// the pattern ends with `*$`, which matches anything from idx onwards.
		if lastSeg == "" {
			return true
		}
		if !strings.HasSuffix(path, lastSeg) {
			return false
		}
		// Ensure the suffix doesn't overlap with already-consumed prefix.
		return len(path)-len(lastSeg) >= idx
	}
	// Not end-anchored: original behavior — last segment can appear anywhere
	// from idx onwards.
	if lastSeg == "" {
		return true
	}
	return strings.Contains(path[idx:], lastSeg)
}
