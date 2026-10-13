package store

import (
	"context"
	"encoding/binary"
	"errors"
	"net/url"
	"strings"

	"github.com/cockroachdb/pebble"
)

// crawledSource marks documents written from the crawler's own page parse, the
// only ones whose ContentSHA a freshly fetched page can be compared against.
const crawledSource = "crawl"

// variantScanCap bounds how many stored documents one probe examines.
const variantScanCap = 256

// HasUncomparableVariant reports whether a live document other than excludeDocID
// whose ContentSHA cannot be compared with a fetched page (anything not written
// from the crawler's page parse, such as a bulk text import) is stored under a
// URL that differs from rawURL only by scheme, a leading "www.", a trailing
// slash, a query, a default port, a trailing host dot or path dot segments. A
// probe too large to examine counts as a hit.
func (p *PebbleStore) HasUncomparableVariant(ctx context.Context, rawURL string, excludeDocID int64) (bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, err
	}
	host, port := variantHost(u)
	path := variantPath(u.EscapedPath())
	examined := 0
	uncomparable := func(id int64) (bool, error) {
		if id == excludeDocID {
			return false, nil
		}
		if examined++; examined > variantScanCap {
			return true, nil
		}
		doc, err := p.GetDocByID(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return doc.Source != crawledSource, nil
	}
	for _, scheme := range []string{"http", "https"} {
		ports := []string{port}
		if port == "" {
			ports = []string{"", ":" + defaultPorts[scheme]}
		}
		for _, h := range []string{host, "www." + host} {
			for _, pt := range ports {
				base := scheme + "://" + h + pt + path
				for _, exact := range []string{base, base + "/"} {
					id, ok, err := p.lookupIDByURL(exact)
					if err != nil {
						return false, err
					}
					if !ok {
						continue
					}
					if hit, err := uncomparable(id); hit || err != nil {
						return hit, err
					}
				}
				for _, prefix := range []string{base + "?", base + "/?"} {
					if hit, err := p.scanURLPrefix(prefix, uncomparable); hit || err != nil {
						return hit, err
					}
				}
			}
		}
	}
	return false, nil
}

var defaultPorts = map[string]string{"http": "80", "https": "443"}

// variantHost returns the lowercased hostname without "www." or a trailing dot,
// and the port as ":n" unless it is the default for u's scheme.
func variantHost(u *url.URL) (host, port string) {
	host = strings.TrimPrefix(strings.TrimRight(strings.ToLower(u.Hostname()), "."), "www.")
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if n := u.Port(); n != "" && n != defaultPorts[strings.ToLower(u.Scheme)] {
		port = ":" + n
	}
	return host, port
}

// variantPath resolves "." and ".." segments (percent-encoded spellings
// included) and drops a trailing slash.
func variantPath(path string) string {
	isDot := func(seg string) bool { return seg == "." || strings.EqualFold(seg, "%2e") }
	isDotDot := func(seg string) bool {
		return seg == ".." || strings.EqualFold(seg, ".%2e") || strings.EqualFold(seg, "%2e.") || strings.EqualFold(seg, "%2e%2e")
	}
	segs := strings.Split(path, "/")
	out := make([]string, 0, len(segs))
	for _, seg := range segs {
		switch {
		case isDot(seg):
		case isDotDot(seg):
			if len(out) > 1 {
				out = out[:len(out)-1]
			}
		default:
			out = append(out, seg)
		}
	}
	return strings.TrimSuffix(strings.Join(out, "/"), "/")
}

// scanURLPrefix feeds the doc ID of every stored URL starting with prefix to
// fn, stopping at the first hit.
func (p *PebbleStore) scanURLPrefix(prefix string, fn func(id int64) (bool, error)) (bool, error) {
	lo := urlKey(prefix)
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: append(append([]byte{}, lo...), 0xFF)})
	if err != nil {
		return false, err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		val := it.Value()
		if len(val) != 8 {
			continue
		}
		if hit, err := fn(int64(binary.BigEndian.Uint64(val))); hit || err != nil {
			return hit, err
		}
	}
	return false, it.Error()
}
