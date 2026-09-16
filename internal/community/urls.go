package community

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"

	"github.com/pilot-protocol/cosift/internal/adultfilter"
)

const MaxURLs = 100

// NormalizeURL accepts web URLs, strips fragments and rejects credentials,
// local addresses and unusual ports. Crawling must also enforce public-only
// egress at connection time; intake validation alone cannot stop DNS rebinding.
func NormalizeURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u == nil || len(raw) > 2048 {
		return "", fmt.Errorf("invalid URL")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if (u.Scheme != "http" && u.Scheme != "https") || host == "" || u.User != nil || u.Opaque != "" {
		return "", fmt.Errorf("use a full http:// or https:// webpage URL without credentials")
	}
	if port := u.Port(); port != "" && port != "80" && port != "443" {
		return "", fmt.Errorf("only web ports 80 and 443 are accepted")
	}
	if net.ParseIP(host) != nil || !strings.Contains(host, ".") || strings.HasSuffix(host, ".") {
		return "", fmt.Errorf("use a public website hostname")
	}
	for _, suffix := range []string{".localhost", ".local", ".internal", ".home", ".lan", ".test", ".invalid"} {
		if strings.HasSuffix(host, suffix) {
			return "", fmt.Errorf("use a public website hostname")
		}
	}
	for _, c := range host {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return "", fmt.Errorf("invalid website hostname")
		}
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	if adultfilter.IsAdult("", "", u.String()) {
		return "", fmt.Errorf("explicit adult websites are not accepted")
	}
	for _, suffix := range []string{".exe", ".msi", ".scr", ".bat", ".cmd", ".ps1", ".apk", ".dmg", ".iso"} {
		if strings.HasSuffix(strings.ToLower(u.Path), suffix) {
			return "", fmt.Errorf("submit a webpage, not an executable or software installer")
		}
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

// ParseCSV accepts a headerless URL column, or a URL column named url/urls/
// webpage/website alongside other columns. It never silently drops bad rows.
func ParseCSV(r io.Reader) ([]string, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	var values []string
	column := 0
	rowNum := 0
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		rowNum++
		if err != nil {
			return nil, fmt.Errorf("CSV row %d: malformed CSV", rowNum)
		}
		if rowNum == 1 {
			found := false
			for i, cell := range row {
				switch strings.ToLower(strings.TrimSpace(strings.TrimPrefix(cell, "\ufeff"))) {
				case "url", "urls", "webpage", "website":
					column = i
					found = true
				}
			}
			if found {
				continue
			}
			if len(row) != 1 {
				return nil, fmt.Errorf("CSV needs a URL header when it has multiple columns")
			}
		}
		if column >= len(row) || strings.TrimSpace(row[column]) == "" {
			return nil, fmt.Errorf("CSV row %d: missing URL", rowNum)
		}
		values = append(values, strings.TrimPrefix(row[column], "\ufeff"))
		if len(values) > MaxURLs {
			return nil, fmt.Errorf("submit at most %d URLs at a time", MaxURLs)
		}
	}
	return values, nil
}

func normalizeURLs(values []string) ([]string, error) {
	if len(values) == 0 || len(values) > MaxURLs {
		return nil, fmt.Errorf("submit between 1 and %d URLs", MaxURLs)
	}
	seen := map[string]bool{}
	out := []string{}
	for i, raw := range values {
		u, err := NormalizeURL(raw)
		if err != nil {
			return nil, fmt.Errorf("URL %d: %w", i+1, err)
		}
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out, nil
}
