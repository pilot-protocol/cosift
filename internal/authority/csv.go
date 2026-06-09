package authority

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// LoadTranco ingests the Tranco top-1M CSV (https://tranco-list.eu/).
// File shape: "rank,host" per line, no header. Rank 1 = most popular.
// Subsequent calls merge into the existing map (re-loading keeps the
// best rank seen).
func (s *Scorer) LoadTranco(r io.Reader) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("authority: nil Scorer")
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	n := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Tranco uses comma; some mirrors use tab.
		var rankStr, host string
		if i := strings.IndexAny(line, ",\t"); i > 0 {
			rankStr = strings.TrimSpace(line[:i])
			host = strings.ToLower(strings.TrimSpace(line[i+1:]))
		} else {
			continue
		}
		rank, err := strconv.Atoi(rankStr)
		if err != nil || rank <= 0 {
			continue
		}
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		if prev, ok := s.tranco[host]; !ok || rank < prev {
			s.tranco[host] = rank
		}
		n++
	}
	return n, scanner.Err()
}

// LoadMajestic ingests the Majestic Million CSV
// (https://majestic.com/reports/majestic-million). The official CSV
// has many columns and a header line; we read column index 2 (Domain)
// and column index 9 (TrustFlow, 0-100). Header line is detected by
// the first column not being a number.
func (s *Scorer) LoadMajestic(r io.Reader) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("authority: nil Scorer")
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)
	n := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 10 {
			continue
		}
		if _, err := strconv.Atoi(fields[0]); err != nil {
			// header line
			continue
		}
		host := strings.ToLower(strings.TrimSpace(fields[2]))
		host = strings.TrimPrefix(host, "www.")
		if host == "" {
			continue
		}
		tf, err := strconv.Atoi(strings.TrimSpace(fields[9]))
		if err != nil || tf < 0 {
			continue
		}
		if tf > 100 {
			tf = 100
		}
		if prev, ok := s.majestic[host]; !ok || tf > prev {
			s.majestic[host] = tf
		}
		n++
	}
	return n, scanner.Err()
}
