package store

import (
	"context"

	"github.com/cockroachdb/pebble"
)

// FrontierRow is a frontier entry as a feeder reads it, without claiming it.
type FrontierRow struct {
	URL       string
	Status    FrontierStatus
	LastError string
}

// ScanFrontier reads up to scan rows after the URL in URL order and returns those with the status,
// the last URL scanned and whether the scan reached the end of the frontier.
func (p *PebbleStore) ScanFrontier(ctx context.Context, status FrontierStatus, after string, scan int) (rows []FrontierRow, next string, end bool, err error) {
	lower := []byte{famFrontier, 'u'}
	if after != "" {
		lower = append(frontierKey(after), 0)
	}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: []byte{famFrontier, 'v'}})
	if err != nil {
		return nil, after, false, err
	}
	defer it.Close()
	next, scanned := after, 0
	for ok := it.First(); ok; ok = it.Next() {
		if scanned == scan {
			return rows, next, false, nil
		}
		if err := ctx.Err(); err != nil {
			return rows, next, false, err
		}
		scanned++
		next = string(it.Key()[2:])
		if len(it.Value()) == 0 || FrontierStatus(it.Value()[0]) != status {
			continue
		}
		e, uerr := unpackFrontierEntry(it.Value())
		if uerr != nil {
			continue
		}
		rows = append(rows, FrontierRow{URL: next, Status: status, LastError: e.LastError})
	}
	return rows, next, it.Error() == nil, it.Error()
}
