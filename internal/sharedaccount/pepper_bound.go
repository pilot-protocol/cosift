package sharedaccount

import (
	"context"
	"strconv"
	"time"
)

const (
	resolveBackoff = 30 * time.Second
	resolveTimeout = 5 * time.Second
	missingTTL     = 60 * time.Second
	maxMissing     = 128
)

type versionBound struct {
	current   int
	resolveAt time.Time
	resolving chan struct{}
	absent    map[string]time.Time
}

func (v *verifier) clock() time.Time {
	if v.now != nil {
		return v.now()
	}
	return time.Now()
}

// admit refuses a token above the current pepper version without pepper I/O; the current version is re-read at most once in 30 s, only for such a token.
// While none has ever been read, any token may trigger the retry and only a version above the highest cached key is refused.
func (v *verifier) admit(ctx context.Context, id string) error {
	if v.latest == nil {
		return nil
	}
	version, _ := strconv.Atoi(id)
	for {
		v.mu.Lock()
		if version <= v.bound.current {
			v.mu.Unlock()
			return nil
		}
		if wait := v.bound.resolving; wait != nil {
			v.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return ErrUnavailable
			}
		}
		now := v.clock()
		if !v.bound.resolveAt.IsZero() && now.Sub(v.bound.resolveAt) < resolveBackoff {
			return v.refuseAbove(version)
		}
		done := make(chan struct{})
		v.bound.resolving, v.bound.resolveAt = done, now
		v.mu.Unlock()
		go v.resolve(ctx, done)
		select {
		case <-done:
		case <-ctx.Done():
			return ErrUnavailable
		}
		v.mu.Lock()
		if version <= v.bound.current {
			v.mu.Unlock()
			return nil
		}
		return v.refuseAbove(version)
	}
}

// resolve reads the current version on a context no request can cancel, so one caller leaving does not fail the read for the rest.
func (v *verifier) resolve(ctx context.Context, done chan struct{}) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resolveTimeout)
	defer cancel()
	current, err := v.latest(ctx)
	v.mu.Lock()
	if err == nil && current > v.bound.current {
		v.bound.current = current
	}
	v.bound.resolving = nil
	v.mu.Unlock()
	close(done)
}

// refuseAbove releases the lock; with no version ever read the highest cached key is the ceiling, and with none cached nothing is refused.
func (v *verifier) refuseAbove(version int) error {
	ceiling := v.bound.current
	if ceiling == 0 {
		for id := range v.keys {
			n, _ := strconv.Atoi(id)
			ceiling = max(ceiling, n)
		}
	}
	v.mu.Unlock()
	if ceiling > 0 && version > ceiling {
		return ErrUnauthorized
	}
	return nil
}

func (v *verifier) missing(id string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	until, ok := v.bound.absent[id]
	return ok && v.clock().Before(until)
}

// noteMissing remembers an unreadable version for a minute, evicting rather than refusing when full.
func (v *verifier) noteMissing(id string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.clock()
	if v.bound.absent == nil {
		v.bound.absent = map[string]time.Time{}
	}
	if _, ok := v.bound.absent[id]; !ok && len(v.bound.absent) >= maxMissing {
		var oldest string
		var oldestUntil time.Time
		for k, until := range v.bound.absent {
			if !now.Before(until) {
				delete(v.bound.absent, k)
			} else if oldest == "" || until.Before(oldestUntil) {
				oldest, oldestUntil = k, until
			}
		}
		if len(v.bound.absent) >= maxMissing {
			delete(v.bound.absent, oldest)
		}
	}
	v.bound.absent[id] = now.Add(missingTTL)
}
