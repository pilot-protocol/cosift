package community

import (
	"context"
	"log"
	"sync"
	"time"
)

var (
	settledEvery = 10 * time.Second
	counterEvery = 60 * time.Second
	pushEvery    = 5 * time.Minute
)

// runWork drives the settled, counter, alert and push loops until ctx ends, then writes what is still buffered.
func (s *Server) runWork(ctx context.Context) {
	var wg sync.WaitGroup
	loop := func(every time.Duration, step func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(every)
			defer tick.Stop()
			for {
				step(ctx)
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		}()
	}
	loop(settledEvery, func(ctx context.Context) {
		if err := s.pollSettled(ctx); err != nil && ctx.Err() == nil {
			log.Printf("community: work_settled_failed")
		}
		s.drainAlerts(ctx)
	})
	loop(counterEvery, s.flushFetchCounters)
	loop(pushEvery, func(ctx context.Context) {
		if err := s.pushBlocked(ctx); err != nil && ctx.Err() == nil {
			log.Printf("community: work_push_failed")
		}
	})
	wg.Wait()
	final, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.flushFetchCounters(final)
	s.drainAlertsAt(final, true)
}
