package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pilot-protocol/cosift/internal/crawler"
)

const (
	maxClaim     = 32
	idleWait     = 30 * time.Second
	idleWaitCap  = 5 * time.Minute
	idleLimit    = 10
	defaultGrace = 10 * time.Second
	workerBody   = 5 << 20
)

type workerOptions struct {
	MaxPages    int
	Duration    time.Duration
	Concurrency int
}

type workerSummary struct {
	Claimed, Submitted, Reported, Skipped, Abandoned, LeaseLost int
	Credits                                                     int
	Verdicts                                                    map[string]int
	StopReason                                                  string
}

type worker struct {
	api       *workAPI
	fetch     *http.Client
	header    http.Header
	opts      workerOptions
	out       io.Writer
	errOut    io.Writer
	grace     time.Duration
	idleLimit int
	sleep     func(context.Context, time.Duration) error
	now       func() time.Time

	mu        sync.Mutex
	sum       workerSummary
	inflight  int
	stopMsg   string
	stopFail  bool
	slotFreed chan struct{}
	fatal     chan struct{}
	fatalOnce sync.Once
	stopClaim context.CancelFunc
	started   time.Time
}

func newWorker(api *workAPI, fetch *http.Client, opts workerOptions, out, errOut io.Writer) *worker {
	opts.Concurrency = max(opts.Concurrency, 1)
	return &worker{
		api: api, fetch: fetch, header: crawler.BrowserHeaders(), opts: opts, out: out, errOut: errOut,
		grace: defaultGrace, idleLimit: idleLimit, sleep: sleepContext, now: time.Now,
		slotFreed: make(chan struct{}, 1), fatal: make(chan struct{}),
		sum: workerSummary{Verdicts: map[string]int{}},
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (w *worker) summary() workerSummary {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.sum
	s.Verdicts = map[string]int{}
	for k, v := range w.sum.Verdicts {
		s.Verdicts[k] = v
	}
	return s
}

func (w *worker) emit(event string, fields map[string]any) {
	line := map[string]any{"event": event}
	for k, v := range fields {
		line[k] = v
	}
	raw, _ := json.Marshal(line)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.out.Write(append(raw, '\n'))
}

type outcome int

const (
	done outcome = iota
	finalDecision
	exhausted
	lost
	interrupted
)

// call runs fn until it succeeds or a refusal is final; transient failures back off and lease losses are retried a bounded number of times.
func (w *worker) call(ctx context.Context, fn func() error) (decision, outcome) {
	backoffs, leaseTries := 0, 0
	for tries := 1; ; tries++ {
		err := fn()
		if err == nil {
			return decision{}, done
		}
		if ctx.Err() != nil {
			return decision{}, interrupted
		}
		d := classifyWorkErr(err)
		var wait time.Duration
		switch d.act {
		case actBackoff:
			if tries >= maxTries {
				var e *workAPIError
				if errors.As(err, &e) {
					d.msg = fmt.Sprintf("the server stayed unavailable (%s) after %d tries", e.Error(), maxTries)
				} else {
					d.msg = fmt.Sprintf("the server could not be reached after %d tries", maxTries)
				}
				d.reason, d.fail = "unavailable", true
				return d, exhausted
			}
			if wait = d.wait; wait == 0 {
				wait = backoff(backoffs)
				backoffs++
			}
			w.emit("waiting", map[string]any{"seconds": wait.Seconds()})
		case actRetryLease:
			if leaseTries >= leaseRetries {
				return d, lost
			}
			wait = backoff(leaseTries)
			leaseTries++
		default:
			return d, finalDecision
		}
		if w.sleep(ctx, wait) != nil {
			return decision{}, interrupted
		}
	}
}

func (w *worker) stop(d decision) {
	if d.reason == "daily_cap" {
		d.msg = "the daily fetch credit cap is reached"
		if d.wait > 0 {
			d.msg += "; it resets at " + w.now().Add(d.wait).UTC().Format(time.RFC3339)
		}
	}
	w.mu.Lock()
	apply := w.sum.StopReason == "" || (d.fail && !w.stopFail)
	if apply {
		w.sum.StopReason, w.stopMsg, w.stopFail = d.reason, d.msg, d.fail
	}
	w.mu.Unlock()
	if !apply {
		return
	}
	if d.msg != "" && !d.fail {
		fmt.Fprintln(w.errOut, d.msg)
	}
	fields := map[string]any{"reason": d.reason}
	if d.msg != "" {
		fields["message"] = d.msg
	}
	w.emit("stopped", fields)
	w.stopClaim()
	if d.fail {
		w.fatalOnce.Do(func() { close(w.fatal) })
	}
}

func (w *worker) run(ctx context.Context) error {
	if w.opts.MaxPages <= 0 && w.opts.Duration <= 0 {
		return errors.New("set -max-pages or -duration so the worker stops")
	}
	w.started = w.now()
	limitCtx, cancelLimit := ctx, context.CancelFunc(func() {})
	if w.opts.Duration > 0 {
		limitCtx, cancelLimit = context.WithTimeout(ctx, w.opts.Duration)
	}
	defer cancelLimit()
	claimCtx, stopClaim := context.WithCancel(limitCtx)
	defer stopClaim()
	w.stopClaim = stopClaim
	opCtx, opCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer opCancel()
	finished := make(chan struct{})
	go func() {
		select {
		case <-limitCtx.Done():
		case <-w.fatal:
		case <-finished:
			return
		}
		t := time.NewTimer(w.grace)
		defer t.Stop()
		select {
		case <-t.C:
			opCancel()
		case <-finished:
		}
	}()

	var wg sync.WaitGroup
	w.claimLoop(claimCtx, opCtx, &wg)
	wg.Wait()
	close(finished)

	w.mu.Lock()
	if w.sum.StopReason == "" {
		if ctx.Err() != nil {
			w.sum.StopReason = "interrupted"
		} else {
			w.sum.StopReason = "duration"
		}
	}
	reason, msg, fail := w.sum.StopReason, w.stopMsg, w.stopFail
	w.mu.Unlock()
	s := w.summary()
	line := map[string]any{"claimed": s.Claimed, "submitted": s.Submitted, "reported": s.Reported, "skipped": s.Skipped, "abandoned": s.Abandoned, "lease_lost": s.LeaseLost, "verdicts": s.Verdicts, "elapsed_s": int(w.now().Sub(w.started).Seconds()), "stop_reason": reason}
	if s.Credits > 0 {
		line["credits_earned"] = s.Credits
	}
	w.emit("summary", line)
	if fail {
		return errors.New(msg)
	}
	return nil
}

// room blocks until a lease slot is free and returns how many leases to ask for, or 0 when the page budget is spent or claiming has stopped.
func (w *worker) room(ctx context.Context) int {
	for {
		w.mu.Lock()
		free := w.opts.Concurrency - w.inflight
		budget := maxClaim
		if w.opts.MaxPages > 0 {
			budget = w.opts.MaxPages - w.sum.Claimed
		}
		w.mu.Unlock()
		if budget <= 0 {
			w.stop(decision{reason: "max_pages"})
			return 0
		}
		if free > 0 {
			return min(maxClaim, free, budget)
		}
		select {
		case <-w.slotFreed:
		case <-ctx.Done():
			return 0
		}
	}
}

func (w *worker) claimLoop(claimCtx, opCtx context.Context, wg *sync.WaitGroup) {
	idle := 0
	for claimCtx.Err() == nil {
		want := w.room(claimCtx)
		if want == 0 {
			return
		}
		var reply claimReply
		var refusal error
		d, res := w.call(claimCtx, func() (err error) {
			reply, err = w.api.claim(claimCtx, want)
			refusal = err
			return err
		})
		switch res {
		case interrupted:
			return
		case finalDecision, exhausted:
			if d.act == actStop || res == exhausted {
				w.stop(d)
				return
			}
			w.stop(decision{reason: "claim_refused", fail: true, msg: fmt.Sprintf("the server refused the claim (%s)", refusal)})
			return
		}
		leases := reply.Leases
		if len(leases) > want {
			leases = leases[:want]
		}
		if len(leases) == 0 {
			if reply.Ceiling {
				w.stop(decision{reason: "moderation_ceiling", msg: "today's moderation ceiling is reached; try again after 00:00 UTC"})
				return
			}
			idle++
			if idle >= w.idleLimit {
				w.stop(decision{reason: "no_work", msg: "no work is available right now; try again later"})
				return
			}
			wait := idleWait
			if reply.RetryAfterS > 0 {
				wait = min(time.Duration(reply.RetryAfterS)*time.Second, idleWaitCap)
			}
			w.emit("waiting", map[string]any{"seconds": wait.Seconds(), "reason": "no_work"})
			if w.sleep(claimCtx, wait) != nil {
				return
			}
			continue
		}
		idle = 0
		w.mu.Lock()
		w.inflight += len(leases)
		w.sum.Claimed += len(leases)
		w.mu.Unlock()
		w.emit("claimed", map[string]any{"count": len(leases)})
		for _, l := range leases {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer w.release()
				w.process(opCtx, l)
			}()
		}
	}
}

func (w *worker) release() {
	w.mu.Lock()
	w.inflight--
	w.mu.Unlock()
	select {
	case w.slotFreed <- struct{}{}:
	default:
	}
}

func (w *worker) count(field *int) {
	w.mu.Lock()
	*field++
	w.mu.Unlock()
}

func (w *worker) skip(reason string) {
	w.count(&w.sum.Skipped)
	w.emit("skipped", map[string]any{"reason": reason})
}

func (w *worker) abandon() {
	w.count(&w.sum.Abandoned)
	w.emit("abandoned", nil)
}

func (w *worker) process(ctx context.Context, l workLease) {
	if l.ExtractVersion != crawler.ExtractVersion {
		w.count(&w.sum.Abandoned)
		w.stop(upgradeStop(l.ExtractVersion))
		return
	}
	u, err := url.Parse(l.URL)
	if err != nil || crawler.CheckPublicTarget(u) != nil {
		w.skip("lease_url")
		return
	}
	page, err := fetchAndExtract(ctx, w.fetch, w.header, l.URL, workerBody)
	var status *fetchStatusError
	switch {
	case ctx.Err() != nil:
		w.abandon()
		return
	case errors.As(err, &status):
		w.send(ctx, map[string]any{"lease_id": l.LeaseID, "http_status": status.Status}, status.Status)
		return
	case err != nil:
		w.skip("fetch_error")
		return
	}
	body, why := submissionFor(l.LeaseID, page)
	if why != "" {
		w.skip(why)
		return
	}
	w.send(ctx, body, http.StatusOK)
}

// send submits a body; httpStatus is the status the page answered, 200 for an attestation.
func (w *worker) send(ctx context.Context, body map[string]any, httpStatus int) {
	var reply submitReply
	d, res := w.call(ctx, func() (err error) {
		reply, err = w.api.submit(ctx, body)
		return err
	})
	switch res {
	case interrupted:
		w.abandon()
	case lost:
		w.count(&w.sum.LeaseLost)
		w.emit("lease_lost", nil)
	case exhausted:
		w.abandon()
	case finalDecision:
		if d.act == actStop {
			w.count(&w.sum.Abandoned)
			w.stop(d)
			return
		}
		w.skip(d.reason)
	case done:
		if httpStatus != http.StatusOK {
			w.count(&w.sum.Reported)
			w.emit("reported", map[string]any{"http_status": httpStatus})
			return
		}
		w.mu.Lock()
		w.sum.Submitted++
		w.sum.Verdicts[reply.Verdict]++
		w.mu.Unlock()
		fields := map[string]any{"verdict": reply.Verdict, "agreeing_count": reply.AgreeingCount, "indexed": reply.Indexed}
		if reply.Reason != "" {
			fields["reason"] = reply.Reason
		}
		if reply.CreditsEarned != nil {
			fields["credits_earned"] = *reply.CreditsEarned
			w.mu.Lock()
			w.sum.Credits += *reply.CreditsEarned
			w.mu.Unlock()
		}
		w.emit("submitted", fields)
	}
}
