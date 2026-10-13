package sharedaccount

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

type pepperFake struct {
	mu          sync.Mutex
	current     int
	latestErr   error
	latestCalls int
	pepperCalls map[string]int
	exists      map[int]bool
	destroyed   map[int]bool
	transient   bool
	hold        chan struct{}
	key         []byte
}

func newPepperFake(current int) *pepperFake {
	f := &pepperFake{current: current, pepperCalls: map[string]int{}, exists: map[int]bool{}, destroyed: map[int]bool{}, key: make([]byte, 32)}
	for i := 1; i <= current; i++ {
		f.exists[i] = true
	}
	return f
}

func (f *pepperFake) latest(ctx context.Context) (int, error) {
	f.mu.Lock()
	hold := f.hold
	f.latestCalls++
	err, current := f.latestErr, f.current
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return current, err
}

func (f *pepperFake) pepper(_ context.Context, id string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pepperCalls[id]++
	n, _ := strconv.Atoi(id)
	switch {
	case f.transient:
		return nil, ErrUnavailable
	case !f.exists[n] || f.destroyed[n]:
		return nil, ErrUnauthorized
	}
	return f.key, nil
}

func (f *pepperFake) calls() (latest, pepper int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.pepperCalls {
		pepper += n
	}
	return f.latestCalls, pepper
}

type tidStore struct {
	mu    sync.Mutex
	valid map[string]bool
	key   []byte
}

func (s *tidStore) Lookup(_ context.Context, tid string) (tokenRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.valid[tid] {
		return tokenRecord{UID: "0123456789abcdef", LastUsed: time.Now()}, nil
	}
	return tokenRecord{}, ErrUnauthorized
}
func (s *tidStore) Account(context.Context, string) (Identity, error) {
	return Identity{UID: "0123456789abcdef", Email: "user@example.com"}, nil
}
func (s *tidStore) Touch(context.Context, string, string, time.Time) error { return nil }

func mintToken(version int) string {
	raw := make([]byte, 24)
	rand.Read(raw)
	return fmt.Sprintf("ck_%d_%s", version, base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw))
}

type boundRig struct {
	v     *verifier
	f     *pepperFake
	store *tidStore
	now   time.Time
}

func newBoundRig(current int) *boundRig {
	r := &boundRig{f: newPepperFake(current), now: time.Unix(1_000_000, 0)}
	r.store = &tidStore{valid: map[string]bool{}, key: r.f.key}
	r.v = &verifier{store: r.store, pepper: r.f.pepper, latest: r.f.latest, now: func() time.Time { return r.now }}
	return r
}

func (r *boundRig) valid(version int) string {
	token := mintToken(version)
	r.store.mu.Lock()
	r.store.valid[TID(r.f.key, token)] = true
	r.store.mu.Unlock()
	return token
}

func (r *boundRig) verify(token string) error {
	_, err := r.v.verify(context.Background(), token)
	return err
}

func TestFirstBearerResolvesTheCurrentVersionOnce(t *testing.T) {
	r := newBoundRig(3)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatal(err)
	}
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatal(err)
	}
	if latest, _ := r.f.calls(); latest != 1 {
		t.Fatalf("%d latest accesses", latest)
	}
}

func TestFloodOfFreshVersionsCostsOneResolutionInThirtySeconds(t *testing.T) {
	r := newBoundRig(3)
	for i := 0; i < 1000; i++ {
		if err := r.verify(mintToken(777)); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("token %d: %v", i, err)
		}
	}
	if latest, pepper := r.f.calls(); latest != 1 || pepper != 0 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
	r.now = r.now.Add(29 * time.Second)
	r.verify(mintToken(777))
	if latest, _ := r.f.calls(); latest != 1 {
		t.Fatalf("re-resolved inside the window: %d", latest)
	}
	r.now = r.now.Add(2 * time.Second)
	for i := 0; i < 100; i++ {
		r.verify(mintToken(777))
	}
	if latest, pepper := r.f.calls(); latest != 2 || pepper != 0 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
}

func TestDestroyedVersionIsNegativeCachedByVersionForAMinute(t *testing.T) {
	r := newBoundRig(3)
	r.f.destroyed[2] = true
	for i := 0; i < 1000; i++ {
		if err := r.verify(mintToken(2)); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("token %d: %v", i, err)
		}
	}
	if r.f.pepperCalls["2"] != 1 {
		t.Fatalf("%d pepper reads for a destroyed version", r.f.pepperCalls["2"])
	}
	r.now = r.now.Add(61 * time.Second)
	r.verify(mintToken(2))
	if r.f.pepperCalls["2"] != 2 {
		t.Fatalf("negative entry outlived its minute: %d", r.f.pepperCalls["2"])
	}
}

func TestTransientPepperErrorIsNotCached(t *testing.T) {
	r := newBoundRig(3)
	r.f.transient = true
	for i := 0; i < 5; i++ {
		if err := r.verify(r.valid(3)); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if r.f.pepperCalls["3"] != 5 {
		t.Fatalf("%d reads", r.f.pepperCalls["3"])
	}
	r.f.transient = false
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("did not recover: %v", err)
	}
}

func TestValidCurrentTokenSurvivesForgeriesThatCameFirst(t *testing.T) {
	r := newBoundRig(3)
	r.verify(mintToken(1))
	for v := 4; v < 1004; v++ {
		r.verify(mintToken(v))
	}
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("a valid version 3 token was refused after forgeries: %v", err)
	}
}

func TestNewVersionIsPickedUpAfterTheWindowAndRefusedWithinIt(t *testing.T) {
	r := newBoundRig(3)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatal(err)
	}
	r.f.exists[4], r.f.current = true, 4
	r.now = r.now.Add(10 * time.Second)
	if err := r.verify(r.valid(4)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("inside the window: %v", err)
	}
	if latest, _ := r.f.calls(); latest != 1 {
		t.Fatalf("%d latest accesses inside the window", latest)
	}
	r.now = r.now.Add(25 * time.Second)
	if err := r.verify(r.valid(4)); err != nil {
		t.Fatalf("after the window: %v", err)
	}
	if latest, _ := r.f.calls(); latest != 2 {
		t.Fatalf("%d latest accesses", latest)
	}
	r.now = r.now.Add(31 * time.Second)
	if err := r.verify(mintToken(5)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("version 5 does not exist: %v", err)
	}
	latest, _ := r.f.calls()
	r.verify(mintToken(5))
	if again, _ := r.f.calls(); again != latest {
		t.Fatal("a refused version cost I/O inside the window")
	}
}

func TestMissingVersionCacheEvictsInsteadOfRefusing(t *testing.T) {
	r := newBoundRig(0)
	r.f.current = 1000
	for i := 1; i <= 999; i++ {
		r.f.destroyed[i] = true
	}
	for v := 1; v <= 999; v++ {
		r.verify(mintToken(v))
	}
	if n := len(r.v.bound.absent); n > maxMissing {
		t.Fatalf("%d negative entries", n)
	}
	before := r.f.pepperCalls["1"]
	r.verify(mintToken(1))
	if r.f.pepperCalls["1"] != before+1 {
		t.Fatal("an evicted version was refused instead of looked up")
	}
	r.f.exists[1000] = true
	if err := r.verify(r.valid(1000)); err != nil {
		t.Fatalf("a valid token was refused with a full cache: %v", err)
	}
}

func TestLatestFailingAtStartupStillAdmitsAValidTokenAndRecovers(t *testing.T) {
	r := newBoundRig(3)
	r.f.latestErr = ErrUnavailable
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("a valid token as the first bearer while latest fails: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if err := r.verify(mintToken(777)); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("flood %d: %v", i, err)
		}
	}
	if latest, pepper := r.f.calls(); latest != 1 || pepper != 1 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
	r.now = r.now.Add(31 * time.Second)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("while latest still fails: %v", err)
	}
	if latest, _ := r.f.calls(); latest != 2 {
		t.Fatalf("%d latest accesses after the window", latest)
	}
	r.f.latestErr = nil
	r.now = r.now.Add(10 * time.Second)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatal(err)
	}
	if latest, _ := r.f.calls(); latest != 2 {
		t.Fatalf("retried inside 30 s: %d", latest)
	}
	r.now = r.now.Add(21 * time.Second)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("did not recover once latest did: %v", err)
	}
	if err := r.verify(mintToken(777)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("judged against the resolved version: %v", err)
	}
	latest, pepper := r.f.calls()
	if latest != 3 || pepper != 1 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
}

func TestForgedLowVersionFirstMakesAValidTokenWaitForTheRetry(t *testing.T) {
	r := newBoundRig(3)
	r.f.latestErr = ErrUnavailable
	if err := r.verify(mintToken(1)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged version 1: %v", err)
	}
	if err := r.verify(r.valid(3)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("version 3 above the only cached key: %v", err)
	}
	r.f.latestErr = nil
	r.now = r.now.Add(31 * time.Second)
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("after the retry: %v", err)
	}
}

func TestUnresolvedLatestWithNothingCachedIsBoundedByTheNegativeCache(t *testing.T) {
	r := newBoundRig(3)
	r.f.latestErr = ErrUnavailable
	for i := 0; i < 1000; i++ {
		if err := r.verify(mintToken(777)); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("token %d: %v", i, err)
		}
	}
	if latest, pepper := r.f.calls(); latest != 1 || pepper != 1 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
}

func TestLatestFailingForGoodCostsAtMostOneAttemptPerWindow(t *testing.T) {
	r := newBoundRig(3)
	r.f.latestErr = ErrUnavailable
	for at := 0; at <= 600; at += 5 {
		for _, token := range []string{r.valid(3), mintToken(777), mintToken(1)} {
			err := r.verify(token)
			if valid := token[:5] == "ck_3_"; valid && err != nil || !valid && !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("t=%d %s: %v", at, token[:5], err)
			}
		}
		r.now = r.now.Add(5 * time.Second)
	}
	if latest, _ := r.f.calls(); latest > 21 {
		t.Fatalf("%d latest attempts in 10 minutes", latest)
	}
}

func TestFailedReResolutionRefusesTheTokenAboveTheCurrentVersion(t *testing.T) {
	r := newBoundRig(3)
	r.verify(r.valid(3))
	r.f.latestErr = ErrUnavailable
	r.now = r.now.Add(31 * time.Second)
	for i := 0; i < 2; i++ {
		if err := r.verify(mintToken(4)); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("outage answered %v", err)
		}
	}
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("the current version during the outage: %v", err)
	}
	if latest, pepper := r.f.calls(); latest != 2 || pepper != 1 {
		t.Fatalf("latest %d pepper %d", latest, pepper)
	}
}

func TestConcurrentFirstBearersShareOneResolution(t *testing.T) {
	r := newBoundRig(3)
	r.f.hold = make(chan struct{})
	token := r.valid(3)
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- r.verify(token) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(r.f.hold)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if latest, _ := r.f.calls(); latest != 1 {
		t.Fatalf("%d latest accesses for concurrent callers", latest)
	}
}

func TestACancelledFirstBearerDoesNotFailTheResolutionForTheNext(t *testing.T) {
	r := newBoundRig(3)
	r.f.hold = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := r.v.verify(ctx, mintToken(777)); first <- err }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if latest, _ := r.f.calls(); latest == 1 {
			break
		} else if time.Now().After(deadline) {
			t.Fatal("the first bearer never started the resolution")
		}
	}
	cancel()
	<-first
	close(r.f.hold)
	r.now = r.now.Add(5 * time.Second)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		r.v.mu.Lock()
		current := r.v.bound.current
		r.v.mu.Unlock()
		if current == 3 {
			break
		} else if time.Now().After(deadline) {
			t.Fatal("the resolution did not finish after its caller left")
		}
	}
	if err := r.verify(r.valid(3)); err != nil {
		t.Fatalf("a valid token after a cancelled first bearer: %v", err)
	}
	if latest, _ := r.f.calls(); latest != 1 {
		t.Fatalf("%d latest accesses", latest)
	}
}
