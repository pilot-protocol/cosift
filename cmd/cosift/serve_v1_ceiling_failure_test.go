package main

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestModerationReservationRefusesWithoutACounter(t *testing.T) {
	s := &pebbleHTTP{}
	err := s.reserveCommunityModeration()
	var ceiling *ceilingError
	if err == nil || errors.As(err, &ceiling) {
		t.Fatalf("a missing store must fail as an error, not as the daily ceiling: %v", err)
	}
}

func TestModerationReservationFailsOnACorruptCounterWithoutSpendingTheAllowance(t *testing.T) {
	s, _ := ceilingServer(t, 5, nil)
	if err := s.store.DB().Set(communityModerationsKey, []byte("not json"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	err := s.reserveCommunityModeration()
	var ceiling *ceilingError
	if err == nil || errors.As(err, &ceiling) {
		t.Fatalf("corrupt counter: %v", err)
	}
	got, closer, gerr := s.store.DB().Get(communityModerationsKey)
	if gerr != nil {
		t.Fatal(gerr)
	}
	defer closer.Close()
	if string(got) != "not json" {
		t.Fatalf("the counter was overwritten: %q", got)
	}
}
