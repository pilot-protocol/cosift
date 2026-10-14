package community

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

const contributionVectorsSHA256 = "730491f4a3d7c065361d7b212a8b0336f85672136b09fd75d9b4935481c9f824"

func TestContributionVectorsAreTheSharedCopy(t *testing.T) {
	raw, err := os.ReadFile("testdata/contribution-vectors-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != contributionVectorsSHA256 {
		t.Fatal("testdata/contribution-vectors-v1.json differs from the shared vectors; take the new copy from the contract set and update the pinned hash")
	}
}
