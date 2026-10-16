package community

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

const contributionVectorsSHA256 = "91a056ce70345aa10558983dad60f7103490426ff774021a33d9209569e857b9"

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

func TestAwardVectorsAreTheSharedGroups(t *testing.T) {
	load := func(name string) map[string]any {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	shared, award := load("testdata/contribution-vectors-v1.json"), load("testdata/award-vectors.json")
	if len(award) != 2 {
		t.Fatalf("testdata/award-vectors.json holds %d groups, want award_ids and ledger_clamp only", len(award))
	}
	for _, group := range []string{"award_ids", "ledger_clamp"} {
		want, ok := shared[group]
		if !ok || !reflect.DeepEqual(award[group], want) {
			t.Errorf("testdata/award-vectors.json: %s differs from the shared vectors; copy the group from testdata/contribution-vectors-v1.json", group)
		}
	}
}
