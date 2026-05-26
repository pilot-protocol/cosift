package config

import "testing"

func TestClusterSingleNodeDefault(t *testing.T) {
	var c Cluster
	if c.IsClustered() {
		t.Errorf("zero-value cluster should be single-node")
	}
	if !c.OwnsURL("https://example.com/anything") {
		t.Errorf("single-node should own every URL")
	}
	if c.PeerForURL("https://x") != "" {
		t.Errorf("single-node should have no peer")
	}
	if c.ShardOf("https://x") != 0 {
		t.Errorf("single-node ShardOf should be 0")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("zero-value Validate: %v", err)
	}
}

func TestClusterRouting(t *testing.T) {
	c := Cluster{
		NumShards:  4,
		MyShardID:  1,
		Peers:      []string{"box0:7777", "box1:7777", "box2:7777", "box3:7777"},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !c.IsClustered() {
		t.Errorf("4-shard cluster should be IsClustered=true")
	}
	urls := []string{
		"https://go.dev/ref/spec",
		"https://en.wikipedia.org/wiki/BM25",
		"https://doc.rust-lang.org/book/",
		"https://kubernetes.io/docs/",
		"https://docs.docker.com/",
		"https://www.python.org/",
		"https://github.com/golang/go",
	}
	// Distribution should hit at least 2 distinct shards across 7 URLs.
	seen := map[int]int{}
	for _, u := range urls {
		seen[c.ShardOf(u)]++
	}
	if len(seen) < 2 {
		t.Errorf("ShardOf distribution too narrow: %v", seen)
	}
	// OwnsURL only true when shard == MyShardID (=1).
	for _, u := range urls {
		own := c.OwnsURL(u)
		expected := c.ShardOf(u) == c.MyShardID
		if own != expected {
			t.Errorf("OwnsURL(%s)=%v, want %v", u, own, expected)
		}
	}
	// PeerForURL returns empty when URL is owned locally, peer address otherwise.
	for _, u := range urls {
		p := c.PeerForURL(u)
		shard := c.ShardOf(u)
		if shard == c.MyShardID {
			if p != "" {
				t.Errorf("PeerForURL(%s) on local shard: want empty, got %q", u, p)
			}
		} else {
			want := c.Peers[shard]
			if p != want {
				t.Errorf("PeerForURL(%s): want %q, got %q", u, want, p)
			}
		}
	}
}

func TestClusterValidateBadConfigs(t *testing.T) {
	cases := []struct {
		name string
		c    Cluster
	}{
		{"NumShards=1 with peers", Cluster{NumShards: 1, Peers: []string{"a"}}},
		{"shard ID out of range", Cluster{NumShards: 4, MyShardID: 5, Peers: []string{"a", "b", "c", "d"}}},
		{"peers count mismatch", Cluster{NumShards: 4, MyShardID: 0, Peers: []string{"a", "b"}}},
	}
	for _, tc := range cases {
		if err := tc.c.Validate(); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
}
