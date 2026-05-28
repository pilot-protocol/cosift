package config

import (
	"errors"
	"fmt"
	"hash/fnv"
)

// IsClustered reports whether the cosift process is participating in a
// multi-shard cluster. Single-node mode (NumShards=0 or 1) returns false,
// and all the forwarding/fan-out paths short-circuit.
func (c Cluster) IsClustered() bool {
	return c.NumShards > 1
}

// ShardOf returns the shard ID that owns a given URL. FNV-32 hashed mod
// NumShards. Single-node mode always returns 0.
func (c Cluster) ShardOf(url string) int {
	if !c.IsClustered() {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(url))
	return int(h.Sum32() % uint32(c.NumShards))
}

// OwnsURL reports whether THIS process should crawl + index a given URL.
// In single-node mode this is always true. In clustered mode, the URL's
// hash must land on MyShardID.
func (c Cluster) OwnsURL(url string) bool {
	if !c.IsClustered() {
		return true
	}
	return c.ShardOf(url) == c.MyShardID
}

// PeerForURL returns the host:port of the shard that owns a given URL.
// Returns the empty string when:
//   - the cluster is single-node (caller should index locally), OR
//   - the URL's shard is MyShardID (same as above), OR
//   - the peers slice doesn't have an entry for that shard ID (config bug).
func (c Cluster) PeerForURL(url string) string {
	if !c.IsClustered() {
		return ""
	}
	shard := c.ShardOf(url)
	if shard == c.MyShardID {
		return ""
	}
	if shard >= len(c.Peers) {
		return ""
	}
	return c.Peers[shard]
}

// Validate checks the cluster config makes sense. Returns nil for the
// single-node default.
func (c Cluster) Validate() error {
	if c.NumShards <= 1 && c.MyShardID == 0 && len(c.Peers) == 0 {
		return nil // single-node default
	}
	if c.NumShards <= 1 {
		return errors.New("cluster.num_shards must be > 1 when other cluster fields are set")
	}
	if c.MyShardID < 0 || c.MyShardID >= c.NumShards {
		return fmt.Errorf("cluster.my_shard_id (%d) out of range [0, %d)", c.MyShardID, c.NumShards)
	}
	if len(c.Peers) != c.NumShards {
		return fmt.Errorf("cluster.peers (len=%d) must have one entry per shard (num_shards=%d)", len(c.Peers), c.NumShards)
	}
	return nil
}
