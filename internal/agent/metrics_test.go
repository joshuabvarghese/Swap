package agent

import (
	"testing"
)

func TestShardKeyPrimary(t *testing.T) {
	s := ShardInfo{Index: "logs-2024", ShardNum: 3, Primary: true}
	want := "logs-2024/3/p"
	if got := s.ShardKey(); got != want {
		t.Errorf("ShardKey() = %q, want %q", got, want)
	}
}

func TestShardKeyReplica(t *testing.T) {
	s := ShardInfo{Index: "metrics", ShardNum: 0, Primary: false}
	want := "metrics/0/r"
	if got := s.ShardKey(); got != want {
		t.Errorf("ShardKey() = %q, want %q", got, want)
	}
}

func TestShardKeyZeroShard(t *testing.T) {
	s := ShardInfo{Index: "idx", ShardNum: 0, Primary: true}
	want := "idx/0/p"
	if got := s.ShardKey(); got != want {
		t.Errorf("ShardKey() = %q, want %q", got, want)
	}
}

func TestShardKeyLargeShard(t *testing.T) {
	s := ShardInfo{Index: "idx", ShardNum: 99, Primary: false}
	want := "idx/99/r"
	if got := s.ShardKey(); got != want {
		t.Errorf("ShardKey() = %q, want %q", got, want)
	}
}

func TestDataNodes(t *testing.T) {
	snap := &ClusterSnapshot{
		NodeMetrics: map[string]*NodeMetrics{
			"n0": {NodeID: "n0", IsDataNode: true},
			"n1": {NodeID: "n1", IsDataNode: false},
			"n2": {NodeID: "n2", IsDataNode: true},
		},
	}
	data := snap.DataNodes()
	if len(data) != 2 {
		t.Errorf("DataNodes() returned %d nodes, want 2", len(data))
	}
	for _, n := range data {
		if !n.IsDataNode {
			t.Errorf("DataNodes() returned non-data node %s", n.NodeID)
		}
	}
}

func TestShardsOnNode(t *testing.T) {
	snap := &ClusterSnapshot{
		Shards: []ShardInfo{
			{Index: "idx", ShardNum: 0, NodeID: "n0"},
			{Index: "idx", ShardNum: 1, NodeID: "n1"},
			{Index: "idx", ShardNum: 2, NodeID: "n0"},
		},
	}
	shards := snap.ShardsOnNode("n0")
	if len(shards) != 2 {
		t.Errorf("ShardsOnNode(n0) returned %d shards, want 2", len(shards))
	}
	for _, s := range shards {
		if s.NodeID != "n0" {
			t.Errorf("ShardsOnNode returned shard on wrong node: %s", s.NodeID)
		}
	}
}

func TestShardsOnNodeEmpty(t *testing.T) {
	snap := &ClusterSnapshot{Shards: []ShardInfo{}}
	if got := snap.ShardsOnNode("n0"); len(got) != 0 {
		t.Errorf("expected empty slice, got %d shards", len(got))
	}
}
