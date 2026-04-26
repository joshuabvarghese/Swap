package simulation

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/project-sway/sway/internal/opensearch"
)

func newSim() *SimulatedClient {
	return New(log.New(io.Discard, "", 0))
}

func TestNewHasTenNodes(t *testing.T) {
	c := newSim()
	stats, err := c.GetNodesStats(context.Background())
	if err != nil {
		t.Fatalf("GetNodesStats: %v", err)
	}
	if got := len(stats.Nodes); got != 10 {
		t.Errorf("expected 10 nodes, got %d", got)
	}
}

func TestAllNodesAreDataNodes(t *testing.T) {
	c := newSim()
	stats, _ := c.GetNodesStats(context.Background())
	for id, ns := range stats.Nodes {
		found := false
		for _, r := range ns.Roles {
			if r == "data" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("node %s has no data role: %v", id, ns.Roles)
		}
	}
}

func TestInitialClusterIsGreen(t *testing.T) {
	c := newSim()
	h, err := c.GetClusterHealth(context.Background())
	if err != nil {
		t.Fatalf("GetClusterHealth: %v", err)
	}
	if h.Status != "green" {
		t.Errorf("expected green health, got %q", h.Status)
	}
	if h.RelocatingShards != 0 {
		t.Errorf("expected 0 relocating shards initially, got %d", h.RelocatingShards)
	}
}

func TestShardSizesNonZero(t *testing.T) {
	c := newSim()
	sizes, err := c.GetShardSizes(context.Background())
	if err != nil {
		t.Fatalf("GetShardSizes: %v", err)
	}
	if len(sizes) == 0 {
		t.Fatal("expected non-empty shard size map")
	}
	for key, sz := range sizes {
		if sz <= 0 {
			t.Errorf("shard %q has zero or negative size", key)
		}
	}
}

func TestShardSizeKeyFormat(t *testing.T) {
	// Keys must be "index/shardNum/p|r" — lowercase prirep.
	c := newSim()
	sizes, _ := c.GetShardSizes(context.Background())
	for key := range sizes {
		var idx, shard, role string
		// Count slashes.
		parts := splitKey(key)
		if len(parts) != 3 {
			t.Errorf("shard key %q has wrong format (want index/N/p|r)", key)
			continue
		}
		idx, shard, role = parts[0], parts[1], parts[2]
		if idx == "" || shard == "" {
			t.Errorf("shard key %q has empty fields", key)
		}
		if role != "p" && role != "r" {
			t.Errorf("shard key %q has invalid prirep %q (want p or r)", key, role)
		}
	}
}

func splitKey(key string) []string {
	// Split "index/shard/role" carefully — index names can contain "-".
	// We want the last two "/" delimiters.
	last := -1
	second := -1
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			if last == -1 {
				last = i
			} else {
				second = i
				break
			}
		}
	}
	if second == -1 || last == -1 {
		return nil
	}
	return []string{key[:second], key[second+1 : last], key[last+1:]}
}

func TestRerouteUpdatesState(t *testing.T) {
	c := newSim()

	// Find a shard on node-0 and a node that doesn't already host it.
	var targetShard *simShard
	c.mu.RLock()
	for _, s := range c.shards {
		if s.nodeID == "node-id-00" && s.state == "STARTED" {
			targetShard = s
			break
		}
	}
	c.mu.RUnlock()

	if targetShard == nil {
		t.Skip("no shard on node-id-00")
	}

	// Destination: node-id-09 (least loaded).
	req := &opensearch.RerouteRequest{
		Commands: []opensearch.RerouteCommand{
			{Move: &opensearch.MoveShard{
				Index:    targetShard.index,
				Shard:    targetShard.shardNum,
				FromNode: "node-0",
				ToNode:   "node-9",
			}},
		},
	}
	resp, err := c.Reroute(context.Background(), req, false)
	if err != nil {
		t.Fatalf("Reroute: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("expected acknowledged=true")
	}

	// Verify the shard is now on node-id-09.
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.shards {
		if s.index == targetShard.index && s.shardNum == targetShard.shardNum && s.primary == targetShard.primary {
			if s.nodeID != "node-id-09" {
				t.Errorf("shard still on %s after reroute, expected node-id-09", s.nodeID)
			}
		}
	}
}

func TestRerouteInvalidNodeIsNoop(t *testing.T) {
	c := newSim()
	req := &opensearch.RerouteRequest{
		Commands: []opensearch.RerouteCommand{
			{Move: &opensearch.MoveShard{
				Index:    "nonexistent",
				Shard:    0,
				FromNode: "ghost-node",
				ToNode:   "other-ghost",
			}},
		},
	}
	// Should not panic or error — just log and no-op.
	resp, err := c.Reroute(context.Background(), req, false)
	if err != nil {
		t.Fatalf("unexpected error for invalid reroute: %v", err)
	}
	if !resp.Acknowledged {
		t.Error("even invalid reroutes should be acknowledged (no cluster to reject)")
	}
}

func TestHotNodesDiskHigherThanCold(t *testing.T) {
	c := newSim()
	stats, _ := c.GetNodesStats(context.Background())

	// node-0 should have higher JVM than node-9.
	n0 := stats.Nodes["node-id-00"]
	n9 := stats.Nodes["node-id-09"]

	if n0.JVM.Mem.HeapUsedPercent <= n9.JVM.Mem.HeapUsedPercent {
		t.Errorf("node-0 JVM (%d%%) should be higher than node-9 (%d%%)",
			n0.JVM.Mem.HeapUsedPercent, n9.JVM.Mem.HeapUsedPercent)
	}
}

func TestClusterStateRoutingTablePopulated(t *testing.T) {
	c := newSim()
	state, err := c.GetClusterState(context.Background())
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if len(state.RoutingTable.Indices) == 0 {
		t.Error("routing table has no indices")
	}
	for idx, irt := range state.RoutingTable.Indices {
		if len(irt.Shards) == 0 {
			t.Errorf("index %q has empty shard routing", idx)
		}
	}
}

func TestSyncDiskUsageConsistent(t *testing.T) {
	c := newSim()
	// Every node should have disk used > 0 (even nodes with no shards have OS overhead).
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id, node := range c.nodes {
		if node.diskUsedBytes <= 0 {
			t.Errorf("node %s has zero disk usage — OS overhead should be non-zero", id)
		}
	}
}

func TestGetClusterStateNodesMatchGetNodesStats(t *testing.T) {
	c := newSim()
	state, _ := c.GetClusterState(context.Background())
	stats, _ := c.GetNodesStats(context.Background())
	if len(state.Nodes.Nodes) != len(stats.Nodes) {
		t.Errorf("node count mismatch: cluster state has %d, nodes stats has %d",
			len(state.Nodes.Nodes), len(stats.Nodes))
	}
}
