package rebalancer

import (
	"testing"
	"time"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/config"
)

// ──────────────────────────────────────────────────────────────
//  Test helpers
// ──────────────────────────────────────────────────────────────

func testAgentCfg() config.AgentConfig {
	return config.AgentConfig{
		HotNodeThreshold: 0.50,
		JVMWeight:        0.40,
		DiskWeight:       0.40,
		ShardWeight:      0.20,
	}
}

func testRebCfg() config.RebalancerConfig {
	return config.RebalancerConfig{
		MaxMovesPerCycle:         5,
		SkewReductionTarget:      0.25,
		LargeShardThresholdBytes: 1 * 1024 * 1024 * 1024,
	}
}

func makeGenerator() *TargetStateGenerator {
	return NewTargetStateGenerator(testAgentCfg(), testRebCfg())
}

// makeSkewedSnap creates a snapshot with two hot nodes (0,1) and two cool nodes (2,3).
// hot nodes have JVM=85%, disk=50%; cool nodes have JVM=30%, disk=10%.
func makeSkewedSnap() *agent.ClusterSnapshot {
	gb := int64(1024 * 1024 * 1024)
	cap500 := int64(500) * gb

	nodes := map[string]*agent.NodeMetrics{
		"n0": {NodeID: "n0", NodeName: "node-0", IsDataNode: true, JVMHeapPercent: 85, DiskUsedPercent: 50, DiskTotalBytes: cap500, TotalShardBytes: 230 * gb, ShardCount: 8},
		"n1": {NodeID: "n1", NodeName: "node-1", IsDataNode: true, JVMHeapPercent: 80, DiskUsedPercent: 45, DiskTotalBytes: cap500, TotalShardBytes: 205 * gb, ShardCount: 7},
		"n2": {NodeID: "n2", NodeName: "node-2", IsDataNode: true, JVMHeapPercent: 30, DiskUsedPercent: 10, DiskTotalBytes: cap500, TotalShardBytes: 45 * gb, ShardCount: 2},
		"n3": {NodeID: "n3", NodeName: "node-3", IsDataNode: true, JVMHeapPercent: 28, DiskUsedPercent: 8, DiskTotalBytes: cap500, TotalShardBytes: 36 * gb, ShardCount: 2},
	}
	// Compute hot scores.
	maxShards := 8
	acfg := testAgentCfg()
	for _, n := range nodes {
		jvm := n.JVMHeapPercent / 100.0
		disk := n.DiskUsedPercent / 100.0
		sf := float64(n.ShardCount) / float64(maxShards)
		n.HotScore = acfg.JVMWeight*jvm + acfg.DiskWeight*disk + acfg.ShardWeight*sf
		n.IsHot = n.HotScore >= acfg.HotNodeThreshold
	}

	// Shards on hot nodes (n0 and n1).
	shards := []agent.ShardInfo{
		{Index: "logs-2024", ShardNum: 0, Primary: true, NodeID: "n0", State: "STARTED", SizeBytes: 22 * gb},
		{Index: "logs-2024", ShardNum: 1, Primary: true, NodeID: "n0", State: "STARTED", SizeBytes: 16 * gb},
		{Index: "logs-2024", ShardNum: 0, Primary: false, NodeID: "n1", State: "STARTED", SizeBytes: 22 * gb},
		{Index: "metrics", ShardNum: 0, Primary: true, NodeID: "n1", State: "STARTED", SizeBytes: 10 * gb},
		// Small shards on cool nodes (to prevent co-location blocking all moves).
		{Index: "audit", ShardNum: 0, Primary: false, NodeID: "n2", State: "STARTED", SizeBytes: 1 * gb},
		{Index: "audit", ShardNum: 0, Primary: true, NodeID: "n3", State: "STARTED", SizeBytes: 1 * gb},
	}

	return &agent.ClusterSnapshot{
		Timestamp:   time.Now(),
		ClusterName: "test",
		Health:      "green",
		NodeMetrics: nodes,
		Shards:      shards,
	}
}

// ──────────────────────────────────────────────────────────────
//  Tests
// ──────────────────────────────────────────────────────────────

func TestGenerateNoDataNodes(t *testing.T) {
	g := makeGenerator()
	snap := &agent.ClusterSnapshot{
		Timestamp:   time.Now(),
		NodeMetrics: map[string]*agent.NodeMetrics{},
	}
	_, err := g.Generate(snap)
	if err == nil {
		t.Error("expected error for snapshot with no data nodes")
	}
}

func TestGenerateBalancedClusterNoMoves(t *testing.T) {
	g := makeGenerator()
	gb := int64(1024 * 1024 * 1024)
	cap500 := int64(500) * gb
	// All nodes are equal — no hot nodes.
	nodes := map[string]*agent.NodeMetrics{
		"n0": {NodeID: "n0", NodeName: "node-0", IsDataNode: true, JVMHeapPercent: 30, DiskUsedPercent: 15, DiskTotalBytes: cap500, ShardCount: 4, HotScore: 0.18, IsHot: false},
		"n1": {NodeID: "n1", NodeName: "node-1", IsDataNode: true, JVMHeapPercent: 30, DiskUsedPercent: 15, DiskTotalBytes: cap500, ShardCount: 4, HotScore: 0.18, IsHot: false},
	}
	snap := &agent.ClusterSnapshot{
		Timestamp:   time.Now(),
		NodeMetrics: nodes,
		Shards:      []agent.ShardInfo{},
	}
	target, err := g.Generate(snap)
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if len(target.Moves) != 0 {
		t.Errorf("expected 0 moves for balanced cluster, got %d", len(target.Moves))
	}
}

func TestGenerateProducesMovesForSkewedCluster(t *testing.T) {
	g := makeGenerator()
	snap := makeSkewedSnap()
	target, err := g.Generate(snap)
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if len(target.Moves) == 0 {
		t.Error("expected at least one move for skewed cluster")
	}
}

func TestGenerateRespectMaxMovesPerCycle(t *testing.T) {
	acfg := testAgentCfg()
	rebCfg := testRebCfg()
	rebCfg.MaxMovesPerCycle = 1
	g := NewTargetStateGenerator(acfg, rebCfg)

	target, err := g.Generate(makeSkewedSnap())
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if len(target.Moves) > 1 {
		t.Errorf("expected <= 1 move, got %d", len(target.Moves))
	}
}

func TestGenerateNoReplicaOnSameNodeAsPrimary(t *testing.T) {
	g := makeGenerator()
	snap := makeSkewedSnap()
	target, err := g.Generate(snap)
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}

	// Build a map of which nodes already have each shard (any role).
	shardNodesBefore := make(map[string][]string)
	for _, s := range snap.Shards {
		gk := groupKey(s)
		shardNodesBefore[gk] = append(shardNodesBefore[gk], s.NodeID)
	}

	for _, m := range target.Moves {
		gk := groupKey(m.Shard)
		for _, existingNode := range shardNodesBefore[gk] {
			if existingNode == m.ToNode {
				t.Errorf("move %s[%d] to %s would co-locate with existing copy",
					m.Shard.Index, m.Shard.ShardNum, m.ToName)
			}
		}
	}
}

func TestGenerateProjectedSkewLowerThanInitial(t *testing.T) {
	g := makeGenerator()
	target, err := g.Generate(makeSkewedSnap())
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if len(target.Moves) == 0 {
		t.Skip("no moves generated — cannot check skew reduction")
	}
	if target.ProjectedSkew >= target.CurrentSkew {
		t.Errorf("projected skew %.4f should be lower than initial %.4f",
			target.ProjectedSkew, target.CurrentSkew)
	}
}

func TestGenerateSkewReductionPositive(t *testing.T) {
	g := makeGenerator()
	target, err := g.Generate(makeSkewedSnap())
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	if len(target.Moves) == 0 {
		t.Skip("no moves generated")
	}
	if target.SkewReduction <= 0 {
		t.Errorf("expected positive SkewReduction, got %.4f", target.SkewReduction)
	}
}

func TestGroupKey(t *testing.T) {
	primary := agent.ShardInfo{Index: "logs", ShardNum: 2, Primary: true}
	replica := agent.ShardInfo{Index: "logs", ShardNum: 2, Primary: false}
	if groupKey(primary) != groupKey(replica) {
		t.Errorf("primary and replica of same shard must share groupKey: %q vs %q",
			groupKey(primary), groupKey(replica))
	}
	other := agent.ShardInfo{Index: "logs", ShardNum: 3, Primary: true}
	if groupKey(primary) == groupKey(other) {
		t.Errorf("different shard numbers must have different groupKeys")
	}
}

func TestComputeProjectedSkewFixedSingleNode(t *testing.T) {
	projected := map[string]*projectedNode{
		"n0": {nodeID: "n0", jvmHeapPercent: 80, diskUsedPercent: 50, shardCount: 5, shardKeys: map[string]bool{}},
	}
	skew := computeProjectedSkewFixed(projected, testAgentCfg(), 5)
	if skew != 0 {
		t.Errorf("single-node skew should be 0, got %.4f", skew)
	}
}

func TestComputeProjectedSkewFixedEqualNodes(t *testing.T) {
	projected := map[string]*projectedNode{
		"n0": {nodeID: "n0", jvmHeapPercent: 50, diskUsedPercent: 30, shardCount: 5, shardKeys: map[string]bool{}},
		"n1": {nodeID: "n1", jvmHeapPercent: 50, diskUsedPercent: 30, shardCount: 5, shardKeys: map[string]bool{}},
	}
	skew := computeProjectedSkewFixed(projected, testAgentCfg(), 5)
	if skew > 0.0001 {
		t.Errorf("equal nodes should have near-zero skew, got %.4f", skew)
	}
}

func TestGenerateDiskCapacityRespected(t *testing.T) {
	// Target nodes are nearly full — moves to them should be blocked.
	gb := int64(1024 * 1024 * 1024)
	cap100 := int64(100) * gb
	acfg := testAgentCfg()
	rebCfg := testRebCfg()

	nodes := map[string]*agent.NodeMetrics{
		"hot": {
			NodeID: "hot", NodeName: "hot-node", IsDataNode: true,
			JVMHeapPercent: 90, DiskUsedPercent: 30,
			DiskTotalBytes: cap100, TotalShardBytes: 30 * gb,
			ShardCount: 10, HotScore: 0.70, IsHot: true,
		},
		"full": {
			NodeID: "full", NodeName: "full-node", IsDataNode: true,
			JVMHeapPercent: 20, DiskUsedPercent: 88,
			DiskTotalBytes: cap100, TotalShardBytes: 80 * gb,
			ShardCount: 2, HotScore: 0.20, IsHot: false,
		},
	}
	// A large shard that would push "full" node over 90%.
	shards := []agent.ShardInfo{
		{Index: "big", ShardNum: 0, Primary: true, NodeID: "hot", State: "STARTED", SizeBytes: 15 * gb},
	}
	snap := &agent.ClusterSnapshot{
		Timestamp:   time.Now(),
		NodeMetrics: nodes,
		Shards:      shards,
	}

	g := NewTargetStateGenerator(acfg, rebCfg)
	target, err := g.Generate(snap)
	if err != nil {
		t.Fatalf("Generate() error: %v", err)
	}
	// The only candidate target is "full", which would exceed 90% — so no moves.
	for _, m := range target.Moves {
		if m.ToNode == "full" {
			t.Errorf("move scheduled to nearly-full node %s", m.ToName)
		}
	}
}
