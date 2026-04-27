// Package rebalancer contains the target-state generator and the rebalancing engine.
package rebalancer

import (
	"fmt"
	"math"
	"sort"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/config"
)

// ShardMove describes a single planned shard relocation.
type ShardMove struct {
	Shard     agent.ShardInfo
	FromNode  string
	FromName  string
	ToNode    string
	ToName    string
	Rationale string
}

// TargetState is the output of the generator: a minimal set of moves that
// achieves the configured skew-reduction target.
type TargetState struct {
	Moves         []ShardMove
	CurrentSkew   float64
	ProjectedSkew float64
	SkewReduction float64 // fraction, e.g. 0.28 = 28%
	TargetMet     bool
}

// ──────────────────────────────────────────────────────────────
//  projectedNode — mutable working copy used during planning
// ──────────────────────────────────────────────────────────────

type projectedNode struct {
	nodeID          string
	nodeName        string
	jvmHeapPercent  float64
	diskUsedPercent float64
	diskTotalBytes  int64
	// totalShardBytes tracks shard data bytes only (no OS/translog overhead).
	// diskUsedPercent is derived from this plus a fixed overhead factor so that
	// projections stay consistent with how the agent computes disk usage.
	totalShardBytes int64
	shardCount      int
	// shardKeys tracks which shards (and shard groups) live here to prevent
	// primary+replica co-location on the same node.
	shardKeys map[string]bool
}

// diskOverheadFactor accounts for OS files, translog, and segment overhead.
// The simulation uses 10%; we apply the same factor here for consistency.
const diskOverheadFactor = 1.10

func (p *projectedNode) hotScore(cfg config.AgentConfig, maxShards int) float64 {
	jvm := p.jvmHeapPercent / 100.0
	disk := p.diskUsedPercent / 100.0
	shardFrac := float64(p.shardCount) / float64(maxShards)
	return cfg.JVMWeight*jvm + cfg.DiskWeight*disk + cfg.ShardWeight*shardFrac
}

// ──────────────────────────────────────────────────────────────
//  TargetStateGenerator
// ──────────────────────────────────────────────────────────────

// TargetStateGenerator calculates the minimum set of shard moves required
// to reduce cluster skew by the configured fraction (default 25%).
//
// Algorithm:
//  1. Build a mutable "projected" copy of the cluster state.
//  2. Capture maxShards once (before any moves) to keep skew scores on a
//     consistent scale throughout the planning loop.
//  3. Identify hot nodes (HotScore >= threshold).
//  4. Sort hot-node shards by size descending ("large shards first").
//     Moving large shards maximises disk-pressure relief per API call.
//  5. For each candidate shard, find the coolest eligible target node:
//     - Must not already host any copy of the same shard (primary or replica).
//     - Must not exceed disk capacity (90% threshold) after the move.
//  6. Apply the move to the projected state and recalculate skew.
//  7. Stop when projected skew reduction >= target, or MaxMovesPerCycle reached.
//
// Projection accuracy note:
// JVM heap is approximated as decreasing by 3% per shard lost and increasing
// by 3% per shard gained. This is intentionally conservative — real heap
// pressure depends on Lucene segment count and field data, not raw byte size.
// The projection is directionally correct but should not be treated as exact.
type TargetStateGenerator struct {
	agentCfg config.AgentConfig
	rebCfg   config.RebalancerConfig
}

// NewTargetStateGenerator constructs a TargetStateGenerator.
func NewTargetStateGenerator(agentCfg config.AgentConfig, rebCfg config.RebalancerConfig) *TargetStateGenerator {
	return &TargetStateGenerator{agentCfg: agentCfg, rebCfg: rebCfg}
}

// Generate produces the target state from the current cluster snapshot.
func (g *TargetStateGenerator) Generate(snap *agent.ClusterSnapshot) (*TargetState, error) {
	if len(snap.DataNodes()) == 0 {
		return nil, fmt.Errorf("no data nodes found in snapshot")
	}

	// Build projected nodes.
	projected := g.buildProjected(snap)

	// Capture maxShards once before any moves so that all hot-score calculations
	// within this planning pass use the same denominator. Without this, early
	// moves can change maxShards, shifting the score scale mid-loop and making
	// the SkewReduction percentage inconsistent.
	maxShards := maxProjectedShards(projected)

	initialSkew := computeProjectedSkewFixed(projected, g.agentCfg, maxShards)

	// Collect candidate moves: shards on hot nodes, sorted by size desc.
	candidates := g.candidateShards(snap, projected, maxShards)

	var moves []ShardMove
	currentSkew := initialSkew

	for _, shard := range candidates {
		if len(moves) >= g.rebCfg.MaxMovesPerCycle {
			break
		}
		// Check if we have already hit the skew reduction target.
		if initialSkew > 0 {
			reduction := 1.0 - currentSkew/initialSkew
			if reduction >= g.rebCfg.SkewReductionTarget {
				break
			}
		}

		fromNode, ok := projected[shard.NodeID]
		if !ok {
			continue
		}
		// Find the best (coolest) target node.
		target := g.findTarget(shard, projected, maxShards)
		if target == nil {
			continue
		}

		// Apply move to projected state.
		g.applyMove(shard, fromNode, target)

		// Recalculate skew using the fixed maxShards baseline.
		currentSkew = computeProjectedSkewFixed(projected, g.agentCfg, maxShards)

		role := "REPLICA"
		if shard.Primary {
			role = "PRIMARY"
		}
		rationale := fmt.Sprintf("%.2f→%.2f hot-score; %s shard (%.1f GiB)",
			fromNode.hotScore(g.agentCfg, maxShards),
			target.hotScore(g.agentCfg, maxShards),
			role, float64(shard.SizeBytes)/float64(1<<30))

		moves = append(moves, ShardMove{
			Shard:     shard,
			FromNode:  fromNode.nodeID,
			FromName:  fromNode.nodeName,
			ToNode:    target.nodeID,
			ToName:    target.nodeName,
			Rationale: rationale,
		})
	}

	skewReduction := 0.0
	if initialSkew > 0 {
		skewReduction = 1.0 - currentSkew/initialSkew
	}

	return &TargetState{
		Moves:         moves,
		CurrentSkew:   initialSkew,
		ProjectedSkew: currentSkew,
		SkewReduction: skewReduction,
		TargetMet:     skewReduction >= g.rebCfg.SkewReductionTarget,
	}, nil
}

// ──────────────────────────────────────────────────────────────
//  Internal helpers
// ──────────────────────────────────────────────────────────────

// buildProjected creates a mutable working copy from the snapshot.
func (g *TargetStateGenerator) buildProjected(snap *agent.ClusterSnapshot) map[string]*projectedNode {
	out := make(map[string]*projectedNode, len(snap.NodeMetrics))
	for id, nm := range snap.NodeMetrics {
		if !nm.IsDataNode {
			continue
		}
		p := &projectedNode{
			nodeID:          id,
			nodeName:        nm.NodeName,
			jvmHeapPercent:  nm.JVMHeapPercent,
			diskUsedPercent: nm.DiskUsedPercent,
			diskTotalBytes:  nm.DiskTotalBytes,
			totalShardBytes: nm.TotalShardBytes,
			shardCount:      nm.ShardCount,
			shardKeys:       make(map[string]bool),
		}
		// Track which shards already live on this node.
		for _, s := range snap.ShardsOnNode(id) {
			p.shardKeys[s.ShardKey()] = true
			p.shardKeys[groupKey(s)] = true
		}
		out[id] = p
	}
	return out
}

// candidateShards returns shards on hot nodes, sorted by size descending.
// Large shards are moved first: they provide the greatest pressure relief
// per shard movement and reduce skew most efficiently.
func (g *TargetStateGenerator) candidateShards(snap *agent.ClusterSnapshot, projected map[string]*projectedNode, maxShards int) []agent.ShardInfo {
	hotNodeIDs := make(map[string]bool)
	for id, p := range projected {
		if p.hotScore(g.agentCfg, maxShards) >= g.agentCfg.HotNodeThreshold {
			hotNodeIDs[id] = true
		}
	}

	var candidates []agent.ShardInfo
	for _, s := range snap.Shards {
		if s.State != "STARTED" {
			continue
		}
		if hotNodeIDs[s.NodeID] {
			candidates = append(candidates, s)
		}
	}

	// Sort: large shards first; break ties by index+shard for determinism.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].SizeBytes != candidates[j].SizeBytes {
			return candidates[i].SizeBytes > candidates[j].SizeBytes
		}
		if candidates[i].Index != candidates[j].Index {
			return candidates[i].Index < candidates[j].Index
		}
		return candidates[i].ShardNum < candidates[j].ShardNum
	})
	return candidates
}

// findTarget selects the coolest eligible target node for a shard.
func (g *TargetStateGenerator) findTarget(shard agent.ShardInfo, projected map[string]*projectedNode, maxShards int) *projectedNode {
	// Sort candidates by hot-score ascending (coolest first).
	nodes := make([]*projectedNode, 0, len(projected))
	for _, p := range projected {
		nodes = append(nodes, p)
	}
	sort.Slice(nodes, func(i, j int) bool {
		si := nodes[i].hotScore(g.agentCfg, maxShards)
		sj := nodes[j].hotScore(g.agentCfg, maxShards)
		if si != sj {
			return si < sj
		}
		return nodes[i].nodeID < nodes[j].nodeID // deterministic tie-break
	})

	for _, target := range nodes {
		// Cannot move to itself.
		if target.nodeID == shard.NodeID {
			continue
		}
		// Any copy of this shard (primary or replica) must not already be on this node.
		if target.shardKeys[groupKey(shard)] {
			continue
		}
		// Disk capacity check: destination must have room after the move.
		// We account for the same overhead factor the agent uses so the projection
		// matches observed disk usage.
		if target.diskTotalBytes > 0 {
			projectedShardBytes := target.totalShardBytes + shard.SizeBytes
			projectedDiskBytes := int64(float64(projectedShardBytes) * diskOverheadFactor)
			projectedPct := float64(projectedDiskBytes) / float64(target.diskTotalBytes) * 100.0
			if projectedPct >= 90.0 {
				continue
			}
		}
		return target
	}
	return nil
}

// applyMove updates projected state to reflect a planned move.
func (g *TargetStateGenerator) applyMove(shard agent.ShardInfo, from, to *projectedNode) {
	// ── Source node ──────────────────────────────────────────────────────────
	from.shardCount--
	from.totalShardBytes -= shard.SizeBytes
	if from.diskTotalBytes > 0 {
		usedBytes := int64(float64(from.totalShardBytes) * diskOverheadFactor)
		from.diskUsedPercent = float64(usedBytes) / float64(from.diskTotalBytes) * 100.0
	}
	// JVM heap decreases as the node sheds data. The 3% multiplier is a
	// conservative approximation; actual relief depends on GC and segment merges.
	from.jvmHeapPercent = math.Max(5.0, from.jvmHeapPercent*0.97)
	delete(from.shardKeys, shard.ShardKey())
	delete(from.shardKeys, groupKey(shard))

	// ── Destination node ─────────────────────────────────────────────────────
	to.shardCount++
	to.totalShardBytes += shard.SizeBytes
	if to.diskTotalBytes > 0 {
		usedBytes := int64(float64(to.totalShardBytes) * diskOverheadFactor)
		to.diskUsedPercent = float64(usedBytes) / float64(to.diskTotalBytes) * 100.0
	}
	to.jvmHeapPercent = math.Min(95.0, to.jvmHeapPercent*1.03)
	to.shardKeys[shard.ShardKey()] = true
	to.shardKeys[groupKey(shard)] = true
}

// computeProjectedSkewFixed returns the population std-dev of projected
// hot-scores using a fixed maxShards value. Using a fixed baseline ensures
// all skew measurements within a single Generate() call are on the same scale,
// making the reported SkewReduction percentage meaningful.
func computeProjectedSkewFixed(projected map[string]*projectedNode, cfg config.AgentConfig, maxShards int) float64 {
	scores := make([]float64, 0, len(projected))
	for _, p := range projected {
		scores = append(scores, p.hotScore(cfg, maxShards))
	}
	if len(scores) < 2 {
		return 0
	}
	sum := 0.0
	for _, s := range scores {
		sum += s
	}
	mean := sum / float64(len(scores))
	variance := 0.0
	for _, s := range scores {
		d := s - mean
		variance += d * d
	}
	return math.Sqrt(variance / float64(len(scores)))
}

// maxProjectedShards returns the maximum shard count across all projected nodes.
func maxProjectedShards(projected map[string]*projectedNode) int {
	max := 1 // avoid div-by-zero
	for _, p := range projected {
		if p.shardCount > max {
			max = p.shardCount
		}
	}
	return max
}

// groupKey returns a key that identifies a shard regardless of primary/replica role.
// Used to prevent a primary and its replica ending up on the same node.
func groupKey(s agent.ShardInfo) string {
	return fmt.Sprintf("%s/%d/any", s.Index, s.ShardNum)
}
