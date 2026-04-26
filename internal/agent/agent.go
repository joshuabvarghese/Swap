package agent

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/project-sway/sway/internal/config"
	"github.com/project-sway/sway/internal/opensearch"
)

// MonitoringAgent scrapes the OpenSearch cluster and produces ClusterSnapshots.
// It is the single source of truth for the rebalancer's view of cluster state.
type MonitoringAgent struct {
	client opensearch.Client
	cfg    config.AgentConfig
	logger *log.Logger
}

// New constructs a MonitoringAgent.
func New(client opensearch.Client, cfg config.AgentConfig, logger *log.Logger) *MonitoringAgent {
	return &MonitoringAgent{client: client, cfg: cfg, logger: logger}
}

// CollectSnapshot performs a complete cluster scrape and returns a ClusterSnapshot.
// It calls _nodes/stats, _cluster/state, _cluster/health, and _cat/shards in parallel
// (conceptually; sequentially here for clarity and safety on rate-limited clusters).
func (a *MonitoringAgent) CollectSnapshot(ctx context.Context) (*ClusterSnapshot, error) {
	// ── 1. Cluster health ────────────────────────────────────────────────────
	health, err := a.client.GetClusterHealth(ctx)
	if err != nil {
		return nil, fmt.Errorf("cluster health: %w", err)
	}

	// ── 2. Node stats (JVM, disk, search latency) ────────────────────────────
	nodeStats, err := a.client.GetNodesStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("nodes stats: %w", err)
	}

	// ── 3. Cluster state (shard → node mapping) ──────────────────────────────
	clusterState, err := a.client.GetClusterState(ctx)
	if err != nil {
		return nil, fmt.Errorf("cluster state: %w", err)
	}

	// ── 4. Shard sizes ────────────────────────────────────────────────────────
	shardSizes, err := a.client.GetShardSizes(ctx)
	if err != nil {
		// Non-fatal: proceed without size data; rebalancer will still work.
		a.logger.Printf("[AGENT] WARNING: could not fetch shard sizes: %v", err)
		shardSizes = make(map[string]int64)
	}

	// ── 5. Build shard list ───────────────────────────────────────────────────
	shards, _ := buildShardList(clusterState, shardSizes)

	// ── 6. Build per-node metrics ─────────────────────────────────────────────
	// Build a map from node name → node ID using cluster state.
	nameToID := make(map[string]string)
	for id, cn := range clusterState.Nodes.Nodes {
		nameToID[cn.Name] = id
	}

	// Count shards per node and total shard bytes per node.
	shardCountByNode := make(map[string]int)
	shardBytesByNode := make(map[string]int64)
	for _, s := range shards {
		if s.State == "STARTED" {
			shardCountByNode[s.NodeID]++
			shardBytesByNode[s.NodeID] += s.SizeBytes
		}
	}

	maxShardCount := 1 // avoid div-by-zero
	for _, cnt := range shardCountByNode {
		if cnt > maxShardCount {
			maxShardCount = cnt
		}
	}

	nodeMetrics := make(map[string]*NodeMetrics, len(nodeStats.Nodes))
	for nodeID, ns := range nodeStats.Nodes {
		diskTotal := ns.FS.Total.TotalInBytes
		diskFree := ns.FS.Total.FreeInBytes
		diskUsed := diskTotal - diskFree
		diskUsedPct := 0.0
		if diskTotal > 0 {
			diskUsedPct = float64(diskUsed) / float64(diskTotal) * 100.0
		}

		avgLatMs := computeAvgLatency(ns.Indices.Search)

		nm := &NodeMetrics{
			NodeID:          nodeID,
			NodeName:        ns.Name,
			Host:            ns.Host,
			Roles:           ns.Roles,
			IsDataNode:      isDataNode(ns.Roles),
			JVMHeapPercent:  float64(ns.JVM.Mem.HeapUsedPercent),
			DiskUsedPercent: diskUsedPct,
			DiskTotalBytes:  diskTotal,
			DiskUsedBytes:   diskUsed,
			ShardCount:      shardCountByNode[nodeID],
			TotalShardBytes: shardBytesByNode[nodeID],
			AvgSearchLatMs:  avgLatMs,
		}
		nm.HotScore = a.hotScore(nm, maxShardCount)
		nm.IsHot = nm.HotScore >= a.cfg.HotNodeThreshold
		nodeMetrics[nodeID] = nm
	}

	// ── 7. Cluster-wide averages ──────────────────────────────────────────────
	clusterLatMs := clusterAvgLatency(nodeMetrics)
	skew := computeSkew(nodeMetrics)

	return &ClusterSnapshot{
		Timestamp:        time.Now().UTC(),
		ClusterName:      clusterState.ClusterName,
		Health:           strings.ToLower(health.Status),
		NodeMetrics:      nodeMetrics,
		Shards:           shards,
		TotalShards:      health.ActiveShards,
		RelocatingShards: health.RelocatingShards,
		SkewScore:        skew,
		AvgSearchLatMs:   clusterLatMs,
	}, nil
}

// ──────────────────────────────────────────────────────────────
//  Internal helpers
// ──────────────────────────────────────────────────────────────

// hotScore computes the weighted hot-score for a node.
// Formula: JVMWeight*(heap/100) + DiskWeight*(disk/100) + ShardWeight*(count/max)
func (a *MonitoringAgent) hotScore(nm *NodeMetrics, maxShards int) float64 {
	if !nm.IsDataNode {
		return 0
	}
	jvm := nm.JVMHeapPercent / 100.0
	disk := nm.DiskUsedPercent / 100.0
	shardFrac := float64(nm.ShardCount) / float64(maxShards)
	return a.cfg.JVMWeight*jvm + a.cfg.DiskWeight*disk + a.cfg.ShardWeight*shardFrac
}

// computeSkew returns the population standard deviation of HotScore values
// across data nodes. Higher = more imbalanced cluster.
func computeSkew(nodes map[string]*NodeMetrics) float64 {
	scores := make([]float64, 0, len(nodes))
	for _, n := range nodes {
		if n.IsDataNode {
			scores = append(scores, n.HotScore)
		}
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
	variance /= float64(len(scores))
	return math.Sqrt(variance)
}

// computeAvgLatency derives average search latency (ms) from cumulative counters.
// NOTE: This is a proxy for p99. For true p99, integrate an APM backend
// (e.g. OpenTelemetry, Prometheus histogram) via a custom agent.Collector.
func computeAvgLatency(s opensearch.SearchStats) float64 {
	if s.QueryTotal == 0 {
		return 0
	}
	return float64(s.QueryTimeInMillis) / float64(s.QueryTotal)
}

// clusterAvgLatency returns the mean average-latency across all data nodes.
func clusterAvgLatency(nodes map[string]*NodeMetrics) float64 {
	sum, count := 0.0, 0
	for _, n := range nodes {
		if n.IsDataNode && n.AvgSearchLatMs > 0 {
			sum += n.AvgSearchLatMs
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// buildShardList converts the routing table into a flat []ShardInfo.
func buildShardList(state *opensearch.ClusterStateResponse, sizes map[string]int64) ([]ShardInfo, int) {
	var shards []ShardInfo
	relocating := 0
	for indexName, irt := range state.RoutingTable.Indices {
		for shardStr, routings := range irt.Shards {
			shardNum, _ := strconv.Atoi(shardStr)
			for _, r := range routings {
				role := "r"
				if r.Primary {
					role = "p"
				}
				key := fmt.Sprintf("%s/%s/%s", indexName, shardStr, role)
				shards = append(shards, ShardInfo{
					Index:     indexName,
					ShardNum:  shardNum,
					Primary:   r.Primary,
					NodeID:    r.Node,
					State:     r.State,
					SizeBytes: sizes[key],
				})
				if r.State == "RELOCATING" {
					relocating++
				}
			}
		}
	}
	// Sort for deterministic output.
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Index != shards[j].Index {
			return shards[i].Index < shards[j].Index
		}
		if shards[i].ShardNum != shards[j].ShardNum {
			return shards[i].ShardNum < shards[j].ShardNum
		}
		return shards[i].Primary // primaries first
	})
	return shards, relocating
}

// isDataNode returns true when the node carries the "data" role.
func isDataNode(roles []string) bool {
	for _, r := range roles {
		if r == "data" || r == "data_content" || r == "data_hot" || r == "data_warm" || r == "data_cold" {
			return true
		}
	}
	return false
}
