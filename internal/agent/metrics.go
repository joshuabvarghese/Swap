// Package agent contains the monitoring agent and its metric types.
package agent

import (
	"fmt"
	"strconv"
	"time"
)

// NodeMetrics holds the computed, normalised metrics for one cluster node.
type NodeMetrics struct {
	NodeID     string
	NodeName   string
	Host       string
	Roles      []string
	IsDataNode bool

	// Raw resource readings.
	JVMHeapPercent  float64 // 0–100
	DiskUsedPercent float64 // 0–100
	DiskTotalBytes  int64
	DiskUsedBytes   int64
	ShardCount      int
	TotalShardBytes int64

	// Derived latency (average from cumulative node stats).
	// NOTE: This is NOT p99. It is the mean query time derived from
	// QueryTimeInMillis/QueryTotal from /_nodes/stats. Typical healthy values
	// are 5–30ms average. For true p99, integrate an APM backend.
	AvgSearchLatMs float64

	// Weighted hot-score: JVMWeight*(heap/100) + DiskWeight*(disk/100) + ShardWeight*(count/max).
	HotScore float64
	// IsHot is true when HotScore >= AgentConfig.HotNodeThreshold.
	IsHot bool
}

// ShardInfo describes a single shard placement in the cluster.
type ShardInfo struct {
	Index     string
	ShardNum  int
	Primary   bool   // true = primary, false = replica
	NodeID    string // node currently hosting this shard
	State     string // STARTED, RELOCATING, INITIALIZING, UNASSIGNED
	SizeBytes int64  // from _cat/shards; 0 if unavailable
}

// ShardKey returns a stable string key for this exact shard copy (index/num/role).
// The format "index/N/p" or "index/N/r" matches the key emitted by
// HTTPClient.GetShardSizes, ensuring size lookups always succeed.
func (s *ShardInfo) ShardKey() string {
	role := "r"
	if s.Primary {
		role = "p"
	}
	return fmt.Sprintf("%s/%s/%s", s.Index, strconv.Itoa(s.ShardNum), role)
}

// ClusterSnapshot is a point-in-time view of the whole cluster,
// captured by the MonitoringAgent.
type ClusterSnapshot struct {
	Timestamp        time.Time
	ClusterName      string
	Health           string
	NodeMetrics      map[string]*NodeMetrics // nodeID → metrics
	Shards           []ShardInfo
	TotalShards      int
	RelocatingShards int
	SkewScore        float64 // std-dev of HotScore across data nodes
	AvgSearchLatMs   float64 // cluster-wide average
}

// DataNodes returns only the subset of NodeMetrics for data nodes.
func (s *ClusterSnapshot) DataNodes() []*NodeMetrics {
	out := make([]*NodeMetrics, 0, len(s.NodeMetrics))
	for _, n := range s.NodeMetrics {
		if n.IsDataNode {
			out = append(out, n)
		}
	}
	return out
}

// ShardsOnNode returns all shards currently placed on nodeID.
func (s *ClusterSnapshot) ShardsOnNode(nodeID string) []ShardInfo {
	var out []ShardInfo
	for _, sh := range s.Shards {
		if sh.NodeID == nodeID {
			out = append(out, sh)
		}
	}
	return out
}
