package agent

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/project-sway/sway/internal/config"
	"github.com/project-sway/sway/internal/opensearch"
)

// ──────────────────────────────────────────────────────────────
//  Helpers
// ──────────────────────────────────────────────────────────────

func defaultAgentCfg() config.AgentConfig {
	return config.AgentConfig{
		PollIntervalSeconds: 30,
		HotNodeThreshold:    0.70,
		JVMWeight:           0.40,
		DiskWeight:          0.40,
		ShardWeight:         0.20,
	}
}

// ──────────────────────────────────────────────────────────────
//  Unit tests for internal helpers
// ──────────────────────────────────────────────────────────────

func TestIsDataNode(t *testing.T) {
	tests := []struct {
		roles []string
		want  bool
	}{
		{[]string{"data"}, true},
		{[]string{"data_hot"}, true},
		{[]string{"data_warm"}, true},
		{[]string{"data_cold"}, true},
		{[]string{"data_content"}, true},
		{[]string{"master"}, false},
		{[]string{"ingest"}, false},
		{[]string{"master", "ingest"}, false},
		{[]string{"master", "data"}, true},
		{[]string{}, false},
	}
	for _, tt := range tests {
		got := isDataNode(tt.roles)
		if got != tt.want {
			t.Errorf("isDataNode(%v) = %v, want %v", tt.roles, got, tt.want)
		}
	}
}

func TestComputeAvgLatency(t *testing.T) {
	tests := []struct {
		name       string
		stats      opensearch.SearchStats
		wantApprox float64
	}{
		{"no queries", opensearch.SearchStats{QueryTotal: 0, QueryTimeInMillis: 0}, 0},
		{"100 queries at 1000ms total = 10ms avg", opensearch.SearchStats{QueryTotal: 100, QueryTimeInMillis: 1000}, 10.0},
		{"1 query at 45ms", opensearch.SearchStats{QueryTotal: 1, QueryTimeInMillis: 45}, 45.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeAvgLatency(tt.stats)
			diff := got - tt.wantApprox
			if diff < -0.01 || diff > 0.01 {
				t.Errorf("computeAvgLatency() = %.2f, want %.2f", got, tt.wantApprox)
			}
		})
	}
}

func TestComputeSkewEqualScores(t *testing.T) {
	nodes := map[string]*NodeMetrics{
		"n0": {IsDataNode: true, HotScore: 0.5},
		"n1": {IsDataNode: true, HotScore: 0.5},
		"n2": {IsDataNode: true, HotScore: 0.5},
	}
	if got := computeSkew(nodes); got > 0.0001 {
		t.Errorf("equal scores should have near-zero skew, got %.4f", got)
	}
}

func TestComputeSkewSingleNode(t *testing.T) {
	nodes := map[string]*NodeMetrics{
		"n0": {IsDataNode: true, HotScore: 0.8},
	}
	if got := computeSkew(nodes); got != 0 {
		t.Errorf("single node should have zero skew, got %.4f", got)
	}
}

func TestComputeSkewNonDataNodesExcluded(t *testing.T) {
	nodes := map[string]*NodeMetrics{
		"n0":     {IsDataNode: true, HotScore: 0.5},
		"n1":     {IsDataNode: true, HotScore: 0.5},
		"master": {IsDataNode: false, HotScore: 0.9}, // must not affect skew
	}
	if got := computeSkew(nodes); got > 0.0001 {
		t.Errorf("master node should not affect skew, got %.4f", got)
	}
}

func TestComputeSkewDetectsImbalance(t *testing.T) {
	nodes := map[string]*NodeMetrics{
		"hot":  {IsDataNode: true, HotScore: 0.9},
		"cold": {IsDataNode: true, HotScore: 0.1},
	}
	if got := computeSkew(nodes); got < 0.1 {
		t.Errorf("imbalanced nodes should have high skew, got %.4f", got)
	}
}

func TestBuildShardList(t *testing.T) {
	state := &opensearch.ClusterStateResponse{
		ClusterName: "test",
		Nodes:       opensearch.ClusterNodes{Nodes: map[string]opensearch.ClusterNode{}},
		RoutingTable: opensearch.RoutingTable{
			Indices: map[string]opensearch.IndexRoutingTable{
				"logs": {
					Shards: map[string][]opensearch.ShardRouting{
						"0": {
							{State: "STARTED", Primary: true, Node: "n0", Index: "logs", Shard: 0},
							{State: "STARTED", Primary: false, Node: "n1", Index: "logs", Shard: 0},
						},
					},
				},
			},
		},
	}
	sizes := map[string]int64{
		"logs/0/p": 100,
		"logs/0/r": 100,
	}

	shards, reloc := buildShardList(state, sizes)
	if len(shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(shards))
	}
	if reloc != 0 {
		t.Errorf("expected 0 relocating, got %d", reloc)
	}
	for _, s := range shards {
		if s.SizeBytes != 100 {
			t.Errorf("shard %s SizeBytes = %d, want 100", s.ShardKey(), s.SizeBytes)
		}
	}
}

func TestBuildShardListRelocatingCount(t *testing.T) {
	state := &opensearch.ClusterStateResponse{
		ClusterName: "test",
		Nodes:       opensearch.ClusterNodes{Nodes: map[string]opensearch.ClusterNode{}},
		RoutingTable: opensearch.RoutingTable{
			Indices: map[string]opensearch.IndexRoutingTable{
				"idx": {
					Shards: map[string][]opensearch.ShardRouting{
						"0": {{State: "RELOCATING", Primary: true, Node: "n0", Index: "idx", Shard: 0}},
					},
				},
			},
		},
	}
	_, reloc := buildShardList(state, nil)
	if reloc != 1 {
		t.Errorf("expected 1 relocating shard, got %d", reloc)
	}
}

// ──────────────────────────────────────────────────────────────
//  Integration-style: CollectSnapshot via a minimal stub client
// ──────────────────────────────────────────────────────────────

type stubClient struct{}

func (s *stubClient) GetClusterHealth(_ context.Context) (*opensearch.ClusterHealthResponse, error) {
	return &opensearch.ClusterHealthResponse{
		Status:            "green",
		NumberOfNodes:     2,
		NumberOfDataNodes: 2,
		ActiveShards:      2,
		RelocatingShards:  0,
	}, nil
}

func (s *stubClient) GetNodesStats(_ context.Context) (*opensearch.NodesStatsResponse, error) {
	gb := int64(1024 * 1024 * 1024)
	return &opensearch.NodesStatsResponse{
		Nodes: map[string]opensearch.NodeStats{
			"n0": {
				Name:  "node-0",
				Host:  "10.0.0.1",
				Roles: []string{"data"},
				JVM:   opensearch.JVMStats{Mem: opensearch.JVMMem{HeapUsedPercent: 80, HeapMaxInBytes: 32 * gb}},
				FS:    opensearch.FSStats{Total: opensearch.FSTotal{TotalInBytes: 500 * gb, FreeInBytes: 400 * gb}},
				Indices: opensearch.NodeIndex{
					Store:  opensearch.StoreStats{SizeInBytes: 50 * gb},
					Search: opensearch.SearchStats{QueryTotal: 100, QueryTimeInMillis: 500},
				},
			},
			"n1": {
				Name:  "node-1",
				Host:  "10.0.0.2",
				Roles: []string{"data"},
				JVM:   opensearch.JVMStats{Mem: opensearch.JVMMem{HeapUsedPercent: 30, HeapMaxInBytes: 32 * gb}},
				FS:    opensearch.FSStats{Total: opensearch.FSTotal{TotalInBytes: 500 * gb, FreeInBytes: 480 * gb}},
				Indices: opensearch.NodeIndex{
					Store:  opensearch.StoreStats{SizeInBytes: 10 * gb},
					Search: opensearch.SearchStats{QueryTotal: 50, QueryTimeInMillis: 100},
				},
			},
		},
	}, nil
}

func (s *stubClient) GetClusterState(_ context.Context) (*opensearch.ClusterStateResponse, error) {
	return &opensearch.ClusterStateResponse{
		ClusterName: "test",
		Nodes: opensearch.ClusterNodes{
			Nodes: map[string]opensearch.ClusterNode{
				"n0": {Name: "node-0"},
				"n1": {Name: "node-1"},
			},
		},
		RoutingTable: opensearch.RoutingTable{
			Indices: map[string]opensearch.IndexRoutingTable{
				"idx": {
					Shards: map[string][]opensearch.ShardRouting{
						"0": {
							{State: "STARTED", Primary: true, Node: "n0", Index: "idx", Shard: 0},
							{State: "STARTED", Primary: false, Node: "n1", Index: "idx", Shard: 0},
						},
					},
				},
			},
		},
	}, nil
}

func (s *stubClient) GetShardSizes(_ context.Context) (map[string]int64, error) {
	return map[string]int64{
		"idx/0/p": 10 * 1024 * 1024 * 1024,
		"idx/0/r": 10 * 1024 * 1024 * 1024,
	}, nil
}

func (s *stubClient) Reroute(_ context.Context, _ *opensearch.RerouteRequest, _ bool) (*opensearch.RerouteResponse, error) {
	return &opensearch.RerouteResponse{Acknowledged: true}, nil
}

func TestCollectSnapshotBasic(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	mon := New(&stubClient{}, defaultAgentCfg(), logger)
	snap, err := mon.CollectSnapshot(context.Background())
	if err != nil {
		t.Fatalf("CollectSnapshot() error: %v", err)
	}
	if snap.Health != "green" {
		t.Errorf("Health = %q, want green", snap.Health)
	}
	if len(snap.NodeMetrics) != 2 {
		t.Errorf("NodeMetrics count = %d, want 2", len(snap.NodeMetrics))
	}
	if len(snap.Shards) != 2 {
		t.Errorf("Shards count = %d, want 2", len(snap.Shards))
	}
	if snap.SkewScore == 0 {
		t.Error("expected non-zero SkewScore for imbalanced cluster")
	}
}

func TestCollectSnapshotHotNodeDetection(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	mon := New(&stubClient{}, defaultAgentCfg(), logger)
	snap, err := mon.CollectSnapshot(context.Background())
	if err != nil {
		t.Fatalf("CollectSnapshot() error: %v", err)
	}
	// n0 has 80% JVM — should be classified HOT at threshold 0.70.
	n0 := snap.NodeMetrics["n0"]
	if n0 == nil {
		t.Fatal("node n0 not found in snapshot")
	}
	if !n0.IsHot {
		t.Errorf("n0 with JVM=80%% should be HOT (score=%.4f, threshold=%.2f)", n0.HotScore, defaultAgentCfg().HotNodeThreshold)
	}
	n1 := snap.NodeMetrics["n1"]
	if n1 == nil {
		t.Fatal("node n1 not found in snapshot")
	}
	if n1.IsHot {
		t.Errorf("n1 with JVM=30%% should NOT be HOT (score=%.4f)", n1.HotScore)
	}
}
