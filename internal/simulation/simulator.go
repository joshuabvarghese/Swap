// Package simulation provides a virtual OpenSearch cluster for demonstration
// and testing. SimulatedClient implements the opensearch.Client interface with
// a realistic 10-node cluster in a skewed state, allowing the full rebalancing
// pipeline to run without a real cluster.
package simulation

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/project-sway/sway/internal/opensearch"
)

const (
	gb                = int64(1024 * 1024 * 1024)
	nodeCapacityGB    = int64(500) // each simulated node has 500 GB total disk
	nodeCapacityBytes = nodeCapacityGB * gb
	clusterName       = "simulated-cluster"
)

// simNode is the internal mutable state of one virtual node.
type simNode struct {
	id            string
	name          string
	host          string
	jvmHeapPct    float64 // 0–100
	diskUsedBytes int64
	queryTotal    int64
	queryTimeMs   int64
}

func (n *simNode) diskPct() float64 {
	return float64(n.diskUsedBytes) / float64(nodeCapacityBytes) * 100.0
}

// simShard is the internal state of one virtual shard.
type simShard struct {
	index     string
	shardNum  int
	primary   bool
	nodeID    string
	sizeBytes int64
	state     string // STARTED | RELOCATING
}

func (s *simShard) prirep() string {
	if s.primary {
		return "p"
	}
	return "r"
}

func (s *simShard) catKey() string {
	return fmt.Sprintf("%s/%d/%s", s.index, s.shardNum, s.prirep())
}

// SimulatedClient implements opensearch.Client against a virtual cluster.
// It is safe for concurrent use. When Reroute is called, the internal state
// is updated so that successive CollectSnapshot calls reflect the new layout.
type SimulatedClient struct {
	mu     sync.RWMutex
	nodes  map[string]*simNode
	shards []*simShard
	logger *log.Logger
}

// ──────────────────────────────────────────────────────────────
//  Constructor — builds the skewed initial state
// ──────────────────────────────────────────────────────────────

// New creates a SimulatedClient with a 10-node cluster in a deliberately
// skewed state: nodes 0–2 are overloaded, nodes 3–9 are underutilised.
func New(logger *log.Logger) *SimulatedClient {
	c := &SimulatedClient{logger: logger}
	c.nodes = buildNodes()
	c.shards = buildShards()
	// Set each node's diskUsedBytes from its initial shard load.
	c.syncDiskUsage()
	return c
}

// buildNodes creates 10 virtual data nodes.
func buildNodes() map[string]*simNode {
	// Initial JVM heap percentages (higher on hot nodes).
	jvmPcts := []float64{85, 79, 71, 45, 42, 38, 35, 32, 30, 28}
	nodes := make(map[string]*simNode, 10)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("node-id-%02d", i)
		nodes[id] = &simNode{
			id:          id,
			name:        fmt.Sprintf("node-%d", i),
			host:        fmt.Sprintf("10.0.0.%d", i+1),
			jvmHeapPct:  jvmPcts[i],
			queryTotal:  int64(1000 + i*200),
			queryTimeMs: int64((20 + i*3) * (1000 + i*200) / 1000), // avg 20–47 ms
		}
	}
	return nodes
}

// buildShards creates a skewed shard distribution across the 10 nodes.
//
// Cluster layout:
//
//	nodes 0–2: carry all primary shards (6, 6, 5 shards respectively)
//	nodes 3–9: carry only replica shards (2 each)
//
// Indices:
//
//	logs-2024     — 5 primary shards, 1 replica
//	metrics-2024  — 3 primary shards, 1 replica
//	traces-2024   — 4 primary shards, 1 replica
//	events-2024   — 3 primary shards, 0 replicas
//	audit-2024    — 2 primary shards, 1 replica
func buildShards() []*simShard {
	// Shard sizes in GB — varies to give the rebalancer meaningful "large first" ordering.
	type shardDef struct {
		index   string
		shardNo int
		primary bool
		nodeIdx int // which node (0–9) hosts this shard
		sizeGB  int64
	}

	defs := []shardDef{
		// logs-2024: large shards on hot nodes
		{"logs-2024", 0, true, 0, 22},
		{"logs-2024", 0, false, 3, 22}, // replica on node-3
		{"logs-2024", 1, true, 0, 16},
		{"logs-2024", 1, false, 4, 16},
		{"logs-2024", 2, true, 1, 14},
		{"logs-2024", 2, false, 5, 14},
		{"logs-2024", 3, true, 1, 7},
		{"logs-2024", 3, false, 6, 7},
		{"logs-2024", 4, true, 2, 4},
		{"logs-2024", 4, false, 7, 4},
		// metrics-2024
		{"metrics-2024", 0, true, 0, 10},
		{"metrics-2024", 0, false, 8, 10},
		{"metrics-2024", 1, true, 1, 9},
		{"metrics-2024", 1, false, 9, 9},
		{"metrics-2024", 2, true, 2, 6},
		{"metrics-2024", 2, false, 3, 6},
		// traces-2024
		{"traces-2024", 0, true, 0, 19},
		{"traces-2024", 0, false, 4, 19},
		{"traces-2024", 1, true, 1, 12},
		{"traces-2024", 1, false, 5, 12},
		{"traces-2024", 2, true, 2, 9},
		{"traces-2024", 2, false, 6, 9},
		{"traces-2024", 3, true, 2, 5},
		{"traces-2024", 3, false, 7, 5},
		// events-2024 (no replicas)
		{"events-2024", 0, true, 0, 4},
		{"events-2024", 1, true, 1, 3},
		{"events-2024", 2, true, 2, 2},
		// audit-2024
		{"audit-2024", 0, true, 0, 1},
		{"audit-2024", 0, false, 8, 1},
		{"audit-2024", 1, true, 1, 1},
		{"audit-2024", 1, false, 9, 1},
	}

	shards := make([]*simShard, 0, len(defs))
	for _, d := range defs {
		nodeID := fmt.Sprintf("node-id-%02d", d.nodeIdx)
		shards = append(shards, &simShard{
			index:     d.index,
			shardNum:  d.shardNo,
			primary:   d.primary,
			nodeID:    nodeID,
			sizeBytes: d.sizeGB * gb,
			state:     "STARTED",
		})
	}
	return shards
}

// syncDiskUsage recalculates diskUsedBytes for every node from shard sizes.
// It adds a 10% overhead to simulate OS, translog, and segment file overhead.
func (c *SimulatedClient) syncDiskUsage() {
	usage := make(map[string]int64)
	for _, s := range c.shards {
		if s.state == "STARTED" {
			usage[s.nodeID] += s.sizeBytes
		}
	}
	// Nodes that have no shards still have OS overhead (~5% of capacity).
	for id, node := range c.nodes {
		base := usage[id]
		overhead := int64(float64(base) * 0.10) // 10% overhead
		osBase := int64(float64(nodeCapacityBytes) * 0.05)
		node.diskUsedBytes = base + overhead + osBase
	}
}

// PrintClusterSummary prints the initial cluster state for reference.
func (c *SimulatedClient) PrintClusterSummary() {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("  SIMULATED CLUSTER INITIAL STATE")
	fmt.Printf("  Cluster: %s  |  Nodes: 10  |  Total Shards: %d\n", clusterName, len(c.shards))
	fmt.Println(strings.Repeat("=", 70))

	// Node order for display
	nodeOrder := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		nodeOrder = append(nodeOrder, id)
	}
	sort.Strings(nodeOrder)

	fmt.Printf("  %-14s  %-12s  %8s  %8s  %-8s  %s\n",
		"Node", "Host", "JVM Heap", "Disk Use", "Shards", "Shard Details")
	fmt.Println(strings.Repeat("-", 90))

	for _, id := range nodeOrder {
		node := c.nodes[id]
		shardNames := c.shardNamesOnNode(id)
		fmt.Printf("  %-14s  %-12s  %7.1f%%  %7.1f%%  %-8d  %s\n",
			node.name, node.host,
			node.jvmHeapPct, node.diskPct(),
			len(shardNames),
			strings.Join(shardNames, ", "),
		)
	}
	fmt.Println(strings.Repeat("=", 70))
}

func (c *SimulatedClient) shardNamesOnNode(nodeID string) []string {
	var names []string
	for _, s := range c.shards {
		if s.nodeID == nodeID {
			role := "R"
			if s.primary {
				role = "P"
			}
			names = append(names, fmt.Sprintf("%s[%d]%s", s.index, s.shardNum, role))
		}
	}
	sort.Strings(names)
	return names
}

// ──────────────────────────────────────────────────────────────
//  opensearch.Client implementation
// ──────────────────────────────────────────────────────────────

// GetNodesStats returns synthetic node statistics derived from internal state.
func (c *SimulatedClient) GetNodesStats(_ context.Context) (*opensearch.NodesStatsResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	resp := &opensearch.NodesStatsResponse{
		Nodes: make(map[string]opensearch.NodeStats, len(c.nodes)),
	}

	for id, node := range c.nodes {
		resp.Nodes[id] = opensearch.NodeStats{
			Name:  node.name,
			Host:  node.host,
			IP:    node.host,
			Roles: []string{"data", "ingest"},
			JVM: opensearch.JVMStats{
				Mem: opensearch.JVMMem{
					HeapUsedPercent: int64(math.Round(node.jvmHeapPct)),
					HeapUsedInBytes: int64(node.jvmHeapPct / 100.0 * float64(32*gb)),
					HeapMaxInBytes:  32 * gb,
				},
			},
			FS: opensearch.FSStats{
				Total: opensearch.FSTotal{
					TotalInBytes:     nodeCapacityBytes,
					FreeInBytes:      nodeCapacityBytes - node.diskUsedBytes,
					AvailableInBytes: nodeCapacityBytes - node.diskUsedBytes,
				},
			},
			Indices: opensearch.NodeIndex{
				Docs:  opensearch.DocStats{Count: node.queryTotal * 1000},
				Store: opensearch.StoreStats{SizeInBytes: node.diskUsedBytes},
				Search: opensearch.SearchStats{
					QueryTotal:        node.queryTotal,
					QueryTimeInMillis: node.queryTimeMs,
				},
			},
			OS: opensearch.OSStats{
				CPU: opensearch.OSCPUStats{Percent: int64(node.jvmHeapPct * 0.6)},
				Mem: opensearch.OSMemStats{UsedPercent: int64(node.diskPct())},
			},
		}
	}
	return resp, nil
}

// GetClusterState returns the current shard routing table.
func (c *SimulatedClient) GetClusterState(_ context.Context) (*opensearch.ClusterStateResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	resp := &opensearch.ClusterStateResponse{
		ClusterName: clusterName,
		Nodes:       opensearch.ClusterNodes{Nodes: make(map[string]opensearch.ClusterNode)},
		RoutingTable: opensearch.RoutingTable{
			Indices: make(map[string]opensearch.IndexRoutingTable),
		},
	}

	// Populate nodes.
	for id, node := range c.nodes {
		resp.Nodes.Nodes[id] = opensearch.ClusterNode{
			Name:             node.name,
			TransportAddress: node.host + ":9300",
		}
	}

	// Populate routing table.
	for _, s := range c.shards {
		irt, ok := resp.RoutingTable.Indices[s.index]
		if !ok {
			irt = opensearch.IndexRoutingTable{
				Shards: make(map[string][]opensearch.ShardRouting),
			}
		}
		key := strconv.Itoa(s.shardNum)
		irt.Shards[key] = append(irt.Shards[key], opensearch.ShardRouting{
			State:   s.state,
			Primary: s.primary,
			Node:    s.nodeID,
			Index:   s.index,
			Shard:   s.shardNum,
		})
		resp.RoutingTable.Indices[s.index] = irt
	}
	return resp, nil
}

// GetClusterHealth returns synthetic cluster health.
func (c *SimulatedClient) GetClusterHealth(_ context.Context) (*opensearch.ClusterHealthResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	relocating := 0
	for _, s := range c.shards {
		if s.state == "RELOCATING" {
			relocating++
		}
	}
	return &opensearch.ClusterHealthResponse{
		ClusterName:         clusterName,
		Status:              "green",
		NumberOfNodes:       len(c.nodes),
		NumberOfDataNodes:   len(c.nodes),
		ActivePrimaryShards: c.primaryCount(),
		ActiveShards:        len(c.shards),
		RelocatingShards:    relocating,
	}, nil
}

// GetShardSizes returns a map of shard key → size in bytes.
func (c *SimulatedClient) GetShardSizes(_ context.Context) (map[string]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]int64, len(c.shards))
	for _, s := range c.shards {
		out[s.catKey()] = s.sizeBytes
	}
	return out, nil
}

// Reroute applies shard move commands to the virtual cluster.
// In simulation mode this always succeeds and always updates state —
// regardless of the dryRun flag — so that successive cycles show improvement.
func (c *SimulatedClient) Reroute(_ context.Context, req *opensearch.RerouteRequest, _ bool) (*opensearch.RerouteResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cmd := range req.Commands {
		if cmd.Move == nil {
			continue
		}
		mv := cmd.Move
		fromID := c.nodeIDByName(mv.FromNode)
		toID := c.nodeIDByName(mv.ToNode)
		if fromID == "" || toID == "" {
			c.logger.Printf("[SIM] Reroute: unknown node names from=%q to=%q", mv.FromNode, mv.ToNode)
			continue
		}

		// Find and move the shard.
		moved := false
		for _, s := range c.shards {
			if s.index == mv.Index && s.shardNum == mv.Shard && s.nodeID == fromID && s.state == "STARTED" {
				c.moveShard(s, fromID, toID)
				moved = true
				break
			}
		}
		if !moved {
			c.logger.Printf("[SIM] Reroute: shard %s[%d] not found on node %s", mv.Index, mv.Shard, mv.FromNode)
		}
	}
	return &opensearch.RerouteResponse{Acknowledged: true}, nil
}

// ──────────────────────────────────────────────────────────────
//  Internal simulation helpers
// ──────────────────────────────────────────────────────────────

// moveShard updates internal state to reflect a shard relocation.
func (c *SimulatedClient) moveShard(s *simShard, fromID, toID string) {
	from := c.nodes[fromID]
	to := c.nodes[toID]
	if from == nil || to == nil {
		return
	}

	// Disk: transfer shard bytes + overhead.
	shardWithOverhead := int64(float64(s.sizeBytes) * 1.10)
	from.diskUsedBytes -= shardWithOverhead
	if from.diskUsedBytes < 0 {
		from.diskUsedBytes = 0
	}
	to.diskUsedBytes += shardWithOverhead

	// JVM: hot nodes' JVM decreases slightly with each shard lost;
	// cool nodes increase slightly. The shard fraction drives a ±3% swing.
	from.jvmHeapPct = math.Max(5.0, from.jvmHeapPct*0.96)
	to.jvmHeapPct = math.Min(95.0, to.jvmHeapPct*1.04)

	// Reassign the shard.
	s.nodeID = toID
}

// nodeIDByName returns the node ID that has the given name.
func (c *SimulatedClient) nodeIDByName(name string) string {
	for id, n := range c.nodes {
		if n.name == name {
			return id
		}
	}
	return ""
}

// primaryCount returns how many started primary shards exist.
func (c *SimulatedClient) primaryCount() int {
	count := 0
	for _, s := range c.shards {
		if s.primary && s.state == "STARTED" {
			count++
		}
	}
	return count
}
