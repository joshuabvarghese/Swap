// Package opensearch contains types that mirror the OpenSearch REST API responses
// and the Client interface. Nothing in this package is cloud-provider specific.
package opensearch

// ──────────────────────────────────────────────────────────────
//  _nodes/stats response
// ──────────────────────────────────────────────────────────────

// NodesStatsResponse is the top-level response from GET /_nodes/stats.
type NodesStatsResponse struct {
	Nodes map[string]NodeStats `json:"nodes"`
}

// NodeStats represents stats for a single node.
type NodeStats struct {
	Name    string    `json:"name"`
	Host    string    `json:"host"`
	IP      string    `json:"ip"`
	Roles   []string  `json:"roles"`
	JVM     JVMStats  `json:"jvm"`
	FS      FSStats   `json:"fs"`
	Indices NodeIndex `json:"indices"`
	OS      OSStats   `json:"os"`
}

// JVMStats holds JVM memory info.
type JVMStats struct {
	Mem JVMMem `json:"mem"`
}

// JVMMem holds heap usage figures.
type JVMMem struct {
	HeapUsedPercent int64 `json:"heap_used_percent"`
	HeapUsedInBytes int64 `json:"heap_used_in_bytes"`
	HeapMaxInBytes  int64 `json:"heap_max_in_bytes"`
}

// FSStats holds filesystem info.
type FSStats struct {
	Total FSTotal `json:"total"`
}

// FSTotal holds aggregate filesystem figures.
type FSTotal struct {
	TotalInBytes     int64 `json:"total_in_bytes"`
	FreeInBytes      int64 `json:"free_in_bytes"`
	AvailableInBytes int64 `json:"available_in_bytes"`
}

// NodeIndex holds per-node index statistics.
type NodeIndex struct {
	Docs     DocStats      `json:"docs"`
	Store    StoreStats    `json:"store"`
	Search   SearchStats   `json:"search"`
	Indexing IndexingStats `json:"indexing"`
}

// DocStats holds document counts.
type DocStats struct {
	Count   int64 `json:"count"`
	Deleted int64 `json:"deleted"`
}

// StoreStats holds on-disk storage figures.
type StoreStats struct {
	SizeInBytes int64 `json:"size_in_bytes"`
}

// SearchStats holds query and fetch counts/times.
type SearchStats struct {
	QueryTotal        int64 `json:"query_total"`
	QueryTimeInMillis int64 `json:"query_time_in_millis"`
	FetchTotal        int64 `json:"fetch_total"`
	FetchTimeInMillis int64 `json:"fetch_time_in_millis"`
}

// IndexingStats holds indexing counts/times.
type IndexingStats struct {
	IndexTotal        int64 `json:"index_total"`
	IndexTimeInMillis int64 `json:"index_time_in_millis"`
}

// OSStats holds OS-level metrics.
type OSStats struct {
	CPU OSCPUStats `json:"cpu"`
	Mem OSMemStats `json:"mem"`
}

// OSCPUStats holds CPU usage.
type OSCPUStats struct {
	Percent int64 `json:"percent"`
}

// OSMemStats holds memory usage.
type OSMemStats struct {
	UsedPercent int64 `json:"used_percent"`
}

// ──────────────────────────────────────────────────────────────
//  _cluster/state response
// ──────────────────────────────────────────────────────────────

// ClusterStateResponse is the top-level response from GET /_cluster/state.
type ClusterStateResponse struct {
	ClusterName  string       `json:"cluster_name"`
	RoutingTable RoutingTable `json:"routing_table"`
	Nodes        ClusterNodes `json:"nodes"`
}

// ClusterNodes maps node IDs to node descriptors.
type ClusterNodes struct {
	Nodes map[string]ClusterNode `json:"nodes"`
}

// ClusterNode is a lightweight node descriptor in cluster state.
type ClusterNode struct {
	Name             string `json:"name"`
	TransportAddress string `json:"transport_address"`
}

// RoutingTable describes where every shard lives.
type RoutingTable struct {
	Indices map[string]IndexRoutingTable `json:"indices"`
}

// IndexRoutingTable maps shard numbers (as strings) to routing entries.
type IndexRoutingTable struct {
	Shards map[string][]ShardRouting `json:"shards"`
}

// ShardRouting describes a single shard placement.
type ShardRouting struct {
	State          string `json:"state"`
	Primary        bool   `json:"primary"`
	Node           string `json:"node"`
	RelocatingNode string `json:"relocating_node,omitempty"`
	Index          string `json:"index"`
	Shard          int    `json:"shard"`
}

// ──────────────────────────────────────────────────────────────
//  _cluster/health response
// ──────────────────────────────────────────────────────────────

// ClusterHealthResponse is the response from GET /_cluster/health.
type ClusterHealthResponse struct {
	ClusterName         string `json:"cluster_name"`
	Status              string `json:"status"`
	NumberOfNodes       int    `json:"number_of_nodes"`
	NumberOfDataNodes   int    `json:"number_of_data_nodes"`
	ActivePrimaryShards int    `json:"active_primary_shards"`
	ActiveShards        int    `json:"active_shards"`
	RelocatingShards    int    `json:"relocating_shards"`
	InitializingShards  int    `json:"initializing_shards"`
	UnassignedShards    int    `json:"unassigned_shards"`
}

// ──────────────────────────────────────────────────────────────
//  _cat/shards response (for shard size data)
// ──────────────────────────────────────────────────────────────

// CatShard represents one row from GET /_cat/shards?format=json.
type CatShard struct {
	Index  string `json:"index"`
	Shard  string `json:"shard"`
	Prirep string `json:"prirep"` // "p" or "r"
	Store  string `json:"store"`  // bytes as string, e.g. "4294967296"
	Node   string `json:"node"`
	State  string `json:"state"`
}

// ──────────────────────────────────────────────────────────────
//  _cluster/reroute request / response
// ──────────────────────────────────────────────────────────────

// RerouteRequest is the body for POST /_cluster/reroute.
type RerouteRequest struct {
	Commands []RerouteCommand `json:"commands"`
}

// RerouteCommand wraps a single reroute operation.
type RerouteCommand struct {
	Move *MoveShard `json:"move,omitempty"`
}

// MoveShard is the payload for a move command.
type MoveShard struct {
	Index    string `json:"index"`
	Shard    int    `json:"shard"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
}

// RerouteResponse is the response from _cluster/reroute.
type RerouteResponse struct {
	Acknowledged bool `json:"acknowledged"`
}
