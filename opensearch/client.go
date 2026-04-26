package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// ──────────────────────────────────────────────────────────────
//  Client interface — the cloud-agnostic abstraction boundary
// ──────────────────────────────────────────────────────────────

// Client is the single interface that every backend must satisfy.
// The HTTP implementation targets vanilla OpenSearch (on-premise or any cloud).
// The SimulatedClient (internal/simulation) satisfies the same interface.
//
// Cloud-specific auth (AWS SigV4, GCP workload identity) is handled by
// supplying a custom http.RoundTripper to NewHTTPClient via WithTransport —
// no interface changes required.
type Client interface {
	GetNodesStats(ctx context.Context) (*NodesStatsResponse, error)
	GetClusterState(ctx context.Context) (*ClusterStateResponse, error)
	GetClusterHealth(ctx context.Context) (*ClusterHealthResponse, error)
	GetShardSizes(ctx context.Context) (map[string]int64, error) // key: "index/shard/p|r"
	Reroute(ctx context.Context, req *RerouteRequest, dryRun bool) (*RerouteResponse, error)
}

// ──────────────────────────────────────────────────────────────
//  HTTP implementation
// ──────────────────────────────────────────────────────────────

// HTTPClient talks directly to the OpenSearch REST API.
// It is stateless and safe for concurrent use. Multiple addresses are tried in
// round-robin order on each request; failed addresses trigger a retry on the
// next one up to MaxRetries total attempts.
type HTTPClient struct {
	addresses  []string
	username   string
	password   string
	maxRetries int
	httpClient *http.Client
}

// HTTPClientOption is a functional option for NewHTTPClient.
type HTTPClientOption func(*HTTPClient)

// WithTransport replaces the underlying http.RoundTripper.
// Use this to inject AWS SigV4 signing, GCP token refresh, or mTLS.
func WithTransport(rt http.RoundTripper) HTTPClientOption {
	return func(c *HTTPClient) {
		c.httpClient.Transport = rt
	}
}

// NewHTTPClient constructs an HTTPClient from connection parameters.
// All provided addresses are tried in order on retries.
func NewHTTPClient(addresses []string, username, password string, tlsVerify bool, timeout time.Duration, maxRetries int, opts ...HTTPClientOption) *HTTPClient {
	if maxRetries < 1 {
		maxRetries = 1
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !tlsVerify}, //nolint:gosec
	}
	c := &HTTPClient{
		addresses:  addresses,
		username:   username,
		password:   password,
		maxRetries: maxRetries,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: tr,
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// get performs a GET request with retry/failover across all addresses.
func (c *HTTPClient) get(ctx context.Context, path string, v interface{}) error {
	return c.doWithRetry(ctx, http.MethodGet, path, nil, v)
}

// post performs a POST request with retry/failover across all addresses.
func (c *HTTPClient) post(ctx context.Context, path string, body interface{}, v interface{}) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshalling body: %w", err)
	}
	return c.doWithRetry(ctx, http.MethodPost, path, b, v)
}

// doWithRetry tries each address in round-robin for up to maxRetries total
// attempts, returning the last error if all fail.
func (c *HTTPClient) doWithRetry(ctx context.Context, method, path string, body []byte, v interface{}) error {
	var lastErr error
	total := len(c.addresses) * c.maxRetries
	for attempt := 0; attempt < total; attempt++ {
		addr := c.addresses[attempt%len(c.addresses)]
		lastErr = c.doOnce(ctx, method, addr+path, body, v)
		if lastErr == nil {
			return nil
		}
	}
	return lastErr
}

// doOnce performs a single HTTP request to the given full URL.
func (c *HTTPClient) doOnce(ctx context.Context, method, url string, body []byte, v interface{}) error {
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return fmt.Errorf("building request for %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s returned %d: %s", method, url, resp.StatusCode, b)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// GetNodesStats calls GET /_nodes/stats.
func (c *HTTPClient) GetNodesStats(ctx context.Context) (*NodesStatsResponse, error) {
	var r NodesStatsResponse
	return &r, c.get(ctx, "/_nodes/stats", &r)
}

// GetClusterState calls GET /_cluster/state/routing_table,nodes.
func (c *HTTPClient) GetClusterState(ctx context.Context) (*ClusterStateResponse, error) {
	var r ClusterStateResponse
	return &r, c.get(ctx, "/_cluster/state/routing_table,nodes", &r)
}

// GetClusterHealth calls GET /_cluster/health.
func (c *HTTPClient) GetClusterHealth(ctx context.Context) (*ClusterHealthResponse, error) {
	var r ClusterHealthResponse
	return &r, c.get(ctx, "/_cluster/health", &r)
}

// GetShardSizes calls GET /_cat/shards?format=json&bytes=b and returns a map
// keyed by "index/shardNum/p|r" → size in bytes.
//
// The key format uses lowercase "p" and "r" as returned by the cat API.
// This matches the key format used by agent.ShardInfo.ShardKey().
func (c *HTTPClient) GetShardSizes(ctx context.Context) (map[string]int64, error) {
	var rows []CatShard
	if err := c.get(ctx, "/_cat/shards?format=json&bytes=b&h=index,shard,prirep,store,node,state", &rows); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		if row.State != "STARTED" {
			continue
		}
		// prirep is "p" or "r" — lowercase, matching ShardInfo.ShardKey().
		size, _ := strconv.ParseInt(row.Store, 10, 64)
		key := fmt.Sprintf("%s/%s/%s", row.Index, row.Shard, row.Prirep)
		out[key] = size
	}
	return out, nil
}

// Reroute calls POST /_cluster/reroute.
// When dryRun is true the call is skipped entirely and a synthetic acknowledged
// response is returned — the engine still logs the full planned payload.
func (c *HTTPClient) Reroute(ctx context.Context, req *RerouteRequest, dryRun bool) (*RerouteResponse, error) {
	if dryRun {
		return &RerouteResponse{Acknowledged: true}, nil
	}
	var r RerouteResponse
	return &r, c.post(ctx, "/_cluster/reroute", req, &r)
}
