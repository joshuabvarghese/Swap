// Package config provides cloud-agnostic configuration for Project Sway.
// Connection details are endpoint-only: no cloud-provider SDK types appear here.
// To target AWS, GCP, or On-Premise, simply change the Addresses field.
package config

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

// Config is the root configuration object.
type Config struct {
	OpenSearch     OpenSearchConfig     `json:"opensearch"`
	Agent          AgentConfig          `json:"agent"`
	Rebalancer     RebalancerConfig     `json:"rebalancer"`
	CircuitBreaker CircuitBreakerConfig `json:"circuit_breaker"`
}

// Validate checks the configuration for common mistakes and returns an error
// if any field is out of range or inconsistent. Called automatically by
// LoadFromFile and should be called manually in tests.
func (c *Config) Validate() error {
	weightSum := c.Agent.JVMWeight + c.Agent.DiskWeight + c.Agent.ShardWeight
	if math.Abs(weightSum-1.0) > 0.01 {
		return fmt.Errorf(
			"agent weights must sum to 1.0 (jvm=%.2f disk=%.2f shard=%.2f sum=%.2f)",
			c.Agent.JVMWeight, c.Agent.DiskWeight, c.Agent.ShardWeight, weightSum,
		)
	}
	if c.Agent.HotNodeThreshold <= 0 || c.Agent.HotNodeThreshold > 1 {
		return fmt.Errorf("agent.hot_node_threshold must be in (0, 1], got %.2f", c.Agent.HotNodeThreshold)
	}
	if c.Rebalancer.MaxMovesPerCycle <= 0 {
		return fmt.Errorf("rebalancer.max_moves_per_cycle must be > 0, got %d", c.Rebalancer.MaxMovesPerCycle)
	}
	if c.Rebalancer.SkewReductionTarget <= 0 || c.Rebalancer.SkewReductionTarget > 1 {
		return fmt.Errorf("rebalancer.skew_reduction_target must be in (0, 1], got %.2f", c.Rebalancer.SkewReductionTarget)
	}
	if c.CircuitBreaker.MaxAvgLatencyMs <= 0 {
		return fmt.Errorf("circuit_breaker.max_avg_latency_ms must be > 0, got %.1f", c.CircuitBreaker.MaxAvgLatencyMs)
	}
	validHealth := map[string]bool{"green": true, "yellow": true, "red": true}
	if !validHealth[c.CircuitBreaker.RequiredHealth] {
		return fmt.Errorf("circuit_breaker.required_health must be green|yellow|red, got %q", c.CircuitBreaker.RequiredHealth)
	}
	return nil
}

// OpenSearchConfig holds connection details.
// Cloud-agnostic: the engine speaks only to the OpenSearch REST API.
// For cloud-specific auth (AWS SigV4, GCP OIDC), plug a custom
// http.RoundTripper into NewHTTPClient via WithTransport — no other changes needed.
type OpenSearchConfig struct {
	// Addresses: one or more reachable OpenSearch endpoints, tried in order.
	// The HTTP client will retry across all addresses before failing a request.
	//   AWS:        ["https://search-my-domain.us-east-1.es.amazonaws.com"]
	//   GCP:        ["https://opensearch.example.internal:9200"]
	//   On-Premise: ["http://10.0.0.1:9200","http://10.0.0.2:9200"]
	Addresses      []string `json:"addresses"`
	Username       string   `json:"username"`
	Password       string   `json:"password"`
	TLSVerify      bool     `json:"tls_verify"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	// MaxRetries is the number of times a failed request is retried across all
	// addresses before returning an error. Defaults to 3.
	MaxRetries int `json:"max_retries"`
}

// AgentConfig controls the monitoring agent.
type AgentConfig struct {
	// PollIntervalSeconds defines how often metrics are scraped.
	PollIntervalSeconds int `json:"poll_interval_seconds"`
	// HotNodeThreshold: weighted score above which a node is classified HOT (0.0–1.0).
	HotNodeThreshold float64 `json:"hot_node_threshold"`
	// Metric weights. MUST sum to 1.0 — enforced by Validate().
	JVMWeight   float64 `json:"jvm_weight"`
	DiskWeight  float64 `json:"disk_weight"`
	ShardWeight float64 `json:"shard_weight"`
}

// PollInterval returns poll interval as a time.Duration.
func (a *AgentConfig) PollInterval() time.Duration {
	return time.Duration(a.PollIntervalSeconds) * time.Second
}

// RebalancerConfig controls rebalancing behaviour.
type RebalancerConfig struct {
	// DryRun: plan moves but make no API calls. Defaults to true (safety first).
	DryRun bool `json:"dry_run"`
	// MaxMovesPerCycle caps the number of shard relocations per pass.
	MaxMovesPerCycle int `json:"max_moves_per_cycle"`
	// SkewReductionTarget: fraction of current skew to eliminate per cycle.
	// 0.25 means "reduce skew by 25% per cycle."
	SkewReductionTarget float64 `json:"skew_reduction_target"`
	// LargeShardThresholdBytes: shards above this size are "large" and
	// prioritised for early movement (moves the most data with fewest API calls).
	LargeShardThresholdBytes int64 `json:"large_shard_threshold_bytes"`
}

// CircuitBreakerConfig defines the safety thresholds.
type CircuitBreakerConfig struct {
	// RequiredHealth: minimum cluster health before automation runs.
	// "green" is recommended for production; "yellow" is permitted for testing.
	RequiredHealth string `json:"required_health"`
	// MaxAvgLatencyMs: if average search latency exceeds this value (ms), the
	// circuit opens.
	//
	// NOTE: This uses average query latency from cumulative node stats, NOT p99.
	// In a healthy cluster, average latency is typically 5–30ms. The default of
	// 40ms catches degraded clusters without false-positives.
	// For true p99 gating, integrate Prometheus/OpenTelemetry and extend
	// CircuitBreaker with a custom Collector.
	MaxAvgLatencyMs float64 `json:"max_avg_latency_ms"`
	// MaxRelocatingShards: block automation if shards are already moving.
	// Set to 0 to enforce strict serial execution (recommended for production).
	MaxRelocatingShards int `json:"max_relocating_shards"`
}

// Default returns a production-safe configuration with conservative settings.
// DryRun is true and thresholds are strict by default.
func Default() *Config {
	return &Config{
		OpenSearch: OpenSearchConfig{
			Addresses:      []string{"http://localhost:9200"},
			TLSVerify:      true,
			TimeoutSeconds: 30,
			MaxRetries:     3,
		},
		Agent: AgentConfig{
			PollIntervalSeconds: 30,
			HotNodeThreshold:    0.70,
			JVMWeight:           0.40,
			DiskWeight:          0.40,
			ShardWeight:         0.20,
		},
		Rebalancer: RebalancerConfig{
			DryRun:                   true,
			MaxMovesPerCycle:         5,
			SkewReductionTarget:      0.25,
			LargeShardThresholdBytes: 2 * 1024 * 1024 * 1024, // 2 GiB
		},
		CircuitBreaker: CircuitBreakerConfig{
			RequiredHealth:      "green",
			MaxAvgLatencyMs:     40.0, // avg latency — see field comment above
			MaxRelocatingShards: 0,
		},
	}
}

// LoadFromFile reads a JSON config file, overlays it on top of defaults, and
// validates the result. Returns an error if the file cannot be read, parsed,
// or fails validation.
func LoadFromFile(path string) (*Config, error) {
	cfg := Default()
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config %q: %w", path, err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return nil, fmt.Errorf("decoding config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}
	return cfg, nil
}
