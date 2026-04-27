// Package circuitbreaker implements the "Safety First" circuit breaker that gates
// every rebalancing operation. The circuit must be CLOSED for automation to proceed.
//
// Design philosophy (Project Sway):
//   - The circuit breaker is the last line of defence before live cluster changes.
//   - It defaults to OPEN: any ambiguity prevents action.
//   - Every check is logged with its measured value AND the configured threshold,
//     so operators can audit exactly why a cycle was blocked.
//   - The circuit is re-evaluated fresh on every cycle — there is no "half-open"
//     state. Cluster conditions can change; we never assume they have recovered.
package circuitbreaker

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/config"
)

// State represents the circuit breaker state.
type State int

const (
	// StateClosed means all safety checks passed; automation is enabled.
	StateClosed State = iota
	// StateOpen means at least one check failed; all automation is blocked.
	StateOpen
)

func (s State) String() string {
	if s == StateClosed {
		return "CLOSED"
	}
	return "OPEN"
}

// CheckItem records the result of a single safety check.
type CheckItem struct {
	Name     string
	Passed   bool
	Measured string // human-readable measured value
	Limit    string // human-readable threshold
}

// Result holds the aggregate outcome of a circuit-breaker evaluation.
type Result struct {
	State  State
	Reason string // non-empty when State == StateOpen
	Checks []CheckItem
}

// CircuitBreaker evaluates cluster safety conditions.
type CircuitBreaker struct {
	cfg    config.CircuitBreakerConfig
	logger *log.Logger
	mu     sync.Mutex
	last   *Result
}

// New constructs a CircuitBreaker.
func New(cfg config.CircuitBreakerConfig, logger *log.Logger) *CircuitBreaker {
	return &CircuitBreaker{cfg: cfg, logger: logger}
}

// Evaluate runs all safety checks against the provided snapshot.
// The circuit is CLOSED only when every check passes.
// Results are stored so the engine can retrieve them without re-evaluating.
func (cb *CircuitBreaker) Evaluate(snap *agent.ClusterSnapshot) *Result {
	checks := []CheckItem{
		cb.checkHealth(snap),
		cb.checkLatency(snap),
		cb.checkRelocating(snap),
	}

	var failures []string
	for _, c := range checks {
		if !c.Passed {
			failures = append(failures, c.Name)
		}
	}

	state := StateClosed
	reason := ""
	if len(failures) > 0 {
		state = StateOpen
		reason = fmt.Sprintf("failed checks: %s", strings.Join(failures, ", "))
	}

	result := &Result{State: state, Reason: reason, Checks: checks}

	cb.mu.Lock()
	cb.last = result
	cb.mu.Unlock()

	return result
}

// Last returns the most recent evaluation result, or nil if Evaluate has not been called.
func (cb *CircuitBreaker) Last() *Result {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.last
}

// ──────────────────────────────────────────────────────────────
//  Individual checks
// ──────────────────────────────────────────────────────────────

// checkHealth verifies the cluster health meets the required level.
// "green" > "yellow" > "red" in terms of acceptability.
func (cb *CircuitBreaker) checkHealth(snap *agent.ClusterSnapshot) CheckItem {
	required := strings.ToLower(cb.cfg.RequiredHealth)
	actual := strings.ToLower(snap.Health)
	passed := healthSatisfies(actual, required)
	return CheckItem{
		Name:     "Cluster Health",
		Passed:   passed,
		Measured: strings.ToUpper(actual),
		Limit:    fmt.Sprintf(">= %s", strings.ToUpper(required)),
	}
}

// checkLatency verifies average search latency is below the threshold.
//
// Important: this uses average query latency (QueryTimeInMillis/QueryTotal),
// not p99. A healthy cluster typically has 5–30ms average. The configured
// threshold (default 40ms) acts as a coarse proxy: if average latency exceeds
// it, the cluster is likely under heavy load and should not be disturbed.
//
// For true p99 gating, integrate an APM backend (Prometheus, OpenTelemetry)
// and extend CircuitBreaker with a custom Collector.
func (cb *CircuitBreaker) checkLatency(snap *agent.ClusterSnapshot) CheckItem {
	lat := snap.AvgSearchLatMs
	// Zero latency means no queries have run yet — safe to proceed.
	passed := lat == 0 || lat <= cb.cfg.MaxAvgLatencyMs
	return CheckItem{
		Name:     "Avg Search Latency",
		Passed:   passed,
		Measured: fmt.Sprintf("%.1fms", lat),
		Limit:    fmt.Sprintf("<= %.0fms (avg; not p99)", cb.cfg.MaxAvgLatencyMs),
	}
}

// checkRelocating ensures no other shard migrations are already in progress.
// Initiating new moves while shards are relocating risks double-replication
// pressure and cluster instability.
func (cb *CircuitBreaker) checkRelocating(snap *agent.ClusterSnapshot) CheckItem {
	relocating := snap.RelocatingShards
	passed := relocating <= cb.cfg.MaxRelocatingShards
	return CheckItem{
		Name:     "Relocating Shards",
		Passed:   passed,
		Measured: fmt.Sprintf("%d", relocating),
		Limit:    fmt.Sprintf("<= %d", cb.cfg.MaxRelocatingShards),
	}
}

// healthSatisfies returns true when actual health meets or exceeds required.
func healthSatisfies(actual, required string) bool {
	rank := map[string]int{"red": 0, "yellow": 1, "green": 2}
	a, aok := rank[actual]
	r, rok := rank[required]
	if !aok || !rok {
		return false
	}
	return a >= r
}
