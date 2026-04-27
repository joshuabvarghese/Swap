package circuitbreaker

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/config"
)

func defaultCfg() config.CircuitBreakerConfig {
	return config.CircuitBreakerConfig{
		RequiredHealth:      "green",
		MaxAvgLatencyMs:     40.0,
		MaxRelocatingShards: 0,
	}
}

func logger() *log.Logger {
	return log.New(os.Stderr, "", 0)
}

func makeSnap(health string, latMs float64, relocating int) *agent.ClusterSnapshot {
	return &agent.ClusterSnapshot{
		Timestamp:        time.Now(),
		ClusterName:      "test",
		Health:           health,
		AvgSearchLatMs:   latMs,
		RelocatingShards: relocating,
		NodeMetrics:      map[string]*agent.NodeMetrics{},
	}
}

func TestHealthSatisfies(t *testing.T) {
	tests := []struct {
		actual, required string
		want             bool
	}{
		{"green", "green", true},
		{"green", "yellow", true},
		{"green", "red", true},
		{"yellow", "green", false},
		{"yellow", "yellow", true},
		{"yellow", "red", true},
		{"red", "green", false},
		{"red", "yellow", false},
		{"red", "red", true},
		{"unknown", "green", false},
		{"green", "unknown", false},
	}
	for _, tt := range tests {
		got := healthSatisfies(tt.actual, tt.required)
		if got != tt.want {
			t.Errorf("healthSatisfies(%q, %q) = %v, want %v", tt.actual, tt.required, got, tt.want)
		}
	}
}

func TestCircuitClosedOnGreenCluster(t *testing.T) {
	cb := New(defaultCfg(), logger())
	snap := makeSnap("green", 10.0, 0)
	result := cb.Evaluate(snap)
	if result.State != StateClosed {
		t.Errorf("expected CLOSED, got %s — reason: %s", result.State, result.Reason)
	}
}

func TestCircuitOpenOnRedHealth(t *testing.T) {
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("red", 0, 0))
	if result.State != StateOpen {
		t.Errorf("expected OPEN for red health, got %s", result.State)
	}
}

func TestCircuitOpenOnHighLatency(t *testing.T) {
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("green", 100.0, 0))
	if result.State != StateOpen {
		t.Errorf("expected OPEN for high latency, got %s", result.State)
	}
}

func TestCircuitClosedOnZeroLatency(t *testing.T) {
	// Zero latency means no queries run yet — should not block.
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("green", 0, 0))
	if result.State != StateClosed {
		t.Errorf("expected CLOSED for zero latency, got %s", result.State)
	}
}

func TestCircuitOpenOnRelocatingShards(t *testing.T) {
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("green", 5.0, 1))
	if result.State != StateOpen {
		t.Errorf("expected OPEN when shards are relocating, got %s", result.State)
	}
}

func TestCircuitOpenOnMultipleFailures(t *testing.T) {
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("red", 500.0, 3))
	if result.State != StateOpen {
		t.Error("expected OPEN for multiple failures")
	}
	// All three checks should have failed.
	for _, c := range result.Checks {
		if c.Passed {
			t.Errorf("check %q should have failed", c.Name)
		}
	}
}

func TestCircuitYellowHealthWhenRequired(t *testing.T) {
	cfg := defaultCfg()
	cfg.RequiredHealth = "yellow"
	cb := New(cfg, logger())
	result := cb.Evaluate(makeSnap("yellow", 5.0, 0))
	if result.State != StateClosed {
		t.Errorf("expected CLOSED with yellow health and yellow requirement, got %s", result.State)
	}
}

func TestLastResultStored(t *testing.T) {
	cb := New(defaultCfg(), logger())
	if cb.Last() != nil {
		t.Error("expected nil Last() before first Evaluate")
	}
	cb.Evaluate(makeSnap("green", 5.0, 0))
	if cb.Last() == nil {
		t.Error("expected non-nil Last() after Evaluate")
	}
}

func TestAllChecksPresent(t *testing.T) {
	cb := New(defaultCfg(), logger())
	result := cb.Evaluate(makeSnap("green", 5.0, 0))
	if len(result.Checks) != 3 {
		t.Errorf("expected 3 checks, got %d", len(result.Checks))
	}
}

func TestMaxRelocatingAllowsSome(t *testing.T) {
	cfg := defaultCfg()
	cfg.MaxRelocatingShards = 2
	cb := New(cfg, logger())
	if r := cb.Evaluate(makeSnap("green", 5.0, 2)); r.State != StateClosed {
		t.Errorf("expected CLOSED for relocating=2 with limit=2, got %s", r.State)
	}
	if r := cb.Evaluate(makeSnap("green", 5.0, 3)); r.State != StateOpen {
		t.Errorf("expected OPEN for relocating=3 with limit=2, got %s", r.State)
	}
}
