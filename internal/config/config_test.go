package config

import (
	"math"
	"os"
	"testing"
)

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default() config failed validation: %v", err)
	}
}

func TestValidateWeights(t *testing.T) {
	tests := []struct {
		name    string
		jvm     float64
		disk    float64
		shard   float64
		wantErr bool
	}{
		{"valid 0.4+0.4+0.2", 0.40, 0.40, 0.20, false},
		{"valid 0.33+0.33+0.34", 0.33, 0.33, 0.34, false},
		{"too high", 0.50, 0.40, 0.20, true},
		{"too low", 0.10, 0.10, 0.10, true},
		{"zero weights", 0, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Agent.JVMWeight = tt.jvm
			cfg.Agent.DiskWeight = tt.disk
			cfg.Agent.ShardWeight = tt.shard
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateHotNodeThreshold(t *testing.T) {
	cfg := Default()
	cfg.Agent.HotNodeThreshold = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for threshold=0")
	}
	cfg.Agent.HotNodeThreshold = 1.1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for threshold=1.1")
	}
	cfg.Agent.HotNodeThreshold = 1.0
	if err := cfg.Validate(); err != nil {
		t.Errorf("unexpected error for threshold=1.0: %v", err)
	}
}

func TestValidateRequiredHealth(t *testing.T) {
	cfg := Default()
	cfg.CircuitBreaker.RequiredHealth = "purple"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid health value")
	}
	for _, h := range []string{"green", "yellow", "red"} {
		cfg.CircuitBreaker.RequiredHealth = h
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error for health=%q: %v", h, err)
		}
	}
}

func TestValidateMaxMovesPerCycle(t *testing.T) {
	cfg := Default()
	cfg.Rebalancer.MaxMovesPerCycle = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for MaxMovesPerCycle=0")
	}
}

func TestValidateLatencyThreshold(t *testing.T) {
	cfg := Default()
	cfg.CircuitBreaker.MaxAvgLatencyMs = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for MaxAvgLatencyMs=0")
	}
}

func TestLoadFromFile(t *testing.T) {
	content := `{
		"opensearch": {"addresses": ["http://localhost:9200"]},
		"agent": {
			"poll_interval_seconds": 60,
			"hot_node_threshold": 0.75,
			"jvm_weight": 0.5,
			"disk_weight": 0.3,
			"shard_weight": 0.2
		},
		"rebalancer": {"dry_run": true, "max_moves_per_cycle": 3, "skew_reduction_target": 0.3},
		"circuit_breaker": {"required_health": "yellow", "max_avg_latency_ms": 100}
	}`
	f, err := os.CreateTemp("", "sway-config-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(content)
	f.Close()

	cfg, err := LoadFromFile(f.Name())
	if err != nil {
		t.Fatalf("LoadFromFile() error: %v", err)
	}
	if cfg.Agent.PollIntervalSeconds != 60 {
		t.Errorf("PollIntervalSeconds = %d, want 60", cfg.Agent.PollIntervalSeconds)
	}
	if math.Abs(cfg.Agent.HotNodeThreshold-0.75) > 0.001 {
		t.Errorf("HotNodeThreshold = %.2f, want 0.75", cfg.Agent.HotNodeThreshold)
	}
	if cfg.CircuitBreaker.RequiredHealth != "yellow" {
		t.Errorf("RequiredHealth = %q, want yellow", cfg.CircuitBreaker.RequiredHealth)
	}
}

func TestLoadFromFileMissing(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadFromFileInvalidJSON(t *testing.T) {
	f, err := os.CreateTemp("", "sway-bad-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("{not valid json}")
	f.Close()

	_, err = LoadFromFile(f.Name())
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}
