// Command sway is the Project Sway OpenSearch Cluster Rebalancing Engine.
//
// Usage:
//
//	sway [flags]
//
// Flags:
//
//	--config string     Path to JSON config file (default: config.json)
//	--simulate          Run against a virtual 10-node cluster (no real cluster needed)
//	--dry-run           Plan moves but do not execute them (overrides config)
//	--once              Run a single rebalancing cycle then exit
//	--cycles int        Number of simulation cycles to run (default: 3)
//
// Examples:
//
//	# Demo with virtual cluster, 3 rebalancing cycles
//	sway --simulate --cycles 3
//
//	# Production dry-run against a real cluster
//	sway --config /etc/sway/prod.json --dry-run --once
//
//	# Live rebalancing (continuous)
//	sway --config /etc/sway/prod.json
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/circuitbreaker"
	"github.com/project-sway/sway/internal/config"
	"github.com/project-sway/sway/internal/opensearch"
	"github.com/project-sway/sway/internal/rebalancer"
	"github.com/project-sway/sway/internal/simulation"
)

func main() {
	// ── CLI flags ────────────────────────────────────────────────────────────
	configPath := flag.String("config", "config.json", "Path to JSON configuration file")
	simulate   := flag.Bool("simulate", false, "Run against a virtual 10-node cluster")
	dryRun     := flag.Bool("dry-run", false, "Plan moves but do not execute (overrides config)")
	once       := flag.Bool("once", false, "Run a single cycle then exit")
	cycles     := flag.Int("cycles", 3, "Number of simulation cycles (--simulate only)")
	flag.Parse()

	// ── Logger ───────────────────────────────────────────────────────────────
	logger := log.New(os.Stderr, "", log.LstdFlags)

	// ── Configuration ────────────────────────────────────────────────────────
	var cfg *config.Config
	if *simulate {
		cfg = config.Default()
		cfg.Rebalancer.DryRun = false // simulation executes moves so we see state converge
		cfg.Agent.PollIntervalSeconds = 2
		// Tuned for the simulated cluster: top 3 nodes score ~0.52-0.62.
		// Threshold at 0.50 classifies them as HOT and triggers moves.
		cfg.Agent.HotNodeThreshold = 0.50
		cfg.Rebalancer.MaxMovesPerCycle = 4
		cfg.Rebalancer.SkewReductionTarget = 0.25
		cfg.Rebalancer.LargeShardThresholdBytes = 5 * 1024 * 1024 * 1024 // 5 GiB
		// Simulation latency is synthetic; set a generous threshold.
		cfg.CircuitBreaker.MaxAvgLatencyMs = 200.0
	} else {
		var err error
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Printf("Config file %q not found, using defaults.", *configPath)
				cfg = config.Default()
			} else {
				logger.Fatalf("Loading config: %v", err)
			}
		}
	}

	// CLI --dry-run overrides config.
	if *dryRun {
		cfg.Rebalancer.DryRun = true
	}

	// ── OpenSearch client ────────────────────────────────────────────────────
	// Cloud-agnosticism lives here: the same engine runs regardless of which
	// Client implementation is plugged in.
	var client opensearch.Client
	if *simulate {
		sim := simulation.New(logger)
		sim.PrintClusterSummary()
		client = sim
	} else {
		if len(cfg.OpenSearch.Addresses) == 0 {
			logger.Fatal("No OpenSearch addresses configured.")
		}
		client = opensearch.NewHTTPClient(
			cfg.OpenSearch.Addresses,
			cfg.OpenSearch.Username,
			cfg.OpenSearch.Password,
			cfg.OpenSearch.TLSVerify,
			time.Duration(cfg.OpenSearch.TimeoutSeconds)*time.Second,
			cfg.OpenSearch.MaxRetries,
		)
	}

	// ── Component wiring ─────────────────────────────────────────────────────
	mon       := agent.New(client, cfg.Agent, logger)
	breaker   := circuitbreaker.New(cfg.CircuitBreaker, logger)
	generator := rebalancer.NewTargetStateGenerator(cfg.Agent, cfg.Rebalancer)
	engine    := rebalancer.NewEngine(client, mon, breaker, generator, *cfg, logger)

	// ── Context (graceful shutdown on SIGINT/SIGTERM) ─────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ── Banner ───────────────────────────────────────────────────────────────
	fmt.Printf("\n=================================================================\n")
	fmt.Printf("  Project Sway — OpenSearch Cluster Rebalancing Engine\n")
	fmt.Printf("  Cloud-Agnostic  |  Safety-First  |  Incremental by Design\n")
	if *simulate {
		fmt.Printf("  Mode: SIMULATION (%d cycles)\n", *cycles)
	} else if cfg.Rebalancer.DryRun {
		fmt.Printf("  Mode: DRY-RUN (no live changes)\n")
	} else {
		fmt.Printf("  Mode: LIVE\n")
	}
	fmt.Printf("=================================================================\n")

	// ── Run ──────────────────────────────────────────────────────────────────
	if *simulate {
		runSimulation(ctx, engine, *cycles, cfg.Agent.PollInterval())
		return
	}

	if *once {
		if err := engine.RunOnce(ctx); err != nil {
			logger.Fatalf("Cycle failed: %v", err)
		}
		return
	}

	// Continuous mode.
	if err := engine.Run(ctx); err != nil {
		logger.Fatalf("Engine error: %v", err)
	}
}

// runSimulation executes a fixed number of rebalancing cycles, printing a
// summary after the final cycle to show before/after improvement.
func runSimulation(ctx context.Context, engine *rebalancer.Engine, cycles int, interval time.Duration) {
	for i := 1; i <= cycles; i++ {
		select {
		case <-ctx.Done():
			fmt.Println("\n[SIM] Interrupted.")
			return
		default:
		}

		fmt.Printf("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
		fmt.Printf("  CYCLE %d of %d\n", i, cycles)
		fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

		if err := engine.RunOnce(ctx); err != nil {
			fmt.Printf("[SIM] Cycle %d error: %v\n", i, err)
		}

		if i < cycles {
			fmt.Printf("\n[SIM] Waiting %s before next cycle...\n", interval)
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}

	fmt.Printf("\n\n=================================================================\n")
	fmt.Printf("  SIMULATION COMPLETE — %d cycles executed\n", cycles)
	fmt.Printf("  The cluster has converged toward a balanced state.\n")
	fmt.Printf("  In production: remove --simulate and point at a real cluster.\n")
	fmt.Printf("=================================================================\n\n")
}
