package rebalancer

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/project-sway/sway/internal/agent"
	"github.com/project-sway/sway/internal/circuitbreaker"
	"github.com/project-sway/sway/internal/config"
	"github.com/project-sway/sway/internal/opensearch"
)

// Engine orchestrates the full rebalancing lifecycle:
//
//	CollectSnapshot → CircuitBreaker.Evaluate → Generate → Execute
//
// Every cycle is gated by the circuit breaker. No moves are ever attempted
// when the circuit is OPEN.
type Engine struct {
	client    opensearch.Client
	mon       *agent.MonitoringAgent
	breaker   *circuitbreaker.CircuitBreaker
	generator *TargetStateGenerator
	cfg       config.Config
	logger    *log.Logger
}

// NewEngine wires up all components.
func NewEngine(
	client opensearch.Client,
	mon *agent.MonitoringAgent,
	breaker *circuitbreaker.CircuitBreaker,
	generator *TargetStateGenerator,
	cfg config.Config,
	logger *log.Logger,
) *Engine {
	return &Engine{
		client:    client,
		mon:       mon,
		breaker:   breaker,
		generator: generator,
		cfg:       cfg,
		logger:    logger,
	}
}

// RunOnce executes a single rebalancing cycle and returns.
func (e *Engine) RunOnce(ctx context.Context) error {
	return e.runCycle(ctx)
}

// Run executes rebalancing cycles continuously until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	for {
		if err := e.runCycle(ctx); err != nil {
			e.logger.Printf("[ENGINE] Cycle error: %v", err)
		}
		select {
		case <-ctx.Done():
			fmt.Println("\n[ENGINE] Shutdown signal received. Exiting gracefully.")
			return nil
		case <-time.After(e.cfg.Agent.PollInterval()):
		}
	}
}

// ──────────────────────────────────────────────────────────────
//  Single cycle
// ──────────────────────────────────────────────────────────────

func (e *Engine) runCycle(ctx context.Context) error {
	// ── Phase 1: Collect metrics ─────────────────────────────────────────────
	fmt.Println("\n[MONITORING AGENT] Collecting cluster metrics...")
	snap, err := e.mon.CollectSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("collecting snapshot: %w", err)
	}
	e.printSnapshot(snap)

	// ── Phase 2: Circuit breaker ──────────────────────────────────────────────
	fmt.Println("\n[CIRCUIT BREAKER] Running pre-flight safety checks...")
	result := e.breaker.Evaluate(snap)
	e.printBreakerResult(result)

	if result.State == circuitbreaker.StateOpen {
		fmt.Printf("\n[ENGINE] Circuit is OPEN (%s). Cycle aborted — no changes made.\n", result.Reason)
		return nil
	}

	// ── Phase 3: Generate target state ───────────────────────────────────────
	fmt.Println("\n[REBALANCER] Generating optimal target state...")
	target, err := e.generator.Generate(snap)
	if err != nil {
		return fmt.Errorf("generating target state: %w", err)
	}
	e.printTargetState(target)

	if len(target.Moves) == 0 {
		fmt.Println("[REBALANCER] Cluster is already within acceptable skew bounds. No moves required.")
		return nil
	}

	// ── Phase 4: Execute moves ────────────────────────────────────────────────
	mode := "LIVE"
	if e.cfg.Rebalancer.DryRun {
		mode = "DRY-RUN"
	}
	fmt.Printf("\n[EXECUTOR] Executing %d shard move(s) [%s mode]...\n", len(target.Moves), mode)
	return e.executeMoves(ctx, target.Moves)
}

// ──────────────────────────────────────────────────────────────
//  Execution
// ──────────────────────────────────────────────────────────────

// executeMoves converts planned ShardMoves into _cluster/reroute calls.
// Each move is issued as a separate API call to allow per-move error handling.
func (e *Engine) executeMoves(ctx context.Context, moves []ShardMove) error {
	for i, m := range moves {
		role := "REPLICA"
		if m.Shard.Primary {
			role = "PRIMARY"
		}
		label := fmt.Sprintf("%s[%d]/%s", m.Shard.Index, m.Shard.ShardNum, role)

		req := &opensearch.RerouteRequest{
			Commands: []opensearch.RerouteCommand{
				{
					Move: &opensearch.MoveShard{
						Index:    m.Shard.Index,
						Shard:    m.Shard.ShardNum,
						FromNode: m.FromName,
						ToNode:   m.ToName,
					},
				},
			},
		}

		prefix := "  [EXECUTE]"
		if e.cfg.Rebalancer.DryRun {
			prefix = "  [DRY-RUN]"
		}

		resp, err := e.client.Reroute(ctx, req, e.cfg.Rebalancer.DryRun)
		if err != nil {
			fmt.Printf("%s Move %d FAILED — %s: %v  (%s → %s)\n",
				prefix, i+1, label, err, m.FromName, m.ToName)
			continue
		}
		ack := "✓"
		if !resp.Acknowledged {
			ack = "✗ (not acknowledged)"
		}
		fmt.Printf("%s Move %d %s — %s: %s → %s\n",
			prefix, i+1, ack, label, m.FromName, m.ToName)
	}
	return nil
}

// ──────────────────────────────────────────────────────────────
//  Display helpers
// ──────────────────────────────────────────────────────────────

func (e *Engine) printSnapshot(snap *agent.ClusterSnapshot) {
	fmt.Printf("\nCLUSTER: %-20s  HEALTH: %-6s  NODES: %d  SHARDS: %d  RELOCATING: %d\n",
		snap.ClusterName,
		strings.ToUpper(snap.Health),
		len(snap.NodeMetrics),
		snap.TotalShards,
		snap.RelocatingShards,
	)
	fmt.Printf("Timestamp: %s  |  Avg Search Latency: %.1fms  |  Skew Score: %.4f\n",
		snap.Timestamp.Format("2006-01-02 15:04:05 UTC"),
		snap.AvgSearchLatMs,
		snap.SkewScore,
	)

	fmt.Println()
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("  %-18s  %-10s  %-10s  %-8s  %-8s  %s\n",
		"NODE NAME", "JVM HEAP", "DISK USE", "SHARDS", "SCORE", "STATUS")
	fmt.Println(strings.Repeat("-", 78))

	// Sort nodes by hot score descending for readability.
	nodes := snap.DataNodes()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].HotScore > nodes[j].HotScore
	})
	for _, n := range nodes {
		status := "[OK]    "
		if n.IsHot {
			status = "[HOT] *"
		}
		fmt.Printf("  %-18s  %6.1f%%    %6.1f%%    %-8d  %-8.4f  %s\n",
			n.NodeName,
			n.JVMHeapPercent,
			n.DiskUsedPercent,
			n.ShardCount,
			n.HotScore,
			status,
		)
	}
	fmt.Println(strings.Repeat("-", 78))

	hotCount := 0
	for _, n := range nodes {
		if n.IsHot {
			hotCount++
		}
	}
	fmt.Printf("  Hot nodes: %d / %d  (threshold: %.2f)\n", hotCount, len(nodes), e.cfg.Agent.HotNodeThreshold)
}

func (e *Engine) printBreakerResult(r *circuitbreaker.Result) {
	for _, c := range r.Checks {
		mark := "  [PASS]"
		if !c.Passed {
			mark = "  [FAIL]"
		}
		fmt.Printf("%s %-22s  measured: %-12s  limit: %s\n",
			mark, c.Name, c.Measured, c.Limit)
	}

	if r.State == circuitbreaker.StateClosed {
		fmt.Printf("  >> Circuit: CLOSED — Automation ENABLED\n")
	} else {
		fmt.Printf("  >> Circuit: OPEN   — Automation BLOCKED  (%s)\n", r.Reason)
	}
}

func (e *Engine) printTargetState(t *TargetState) {
	fmt.Printf("  Current Skew  : %.4f\n", t.CurrentSkew)
	fmt.Printf("  Target Skew   : %.4f  (%.0f%% reduction target)\n",
		t.CurrentSkew*(1.0-e.cfg.Rebalancer.SkewReductionTarget),
		e.cfg.Rebalancer.SkewReductionTarget*100)
	fmt.Printf("  Projected Skew: %.4f  (%.1f%% reduction)\n",
		t.ProjectedSkew, t.SkewReduction*100)

	if len(t.Moves) == 0 {
		return
	}

	fmt.Println()
	fmt.Println("  PLANNED SHARD MOVEMENTS (sorted by size — largest first)")
	fmt.Println(strings.Repeat("-", 78))
	fmt.Printf("  %-4s  %-28s  %-9s  %-14s  %-14s\n",
		"#", "SHARD", "SIZE", "FROM", "TO")
	fmt.Println(strings.Repeat("-", 78))

	for i, m := range t.Moves {
		role := "replica"
		if m.Shard.Primary {
			role = "primary"
		}
		label := fmt.Sprintf("%s[%d]/%s", m.Shard.Index, m.Shard.ShardNum, role)
		size := humanBytes(m.Shard.SizeBytes)
		fmt.Printf("  %-4d  %-28s  %-9s  %-14s  %-14s\n",
			i+1, label, size, m.FromName, m.ToName)
	}
	fmt.Println(strings.Repeat("-", 78))
}

// humanBytes formats bytes as a human-readable string.
func humanBytes(b int64) string {
	if b == 0 {
		return "unknown"
	}
	const gib = 1 << 30
	const mib = 1 << 20
	if b >= gib {
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(gib))
	}
	if b >= mib {
		return fmt.Sprintf("%.0f MiB", float64(b)/float64(mib))
	}
	return fmt.Sprintf("%d B", b)
}
