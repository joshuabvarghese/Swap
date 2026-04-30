# Sway — OpenSearch Cluster Rebalancing Engine

Sway is a cloud-agnostic, safety-first shard rebalancer for OpenSearch. It monitors cluster hot-spots and incrementally redistributes shards to reduce load imbalance — without disrupting live traffic.

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────┐
│  Monitoring  │────▶│  Circuit Breaker │────▶│  Target State    │────▶│  Engine  │
│  Agent       │     │  (Safety Gate)   │     │  Generator       │     │  Execute │
└──────────────┘     └──────────────────┘     └──────────────────┘     └──────────┘
       ▲                                                                      │
       │                    opensearch.Client interface                       │
       └──────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# 1. Clone and install dependencies
git clone https://github.com/joshuabvarghese/sway
cd sway
go mod tidy

# 2. Run the simulation (no cluster needed — fastest way to see it work)
make simulate

# 3. Dry-run against a real cluster
make dry-run

# 4. Run all tests
make test
```

---

## Modes

| Flag | What it does |
|------|-------------|
| `--simulate` | Runs against a virtual 10-node cluster. No config or real cluster needed. |
| `--dry-run` | Plans all moves but makes zero API calls. Safe for production inspection. |
| `--once` | Runs a single cycle then exits. Combine with `--dry-run` for CI checks. |
| `--cycles N` | Number of cycles to run in simulation mode (default: 3). |
| `--config path` | Path to JSON config file (default: `config.json`). |

```bash
# Simulation: 5 cycles
./bin/rebalancer --simulate --cycles 5

# Real cluster, dry-run, one pass
./bin/rebalancer --config config.json --dry-run --once

# Real cluster, live mode, continuous
./bin/rebalancer --config config.json
```

---

## Configuration

`config.json` is overlaid on top of safe defaults. Any field omitted keeps its default.

```json
{
  "opensearch": {
    "addresses": ["http://localhost:9200"],
    "username": "",
    "password": "",
    "tls_verify": false,
    "timeout_seconds": 30,
    "max_retries": 3
  },
  "agent": {
    "poll_interval_seconds": 30,
    "hot_node_threshold": 0.70,
    "jvm_weight": 0.40,
    "disk_weight": 0.40,
    "shard_weight": 0.20
  },
  "rebalancer": {
    "dry_run": true,
    "max_moves_per_cycle": 5,
    "skew_reduction_target": 0.25,
    "large_shard_threshold_bytes": 2147483648
  },
  "circuit_breaker": {
    "required_health": "green",
    "max_avg_latency_ms": 40.0,
    "max_relocating_shards": 0
  }
}
```

### Key fields

**`agent.hot_node_threshold`** — Weighted score (0.0–1.0) above which a node is classified HOT. Formula:
```
score = jvm_weight × (heap%) + disk_weight × (disk%) + shard_weight × (shards/max_shards)
```
Weights must sum to 1.0 — validation is enforced on startup.

**`circuit_breaker.max_avg_latency_ms`** — Average search latency threshold in ms. This is the mean query time from cumulative `/_nodes/stats` counters, **not p99**. In a healthy cluster, average latency is typically 5–30ms; the default of 40ms catches degraded clusters without false positives. For true p99 gating, integrate Prometheus or OpenTelemetry.

**`rebalancer.dry_run`** — Defaults to `true`. Set to `false` explicitly for live execution.

---

## Cloud Deployment

Sway speaks only to the OpenSearch REST API — there are no cloud SDK dependencies.

```json
// AWS OpenSearch Service
{
  "opensearch": {
    "addresses": ["https://search-my-domain.us-east-1.es.amazonaws.com"],
    "username": "admin",
    "password": "..."
  }
}

// Multi-node on-premise with failover
{
  "opensearch": {
    "addresses": ["http://10.0.0.1:9200", "http://10.0.0.2:9200", "http://10.0.0.3:9200"],
    "max_retries": 3
  }
}
```

For cloud-specific auth (AWS SigV4, GCP OIDC, mTLS), supply a custom `http.RoundTripper` via `opensearch.WithTransport(rt)` — no engine or interface changes needed.

---

## How It Works

### 1. Monitoring Agent
Collects `/_nodes/stats`, `/_cluster/state`, `/_cluster/health`, and `/_cat/shards` each cycle. Computes a weighted hot-score per node and cluster-wide skew (population std-dev of scores).

### 2. Circuit Breaker
Gates every cycle. The circuit is **OPEN** (blocked) if any check fails:

| Check | What it measures | Default limit |
|-------|-----------------|---------------|
| Cluster Health | Must meet `required_health` | `green` |
| Avg Search Latency | Average query time from node stats | `≤ 40ms` |
| Relocating Shards | Active shard moves in progress | `= 0` |

Every check logs its measured value and threshold so operators can audit exactly why a cycle was blocked.

### 3. Target State Generator
Plans the minimum moves to reduce skew by `skew_reduction_target` (default 25%):

1. Captures `maxShards` once (before any moves) to keep skew scores on a consistent scale throughout the planning loop.
2. Identifies hot nodes (score ≥ threshold).
3. Sorts candidate shards by size descending — large shards are moved first for maximum pressure relief per API call.
4. For each candidate, finds the coolest eligible target: must not already host any copy of the same shard, and must stay under 90% disk after the move.
5. Applies the move to a projected copy of state and recalculates skew.
6. Stops when projected skew reduction ≥ target or `max_moves_per_cycle` is reached.

**Projection accuracy note:** JVM heap is approximated with a ±3% multiplier per shard moved. This is directionally correct but intentionally conservative — actual heap pressure depends on Lucene segment count and GC behaviour, not raw byte size.

### 4. Execution
Each planned move becomes one `POST /_cluster/reroute` call. In `dry_run` mode the call is skipped entirely; the full planned payload is still logged.

---

## Testing

```bash
# All unit tests
make test

# Verbose output
make test-verbose

# Benchmarks
make bench

# Lint (go vet)
make lint
```

Tests cover:
- `config`: weight validation, threshold ranges, `LoadFromFile` (valid, missing, invalid JSON)
- `circuitbreaker`: all check combinations, `healthSatisfies`, zero-latency edge case, thread-safe `Last()`
- `rebalancer/target`: no-data-nodes error, balanced cluster (no moves), skewed cluster (moves generated), max-moves cap, no primary+replica co-location, projected skew always decreases, disk capacity enforcement
- `agent`: `isDataNode`, `computeAvgLatency`, `computeSkew` (equal, single, non-data excluded, imbalanced), `buildShardList`, `CollectSnapshot` via stub client, hot-node detection
- `simulation`: 10-node cluster, all-data-node roles, green health, shard size key format (`index/N/p|r`), reroute updates state, invalid reroute is a no-op, disk consistency

---

## Docker

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /sway ./cmd/rebalancer

FROM alpine:latest
COPY --from=builder /sway /usr/local/bin/sway
ENTRYPOINT ["/usr/local/bin/sway"]
```

```bash
# Build
docker build -t sway:latest .

# Simulate
docker run --rm sway:latest --simulate --cycles 3

# Real cluster
docker run -v $(pwd)/config.json:/config.json sway:latest --config /config.json --dry-run
```

---

## What Was Fixed

This version addresses all issues identified in the initial code review:

| Issue | Fix |
|-------|-----|
| `maxShards` was mutable mid-planning loop, making skew reduction % inconsistent | Captured once before the loop in `Generate()`; all scores use a fixed denominator |
| `diskUsedPercent` projection didn't apply OS/translog overhead | Added `diskOverheadFactor = 1.10` to `applyMove` — consistent with simulation |
| `applyMove` JVM approximation was undocumented | Added explicit comment: ±3% is conservative; actual relief depends on GC/segments |
| HTTP client only used first address; no retry across addresses | `HTTPClient` now accepts `[]string` addresses and retries across all on failure |
| Circuit breaker latency default was 200ms (too high to catch degraded clusters) | Reduced to 40ms with a clear comment explaining avg-vs-p99 tradeoff |
| Zero latency (no queries run yet) was incorrectly blocking the circuit | Added `lat == 0` short-circuit in `checkLatency` |
| `metrics.go` used hand-rolled `itoa` | Replaced with `strconv.Itoa` from the standard library |
| Config weight validation was absent | Added `Validate()` which returns an error when weights don't sum to 1.0 |
| No test files | Added tests for all five packages: config, circuitbreaker, rebalancer, agent, simulation |
| Banner printed in both `main.go` and `engine.go` | Banner lives only in `main.go`; engine no longer prints it |
| `main.go` only passed `Addresses[0]` to `NewHTTPClient` | Now passes the full `[]string` slice |
