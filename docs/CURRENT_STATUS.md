# PulseOps — Current Status

**Active Phase:** Day 2: Performance Baseline & Bottleneck Profiling — **COMPLETE**  
**Last Updated:** 2026-10-12  
**Current Active Checkpoint:** Day 2 Completed (Checkpoints 1 - 3 Verified)

---

## Day 2 Benchmark Matrix Summary

All 5 progressive scenarios were executed against the direct-persistence FastAPI + asyncpg + PostgreSQL 16 engine using `backend/benchmarks/load_test.py`. Total spans tested and verified: **177,100 spans** with **zero data loss** (0 database discrepancies).

| Scenario | Batch Size | Concurrency | Total Requests | Total Spans | Wall Time (s) | Request Rate (req/s) | Telemetry Throughput (spans/s) | p50 Latency (ms) | p90 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | Max Latency (ms) | Errors | DB Discrepancy |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **A: Smoke Test** | 1 | 1 | 100 | 100 | 0.830 s | 120.53 | 120.53 | 7.99 | 10.01 | 10.70 | 11.61 | 11.67 | 0 | 0 |
| **B: Small Batches** | 10 | 1 | 200 | 2,000 | 1.688 s | 118.45 | 1,184.50 | 8.03 | 9.73 | 10.72 | 12.74 | 24.12 | 0 | 0 |
| **C: Medium Batches** | 50 | 5 | 500 | 25,000 | 2.621 s | 190.74 | 9,537.19 | 23.63 | 28.92 | 34.40 | 75.07 | 129.13 | 0 | 0 |
| **D: Large Batches** | 100 | 10 | 500 | 50,000 | 3.818 s | 130.98 | **13,097.58** | 68.66 | 85.45 | 91.81 | **257.22** | 297.20 | 0 | 0 |
| **E: Stress Test** | 100 | 25 | 1,000 | 100,000 | 11.522 s | 86.79 | **8,679.33** | 227.48 | 515.34 | **611.47** | **991.33** | 1,616.30 | 0 | 0 |

---

## Key Performance Findings

1. **Empirical Throughput Ceiling:**
   - Peak sustained ingestion throughput is **13,097.58 spans/second** (Scenario D: 100 spans/req, Concurrency 10).
2. **Batching Efficiency:**
   - Packing 10 spans per request amortizes round-trip network and WAL commit overhead, yielding an immediate **9.8x throughput increase** (from 120 to 1,184 spans/s) with flat p50 (~8.0 ms) and p95 (~10.7 ms) latency.
3. **Pool Boundary Breakdown (Scenario E):**
   - With client concurrency (25) exceeding `DB_POOL_MAX_SIZE = 20`, connection checkout queuing and synchronous WAL commit contention degraded throughput by **33.7%** (down to 8,679 spans/s).
   - Latency collapsed under contention: p50 rose to **227.48 ms**, p95 reached **611.47 ms**, and p99 reached **991.33 ms** (max: 1.62 s).
4. **Architectural Recommendation:**
   - Recorded in [docs/DECISION_LOG.md](file:///c:/Users/Admin/OneDrive/Desktop/PulseOPS/docs/DECISION_LOG.md) as proposed **ADR-002**. An asynchronous ingestion buffer (e.g., Redis Streams) is justified to decouple caller response times from database disk flushes under high concurrency.

---

## Day 1 Foundation Tracking (Reference)

- [x] Checkpoint 1: Project Structure & Memory Initialization
- [x] Checkpoint 2: Schemas & Database Lifecycle
- [x] Checkpoint 3: Endpoints Implementation
- [x] Checkpoint 4: Day 1 Verification Suite (6/6 Scenarios Passed)
