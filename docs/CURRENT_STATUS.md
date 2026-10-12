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

### 3. Measured Bottleneck Analysis: Facts vs. Working Hypotheses

#### A. Empirical Facts (Verified by Measurement)
- Direct PostgreSQL persistence achieves a sweet spot around **13,098 spans/sec** at concurrency 10 (Scenario D) with $p_{50} = 68.66\text{ ms}$ and $p_{95} = 91.81\text{ ms}$.
- Under concurrency 25 (Scenario E), throughput dropped by 33.7% to **8,679 spans/sec**, while $p_{95}$ degraded to **611.47 ms** and $p_{99}$ touched **991.33 ms** (Max: 1,616 ms).
- **Core Platform Takeaway:** Synchronous persistence couples the monitored application's request duration directly to database write latency, causing client tail latency to exceed 600 ms–1.6 s during high concurrency.

#### B. Working Hypotheses (Unisolated Contributors)
The exact internal source of the Scenario E degradation remains unisolated among three concurrent factors:
1. *Connection pool checkout queuing:* 25 workers competed for `DB_POOL_MAX_SIZE = 20`.
2. *PostgreSQL WAL disk contention:* 20 active parallel transactions committing under `synchronous_commit = on`.
3. *Python event loop CPU overhead:* Synchronous Pydantic v2 validation and list parsing on a single core during parallel requests.

---

### 4. Proposed Direction & Hypotheses to Validate
We hypothesize that decoupling ingestion via an in-memory or append-only stream (such as **Redis Streams**) will insulate telemetry callers from storage write latency:

- **Target Metric to Validate:** Can an asynchronous ingestion gateway sustain $p_{95} < 20\text{ ms}$ under Scenario E concurrency (25 workers) by returning `HTTP 202 Accepted` after appending to a stream buffer?
- **Durability Trade-off & Contract Definition:** 
  `HTTP 202 Accepted` signals that telemetry has been safely accepted into the temporary buffer, *not* that it has been flushed to PostgreSQL disk.
  - *Failure mode to address:* If the stream buffer restarts or a worker crashes before bulk insertion, in-flight spans in memory could be lost unless backed by Redis append-only files (AOF) and consumer group acknowledgments (`XACK` after DB commit).
- **Next Step:** Build a prototype of the decoupled stream + consumer worker to empirically measure whether this architecture resolves the Scenario E latency regression without unacceptable durability trade-offs.

---

## Current Status & Benchmarks

- [x] **Day 1: Grounded Foundation** — Direct PostgreSQL persistence, OTel W3C schema validation, idempotent upserts, and trace DAG queries.
- [x] **Day 2: Performance Baseline** — 5-scenario benchmark matrix completed across 177,100 spans with zero data loss.
  - **Peak Throughput:** ~13,098 spans/sec at $p_{95} = 91.81\text{ ms}$ (concurrency 10, batch 100).
  - **Observed Boundary:** Concurrency 25 caused a 33.7% throughput drop and elongated $p_{95}$ to $611.47\text{ ms}$.
  - Full findings and artifacts recorded in [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md) (ADR-002) and [`backend/benchmarks/results/`](backend/benchmarks/results/).
- [ ] **Day 3: Decoupled Ingestion Prototype** — Evaluating asynchronous stream buffering to test caller insulation under burst load.
