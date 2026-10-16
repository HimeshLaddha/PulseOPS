# PulseOps — Current Status

**Active Phase:** Day 5: 1 -> 2 -> 4 Worker Scaling Experiment — **COMPLETE**<br>
**Last Updated:** 2026-10-16<br>
**Current Active Checkpoint:** Day 5 Controlled Scaling Experiment Verified (1 vs 2 vs 4 Workers)

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
- [x] **Day 3: Decoupled Ingestion Prototype** — Minimal asynchronous stream slice verified end-to-end:
  - Redis 7 Alpine with persistent volume and `--appendonly yes` added to `docker-compose.yml`.
  - Dual-mode ingestion in `POST /v1/telemetry/spans`: `?mode=stream` returns `HTTP 202 Accepted` after appending to `telemetry:stream`.
  - Standalone consumer worker `backend/app/worker.py` reads with `XREADGROUP`, executes batch `UNNEST` insertion in PostgreSQL transaction, and executes `XACK` only after commit.
  - End-to-end slice verified: stream append, schema validation guard (422 without stream leak), worker processing, XACK confirmation (0 pending), and PostgreSQL trace retrieval.
- [x] **Day 3 Checkpoint 4: Fault Injection & Correctness Verification** — 7/7 failure scenarios verified via `backend/tests/test_day3_fault_tolerance.py`:
  - **Scenario 1 (Worker Crash Simulation):** Worker reading via `XREADGROUP` crashing pre-commit leaves message in PEL (`XPENDING` >= 1); recovery worker claims via `XAUTOCLAIM`, inserts to PostgreSQL, and calls `XACK` (`XPENDING` -> 0).
  - **Scenario 2 (Idempotent Replay):** Duplicate replay handled gracefully with `ON CONFLICT DO NOTHING` without constraint violations or duplicate rows.
  - **Scenario 3 (Database Outage Resilience):** Ingestion API continues returning `HTTP 202 Accepted` while PostgreSQL is down; worker handles connection drops gracefully without unhandled crashes; queued messages drained and persisted once PostgreSQL recovers.
  - **Scenario 4 (Redis AOF Persistence):** In-flight messages in `telemetry:stream` survive container restarts (`docker compose restart redis`) with zero data loss under `--appendfsync everysec`.
  - **Scenario 5 (Poison Pill Handling):** Malformed non-JSON messages caught, logged, and routed to `telemetry:stream:deadletter` with `XACK`, preventing head-of-line blocking and allowing subsequent valid batches to process normally.
  - **Scenario 6 (DLQ Resilience):** Simulated failures during deadletter stream writes prevent calling `XACK`/`XDEL`, keeping the original poison pill safely in the PEL until the deadletter queue recovers.
  - **Scenario 7 (Concurrent Worker Idle Window):** Verified that active messages whose idle time is less than `REDIS_STREAM_MIN_IDLE_MS` (5,000 ms) are protected from premature reclamation by competing workers.
- [x] **Day 3 Checkpoint 5: Asynchronous Ingestion Benchmarks & Comparative Analysis** — 5-scenario benchmark matrix executed in `--mode stream`:
  - **177,100 / 177,100 spans persisted** with 0 discrepancy and 0 data loss across all 5 scenarios.
  - **Peak API Acceptance Throughput (Scenario D):** **14,032.42 spans/s** at $p_{50} = 63.16\text{ ms}$ and $p_{95} = 94.82\text{ ms}$ (+7.1% throughput improvement over Direct PG).
  - **Bottleneck Isolation (Scenario E):** While Redis Streams decoupled producers from storage outages and database lock contention, Scenario E latency ($p_{95} = 700.11\text{ ms}$, $p_{50} = 242.30\text{ ms}$) reflected single-process asyncio event loop task scheduling contention under high concurrency, while background persistence throughput in a single worker was constrained by the ~50–53 ms per-batch database roundtrip.
  - **ADR-002 Accepted:** Transitioned from `PROPOSED` to `ACCEPTED` in [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md).
- [x] **Day 3.5: Worker Reliability & Documentation Hardening:**
  - Configurable reclamation idle threshold (`REDIS_STREAM_MIN_IDLE_MS = 5000`).
  - Atomic dead-letter stream routing with PEL fail-safety.
  - Explicit Redis AOF configuration (`--appendonly yes --appendfsync everysec`).
  - System architecture and README documentation synchronized.
- [x] **Day 4 Checkpoint 1: Targeted Reliability Edge-Case Verification:**
  - Defensive `xautoclaim` unpacking separating phantom deleted IDs from malformed stream payloads.
  - Idempotent DLQ replay handling for un-ACKed poison pills following worker restarts.
  - Durability contract defined in ADR-003: zero loss across clean container restarts; explicit 1-second potential loss window under ungraceful power cuts.
  - All 9 integration fault tolerance scenarios verified in `backend/tests/test_day3_fault_tolerance.py`.
- [x] **Day 4 Checkpoint 2: Worker Pipeline Profiling & Drain Bottleneck Isolation:**
  - Isolated the single-worker 63-second Scenario E drain duration: worker fetch took 2.78 ms, parse took 0.28 ms, ack/del took 1.48 ms, but database persistence ($T_3$) took **52.28 ms per batch**. Over 1,000 batches, this created an irreducible serial bottleneck of ~52.3 seconds.
- [x] **Day 4 Checkpoint 3: Fine-Grained DB Profiling & Worker Concurrency Experiment:**
  - **Deconstructed Database Stage ($T_3$):**
    - $T_{3\text{a}}$ (Connection Pool Acquisition): **0.025 ms** ($p_{50} = 0.021\text{ ms}, p_{95} = 0.044\text{ ms}$) — zero pool checkout contention.
    - $T_{3\text{b}}$ (SQL Fetch/Execute Duration): **48.35 ms** ($p_{50} = 47.78\text{ ms}, p_{95} = 52.82\text{ ms}$) — client-observed query execution and row fetch duration via `conn.fetch` (does not isolate PostgreSQL internal execution). Accounts for ~92.4% of DB stage time.
    - $T_{3\text{c}}$ (Transaction Commit Duration): **2.40 ms** ($p_{50} = 2.18\text{ ms}, p_{95} = 4.37\text{ ms}$) — client-observed transaction commit duration via `tr.commit` (does not independently measure physical WAL fsync). Accounts for ~4.6% of DB stage time.
  - **Audit of $T_{\text{api\_redis}}$ Gateway Timer:**
    - Isolated $T_{\text{api\_parse}}$ (0.63 ms) and $T_{\text{api\_serialize}}$ (0.12 ms). The remaining ~44 ms in `await redis.xadd()` under $C=10$ is hypothesized to reflect Python asyncio event loop task scheduling contention and coroutine multiplexing overhead under concurrency rather than pure Redis network round-trip time (<1 ms), though definitive isolation from socket buffer backpressure requires deeper event loop tracing.
  - **Controlled Concurrency Scaling Experiment (Scenario D: 50,000 spans):**
    - **Run 1 (Single Worker):** Worker active drain time = **27.168 s** (1,840.4 spans/s). Total E2E time = 27.839 s (1,796.0 spans/s).
    - **Run 2 (Dual Workers):** Worker active drain time = **14.284 s** (3,500.5 spans/s, **+90.2% throughput increase**). Total E2E time = 14.822 s (3,373.5 spans/s, **+87.8% throughput increase**).
    - **100% Database Correctness:** Exactly 50,000 / 50,000 spans persisted in both runs with 0 duplicates and 0 errors.
    - **ADR-004 Accepted:** Linear worker concurrency scaling documented in [`docs/DECISION_LOG.md`](docs/DECISION_LOG.md).
- [x] **Day 5: 1 -> 2 -> 4 Worker Scaling Experiment:**
  - **Workload:** 50,000 spans (500 requests $\times$ 100 spans/req at $C=10$) executed under isolated benchmark consumer group and stream settings (raw dataset: [`backend/benchmarks/results/day5_worker_scaling_experiment.json`](backend/benchmarks/results/day5_worker_scaling_experiment.json)).
  - **Measured 3-Way Scaling Results:**

    | Metric | 1 worker | 2 workers | 4 workers |
    |---|---:|---:|---:|
    | Persistence throughput | 1,503.1 spans/s | 3,032.6 spans/s | 5,541.9 spans/s |
    | End-to-end drain time | 33.931 s | 17.074 s | 9.773 s |
    | API p95 latency | 127.48 ms | 157.22 ms | 197.24 ms |
    | API p99 latency | 289.53 ms | 302.50 ms | 319.37 ms |
    | Missing spans reported | 0 | 0 | 0 |
    | Duplicate spans reported | 0 | 0 | 0 |

  - **Scaling Efficiency Arithmetic:**
    - 1 -> 2 workers: Speedup = 2.02x $\rightarrow$ Efficiency = $\mathbf{100.9\%}$ ($2.018 / 2$, or $+101.8\%$ incremental; throughput $+101.8\%$, end-to-end drain time $-49.7\%$).
    - 2 -> 4 workers (incremental): Speedup = 1.83x $\rightarrow$ Efficiency = $\mathbf{91.4\%}$ ($1.827 / 2$; throughput $+82.7\%$, end-to-end drain time $-42.8\%$).
    - 1 -> 4 workers (overall): Speedup = 3.687x $\rightarrow$ Efficiency = $\mathbf{92.2\%}$ ($3.687 / 4$), calculated relative to Run 1 ($1,503.1\text{ spans/s}$) of the same Day 5 benchmark (throughput $+268.7\%$, end-to-end drain time $-71.2\%$).
  - **Operational Trade-Off & Non-Optimality of 4 Workers:**
    While 4 workers maximize persistence throughput and reduce drain time from 33.9 s to 9.8 s, upstream API latencies increased across the runs ($p_{95}$ from 127.5 ms to 197.2 ms; $p_{99}$ from 289.5 ms to 319.4 ms) due to host-level CPU contention across the API, workers, and database. Four workers are not automatically optimal and should be tuned against host core capacity.
  - **Database Stage Invariance & Hypothesis Framing:**
    - Client-observed SQL query duration ($T_{3\text{b}}$) remained stable across workers: **54.49 ms** (1 worker) vs **53.69 ms** (2 workers) vs **54.88 ms** (4 workers).
    - Client-observed commit duration ($T_{3\text{c}}$) remained stable: **4.91 ms** (1 worker) vs **4.16 ms** (2 workers) vs **4.19 ms** (4 workers).
    - Stable query timings are consistent with minimal database lock contention under this workload, suggesting per-connection query latency behaves independently; internal lock contention and WAL fsync behaviors remain working hypotheses requiring deeper engine-level instrumentation.
  - **Database Correctness:** Exactly 50,000 / 50,000 spans persisted in every run with **0 duplicates and 0 lost spans** verified via primary key constraint check.
