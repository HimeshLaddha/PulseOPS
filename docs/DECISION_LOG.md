# Architecture Decision Log (ADR)

## ADR-001: Day 1 Direct Persistence & Empirical Scaling

### Status
Accepted

### Context
When building distributed telemetry and observability backends, standard industry instinct frequently introduces distributed messaging queues (e.g., Redis Streams, Apache Kafka, RabbitMQ) and decoupled worker pools right at inception. While necessary for high-scale ingestion, introducing distributed messaging prematurely adds:
- Operational overhead (cluster maintenance, partitioning, offset commits).
- Testing friction (mocking queues, running multi-container topologies locally).
- Delayed write visibility (eventual consistency vs immediate read-after-write).
- Obscured performance baselines (cannot isolate database write bottlenecks from broker network round-trips).

### Decision
For Day 1, PulseOps adopts a direct asynchronous persistence pattern:
1. Telemetry spans are received over HTTP via FastAPI.
2. Ingestion batches are strictly validated in-memory with Pydantic V2.
3. Spans are persisted directly to PostgreSQL 16 using an `asyncpg` connection pool with parameterized batch insertion (`executemany` / `UNNEST`) and `ON CONFLICT (trace_id, span_id) DO NOTHING`.
4. Traces are queried directly from PostgreSQL using indexed lookups.

### Consequences
- **Positive:**
  - High engineering velocity: Single datastore dependency (PostgreSQL 16).
  - Immediate consistency: Ingested traces are instantaneously retrievable by `trace_id`.
  - Clean baseline metrics: Day 2 benchmarks measure the true, unmasked I/O limits of asyncpg and PostgreSQL on hardware without broker overhead.
- **Negative / Trade-offs:**
  - HTTP ingestion latency is tied directly to PostgreSQL batch insertion latency.
  - Ingestion bursts beyond connection pool capacity could lead to backpressure (HTTP 503 or higher latency).

### Evaluation Trigger for Day 2
On Day 2, we will execute load and stress testing suites measuring:
- Peak sustained writes per second (spans/sec).
- Ingestion endpoint p95 and p99 response times.
- PostgreSQL connection pool saturation under simulated spike traffic.

If p95 write latency degrades unacceptably or database connection exhaustion occurs before target throughput is met, we will introduce Redis Streams or background workers based strictly on empirical evidence.

---

## ADR-002: Architectural Transition from Direct Persistence to Decoupled Redis Stream Buffering

### Status: ACCEPTED

### 1. Empirical Findings & Baseline Comparison

Across Day 2 (Direct PostgreSQL) and Day 3 (Decoupled Redis Stream + Asynchronous Worker), the full 5-scenario benchmark matrix was executed on the identical machine architecture:

| Metric | Scenario D (Direct PG) | Scenario D (Redis Stream) | Scenario E (Direct PG) | Scenario E (Redis Stream) |
|---|:---:|:---:|:---:|:---:|
| **Batch Size / Concurrency** | 100 spans / C=10 | 100 spans / C=10 | 100 spans / C=25 | 100 spans / C=25 |
| **API Wall Time** | 3.818 s | 3.563 s (-6.7%) | 11.522 s | 12.764 s |
| **API Ingestion Rate** | 13,097.58 spans/s | 14,032.42 spans/s (+7.1%) | 8,679.33 spans/s | 7,834.34 spans/s |
| **p50 Latency** | 68.66 ms | 63.16 ms (-8.0%) | 227.48 ms | 242.30 ms |
| **p90 Latency** | 85.45 ms | 81.50 ms (-4.6%) | 515.34 ms | 566.36 ms |
| **p95 Latency** | 91.81 ms | 94.82 ms | 611.47 ms | 700.11 ms |
| **p99 Latency** | 257.22 ms | 261.84 ms | 991.33 ms | 1,142.81 ms |
| **PostgreSQL Discrepancy** | 0 (50,000 / 50,000) | 0 (50,000 / 50,000) | 0 (100,000 / 100,000) | 0 (100,000 / 100,000) |
| **End-to-End Pipeline Drain** | 3.818 s (Synchronous) | 31.100 s (Decoupled) | 11.522 s (Synchronous) | 63.281 s (Decoupled) |

Total telemetry spans ingested across Day 3: **177,100 / 177,100 spans** verified in PostgreSQL with **0 data loss and 0 discrepancy**.

### 2. Bottleneck Isolation: Resolving Day 2 Hypotheses

On Day 2, we formulated three unisolated hypotheses for the high-concurrency degradation observed under 25 workers:
1. *PostgreSQL connection pool checkout queue starvation* (`DB_POOL_MAX_SIZE = 20 < 25`).
2. *PostgreSQL WAL disk write contention* from 20 parallel transactions.
3. *Single-threaded Python event loop contention* under concurrent requests.

**Empirical Isolation Verdict:**
Day 3 decisively proves that request degradation under high concurrency is decoupled from downstream database pool limits or WAL locks:
- In `mode=stream`, the database connection pool was entirely removed from the HTTP request path (FastAPI DB checkouts = 0).
- Telemetry batches are published directly to an in-memory Redis 7 instance via single async `XADD` operations.
- Despite eliminating database writes from the request path, tail latencies at Concurrency 25 remained in the ~240 ms (p50) to ~700 ms (p95) range.
- **Root Cause Analysis (Refined in Day 4):** While initially hypothesized as CPU saturation from Pydantic validation, fine-grained microsecond profiling established that Pydantic validation takes only ~0.3–0.6 ms per request. The tail latency under high concurrency is primarily driven by Python asyncio event loop task scheduling contention: cooperative multitasking across 25 concurrent connections with 100-span batches multiplexed over a single thread causes coroutines to wait in scheduler queues while awaiting I/O readiness.

> [!NOTE]
> While Redis Streams successfully decoupled producers from downstream storage outages and database lock contention, Scenario E latency ($p_{95} = 700.11\text{ ms}$) demonstrates that single-process event loop scheduling remains an API acceptance constraint under high concurrency, informing the roadmap toward multi-process Uvicorn workers.

### 3. Architectural Decision

1. **Adopt Decoupled Stream Ingestion as Platform Standard:**
   - Asynchronous buffering via Redis Streams is **ACCEPTED**. It isolates the telemetry producer from downstream storage outages, maintenance, and query contention.
   - Dual-mode support (`?mode=direct` and `?mode=stream`) is preserved for local development and integration testing.

2. **Decoupled Worker Scaling:**
   - Ingestion throughput is decoupled from database commit speed. While the API accepts batches at up to 14,032 spans/sec, a single background consumer worker drains the stream at ~1,800 spans/sec without blocking upstream callers.
   - Horizontal scaling of workers across multiple processes/containers (`pulseops-ingest-group`) enables matching drain rate to ingestion rate without altering client APIs.

3. **Follow-Up Platform Optimization:**
   - To achieve sub-10ms p95 latencies under burst concurrency, horizontal multi-worker Uvicorn processes (`--workers $(nproc)`), zero-copy JSON streaming, or offloading validation to Rust/Cython gateways will be evaluated in subsequent iterations.

---

## ADR-003: Stream Processing Edge-Case Reliability & Durability Guarantees

### Status: ACCEPTED

### Context
During stream processing operations with Redis Streams, edge cases occur when messages in the Pending Entries List (PEL) have their underlying stream data deleted or truncated before acknowledgment, or when consumer worker crashes occur immediately following dead-letter queue (DLQ) writes but prior to stream ACK/DEL. Furthermore, operator expectations regarding data durability during container restarts vs. ungraceful host power outages required explicit architectural definition.

### Decision
1. **Defensive XAUTOCLAIM & Stream Unpacking (Phantom vs. Poison Pill Distinction):**
   - In [backend/app/worker.py](file:///c:/Users/Admin/OneDrive/Desktop/PulseOPS/backend/app/worker.py), both `xautoclaim` and `xreadgroup` unpack stream messages defensively.
   - If an entry has `fields is None` (confirmed phantom entry from deleted stream ID), the worker acknowledges it immediately to evict the phantom entry from the consumer group's PEL without attempting to route an empty ghost object.
   - If `fields` is a dictionary but empty (`{}`) or missing the `"data"` field, it is NOT silently discarded; it is treated as a poison pill, routed to `telemetry:stream:deadletter` first, and only acknowledged/deleted after DLQ append succeeds.

2. **Idempotent DLQ Replay Safety:**
   - Un-ACKed poison pills reclaimed following pre-ACK worker crashes are processed idempotently. Re-delivery safely appends diagnostic metadata to `telemetry:stream:deadletter` and completes `XACK` + `XDEL` without pipeline disruption or unhandled exceptions.

3. **Explicit Durability Model:**
   - Redis operates with `--appendonly yes --appendfsync everysec`.
   - **Clean Container & Service Restarts (Zero Loss):** Preserves 100% of stream data as the operating system flushes process buffers on clean shutdown before unmounting.
   - **Ungraceful Termination (Potential Loss Window):** Any ungraceful termination (abrupt `kill -9` of Redis process, Docker engine crash, kernel panic, or sudden host reboot / power cut) before the 1-second background fsync cycle completes carries a potential loss window of recently acknowledged writes (up to 1 second of buffered telemetry). This trade-off is accepted to avoid the severe throughput penalty of synchronous per-write fsync.

---

## ADR-004: Fine-Grained DB Profiling, Reconciled Drain Metrics & Worker Concurrency Scaling

### Status: ACCEPTED

### Context
In Day 3 benchmarks, enqueuing to Redis was fast, but the single consumer worker required extended drain times (e.g. ~63 s for Scenario E, ~27 s for Scenario D). Furthermore, initial pipeline measurements conflated worker active processing duration with overall end-to-end drain duration, and the root cause of the ~52 ms per-batch database stage ($T_3$) was unisolated between connection pool checkout queuing, SQL query fetch/execution, and transaction commit.

### Decision & Empirical Findings

1. **Reconciliation of Pipeline Drain Metrics:**
   - **Worker Active Drain Time:** $T_{\text{last\_batch\_acked}} - T_{\text{first\_batch\_fetched}}$  
     Measures pure worker execution throughput from the timestamp when the first message was claimed/fetched to the timestamp when the final batch completed acknowledgment.
   - **Total End-to-End Drain Time:** $T_{\text{queue\_empty}} - T_{\text{ingest\_start}}$  
     Measures the comprehensive pipeline lifecycle from the moment the first client request was initiated until the stream and PEL reached depth 0.
   - **Audit of $T_{\text{api\_redis}}$ Gateway Timer:**  
     Isolated measurement reveals $T_{\text{api\_parse}}$ takes 0.63 ms ($p_{50} = 0.51\text{ ms}$) and $T_{\text{api\_serialize}}$ takes 0.12 ms ($p_{50} = 0.10\text{ ms}$). The remaining ~44 ms in `await redis.xadd()` under $C=10$ is not network I/O to Redis (which is <1 ms), but Python asyncio event loop task scheduling and context switching while 10 concurrent HTTP coroutines are multiplexed across a single thread.

2. **Deconstruction of the 52 ms Database Persistence Bottleneck ($T_3$):**
   Deconstructing the asyncpg database write step across 500 batches (50,000 spans) yielded:
   - **$T_{3\text{a}}$ (Connection Pool Acquisition):** Mean = **0.025 ms** ($p_{50} = 0.021\text{ ms}, p_{95} = 0.044\text{ ms}$). Pool checkout contention is negligible.
   - **$T_{3\text{b}}$ (SQL Fetch/Execute Duration):** Mean = **48.35 ms** ($p_{50} = 47.78\text{ ms}, p_{95} = 52.82\text{ ms}$). Client-observed duration executing the parameterized SQL UNNEST query and fetching returned rows (does not isolate PostgreSQL internal execution). Accounts for ~92.4% of total database roundtrip time.
   - **$T_{3\text{c}}$ (Transaction Commit Duration):** Mean = **2.40 ms** ($p_{50} = 2.18\text{ ms}, p_{95} = 4.37\text{ ms}$). Client-observed transaction commit duration via asyncpg (does not independently measure physical WAL fsync). Accounts for ~4.6% of database roundtrip time.

3. **Horizontal Worker Scaling (1-Worker vs. 2-Worker Controlled Experiment):**
   Testing on the identical Scenario D workload (500 requests $\times$ 100 spans = 50,000 spans at $C=10$):
   - **Single Worker Baseline:** Worker active drain time = **27.168 s** (1,840.4 spans/s). Total E2E time = 27.839 s (1,796.0 spans/s).
   - **Dual Concurrent Workers (`worker-1` + `worker-2`):** Worker active drain time = **14.284 s** (3,500.5 spans/s, **+90.2% throughput increase**). Total E2E time = 14.822 s (3,373.5 spans/s, **+87.8% throughput increase**).
   - **Database Correctness:** Exactly 50,000 / 50,000 spans persisted in both runs with 0 duplicates and 0 errors.
   - **Contention Impact:** Scaling to 2 workers caused zero database pool degradation ($T_{3\text{a}}$ remained 0.025 ms) and zero SQL query degradation ($T_{3\text{b}}$ remained 48.88 ms), demonstrating linear throughput scaling through horizontal worker concurrency without requiring schema alterations or unsafe PostgreSQL configuration adjustments.


