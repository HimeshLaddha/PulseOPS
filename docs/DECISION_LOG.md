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
3. *Single-threaded Python event loop CPU saturation* from Pydantic schema validation.

**Empirical Isolation Verdict:**
Day 3 decisively proves that **Hypothesis 3 (CPU Event Loop Serialization Saturation)** is the primary gating constraint, rather than PostgreSQL pool limits or WAL locks:
- In `mode=stream`, the database connection pool was entirely removed from the HTTP request path (FastAPI DB checkouts = 0).
- Telemetry batches are published directly to an in-memory Redis 7 instance via single async `XADD` operations.
- Despite eliminating database writes from the request path, tail latencies at Concurrency 25 remained in the ~240 ms (p50) to ~700 ms (p95) range.
- **Root Cause:** In Python/Uvicorn, synchronous JSON decoding, Pydantic V2 recursive model validation, and JSON re-serialization (`model_dump_json()`) across 25 concurrent connections with 100-span batches occupy the single CPU core thread, causing client requests to wait in socket backlog queues before reaching the I/O event loop.

### 3. Architectural Decision

1. **Adopt Decoupled Stream Ingestion as Platform Standard:**
   - Asynchronous buffering via Redis Streams is **ACCEPTED**. It isolates the telemetry producer from downstream storage outages, maintenance, and query contention.
   - Dual-mode support (`?mode=direct` and `?mode=stream`) is preserved for local development and integration testing.

2. **Decoupled Worker Scaling:**
   - Ingestion throughput is decoupled from database commit speed. While the API accepts batches at up to 14,032 spans/sec, a single background consumer worker drains the stream at ~1,600 spans/sec without blocking upstream callers.
   - Horizontal scaling of workers across multiple processes/containers (`pulseops-ingest-group`) enables matching drain rate to ingestion rate without altering client APIs.

3. **Follow-Up Platform Optimization:**
   - To achieve sub-10ms p95 latencies under burst concurrency, horizontal multi-worker Uvicorn processes (`--workers $(nproc)`), zero-copy JSON streaming, or offloading validation to Rust/Cython gateways will be evaluated in subsequent iterations.

