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

## ADR-002: Evaluation of Direct Persistence and Asynchronous Buffering Need

### Status: PROPOSED (Pending Isolation Profiling)

### 1. Empirical Observations (Verified Facts)
- **Zero Loss / Strong Correctness:** Across all 5 scenarios, 177,100 spans were ingested with 0 dropped records and 0 database discrepancies.
- **Normal-Load Sweet Spot (Scenario D):** At 10 concurrent clients (100 spans/batch), direct persistence achieved ~13,098 spans/sec with p95 latency of 91.81 ms and 0 errors.
- **High-Concurrency Degradation (Scenario E):** At 25 concurrent clients, throughput dropped by 33.7% (to 8,679 spans/sec), while p95 latency spiked by 566% to 611.47 ms (p99 at 991 ms, Max at 1.62 s).

### 2. Bottleneck Hypotheses (Unisolated)
The degradation in Scenario E coincided with three potential bottlenecks:
1. **Connection Pool Starvation:** Client concurrency (25) exceeded `DB_POOL_MAX_SIZE` (20), forcing requests into an async checkout wait queue.
2. **PostgreSQL Write Contention:** 20 active transactions committing simultaneously under `synchronous_commit = on` causing disk fsync serialization.
3. **Event Loop CPU Saturation:** A single Uvicorn process handling synchronous Pydantic v2 parsing and JSON stringification across 25 concurrent batches.

### 3. Architectural Verdict
Regardless of which internal factor is the primary bottleneck, Scenario E proves a critical platform reality:
**Direct persistence couples client response times directly to backend storage and processing latency.** 

Under burst concurrency, callers experienced response times exceeding 600 ms to 1.6 s. For an observability platform, this violates the prime directive: *telemetry collection must never degrade or stall the monitored application*.

Therefore, introducing an asynchronous ingestion buffer (e.g., Redis Streams) is **architecturally justified** to guarantee sub-10ms `HTTP 202 Accepted` response times to producers, moving the database write path out of the critical request-response cycle.

