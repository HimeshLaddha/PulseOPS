# Architecture Decision Log (ADR)

## ADR-001: Day 1 Direct Persistence & Empirical Scaling

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
3. Spans are persisted directly to PostgreSQL 16 using an `asyncpg` connection pool with parameterized batch insertion (`executemany`) and `ON CONFLICT (trace_id, span_id) DO NOTHING`.
4. Traces are queried directly from PostgreSQL using indexed lookups.

### Consequences
- **Positive:**
  - High engineering velocity: Single datastore dependency (PostgreSQL 16).
  - Immediate consistency: Ingested traces are instantaneously retrievable by `trace_id`.
  - Clean baseline metrics: Day 2 benchmarks will measure the true, unmasked I/O limits of asyncpg and PostgreSQL on hardware without broker overhead.
- **Negative / Trade-offs:**
  - HTTP ingestion latency is tied directly to PostgreSQL batch insertion latency.
  - Ingestion bursts beyond connection pool capacity could lead to backpressure (HTTP 503 or higher latency).

### Evaluation Trigger for Day 2
On Day 2, we will execute load and stress testing suites measuring:
- Peak sustained writes per second (spans/sec).
- Ingestion endpoint p95 and p99 response times.
- PostgreSQL connection pool saturation under simulated spike traffic.

If p95 write latency degrades unacceptably or database connection exhaustion occurs before target throughput is met, we will introduce Redis Streams or background workers based strictly on empirical evidence.
