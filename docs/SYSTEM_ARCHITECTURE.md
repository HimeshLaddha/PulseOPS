# System Architecture — PulseOps Platform

## Overview

PulseOps implements a high-throughput, fault-tolerant telemetry ingestion and trace analysis engine. The platform provides dual-mode ingestion: direct PostgreSQL persistence for single-node low-complexity deployments, and decoupled asynchronous Redis Stream buffering with consumer workers for resilient burst handling and storage outage isolation.

---

## 1. Dual-Mode Ingestion Architecture

```
                       [ Telemetry Producer / Client ]
                                      │
                        POST /v1/telemetry/spans
                                      │
                       ┌──────────────┴──────────────┐
                       │  mode=direct                │  mode=stream (Default)
                       ▼                             ▼
         ┌───────────────────────────┐ ┌───────────────────────────┐
         │ FastAPI Direct Path       │ │ FastAPI Async Stream Path │
         │                           │ │                           │
         │ 1. Pydantic V2 Validation │ │ 1. Pydantic V2 Validation │
         │ 2. asyncpg Pool Checkout  │ │ 2. Redis XADD             │
         │ 3. Parameterized UNNEST   │ │    (telemetry:stream)     │
         │ 4. HTTP 201 Created       │ │ 3. HTTP 202 Accepted      │
         └─────────────┬─────────────┘ └─────────────┬─────────────┘
                       │                             │
                       │                             ▼
                       │               ┌───────────────────────────┐
                       │               │ Redis 7 (AOF everysec)    │
                       │               │ Key: telemetry:stream     │
                       │               └─────────────┬─────────────┘
                       │                             │
                       │                             ▼ Consumer Group:
                       │               ┌───────────────────────────┐
                       │               │ Ingestion Worker Daemon   │
                       │               │ (pulseops-ingest-group)   │
                       │               │                           │
                       │               │ 1. XAUTOCLAIM (idle > 5s) │
                       │               │ 2. XREADGROUP (count=50)  │
                       │               │ 3. Pydantic Batch Parse   │
                       │               │ 4. Poison Pill -> DLQ     │
                       │               │ 5. DB UNNEST Batch Commit │
                       │               │ 6. XACK + XDEL on success │
                       │               └─────────────┬─────────────┘
                       │                             │
                       ▼                             ▼
         ┌─────────────────────────────────────────────────────────┐
         │ PostgreSQL 16 (pulseops_db)                             │
         │  - Table: spans (trace_id, span_id, start, end, attrs) │
         │  - Idempotent upserts: ON CONFLICT DO NOTHING           │
         │  - Generated column: duration_ms STORED                 │
         └─────────────────────────────────────────────────────────┘
```

---

## 2. Ingestion Modes & Request Contracts

| Mode | Endpoint Query | Response Code | Contract Meaning | Downstream Coupling |
|---|---|:---:|---|---|
| **Direct** | `POST /v1/telemetry/spans?mode=direct` | `201 Created` | Telemetry is committed to PostgreSQL disk before response. | Coupled to database connection pool and WAL commit latency. |
| **Stream** | `POST /v1/telemetry/spans?mode=stream` | `202 Accepted` | Telemetry is validated and buffered into durable Redis Stream. | Fully decoupled from database availability, lock waits, and disk I/O. |

---

## 3. Worker Lifecycle & Stream Reliability Mechanics

The background consumer worker (`backend/app/worker.py`) implements enterprise stream processing semantics over Redis Streams:

### A. Consumer Group Initialization
- Workers join the consumer group `pulseops-ingest-group` on `telemetry:stream`.
- Stream and consumer group creation is idempotent using `XGROUP CREATE ... MKSTREAM`.

### B. Crash Recovery & Message Reclamation via `XAUTOCLAIM`
- Before reading new stream messages, workers run `XAUTOCLAIM` to scan the Pending Entries List (PEL) for unacknowledged messages from crashed or stalled workers.
- **Minimum Idle Threshold (`REDIS_STREAM_MIN_IDLE_MS` = 5000 ms):** Messages become eligible for reclaim only after remaining unacknowledged in the PEL for at least 5 seconds.
- **Not a Guaranteed Lease:** This threshold represents a minimum idle duration, not a guaranteed lease or distributed lock. If a worker's processing duration exceeds `REDIS_STREAM_MIN_IDLE_MS` (e.g., due to downstream database stalls, GC pauses, or transient slowdowns), a competing worker may reclaim the message while the first worker is still actively processing it.
- **Duplicate Delivery & Idempotency Requirement:** Because concurrent processing can result in duplicate delivery of the same message batch, downstream database persistence must be idempotent. In PulseOps, this is guaranteed by PostgreSQL primary key constraints on `(trace_id, span_id)` combined with `ON CONFLICT (trace_id, span_id) DO NOTHING`.

### C. Batch Fetch & Database Commit
- New messages are fetched via `XREADGROUP ... STREAMS telemetry:stream >` in batches (default: 50 messages).
- Spans are bulk-inserted into PostgreSQL using parameterized SQL `UNNEST` within an atomic transaction.
- **Idempotency:** Spans enforce `PRIMARY KEY (trace_id, span_id)` with `ON CONFLICT (trace_id, span_id) DO NOTHING`. Re-processing a claimed message produces zero duplicate rows and zero constraint errors.

### D. Atomic Acknowledgement & Stream Compaction
- `XACK` and `XDEL` are executed **strictly after** the PostgreSQL transaction commits successfully.
- Calling `XDEL` immediately after `XACK` prevents unbounded stream growth, keeping Redis memory bounded.

### E. Atomic Dead-Letter Queue (DLQ)
- Malformed payloads (poison pills) are intercepted and routed to `telemetry:stream:deadletter` with diagnostic error metadata.
- **Atomic Routing Guarantee:** If the write to `telemetry:stream:deadletter` fails, `XACK` is **never called** on the original message, ensuring it remains in the PEL for retry rather than being silently dropped.

### F. Redis Stream Persistence & Explicit Durability Guarantees
Redis is configured with `--appendonly yes --appendfsync everysec` and backed by a mounted volume (`redis_data`):
- **Clean Container & Service Restarts (Zero Loss):** Clean container, service, or host restarts preserve 100% of stream data because the operating system flushes process buffers on clean termination before unmounting.
- **Ungraceful Termination (Potential Loss Window):** Any ungraceful termination (abrupt Redis process kill e.g. `kill -9`, Docker daemon crash, OS kernel panic, or sudden host reboot / power cut) before the 1-second background sync completes carries a potential loss window of recently acknowledged writes (up to 1 second of in-flight buffered telemetry).
- **Engineering Trade-off Rationale:** `appendfsync everysec` provides near-instantaneous in-memory write performance (~15,000–30,000 operations/sec) with bounded 1-second exposure, avoiding the severe 10x–100x disk I/O bottleneck of `appendfsync always` (fsync on every write), which would compromise the high-throughput ingest buffering requirement.

---

## 4. Trace Retrieval Flow

```
[ Observability Consumer / Debugger ]
                 │
                 │ GET /v1/traces/{trace_id}
                 ▼
┌────────────────────────────────────────────────────────┐
│ FastAPI Application                                    │
│  - Validate trace_id regex (32-char lowercase hex)     │
│  - Query: SELECT * FROM spans                          │
│           WHERE trace_id = $1                          │
│           ORDER BY start_time_unix_nano ASC            │
│  - Format as TraceResponse                             │
│  - Return 200 OK with float duration_ms                │
│    or 404 Not Found if empty                           │
└────────────────────────────────────────────────────────┘
```

---

## 5. Database Schema & Generated Precision

```sql
CREATE TABLE IF NOT EXISTS spans (
    trace_id CHAR(32) NOT NULL,
    span_id CHAR(16) NOT NULL,
    parent_span_id CHAR(16),
    service_name VARCHAR(128) NOT NULL,
    operation_name VARCHAR(256) NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    duration_ms DOUBLE PRECISION GENERATED ALWAYS AS (
        (end_time_unix_nano - start_time_unix_nano)::double precision / 1000000.0
    ) STORED,
    status_code VARCHAR(16) NOT NULL DEFAULT 'OK',
    attributes JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans (trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_service_start ON spans (service_name, start_time_unix_nano DESC);
```

### Invariants:
1. **Computed Precision:** `duration_ms` is generated at the database level using `STORED` generated columns to guarantee mathematical consistency across all clients.
2. **Storage Efficiency:** `CHAR(32)` and `CHAR(16)` provide deterministic storage sizes for hexadecimal trace and span IDs.
3. **Signed 64-bit Timestamps:** Unix nanoseconds fit PostgreSQL signed 64-bit integer limits (`-9223372036854775808` to `9223372036854775807`).
