# System Architecture — Day 1 Vertical Slice

## Overview

PulseOps Day 1 implements a high-integrity vertical slice designed for direct telemetry ingestion and retrieval:

```
[ Telemetry Producer ]
          │
          │ HTTP POST /v1/telemetry/spans (JSON batch)
          ▼
┌────────────────────────────────────────────────────────┐
│ FastAPI Application (uvicorn)                         │
│                                                        │
│  1. Pydantic V2 Strict Batch Validation                │
│     - Schema conformance (16/32 char hex IDs)          │
│     - Nanosecond BIGINT range checking                 │
│     - end_time >= start_time validation                │
│     - Min length >= 1 check                            │
│                                                        │
│  2. Asynchronous Database Persistence (asyncpg)        │
│     - Direct connection pool execution                 │
│     - Batch parameter unnest / executemany             │
│     - ON CONFLICT (trace_id, span_id) DO NOTHING       │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────┐
│ PostgreSQL 16 (pulseops_db)                            │
│                                                        │
│  Table: spans                                          │
│   - trace_id CHAR(32)                                  │
│   - span_id CHAR(16)                                   │
│   - parent_span_id CHAR(16)                            │
│   - service_name VARCHAR(128)                          │
│   - operation_name VARCHAR(256)                        │
│   - start_time_unix_nano BIGINT                        │
│   - end_time_unix_nano BIGINT                          │
│   - duration_ms DOUBLE PRECISION GENERATED ALWAYS AS ( │
│       (end_time - start_time)::float / 1e6             │
│     ) STORED                                           │
│   - status_code VARCHAR(16)                            │
│   - attributes JSONB                                   │
│   - created_at TIMESTAMPTZ                             │
│                                                        │
│  Indexes:                                              │
│   - PRIMARY KEY (trace_id, span_id)                    │
│   - idx_spans_trace_id ON spans (trace_id)             │
│   - idx_spans_service_start (service_name, start DESC) │
└────────────────────────────────────────────────────────┘
```

## Trace Retrieval Flow

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

## Architectural Invariants
1. **Direct Persistence:** Ingestion directly accesses the connection pool without intermediary queues on Day 1.
2. **Computed Precision:** `duration_ms` is generated at the database level using `STORED` generated columns to guarantee mathematical consistency across all clients.
3. **Storage Efficiency:** `CHAR(32)` and `CHAR(16)` provide deterministic storage sizes for hexadecimal trace and span IDs.
