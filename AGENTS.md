# PulseOps — Agent Operational Guidelines

This document outlines architectural constraints, engineering conventions, and technical invariants for AI agents and engineers operating within the **PulseOps** codebase.

---

## 1. System Invariants & Non-Negotiables

1. **Empirical Scaling (No Premature Queues):**
   - Day 1 is strictly direct persistence to PostgreSQL.
   - Do NOT introduce background worker queues, Redis Streams, or Celery without benchmark proof.
2. **Batch Atomicity:**
   - Ingestion (`POST /v1/telemetry/spans`) must validate batches with all-or-nothing semantics.
   - Any single schema violation, regex mismatch, or timestamp inconsistency rejects the entire batch with `HTTP 422 Unprocessable Entity` before any database write occurs.
3. **Idempotency:**
   - Duplicate spans must be swallowed gracefully using `ON CONFLICT (trace_id, span_id) DO NOTHING`. Re-sending an identical batch must never throw 500 errors.
4. **Timestamp Range & Ordering:**
   - Spans record timestamps as integer nanoseconds since Unix epoch.
   - Values must strictly fit PostgreSQL signed 64-bit integer range (`-9223372036854775808` to `9223372036854775807`).
   - `end_time_unix_nano` must be `>= start_time_unix_nano`.
5. **Exact Serialization:**
   - `duration_ms` computed by PostgreSQL generated column must be returned as a standard IEEE float/number in JSON responses, not a string or Decimal object.
6. **Service Name Scope:**
   - `service_name` is declared at the root batch request level (`SpanBatchIngestRequest`) and inherited by each persisted span.

---

## 2. Directory Layout & Roles

- `docs/`: Architecture decision records, API contracts, status tracking, and architecture blueprints.
- `backend/app/config.py`: Environment-driven settings using Pydantic Settings.
- `backend/app/db.py`: Asyncpg database connection pool lifecycle and table schema migrations.
- `backend/app/schemas.py`: Pydantic V2 models for ingestion, query responses, and validation.
- `backend/app/main.py`: FastAPI application, lifecycle hooks, and route handlers.

---

## 3. Communication & Verification Protocol

- Follow checkpoint boundaries strictly.
- Always verify changes against running services (PostgreSQL, FastAPI test client or HTTP requests).
- Document all architectural choices in `docs/DECISION_LOG.md`.
