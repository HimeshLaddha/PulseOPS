# PulseOps — Current Status

**Active Phase:** Day 1 Vertical Slice & Foundation — **COMPLETE**  
**Last Updated:** 2026-10-12  
**Current Active Checkpoint:** Completed (Checkpoints 1 - 4 Verified)

---

## Checkpoint Tracking Summary

- [x] **Checkpoint 1: Project Structure & Memory Initialization**
  - [x] Scaffolding: Exact repository structure created.
  - [x] Documentation: `README.md`, `AGENTS.md`, `docs/DECISION_LOG.md` (ADR-001), `docs/API_CONTRACTS.md`, `docs/SYSTEM_ARCHITECTURE.md`, `docs/CURRENT_STATUS.md`.
  - [x] Infrastructure: `docker-compose.yml` with PostgreSQL 16 Alpine, port 5432, healthcheck (`pg_isready`), and named volume `pgdata`. Container running and healthy.

- [x] **Checkpoint 2: Schemas & Database Lifecycle**
  - [x] Requirements: `backend/requirements.txt` (`fastapi`, `uvicorn`, `pydantic>=2.0`, `asyncpg`, `python-dotenv`).
  - [x] Schema Rules: `backend/app/schemas.py` with 16-char hex `span_id`, 32-char hex `trace_id`, signed 64-bit int timestamp ranges, `end_time >= start_time` validator, literal status enum, and non-empty batch constraint.
  - [x] Database Lifecycle: `backend/app/db.py` asyncpg connection pool lifespan and DDL execution creating `spans` table with stored generated `duration_ms` column and indexes (`spans_pkey`, `idx_spans_trace_id`, `idx_spans_service_start`).

- [x] **Checkpoint 3: Endpoints Implementation**
  - [x] `POST /v1/telemetry/spans`: Batch atomicity (all-or-nothing validation with 422), unnested batch insert with `ON CONFLICT (trace_id, span_id) DO NOTHING`, returning `HTTP 201 Created` with `inserted_count`.
  - [x] `GET /v1/traces/{trace_id}`: Regex validation for 32-hex `trace_id` (422 if invalid), chronological sort (`start_time_unix_nano ASC`), `HTTP 404` if not found, IEEE float `duration_ms`.
  - [x] `GET /health`: Database pool ping and health check.

- [x] **Checkpoint 4: Verification Suite Execution**
  - [x] Scenario 1: Happy Path Ingestion (HTTP 201, `inserted_count: 2`)
  - [x] Scenario 2: Trace Retrieval (HTTP 200, parent-child ordering, matching attributes, float `duration_ms`)
  - [x] Scenario 3: Empty Batch Rejection (HTTP 422)
  - [x] Scenario 4: All-or-Nothing Rejection (HTTP 422 on invalid span, span 1 not inserted -> HTTP 404)
  - [x] Scenario 5: Idempotency Duplicate Handling (HTTP 201, `inserted_count: 0`)
  - [x] Scenario 6: Persistence Verification (Postgres restart, zero data loss, HTTP 200)
  - [x] Pytest Suite: 4/4 passed in 0.75s

---

## Verification Suite Execution Log

```text
======================================================================
PULSEOPS DAY 1 — VERIFICATION SUITE
======================================================================
[INIT] API Healthcheck Passed: {'status': 'healthy', 'database': 'connected'}

======================================================================
Scenario 1: Happy Path Ingestion
======================================================================
[SCENARIO 1] POST /v1/telemetry/spans -> HTTP 201
[SCENARIO 1] Response: {"status":"accepted","inserted_count":2}
[PASS] Scenario 1 verified: HTTP 201 and inserted_count=2

======================================================================
Scenario 2: Trace Retrieval
======================================================================
[SCENARIO 2] GET /v1/traces/0123456789abcdef0123456789abcdef -> HTTP 200
[SCENARIO 2] Response: {
  "trace_id": "0123456789abcdef0123456789abcdef",
  "service_name": "checkout-service",
  "span_count": 2,
  "spans": [
    {
      "span_id": "4a7f9b2c1d8e3f01",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": null,
      "service_name": "checkout-service",
      "operation_name": "process_checkout",
      "start_time_unix_nano": 1718000000000000000,
      "end_time_unix_nano": 1718000000250000000,
      "duration_ms": 250.0,
      "status_code": "OK",
      "attributes": {"http.method": "POST", "customer.tier": "platinum", "http.status_code": 200}
    },
    {
      "span_id": "8b3e2a1c9f4d7e02",
      "trace_id": "0123456789abcdef0123456789abcdef",
      "parent_span_id": "4a7f9b2c1d8e3f01",
      "service_name": "checkout-service",
      "operation_name": "charge_credit_card",
      "start_time_unix_nano": 1718000000050000000,
      "end_time_unix_nano": 1718000000200000000,
      "duration_ms": 150.0,
      "status_code": "OK",
      "attributes": {"amount": 99.95, "payment.provider": "stripe"}
    }
  ]
}
[PASS] Scenario 2 verified: Parent-child ordering, matching attributes, float duration_ms (250.0ms, 150.0ms)

======================================================================
Scenario 3: Empty Batch Rejection
======================================================================
[SCENARIO 3] POST /v1/telemetry/spans (empty) -> HTTP 422
[SCENARIO 3] Response: {"detail":[{"type":"too_short","loc":["body","spans"],"msg":"List should have at least 1 item after validation, not 0","input":[],"ctx":{"field_type":"List","min_length":1,"actual_length":0}}]}
[PASS] Scenario 3 verified: HTTP 422 returned on empty spans batch

======================================================================
Scenario 4: All-or-Nothing Rejection
======================================================================
[SCENARIO 4] POST /v1/telemetry/spans (mixed batch) -> HTTP 422
[SCENARIO 4] Response: {"detail":[{"type":"string_pattern_mismatch","loc":["body","spans",1,"trace_id"],"msg":"String should match pattern '^[0-9a-f]{32}$'","input":"1234567890","ctx":{"pattern":"^[0-9a-f]{32}$"}}]}
[SCENARIO 4] GET /v1/traces/ffffffffffffffffffffffffffffffff -> HTTP 404
[PASS] Scenario 4 verified: HTTP 422 rejected entire batch, span 1 was NOT inserted (404 on query)

======================================================================
Scenario 5: Idempotency (Duplicate Handling)
======================================================================
[SCENARIO 5] Re-sending batch 1 -> HTTP 201
[SCENARIO 5] Response: {"status":"accepted","inserted_count":0}
[PASS] Scenario 5 verified: Duplicate batch handled gracefully, HTTP 201 with inserted_count=0

======================================================================
Scenario 6: Persistence Verification (Postgres Restart)
======================================================================
[SCENARIO 6] Restarting postgres container via docker compose...
[SCENARIO 6] docker compose restart output:
[SCENARIO 6] Waiting for PostgreSQL readiness...
[SCENARIO 6] PostgreSQL is ready.
[SCENARIO 6] GET /v1/traces/0123456789abcdef0123456789abcdef after restart -> HTTP 200
[PASS] Scenario 6 verified: Zero data loss verified after PostgreSQL container restart!

======================================================================
ALL 6 MANDATORY SCENARIOS PASSED WITH ZERO ERRORS!
======================================================================
```

---

## Next Steps: Day 2 Preparation

On Day 2, we will execute load and stress testing suites measuring:
1. Peak sustained writes per second (`spans/sec`) directly to PostgreSQL.
2. Ingestion endpoint p95 and p99 response times under concurrency.
3. Connection pool saturation under simulated spike traffic.
4. Decision gate: evaluate whether introducing Redis Streams or background workers is empirically justified based on `docs/DECISION_LOG.md` (ADR-001).
