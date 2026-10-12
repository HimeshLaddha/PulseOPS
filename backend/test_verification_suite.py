import subprocess
import time
import uuid
import httpx

BASE_URL = "http://127.0.0.1:8000"


def print_banner(text: str):
    line = "=" * 70
    print(f"\n{line}\n{text}\n{line}")


def main():
    print_banner("PULSEOPS DAY 1 -- VERIFICATION SUITE")
    client = httpx.Client(base_url=BASE_URL, timeout=10.0)

    # Pre-check: Health check
    resp = client.get("/health")
    assert resp.status_code == 200, f"Healthcheck failed: {resp.status_code} {resp.text}"
    print("[INIT] API Healthcheck Passed:", resp.json())

    # -------------------------------------------------------------
    # Scenario 1: Happy Path Ingestion
    # -------------------------------------------------------------
    print_banner("Scenario 1: Happy Path Ingestion")
    trace_id_1 = uuid.uuid4().hex  # 32-char lowercase hex
    root_span_id = uuid.uuid4().hex[:16]  # 16-char lowercase hex
    child_span_id = uuid.uuid4().hex[:16]  # 16-char lowercase hex

    batch_1 = {
        "service_name": "checkout-service",
        "spans": [
            {
                "span_id": root_span_id,
                "trace_id": trace_id_1,
                "parent_span_id": None,
                "operation_name": "process_checkout",
                "start_time_unix_nano": 1718000000000000000,
                "end_time_unix_nano": 1718000000250000000,
                "status_code": "OK",
                "attributes": {
                    "http.method": "POST",
                    "http.status_code": 200,
                    "customer.tier": "platinum",
                },
            },
            {
                "span_id": child_span_id,
                "trace_id": trace_id_1,
                "parent_span_id": root_span_id,
                "operation_name": "charge_credit_card",
                "start_time_unix_nano": 1718000000050000000,
                "end_time_unix_nano": 1718000000200000000,
                "status_code": "OK",
                "attributes": {
                    "payment.provider": "stripe",
                    "amount": 99.95,
                },
            },
        ],
    }

    r1 = client.post("/v1/telemetry/spans", json=batch_1)
    print(f"[SCENARIO 1] POST /v1/telemetry/spans -> HTTP {r1.status_code}")
    print(f"[SCENARIO 1] Response: {r1.text}")
    assert r1.status_code == 201, f"Expected 201, got {r1.status_code}: {r1.text}"
    body1 = r1.json()
    assert body1["status"] == "accepted"
    assert body1["inserted_count"] == 2
    print("[PASS] Scenario 1 verified: HTTP 201 and inserted_count=2")

    # -------------------------------------------------------------
    # Scenario 2: Trace Retrieval
    # -------------------------------------------------------------
    print_banner("Scenario 2: Trace Retrieval")
    r2 = client.get(f"/v1/traces/{trace_id_1}")
    print(f"[SCENARIO 2] GET /v1/traces/{trace_id_1} -> HTTP {r2.status_code}")
    print(f"[SCENARIO 2] Response: {r2.text}")
    assert r2.status_code == 200, f"Expected 200, got {r2.status_code}: {r2.text}"
    trace = r2.json()
    assert trace["trace_id"] == trace_id_1
    assert trace["service_name"] == "checkout-service"
    assert trace["span_count"] == 2
    spans = trace["spans"]
    assert len(spans) == 2

    # Verify chronological ordering
    assert spans[0]["span_id"] == root_span_id
    assert spans[1]["span_id"] == child_span_id
    assert spans[0]["start_time_unix_nano"] < spans[1]["start_time_unix_nano"]

    # Verify duration_ms is a standard float and accurate
    # Root: (1718000000250000000 - 1718000000000000000) / 1e6 = 250.0 ms
    # Child: (1718000000200000000 - 1718000000050000000) / 1e6 = 150.0 ms
    assert isinstance(spans[0]["duration_ms"], float)
    assert isinstance(spans[1]["duration_ms"], float)
    assert abs(spans[0]["duration_ms"] - 250.0) < 0.001
    assert abs(spans[1]["duration_ms"] - 150.0) < 0.001

    # Verify attributes
    assert spans[0]["attributes"]["customer.tier"] == "platinum"
    assert spans[1]["attributes"]["payment.provider"] == "stripe"
    assert spans[1]["attributes"]["amount"] == 99.95
    print("[PASS] Scenario 2 verified: Parent-child ordering, matching attributes, float duration_ms (250.0ms, 150.0ms)")

    # -------------------------------------------------------------
    # Scenario 3: Empty Batch Rejection
    # -------------------------------------------------------------
    print_banner("Scenario 3: Empty Batch Rejection")
    empty_batch = {"service_name": "test-service", "spans": []}
    r3 = client.post("/v1/telemetry/spans", json=empty_batch)
    print(f"[SCENARIO 3] POST /v1/telemetry/spans (empty) -> HTTP {r3.status_code}")
    print(f"[SCENARIO 3] Response: {r3.text}")
    assert r3.status_code == 422, f"Expected 422, got {r3.status_code}"
    print("[PASS] Scenario 3 verified: HTTP 422 returned on empty spans batch")

    # -------------------------------------------------------------
    # Scenario 4: All-or-Nothing Rejection
    # -------------------------------------------------------------
    print_banner("Scenario 4: All-or-Nothing Rejection")
    trace_id_4 = uuid.uuid4().hex
    batch_with_invalid_span = {
        "service_name": "order-service",
        "spans": [
            {
                "span_id": uuid.uuid4().hex[:16],
                "trace_id": trace_id_4,
                "parent_span_id": None,
                "operation_name": "valid_operation",
                "start_time_unix_nano": 1718000000000000000,
                "end_time_unix_nano": 1718000000100000000,
                "status_code": "OK",
                "attributes": {},
            },
            {
                "span_id": uuid.uuid4().hex[:16],
                "trace_id": "1234567890",  # INVALID: 10 chars, regex requires 32 hex chars
                "parent_span_id": None,
                "operation_name": "invalid_trace_id_op",
                "start_time_unix_nano": 1718000000010000000,
                "end_time_unix_nano": 1718000000080000000,
                "status_code": "OK",
                "attributes": {},
            },
        ],
    }

    r4 = client.post("/v1/telemetry/spans", json=batch_with_invalid_span)
    print(f"[SCENARIO 4] POST /v1/telemetry/spans (mixed batch) -> HTTP {r4.status_code}")
    print(f"[SCENARIO 4] Response: {r4.text}")
    assert r4.status_code == 422, f"Expected 422, got {r4.status_code}"

    # Verify span 1 was NOT inserted into database
    r4_check = client.get(f"/v1/traces/{trace_id_4}")
    print(f"[SCENARIO 4] GET /v1/traces/{trace_id_4} -> HTTP {r4_check.status_code}")
    assert r4_check.status_code == 404, f"Expected 404 (no record persisted), got {r4_check.status_code}"
    print("[PASS] Scenario 4 verified: HTTP 422 rejected entire batch, span 1 was NOT inserted (404 on query)")

    # -------------------------------------------------------------
    # Scenario 5: Idempotency (Duplicate Handling)
    # -------------------------------------------------------------
    print_banner("Scenario 5: Idempotency (Duplicate Handling)")
    r5 = client.post("/v1/telemetry/spans", json=batch_1)
    print(f"[SCENARIO 5] Re-sending batch 1 -> HTTP {r5.status_code}")
    print(f"[SCENARIO 5] Response: {r5.text}")
    assert r5.status_code == 201, f"Expected 201, got {r5.status_code}: {r5.text}"
    body5 = r5.json()
    assert body5["status"] == "accepted"
    assert body5["inserted_count"] == 0, f"Expected inserted_count=0, got {body5['inserted_count']}"
    print("[PASS] Scenario 5 verified: Duplicate batch handled gracefully, HTTP 201 with inserted_count=0")

    # -------------------------------------------------------------
    # Scenario 6: Persistence Verification (Postgres Restart)
    # -------------------------------------------------------------
    print_banner("Scenario 6: Persistence Verification (Postgres Restart)")
    print("[SCENARIO 6] Restarting postgres container via docker compose...")
    restart_res = subprocess.run(
        ["docker", "compose", "restart", "postgres"],
        capture_output=True,
        text=True,
    )
    print("[SCENARIO 6] docker compose restart output:", restart_res.stdout)
    assert restart_res.returncode == 0, f"Failed to restart postgres: {restart_res.stderr}"

    # Wait for postgres to be healthy again
    print("[SCENARIO 6] Waiting for PostgreSQL readiness...")
    for _ in range(30):
        check = subprocess.run(
            ["docker", "exec", "pulseops-postgres", "pg_isready", "-U", "pulseops", "-d", "pulseops_db"],
            capture_output=True,
            text=True,
        )
        if check.returncode == 0:
            print("[SCENARIO 6] PostgreSQL is ready.")
            break
        time.sleep(1)
    else:
        raise RuntimeError("PostgreSQL did not become ready in time after restart")

    # Small delay for pool connections to reconnect if needed
    time.sleep(2)

    # Query trace again
    r6 = client.get(f"/v1/traces/{trace_id_1}")
    print(f"[SCENARIO 6] GET /v1/traces/{trace_id_1} after restart -> HTTP {r6.status_code}")
    print(f"[SCENARIO 6] Response: {r6.text}")
    assert r6.status_code == 200, f"Expected 200, got {r6.status_code}: {r6.text}"
    trace6 = r6.json()
    assert trace6["trace_id"] == trace_id_1
    assert trace6["span_count"] == 2
    assert len(trace6["spans"]) == 2
    print("[PASS] Scenario 6 verified: Zero data loss verified after PostgreSQL container restart!")

    print_banner("ALL 6 MANDATORY SCENARIOS PASSED WITH ZERO ERRORS!")


if __name__ == "__main__":
    main()
