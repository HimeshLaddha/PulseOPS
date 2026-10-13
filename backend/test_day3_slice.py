import json
import subprocess
import time
import uuid
import httpx

BASE_URL = "http://127.0.0.1:8000"


def print_banner(msg: str):
    print("\n" + "=" * 70)
    print(msg)
    print("=" * 70)


def exec_redis_cmd(*args: str) -> str:
    res = subprocess.run(
        ["docker", "exec", "pulseops-redis", "redis-cli", *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return res.stdout.strip()


def main():
    print_banner("DAY 3 — ASYNCHRONOUS INGESTION VERTICAL SLICE VERIFICATION")
    client = httpx.Client(base_url=BASE_URL, timeout=10.0)

    # -------------------------------------------------------------
    # 1. Healthcheck: PostgreSQL and Redis Connectivity
    # -------------------------------------------------------------
    print_banner("1. Checking GET /health (PostgreSQL + Redis)")
    resp_health = client.get("/health")
    print(f"Status: HTTP {resp_health.status_code}")
    print(f"Response: {resp_health.text}")
    assert resp_health.status_code == 200, f"Expected 200, got {resp_health.status_code}"
    health_data = resp_health.json()
    assert health_data["status"] == "healthy"
    assert health_data["database"] == "connected"
    assert health_data["redis"] == "connected"
    print("[PASS] Both PostgreSQL and Redis report 'connected'.")

    # Initial Stream Length check
    initial_xlen = int(exec_redis_cmd("xlen", "telemetry:stream"))
    print(f"Current Redis Stream 'telemetry:stream' length: {initial_xlen}")

    # -------------------------------------------------------------
    # 2. Ingest 1 Valid Batch with ?mode=stream
    # -------------------------------------------------------------
    print_banner("2. Ingesting Valid Batch of 2 Spans via ?mode=stream")
    test_trace_id = uuid.uuid4().hex  # 32-char hex
    root_span_id = uuid.uuid4().hex[:16]  # 16-char hex
    child_span_id = uuid.uuid4().hex[:16]  # 16-char hex

    valid_batch = {
        "service_name": "checkout-async-service",
        "spans": [
            {
                "span_id": root_span_id,
                "trace_id": test_trace_id,
                "parent_span_id": None,
                "operation_name": "async_process_order",
                "start_time_unix_nano": 1720000000000000000,
                "end_time_unix_nano": 1720000000150000000,
                "status_code": "OK",
                "attributes": {
                    "http.method": "POST",
                    "customer.type": "vip",
                    "mode": "stream_test",
                },
            },
            {
                "span_id": child_span_id,
                "trace_id": test_trace_id,
                "parent_span_id": root_span_id,
                "operation_name": "async_charge_customer",
                "start_time_unix_nano": 1720000000030000000,
                "end_time_unix_nano": 1720000000120000000,
                "status_code": "OK",
                "attributes": {
                    "payment.gateway": "stripe",
                    "amount": 249.99,
                },
            },
        ],
    }

    resp_valid = client.post("/v1/telemetry/spans?mode=stream", json=valid_batch)
    print(f"POST /v1/telemetry/spans?mode=stream -> HTTP {resp_valid.status_code}")
    print(f"Response: {resp_valid.text}")
    assert resp_valid.status_code == 202, f"Expected 202, got {resp_valid.status_code}"
    queued_data = resp_valid.json()
    assert queued_data["status"] == "queued"
    assert "stream_id" in queued_data
    assert queued_data["span_count"] == 2
    stream_id = queued_data["stream_id"]
    print(f"[PASS] HTTP 202 Accepted with stream_id='{stream_id}' and span_count=2.")

    # Verify message in Redis stream
    after_valid_xlen = int(exec_redis_cmd("xlen", "telemetry:stream"))
    print(f"Redis Stream 'telemetry:stream' length after ingest: {after_valid_xlen}")
    assert after_valid_xlen == initial_xlen + 1, f"Expected stream length {initial_xlen + 1}, got {after_valid_xlen}"
    print("[PASS] Stream length incremented by 1 in Redis.")

    # Inspect last message in Redis
    stream_content = exec_redis_cmd("xrange", "telemetry:stream", stream_id, stream_id)
    print(f"XRANGE for message {stream_id}:\n{stream_content}")

    # -------------------------------------------------------------
    # 3. Ingest 1 Invalid Batch (10-char trace_id) with ?mode=stream
    # -------------------------------------------------------------
    print_banner("3. Ingesting Invalid Batch (10-char trace_id) via ?mode=stream")
    invalid_batch = {
        "service_name": "broken-service",
        "spans": [
            {
                "span_id": "1111222233334444",
                "trace_id": "invalid10c",  # 10 chars, regex requires 32 hex chars
                "parent_span_id": None,
                "operation_name": "broken_op",
                "start_time_unix_nano": 1720000000000000000,
                "end_time_unix_nano": 1720000000100000000,
                "status_code": "OK",
                "attributes": {},
            }
        ],
    }

    resp_invalid = client.post("/v1/telemetry/spans?mode=stream", json=invalid_batch)
    print(f"POST /v1/telemetry/spans?mode=stream (invalid) -> HTTP {resp_invalid.status_code}")
    print(f"Response: {resp_invalid.text}")
    assert resp_invalid.status_code == 422, f"Expected 422, got {resp_invalid.status_code}"
    print("[PASS] HTTP 422 Unprocessable Entity returned.")

    # Verify Redis stream length DID NOT increase
    after_invalid_xlen = int(exec_redis_cmd("xlen", "telemetry:stream"))
    print(f"Redis Stream length after invalid request: {after_invalid_xlen}")
    assert after_invalid_xlen == after_valid_xlen, "Stream length must not increase on rejected batch"
    print("[PASS] Stream length remained unchanged. Zero items added to Redis.")

    # -------------------------------------------------------------
    # 4. Execute worker.py --once
    # -------------------------------------------------------------
    print_banner("4. Running backend/app/worker.py --once")
    worker_proc = subprocess.run(
        [
            r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\.venv\Scripts\python.exe",
            r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\app\worker.py",
            "--once",
        ],
        capture_output=True,
        text=True,
    )
    print("Worker stdout:")
    print(worker_proc.stdout)
    if worker_proc.stderr:
        print("Worker stderr:")
        print(worker_proc.stderr)
    assert worker_proc.returncode == 0, f"Worker exited with code {worker_proc.returncode}"
    print("[PASS] worker.py executed cleanly with exit code 0.")

    # Verify XPENDING is 0 (all messages processed and XACKed)
    xpending_out = exec_redis_cmd("xpending", "telemetry:stream", "pulseops-ingest-group")
    print(f"XPENDING telemetry:stream pulseops-ingest-group:\n{xpending_out}")
    # Format of xpending:
    # 1) count
    # 2) lowest id
    # 3) greatest id
    # 4) consumers count
    # Or '0 (nil) (nil)'
    assert xpending_out.startswith("0") or "0" in xpending_out.splitlines()[0], f"Pending messages found: {xpending_out}"
    print("[PASS] XPENDING confirms 0 pending (un-ACKed) messages.")

    # -------------------------------------------------------------
    # 5. Query GET /v1/traces/{trace_id}
    # -------------------------------------------------------------
    print_banner("5. Querying GET /v1/traces/{trace_id} from PostgreSQL")
    resp_trace = client.get(f"/v1/traces/{test_trace_id}")
    print(f"GET /v1/traces/{test_trace_id} -> HTTP {resp_trace.status_code}")
    print(f"Response: {resp_trace.text}")
    assert resp_trace.status_code == 200, f"Expected 200, got {resp_trace.status_code}"
    trace_data = resp_trace.json()
    assert trace_data["trace_id"] == test_trace_id
    assert trace_data["service_name"] == "checkout-async-service"
    assert trace_data["span_count"] == 2
    spans = trace_data["spans"]
    assert len(spans) == 2
    assert spans[0]["span_id"] == root_span_id
    assert spans[1]["span_id"] == child_span_id
    assert spans[0]["operation_name"] == "async_process_order"
    assert spans[1]["operation_name"] == "async_charge_customer"
    assert isinstance(spans[0]["duration_ms"], float)
    assert abs(spans[0]["duration_ms"] - 150.0) < 0.01
    assert abs(spans[1]["duration_ms"] - 90.0) < 0.01
    assert spans[0]["attributes"]["customer.type"] == "vip"
    assert spans[1]["attributes"]["amount"] == 249.99
    print(f"[PASS] Trace verified in PostgreSQL! Root duration: {spans[0]['duration_ms']}ms, Child duration: {spans[1]['duration_ms']}ms.")

    print_banner("ALL 5 DAY 3 VERIFICATION CHECKS PASSED PERFECTLY!")


if __name__ == "__main__":
    main()
