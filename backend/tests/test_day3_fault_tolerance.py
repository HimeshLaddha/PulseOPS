import asyncio
import json
import subprocess
import time
import uuid
import httpx
import redis.asyncio as aioredis

BASE_URL = "http://127.0.0.1:8000"
REDIS_URL = "redis://localhost:6379/0"
STREAM_KEY = "telemetry:stream"
GROUP_NAME = "pulseops-ingest-group"


def print_banner(msg: str):
    print("\n" + "=" * 75)
    print(msg)
    print("=" * 75)


def run_docker_cmd(*args: str) -> str:
    res = subprocess.run(["docker", *args], capture_output=True, text=True, check=True)
    return res.stdout.strip()


def exec_redis_cmd(*args: str) -> str:
    return run_docker_cmd("exec", "pulseops-redis", "redis-cli", *args)


def run_worker_once() -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\.venv\Scripts\python.exe",
            r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\app\worker.py",
            "--once",
        ],
        capture_output=True,
        text=True,
    )


def wait_for_postgres(timeout: int = 30):
    start = time.time()
    while time.time() - start < timeout:
        res = subprocess.run(
            ["docker", "exec", "pulseops-postgres", "pg_isready", "-U", "pulseops", "-d", "pulseops_db"],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0:
            time.sleep(1)
            return
        time.sleep(1)
    raise TimeoutError("PostgreSQL did not become ready in time.")


def wait_for_redis(timeout: int = 30):
    start = time.time()
    while time.time() - start < timeout:
        res = subprocess.run(
            ["docker", "exec", "pulseops-redis", "redis-cli", "ping"],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and "PONG" in res.stdout:
            time.sleep(1)
            return
        time.sleep(1)
    raise TimeoutError("Redis did not become ready in time.")


async def main():
    print_banner("DAY 3 CHECKPOINT 4: FAULT INJECTION & CORRECTNESS VERIFICATION")
    client = httpx.Client(base_url=BASE_URL, timeout=10.0)

    # Pre-flight check
    resp_health = client.get("/health")
    print("Pre-flight Healthcheck:", resp_health.json())
    assert resp_health.status_code == 200

    r = aioredis.from_url(REDIS_URL, decode_responses=True)

    try:
        # Ensure consumer group exists and stream is drained
        try:
            await r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        except Exception:
            pass
        run_worker_once()

        # -----------------------------------------------------------------
        # SCENARIO 1: Worker Crash Simulation (Pre-Commit)
        # -----------------------------------------------------------------
        print_banner("SCENARIO 1: Worker Crash Simulation (Pre-Commit)")
        s1_trace_id = uuid.uuid4().hex
        s1_span_1 = uuid.uuid4().hex[:16]
        s1_span_2 = uuid.uuid4().hex[:16]

        s1_payload = {
            "service_name": "fault-s1-service",
            "spans": [
                {
                    "span_id": s1_span_1,
                    "trace_id": s1_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s1_root_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000100000000,
                    "status_code": "OK",
                    "attributes": {"scenario": 1},
                },
                {
                    "span_id": s1_span_2,
                    "trace_id": s1_trace_id,
                    "parent_span_id": s1_span_1,
                    "operation_name": "s1_child_op",
                    "start_time_unix_nano": 1721000000020000000,
                    "end_time_unix_nano": 1721000000080000000,
                    "status_code": "OK",
                    "attributes": {"scenario": 1},
                },
            ],
        }

        # 1. Ingest via stream
        resp = client.post("/v1/telemetry/spans?mode=stream", json=s1_payload)
        assert resp.status_code == 202
        s1_stream_id = resp.json()["stream_id"]
        print(f"[S1] Ingested batch with stream_id={s1_stream_id}")

        # 2. Simulate crashed worker reading message but crashing before commit
        dummy_consumer = f"crashed-worker-{uuid.uuid4().hex[:6]}"
        read_res = await r.xreadgroup(
            groupname=GROUP_NAME,
            consumername=dummy_consumer,
            streams={STREAM_KEY: ">"},
            count=1,
        )
        assert read_res, "Dummy crashed worker should have read 1 message"
        read_msg_id = read_res[0][1][0][0]
        assert read_msg_id == s1_stream_id
        print(f"[S1] Worker '{dummy_consumer}' read message '{read_msg_id}' and simulated CRASH pre-commit (no DB write, no XACK)")

        # 3. Assert XPENDING reports 1 unacknowledged message
        pending_info = await r.xpending(STREAM_KEY, GROUP_NAME)
        pending_count = pending_info["pending"]
        print(f"[S1] Current XPENDING count: {pending_count}")
        assert pending_count >= 1, "Expected at least 1 pending un-ACKed message"
        print("[S1 PASS] Verified unacknowledged message in Redis PEL.")

        # 4. Start recovery worker pass
        print("[S1] Launching recovery worker pass (worker.py --once)...")
        w1_res = run_worker_once()
        print("[S1] Recovery Worker Output:\n", w1_res.stderr or w1_res.stdout)
        assert w1_res.returncode == 0

        # 5. Assert message is in PostgreSQL and XPENDING is 0
        resp_check = client.get(f"/v1/traces/{s1_trace_id}")
        assert resp_check.status_code == 200
        assert resp_check.json()["span_count"] == 2
        print("[S1 PASS] Trace successfully recovered and queryable from PostgreSQL.")

        pending_info_after = await r.xpending(STREAM_KEY, GROUP_NAME)
        print(f"[S1] XPENDING count after recovery: {pending_info_after['pending']}")
        assert pending_info_after["pending"] == 0
        print("[S1 PASS] XACK completed. Zero pending messages remain.")

        # -----------------------------------------------------------------
        # SCENARIO 2: Idempotent Replay (Crash Post-Commit, Pre-XACK)
        # -----------------------------------------------------------------
        print_banner("SCENARIO 2: Idempotent Replay (Crash Post-Commit, Pre-XACK)")
        # Re-inject the exact same spans into the stream
        re_stream_id = await r.xadd(STREAM_KEY, {"data": json.dumps(s1_payload)})
        print(f"[S2] Injected duplicate batch for trace {s1_trace_id} with stream_id={re_stream_id}")

        # Worker processes the duplicate message
        w2_res = run_worker_once()
        print("[S2] Worker Replay Output:\n", w2_res.stderr or w2_res.stdout)
        assert w2_res.returncode == 0
        assert "Persisted 0 spans" in (w2_res.stderr or w2_res.stdout) or "Persisted" in (w2_res.stderr or w2_res.stdout)

        # Assert no unique constraint error, XACK succeeded, and DB trace still has exactly 2 spans
        resp_s2 = client.get(f"/v1/traces/{s1_trace_id}")
        assert resp_s2.status_code == 200
        spans_count = resp_s2.json()["span_count"]
        print(f"[S2] Spans count for trace {s1_trace_id}: {spans_count}")
        assert spans_count == 2, f"Expected exactly 2 spans (no duplicates), got {spans_count}"

        pending_s2 = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert pending_s2["pending"] == 0
        print("[S2 PASS] Idempotent replay succeeded with zero duplicate rows and zero un-ACKed messages.")

        # -----------------------------------------------------------------
        # SCENARIO 3: Database Outage Resilience
        # -----------------------------------------------------------------
        print_banner("SCENARIO 3: Database Outage Resilience")
        print("[S3] Stopping PostgreSQL container (docker compose stop postgres)...")
        run_docker_cmd("compose", "stop", "postgres")

        s3_trace_id = uuid.uuid4().hex
        s3_payload = {
            "service_name": "fault-s3-service",
            "spans": [
                {
                    "span_id": uuid.uuid4().hex[:16],
                    "trace_id": s3_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s3_outage_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000050000000,
                    "status_code": "OK",
                    "attributes": {"during_outage": True},
                }
            ],
        }

        # Ingestion API must STILL accept message with HTTP 202
        print("[S3] Sending batch to POST /v1/telemetry/spans?mode=stream during PostgreSQL outage...")
        resp_s3 = client.post("/v1/telemetry/spans?mode=stream", json=s3_payload)
        print(f"[S3] API Response during DB outage: HTTP {resp_s3.status_code} {resp_s3.text}")
        assert resp_s3.status_code == 202, f"Expected 202 during DB outage, got {resp_s3.status_code}"
        s3_stream_id = resp_s3.json()["stream_id"]
        print(f"[S3 PASS] Ingestion API insulated caller and buffered message {s3_stream_id} in Redis.")

        # Run worker while DB is down
        print("[S3] Running worker while PostgreSQL is stopped...")
        w3_outage = run_worker_once()
        print("[S3] Worker Output (PostgreSQL stopped):\n", w3_outage.stderr or w3_outage.stdout)
        # Worker should handle connection error gracefully without unhandled crash
        assert w3_outage.returncode == 0
        print("[S3 PASS] Worker handled database outage gracefully without crashing.")

        # Message must still exist in Redis
        s3_xlen = int(exec_redis_cmd("xlen", STREAM_KEY))
        print(f"[S3] Redis stream length while DB stopped: {s3_xlen}")
        assert s3_xlen >= 1

        # Restart PostgreSQL
        print("[S3] Starting PostgreSQL container (docker compose start postgres)...")
        run_docker_cmd("compose", "start", "postgres")
        wait_for_postgres()
        print("[S3] PostgreSQL is healthy and accepting connections.")

        # Drain queued message with worker
        print("[S3] Running worker after PostgreSQL recovery...")
        w3_recovery = run_worker_once()
        print("[S3] Worker Output (After PG recovery):\n", w3_recovery.stderr or w3_recovery.stdout)
        assert w3_recovery.returncode == 0

        # Verify trace now exists in PostgreSQL
        resp_s3_check = client.get(f"/v1/traces/{s3_trace_id}")
        assert resp_s3_check.status_code == 200
        assert resp_s3_check.json()["span_count"] == 1
        print("[S3 PASS] Queued spans persisted with zero loss after database recovery.")

        # -----------------------------------------------------------------
        # SCENARIO 4: Redis AOF Persistence Across Restarts
        # -----------------------------------------------------------------
        print_banner("SCENARIO 4: Redis AOF Persistence Across Restarts")
        s4_trace_id = uuid.uuid4().hex
        s4_payload = {
            "service_name": "fault-s4-service",
            "spans": [
                {
                    "span_id": uuid.uuid4().hex[:16],
                    "trace_id": s4_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s4_aof_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000070000000,
                    "status_code": "OK",
                    "attributes": {"test": "aof"},
                }
            ],
        }

        # Ingest message
        resp_s4 = client.post("/v1/telemetry/spans?mode=stream", json=s4_payload)
        assert resp_s4.status_code == 202
        s4_msg_id = resp_s4.json()["stream_id"]
        print(f"[S4] Ingested message {s4_msg_id} prior to Redis restart.")

        # Close client connection before restarting Redis
        await r.aclose()

        # Restart Redis container
        print("[S4] Restarting Redis container (docker compose restart redis)...")
        run_docker_cmd("compose", "restart", "redis")
        wait_for_redis()
        print("[S4] Redis restarted.")

        # Re-open client connection and verify stream length
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        stream_len_after_restart = await r.xlen(STREAM_KEY)
        print(f"[S4] Stream length after Redis restart: {stream_len_after_restart}")
        assert stream_len_after_restart >= 1, "Stream message was lost on Redis restart!"
        print("[S4 PASS] Redis AOF persistence preserved in-flight stream entries across restart.")

        # Drain with worker
        w4_res = run_worker_once()
        assert w4_res.returncode == 0
        resp_s4_check = client.get(f"/v1/traces/{s4_trace_id}")
        assert resp_s4_check.status_code == 200
        assert resp_s4_check.json()["span_count"] == 1
        print("[S4 PASS] In-flight message drained and persisted into PostgreSQL after restart.")

        # Ensure API health check after Redis restart
        time.sleep(2)
        h_check = client.get("/health")
        print("[S4] Healthcheck after Redis restart:", h_check.json())
        assert h_check.status_code == 200

        # -----------------------------------------------------------------
        # SCENARIO 5: Poison Pill Handling
        # -----------------------------------------------------------------
        print_banner("SCENARIO 5: Poison Pill Handling")
        # 1. Inject malformed message into stream
        poison_id = await r.xadd(STREAM_KEY, {"data": "not-valid-json-string-content{{{!"})
        print(f"[S5] Injected poison pill message {poison_id} into {STREAM_KEY}")

        # 2. Inject a subsequent VALID batch
        s5_valid_trace_id = uuid.uuid4().hex
        s5_valid_payload = {
            "service_name": "fault-s5-subsequent",
            "spans": [
                {
                    "span_id": uuid.uuid4().hex[:16],
                    "trace_id": s5_valid_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s5_subsequent_valid_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000040000000,
                    "status_code": "OK",
                    "attributes": {"subsequent": True},
                }
            ],
        }
        resp_s5 = client.post("/v1/telemetry/spans?mode=stream", json=s5_valid_payload)
        print(f"[S5] API Response: HTTP {resp_s5.status_code} {resp_s5.text}")
        assert resp_s5.status_code == 202, f"Expected 202, got {resp_s5.status_code}: {resp_s5.text}"
        s5_valid_msg_id = resp_s5.json()["stream_id"]
        print(f"[S5] Injected subsequent valid message {s5_valid_msg_id}")

        # 3. Run worker
        w5_res = run_worker_once()
        print("[S5] Worker Output:\n", w5_res.stderr or w5_res.stdout)
        assert w5_res.returncode == 0
        assert "Poison pill detected" in (w5_res.stderr or w5_res.stdout)
        print("[S5 PASS] Worker intercepted poison pill without crashing.")

        # 4. Verify deadletter stream received the poison pill
        deadletter_entries = await r.xlen(f"{STREAM_KEY}:deadletter")
        print(f"[S5] Deadletter stream '{STREAM_KEY}:deadletter' length: {deadletter_entries}")
        assert deadletter_entries >= 1
        print("[S5 PASS] Poison pill was safely routed to deadletter stream.")

        # 5. Verify subsequent valid batch was processed and exists in PostgreSQL
        resp_s5_check = client.get(f"/v1/traces/{s5_valid_trace_id}")
        assert resp_s5_check.status_code == 200
        assert resp_s5_check.json()["span_count"] == 1
        print(f"[S5 PASS] Subsequent valid trace {s5_valid_trace_id} successfully persisted in PostgreSQL.")

        # 6. Verify XPENDING is 0
        pending_final = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert pending_final["pending"] == 0
        print("[S5 PASS] Main stream PEL has 0 unacknowledged messages.")

        print_banner("ALL 5 FAULT-TOLERANCE SCENARIOS PASSED WITH ZERO ERRORS!")

    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
