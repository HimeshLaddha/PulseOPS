import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
import httpx
import redis.asyncio as aioredis

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.worker import process_batch_message

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


def run_worker_once(*extra_args: str) -> subprocess.CompletedProcess:
    cmd = [
        r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\.venv\Scripts\python.exe",
        r"c:\Users\Admin\OneDrive\Desktop\PulseOPS\backend\app\worker.py",
        "--once",
        *extra_args,
    ]
    return subprocess.run(
        cmd,
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
        print("[S1] Launching recovery worker pass (worker.py --once --min-idle-ms 0)...")
        w1_res = run_worker_once("--min-idle-ms", "0")
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

        # -----------------------------------------------------------------
        # SCENARIO 6: DLQ Resilience Test (Simulated Failure During DLQ Write)
        # -----------------------------------------------------------------
        print_banner("SCENARIO 6: DLQ Resilience Test (Failure During DLQ Write)")
        # 1. Inject a poison pill into main stream
        s6_poison_id = await r.xadd(STREAM_KEY, {"data": "malformed-unparseable-data-xyz!{}"})
        print(f"[S6] Injected poison pill message {s6_poison_id} into {STREAM_KEY}")

        # 2. Consumer reads poison pill with XREADGROUP (putting it into PEL)
        s6_consumer = f"dlq-worker-{uuid.uuid4().hex[:6]}"
        s6_read = await r.xreadgroup(
            groupname=GROUP_NAME,
            consumername=s6_consumer,
            streams={STREAM_KEY: ">"},
            count=1,
        )
        assert s6_read, "Expected to read 1 message"
        read_msg = s6_read[0][1][0]
        s6_msg_id = read_msg[0]
        s6_fields = read_msg[1]
        assert s6_msg_id == s6_poison_id

        # Verify message is currently in PEL
        p_before = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_before["pending"] >= 1, "Poison pill must be in PEL"

        # 3. Simulate failure during deadletter stream write using proxy
        class FailingRedisDLQProxy:
            """Proxy wrapping redis client that raises on deadletter XADD to simulate DLQ failure."""
            def __init__(self, target_redis):
                self._target = target_redis

            async def xadd(self, name, fields, *args, **kwargs):
                if "deadletter" in name:
                    raise ConnectionError("Simulated Redis ConnectionError during DLQ write")
                return await self._target.xadd(name, fields, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._target, name)

        failing_redis = FailingRedisDLQProxy(r)

        # 4. Attempt to process message with failing DLQ write
        dlq_write_failed = False
        try:
            await process_batch_message(
                pool=None,
                redis_client=failing_redis,
                stream_key=STREAM_KEY,
                group_name=GROUP_NAME,
                msg_id=s6_msg_id,
                fields=s6_fields,
            )
        except ConnectionError as dl_err:
            dlq_write_failed = True
            print(f"[S6] Caught expected DLQ failure: {dl_err}")

        assert dlq_write_failed, "Expected process_batch_message to fail when DLQ write fails"

        # 5. Assert that XACK was NEVER called and message remains in XPENDING
        p_after = await r.xpending(STREAM_KEY, GROUP_NAME)
        print(f"[S6] XPENDING count after simulated DLQ failure: {p_after['pending']}")
        assert p_after["pending"] >= 1, "Original message must remain in PEL when DLQ write fails!"

        # Assert message is still present in main stream (not deleted)
        s6_stream_range = await r.xrange(STREAM_KEY, min=s6_msg_id, max=s6_msg_id)
        assert len(s6_stream_range) == 1, "Original message must NOT be deleted from stream if DLQ write fails!"
        print("[S6 PASS] Verified message remains un-ACKed in XPENDING and undeleted from stream upon DLQ failure.")

        # 6. Simulate DLQ recovery: process with healthy Redis client
        print("[S6] Simulating DLQ recovery: processing with healthy Redis client...")
        inserted_res = await process_batch_message(
            pool=None,
            redis_client=r,
            stream_key=STREAM_KEY,
            group_name=GROUP_NAME,
            msg_id=s6_msg_id,
            fields=s6_fields,
        )
        assert inserted_res == 0

        # Verify deadletter stream received the message and PEL is now clear
        dl_len = await r.xlen(f"{STREAM_KEY}:deadletter")
        assert dl_len >= 1
        p_recovered = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_recovered["pending"] == 0
        print("[S6 PASS] Message successfully routed to DLQ and ACKed once DLQ recovered.")

        # -----------------------------------------------------------------
        # SCENARIO 7: Concurrent Worker Idle Window Test
        # -----------------------------------------------------------------
        print_banner("SCENARIO 7: Concurrent Worker Idle Window Test")
        s7_trace_id = uuid.uuid4().hex
        s7_span_id = uuid.uuid4().hex[:16]
        s7_payload = {
            "service_name": "fault-s7-idle-service",
            "spans": [
                {
                    "span_id": s7_span_id,
                    "trace_id": s7_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s7_idle_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000050000000,
                    "status_code": "OK",
                    "attributes": {"scenario": 7},
                }
            ],
        }

        # 1. Ingest batch via stream
        resp_s7 = client.post("/v1/telemetry/spans?mode=stream", json=s7_payload)
        assert resp_s7.status_code == 202
        s7_stream_id = resp_s7.json()["stream_id"]
        print(f"[S7] Ingested message {s7_stream_id}")

        # 2. Worker A reads the message with XREADGROUP (putting it in PEL under worker-A)
        consumer_a = f"worker-A-{uuid.uuid4().hex[:4]}"
        read_a = await r.xreadgroup(
            groupname=GROUP_NAME,
            consumername=consumer_a,
            streams={STREAM_KEY: ">"},
            count=1,
        )
        assert read_a, "Worker A should have read 1 message"
        print(f"[S7] Worker A ('{consumer_a}') read message '{s7_stream_id}'. Message idle time is ~0ms.")

        # 3. Worker B attempts to reclaim with min_idle_time = 10000 ms (10 seconds)
        consumer_b = f"worker-B-{uuid.uuid4().hex[:4]}"
        claim_attempt_1 = await r.xautoclaim(
            name=STREAM_KEY,
            groupname=GROUP_NAME,
            consumername=consumer_b,
            min_idle_time=10000,
            start_id="0-0",
            count=10,
        )
        claimed_messages_1 = claim_attempt_1[1] if len(claim_attempt_1) > 1 else []
        print(f"[S7] Worker B claim attempt with min_idle_time=10000ms claimed: {len(claimed_messages_1)} messages")
        assert len(claimed_messages_1) == 0, "Worker B must NOT claim message whose idle time is < 10000ms!"

        # Also test with CLI worker using default 5000ms idle window
        w_b_res = run_worker_once("--consumer-name", consumer_b, "--min-idle-ms", "5000")
        assert w_b_res.returncode == 0
        # Trace should NOT be in PostgreSQL yet because it was not claimed
        resp_s7_check = client.get(f"/v1/traces/{s7_trace_id}")
        assert resp_s7_check.status_code == 404, "Trace must NOT be persisted by Worker B before idle window expires!"
        print("[S7 PASS] Confirmed: Active message (idle < min_idle_ms) is protected from premature reclamation.")

        # 4. Now Worker B reclaims with min_idle_time = 0ms (simulating expired idle window)
        print("[S7] Worker B reclaiming with min_idle_ms = 0 (simulating expired idle window)...")
        w_b_reclaim = run_worker_once("--consumer-name", consumer_b, "--min-idle-ms", "0")
        assert w_b_reclaim.returncode == 0

        # Verify trace is now persisted in PostgreSQL
        resp_s7_persisted = client.get(f"/v1/traces/{s7_trace_id}")
        assert resp_s7_persisted.status_code == 200
        assert resp_s7_persisted.json()["span_count"] == 1

        # Verify PEL is 0
        p_s7_final = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_s7_final["pending"] == 0
        print("[S7 PASS] Message successfully reclaimed and persisted after idle window satisfied.")

        # -----------------------------------------------------------------
        # SCENARIO 8: DLQ Duplicate Delivery Safety (Un-ACKed Poison Pill Processed Twice)
        # -----------------------------------------------------------------
        print_banner("SCENARIO 8: DLQ Duplicate Delivery Safety (Un-ACKed Poison Pill Reprocessed)")
        # 1. Inject a poison pill message into the stream
        s8_poison_id = await r.xadd(STREAM_KEY, {"data": "malformed-json-test-duplicate-dlq{{{!!!"})
        print(f"[S8] Injected poison pill message {s8_poison_id} into {STREAM_KEY}")

        # 2. Worker reads the poison pill into PEL via XREADGROUP
        s8_consumer = f"poison-worker-{uuid.uuid4().hex[:6]}"
        s8_read = await r.xreadgroup(
            groupname=GROUP_NAME,
            consumername=s8_consumer,
            streams={STREAM_KEY: ">"},
            count=1,
        )
        assert s8_read, "Expected to read 1 poison pill message"
        s8_read_id = s8_read[0][1][0][0]
        s8_fields = s8_read[0][1][0][1]
        assert s8_read_id == s8_poison_id

        # 3. Simulate first attempt: Worker routes poison pill to DLQ, but crashes immediately BEFORE XACK/XDEL
        print("[S8] Attempt 1: Routing to DLQ but simulating crash before XACK/XDEL...")
        await r.xadd(
            f"{STREAM_KEY}:deadletter",
            {"original_id": s8_poison_id, "error": "Simulated pre-ACK crash", "data": str(s8_fields.get("data"))},
        )
        # Poison pill is now in DLQ, but still un-ACKed in PEL!
        p_s8_before = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_s8_before["pending"] >= 1, "Poison pill must remain pending in PEL before attempt 2"

        # 4. Attempt 2: Recovery worker pass re-processes the un-ACKed poison pill
        print("[S8] Attempt 2: Recovery worker reclaiming and re-processing un-ACKed poison pill...")
        w8_res = run_worker_once("--min-idle-ms", "0")
        print("[S8] Worker Replay Output:\n", w8_res.stderr or w8_res.stdout)
        assert w8_res.returncode == 0, f"Worker crashed on duplicate delivery! stderr: {w8_res.stderr}"
        assert "Poison pill detected" in (w8_res.stderr or w8_res.stdout)

        # 5. Assert PEL is cleared and pipeline remains fully operational
        p_s8_after = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_s8_after["pending"] == 0, f"Expected 0 pending in PEL, got {p_s8_after['pending']}"

        # 6. Verify pipeline processes subsequent valid message without any issue
        s8_valid_trace_id = uuid.uuid4().hex
        s8_valid_payload = {
            "service_name": "fault-s8-subsequent",
            "spans": [
                {
                    "span_id": uuid.uuid4().hex[:16],
                    "trace_id": s8_valid_trace_id,
                    "parent_span_id": None,
                    "operation_name": "s8_subsequent_op",
                    "start_time_unix_nano": 1721000000000000000,
                    "end_time_unix_nano": 1721000000030000000,
                    "status_code": "OK",
                    "attributes": {"after_s8": True},
                }
            ],
        }
        resp_s8 = client.post("/v1/telemetry/spans?mode=stream", json=s8_valid_payload)
        assert resp_s8.status_code == 202
        run_worker_once()
        resp_s8_check = client.get(f"/v1/traces/{s8_valid_trace_id}")
        assert resp_s8_check.status_code == 200
        assert resp_s8_check.json()["span_count"] == 1
        print("[S8 PASS] DLQ duplicate delivery safely processed without crashing and cleared PEL.")

        # -----------------------------------------------------------------
        # SCENARIO 9: XAUTOCLAIM Nil / Deleted Message Handling
        # -----------------------------------------------------------------
        print_banner("SCENARIO 9: XAUTOCLAIM Nil / Deleted Message Handling")
        # 1. Add a temporary message to the stream
        s9_msg_id = await r.xadd(STREAM_KEY, {"data": "temporary-message-to-be-deleted"})
        print(f"[S9] Injected temporary message {s9_msg_id}")

        # 2. Read it into PEL with a consumer
        s9_consumer = f"phantom-consumer-{uuid.uuid4().hex[:6]}"
        s9_read = await r.xreadgroup(
            groupname=GROUP_NAME,
            consumername=s9_consumer,
            streams={STREAM_KEY: ">"},
            count=1,
        )
        assert s9_read, "Expected to read temporary message into PEL"
        assert s9_read[0][1][0][0] == s9_msg_id

        # 3. Delete the message directly from the stream before ACKing (creates phantom entry in PEL)
        await r.xdel(STREAM_KEY, s9_msg_id)
        print(f"[S9] Deleted message {s9_msg_id} from stream while remaining un-ACKed in PEL.")

        # Verify it is still in PEL
        p_s9_before = await r.xpending(STREAM_KEY, GROUP_NAME)
        assert p_s9_before["pending"] >= 1, "Deleted message must still be in PEL"

        # 4. Run worker with min-idle-ms 0 to trigger XAUTOCLAIM on the deleted message
        print("[S9] Running worker to reclaim pending deleted/nil message...")
        w9_res = run_worker_once("--min-idle-ms", "0")
        print("[S9] Worker Output:\n", w9_res.stderr or w9_res.stdout)
        assert w9_res.returncode == 0, f"Worker crashed on nil/deleted message! stderr: {w9_res.stderr}"

        # 5. Verify the phantom entry was safely discarded and cleared from PEL
        p_s9_after = await r.xpending(STREAM_KEY, GROUP_NAME)
        print(f"[S9] PEL count after XAUTOCLAIM nil handling: {p_s9_after['pending']}")
        assert p_s9_after["pending"] == 0, f"Phantom message was not evicted from PEL! Remaining: {p_s9_after['pending']}"
        print("[S9 PASS] XAUTOCLAIM safely handled deleted/nil message and evicted phantom entry from PEL.")

        print_banner("ALL 9 FAULT-TOLERANCE SCENARIOS PASSED WITH ZERO ERRORS!")

    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
