import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, List

import httpx
import redis.asyncio as aioredis
import asyncpg

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.config import get_settings

BASE_URL = "http://127.0.0.1:8000"
RESULTS_FILE = os.path.join(SCRIPT_DIR, "results", "profiling_breakdown.json")
WORKER_RESULTS_FILE = os.path.join(SCRIPT_DIR, "results", "worker_profiling.json")


def generate_span_batch(service_name: str, spans_count: int, trace_id: str) -> Dict[str, Any]:
    now_nano = time.time_ns()
    spans = []
    for i in range(spans_count):
        s_start = now_nano + (i * 1_000_000)
        s_end = s_start + 5_000_000
        spans.append({
            "span_id": uuid.uuid4().hex[:16],
            "trace_id": trace_id,
            "parent_span_id": None if i == 0 else spans[0]["span_id"],
            "operation_name": f"profile_op_{i}",
            "start_time_unix_nano": s_start,
            "end_time_unix_nano": s_end,
            "status_code": "OK",
            "attributes": {"batch_index": i},
        })
    return {"service_name": service_name, "spans": spans}


async def main():
    settings = get_settings()
    print("=" * 80)
    print("PULSEOPS DAY 4: PIPELINE PROFILING RUN (SCENARIO D: 50,000 SPANS)")
    print("=" * 80)

    # 1. Pre-flight check
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        resp = await client.get("/health")
        print("API Healthcheck:", resp.json())
        assert resp.status_code == 200, "API is not healthy!"

        # Reset Gateway metrics
        reset_resp = await client.post("/v1/telemetry/profiling/reset")
        print("Gateway Profiler Reset:", reset_resp.json())

    # 2. Redis & PostgreSQL pre-cleaning
    r = aioredis.from_url(settings.redis_url, decode_responses=True)
    await r.delete(settings.REDIS_STREAM_KEY, f"{settings.REDIS_STREAM_KEY}:deadletter")
    try:
        await r.xgroup_create(settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass
    await r.aclose()

    pg_init = await asyncpg.connect(settings.database_url)
    await pg_init.execute("DELETE FROM spans WHERE service_name = 'profile-service';")
    await pg_init.close()

    if os.path.exists(WORKER_RESULTS_FILE):
        os.remove(WORKER_RESULTS_FILE)

    # 3. Start Background Worker Process
    worker_log_path = os.path.join(SCRIPT_DIR, "results", "worker.log")
    worker_log_file = open(worker_log_path, "w", encoding="utf-8")
    worker_cmd = [
        sys.executable,
        "-u",
        os.path.join(BACKEND_DIR, "app", "worker.py"),
        "--loop",
        "--batch-size", "50",
        "--block-ms", "500",
    ]
    print(f"Starting consumer worker: {' '.join(worker_cmd)} (logging to {worker_log_path})")
    worker_proc = subprocess.Popen(worker_cmd, stdout=worker_log_file, stderr=subprocess.STDOUT, text=True)
    time.sleep(1.5)  # Let worker initialize DB pool and Redis

    TOTAL_REQUESTS = 500
    SPANS_PER_REQ = 100
    CONCURRENCY = 10
    EXPECTED_SPANS = TOTAL_REQUESTS * SPANS_PER_REQ  # 50,000 spans

    print(f"\n[WORKLOAD] Dispatching {TOTAL_REQUESTS} requests ({SPANS_PER_REQ} spans/req) at concurrency {CONCURRENCY} ...")

    # 4. Queue depth tracker task
    queue_progression: List[Dict[str, Any]] = []
    tracking = True

    async def track_queue():
        r_track = aioredis.from_url(settings.redis_url, decode_responses=True)
        t_start = time.perf_counter()
        while tracking:
            try:
                xlen = await r_track.xlen(settings.REDIS_STREAM_KEY)
                queue_progression.append({
                    "time_s": round(time.perf_counter() - t_start, 2),
                    "backlog_xlen": xlen,
                })
            except Exception:
                pass
            await asyncio.sleep(0.25)
        await r_track.aclose()

    tracker_task = asyncio.create_task(track_queue())

    # 5. Fire HTTP Requests
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies: List[float] = []

    async with httpx.AsyncClient(base_url=BASE_URL, limits=limits, timeout=timeout) as client:
        wall_start = time.perf_counter()

        async def send_req(i: int):
            trace_id = uuid.uuid4().hex
            payload = generate_span_batch("profile-service", SPANS_PER_REQ, trace_id)
            async with sem:
                t0 = time.perf_counter()
                resp = await client.post("/v1/telemetry/spans?mode=stream", json=payload)
                t_elapsed = (time.perf_counter() - t0) * 1000.0
                assert resp.status_code == 202
                latencies.append(t_elapsed)

        tasks = [send_req(i) for i in range(TOTAL_REQUESTS)]
        await asyncio.gather(*tasks)
        api_wall_time = time.perf_counter() - wall_start

    print(f"[API COMPLETE] Ingested {TOTAL_REQUESTS} requests in {api_wall_time:.3f} s ({EXPECTED_SPANS / api_wall_time:.1f} spans/s)")

    # 6. Monitor Stream Drain
    print("\n[DRAIN] Monitoring stream drain until queue depth = 0 and DB = 50,000 spans ...")
    r_drain = aioredis.from_url(settings.redis_url, decode_responses=True)
    pg_conn = await asyncpg.connect(settings.database_url)

    drain_start = time.perf_counter()
    timeout_s = 90.0

    while (time.perf_counter() - drain_start) < timeout_s:
        xlen = await r_drain.xlen(settings.REDIS_STREAM_KEY)
        p_info = await r_drain.xpending(settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP)
        pending = p_info["pending"] if isinstance(p_info, dict) else p_info[0]
        db_count = await pg_conn.fetchval(
            "SELECT count(*) FROM spans WHERE service_name = 'profile-service';"
        )
        if xlen == 0 and pending == 0 and db_count >= EXPECTED_SPANS:
            break
        await asyncio.sleep(0.5)

    drain_duration = time.perf_counter() - drain_start
    total_pipeline_duration = api_wall_time + drain_duration
    print(f"[DRAIN COMPLETE] Stream drained in {drain_duration:.3f} s. Total pipeline wall time: {total_pipeline_duration:.3f} s.")

    tracking = False
    await tracker_task
    await r_drain.aclose()
    await pg_conn.close()

    # 7. Stop Worker Process
    worker_proc.terminate()
    try:
        worker_proc.wait(timeout=5)
    except Exception:
        worker_proc.kill()
    try:
        worker_log_file.close()
    except Exception:
        pass

    # 8. Fetch Gateway Metrics
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        gw_resp = await client.get("/v1/telemetry/profiling/gateway-metrics")
        gw_metrics = gw_resp.json()

    # 9. Read Worker Metrics
    worker_metrics = {}
    if os.path.exists(WORKER_RESULTS_FILE):
        with open(WORKER_RESULTS_FILE, "r", encoding="utf-8") as f:
            worker_metrics = json.load(f)

    # 10. Display Complete Timing Breakdown
    latencies.sort()
    n = len(latencies)
    api_p50 = latencies[int(n * 0.5)] if n else 0
    api_p95 = latencies[min(int(n * 0.95), n - 1)] if n else 0
    api_p99 = latencies[min(int(n * 0.99), n - 1)] if n else 0

    print("\n" + "=" * 80)
    print("EMPIRICAL TIMING BREAKDOWN (SCENARIO D: 500 REQUESTS, 50,000 SPANS)")
    print("=" * 80)

    print("\n--- 1. API INGESTION GATEWAY BREAKDOWN ---")
    print(f"Total Requests:       {gw_metrics.get('total_requests', TOTAL_REQUESTS)}")
    print(f"API Wall Time:        {api_wall_time:.3f} s")
    print(f"Client End-to-End:    p50: {api_p50:.2f} ms | p95: {api_p95:.2f} ms | p99: {api_p99:.2f} ms")
    print("-" * 80)
    print(f"{'Component':<28} | {'Mean (ms)':<10} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Max (ms)':<10}")
    print("-" * 80)
    parse_st = gw_metrics.get("t_api_parse", {})
    redis_st = gw_metrics.get("t_api_redis", {})
    print(f"{'T_api_parse (Pydantic V2)':<28} | {parse_st.get('mean_ms', 0):<10.2f} | {parse_st.get('p50_ms', 0):<10.2f} | {parse_st.get('p95_ms', 0):<10.2f} | {parse_st.get('p99_ms', 0):<10.2f} | {parse_st.get('max_ms', 0):<10.2f}")
    print(f"{'T_api_redis (Redis XADD)':<28} | {redis_st.get('mean_ms', 0):<10.2f} | {redis_st.get('p50_ms', 0):<10.2f} | {redis_st.get('p95_ms', 0):<10.2f} | {redis_st.get('p99_ms', 0):<10.2f} | {redis_st.get('max_ms', 0):<10.2f}")

    print("\n--- 2. CONSUMER WORKER PIPELINE BREAKDOWN ---")
    w_msgs = worker_metrics.get("messages_processed", 0)
    w_spans = worker_metrics.get("spans_persisted", 0)
    w_rate = worker_metrics.get("spans_per_sec", 0)
    print(f"Batches Processed:    {w_msgs} batches")
    print(f"Spans Persisted:      {w_spans} spans")
    print(f"Drain Rate:           {w_rate:.1f} spans/sec")
    print("-" * 80)
    print(f"{'Worker Stage':<28} | {'Mean (ms)':<10} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Max (ms)':<10}")
    print("-" * 80)
    for name, key in [
        ("T1: Fetch (XREADGROUP)", "t_fetch"),
        ("T2: Parse (JSON/Pydantic)", "t_parse"),
        ("T3: DB Commit (UNNEST)", "t_db"),
        ("T4: Ack/Del (XACK+XDEL)", "t_ack"),
    ]:
        st = worker_metrics.get(key, {})
        print(f"{name:<28} | {st.get('mean_ms', 0):<10.2f} | {st.get('p50_ms', 0):<10.2f} | {st.get('p95_ms', 0):<10.2f} | {st.get('p99_ms', 0):<10.2f} | {st.get('max_ms', 0):<10.2f}")

    print("\n--- 3. QUEUE DEPTH PROGRESSION (SAMPLES) ---")
    stride = max(1, len(queue_progression) // 10)
    for sample in queue_progression[::stride]:
        print(f"  T + {sample['time_s']:>5.2f}s  -->  Stream Backlog (xlen): {sample['backlog_xlen']:>4} messages")

    # Save consolidated results
    consolidated = {
        "timestamp": time.time(),
        "total_requests": TOTAL_REQUESTS,
        "spans_per_req": SPANS_PER_REQ,
        "concurrency": CONCURRENCY,
        "api_wall_time_s": api_wall_time,
        "drain_duration_s": drain_duration,
        "total_pipeline_s": total_pipeline_duration,
        "gateway": gw_metrics,
        "worker": worker_metrics,
        "queue_progression": queue_progression,
    }
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(consolidated, f, indent=2)
    print(f"\nConsolidated profiling data saved to {RESULTS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
