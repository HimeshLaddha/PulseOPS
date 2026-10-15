import asyncio
import json
import math
import os
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, List, Tuple

import asyncpg
import httpx
import redis.asyncio as aioredis

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.config import get_settings

BASE_URL = "http://127.0.0.1:8000"
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)


BENCHMARK_STREAM_KEY = "telemetry:stream:benchmark"
BENCHMARK_CONSUMER_GROUP = "pulseops-benchmark-group"
BENCHMARK_DLQ_KEY = f"{BENCHMARK_STREAM_KEY}:deadletter"


def calculate_percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def compute_stats(arr_ms: List[float]) -> Dict[str, float]:
    if not arr_ms:
        return {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
    s = sorted(arr_ms)
    n = len(s)
    return {
        "count": n,
        "mean_ms": sum(s) / n,
        "p50_ms": calculate_percentile(s, 50.0),
        "p95_ms": calculate_percentile(s, 95.0),
        "p99_ms": calculate_percentile(s, 99.0),
        "max_ms": s[-1],
    }


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
            "operation_name": f"experiment_op_{i}",
            "start_time_unix_nano": s_start,
            "end_time_unix_nano": s_end,
            "status_code": "OK",
            "attributes": {"exp_idx": i},
        })
    return {"service_name": service_name, "spans": spans}


async def run_workload_run(
    run_name: str,
    worker_names: List[str],
    total_requests: int = 500,
    spans_per_req: int = 100,
    concurrency: int = 10,
) -> Dict[str, Any]:
    settings = get_settings()
    expected_spans = total_requests * spans_per_req
    service_name = f"profile-svc-{run_name.lower()}"

    print("\n" + "=" * 80)
    print(f"STARTING {run_name.upper()} ({len(worker_names)} WORKER{'S' if len(worker_names) > 1 else ''})")
    print(f"Workload: {total_requests} requests x {spans_per_req} spans/req = {expected_spans:,} spans at C={concurrency}")
    print("=" * 80)

    # 1. Pre-cleaning
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        health = await client.get("/health")
        assert health.status_code == 200, "API is unhealthy"
        await client.post("/v1/telemetry/profiling/reset")

    # Safe isolated Redis stream cleanup: ONLY reset dedicated benchmark stream & DLQ.
    # NEVER touch or clear the application's default telemetry:stream.
    r_clean = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        await r_clean.delete(BENCHMARK_STREAM_KEY)
        await r_clean.delete(BENCHMARK_DLQ_KEY)
        await r_clean.xgroup_create(
            BENCHMARK_STREAM_KEY,
            BENCHMARK_CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except Exception as e:
        print(f"ERROR: Benchmark Redis setup failed on {BENCHMARK_STREAM_KEY}: {e}", file=sys.stderr)
        raise RuntimeError(f"Failed to initialize isolated benchmark stream {BENCHMARK_STREAM_KEY}: {e}") from e
    finally:
        await r_clean.aclose()

    # Scoped database cleanup: strictly delete records for this benchmark run's service_name
    pg_clean = await asyncpg.connect(settings.database_url)
    try:
        await pg_clean.execute(
            "DELETE FROM spans WHERE service_name = $1;",
            service_name,
        )
    except Exception as e:
        print(f"ERROR: Benchmark DB cleanup failed for {service_name}: {e}", file=sys.stderr)
        raise RuntimeError(f"Failed to clean benchmark spans for {service_name}: {e}") from e
    finally:
        await pg_clean.close()

    # Clear old profiling files for these workers
    for w_name in worker_names:
        w_file = os.path.join(RESULTS_DIR, f"worker_profiling_{w_name}.json")
        if os.path.exists(w_file):
            os.remove(w_file)
    single_file = os.path.join(RESULTS_DIR, "worker_profiling.json")
    if os.path.exists(single_file):
        os.remove(single_file)

    # 2. Launch Worker Processes with isolated benchmark stream & group
    worker_procs: List[Tuple[subprocess.Popen, Any]] = []
    for w_name in worker_names:
        log_path = os.path.join(RESULTS_DIR, f"worker_{w_name}.log")
        log_f = open(log_path, "w", encoding="utf-8")
        cmd = [
            sys.executable,
            "-u",
            os.path.join(BACKEND_DIR, "app", "worker.py"),
            "--loop",
            "--consumer-name", w_name,
            "--stream-key", BENCHMARK_STREAM_KEY,
            "--group-name", BENCHMARK_CONSUMER_GROUP,
            "--batch-size", "50",
            "--block-ms", "200",
        ]
        print(f"Launching worker '{w_name}' (logging to {log_path})")
        proc = subprocess.Popen(cmd, stdout=log_f, stderr=subprocess.STDOUT, text=True)
        worker_procs.append((proc, log_f))

    time.sleep(1.5)  # Let worker(s) initialize

    # 3. Queue depth tracker
    queue_samples: List[Dict[str, Any]] = []
    tracking = True

    async def track_queue():
        r_track = aioredis.from_url(settings.redis_url, decode_responses=True)
        t0 = time.perf_counter()
        try:
            while tracking:
                try:
                    xlen = await r_track.xlen(BENCHMARK_STREAM_KEY)
                    queue_samples.append({
                        "time_s": round(time.perf_counter() - t0, 2),
                        "backlog_xlen": xlen,
                    })
                except Exception:
                    pass
                await asyncio.sleep(0.2)
        finally:
            await r_track.aclose()

    tracker_task = asyncio.create_task(track_queue())

    # 4. Ingest Requests into isolated benchmark stream
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    timeout = httpx.Timeout(30.0, connect=10.0)
    sem = asyncio.Semaphore(concurrency)
    latencies: List[float] = []

    ingest_start_time = time.perf_counter()

    async with httpx.AsyncClient(base_url=BASE_URL, limits=limits, timeout=timeout) as client:
        async def send_req(i: int):
            trace_id = uuid.uuid4().hex
            payload = generate_span_batch(service_name, spans_per_req, trace_id)
            async with sem:
                t0 = time.perf_counter()
                resp = await client.post(
                    f"/v1/telemetry/spans?mode=stream&stream_key={BENCHMARK_STREAM_KEY}",
                    json=payload,
                )
                t_elapsed = (time.perf_counter() - t0) * 1000.0
                assert resp.status_code == 202
                latencies.append(t_elapsed)

        tasks = [send_req(i) for i in range(total_requests)]
        await asyncio.gather(*tasks)
        api_wall_time = time.perf_counter() - ingest_start_time

    print(f"[API INGEST COMPLETE] {total_requests} requests accepted in {api_wall_time:.3f} s ({expected_spans / api_wall_time:.1f} spans/s)")

    # 5. Monitor Stream Drain on isolated benchmark stream & group
    print(f"[DRAIN] Monitoring stream drain until queue depth = 0 and DB = {expected_spans:,} spans ...")
    r_drain = aioredis.from_url(settings.redis_url, decode_responses=True)
    pg_conn = await asyncpg.connect(settings.database_url)

    try:
        drain_start = time.perf_counter()
        timeout_s = 90.0

        while (time.perf_counter() - drain_start) < timeout_s:
            xlen = await r_drain.xlen(BENCHMARK_STREAM_KEY)
            p_info = await r_drain.xpending(BENCHMARK_STREAM_KEY, BENCHMARK_CONSUMER_GROUP)
            pending = p_info["pending"] if isinstance(p_info, dict) else p_info[0]
            db_count = await pg_conn.fetchval(
                "SELECT count(*) FROM spans WHERE service_name = $1;",
                service_name,
            )
            if xlen == 0 and pending == 0 and db_count >= expected_spans:
                break
            await asyncio.sleep(0.3)
    finally:
        await r_drain.aclose()
        await pg_conn.close()

    queue_empty_time = time.perf_counter()
    total_e2e_drain_time = queue_empty_time - ingest_start_time
    wait_after_api_time = queue_empty_time - (ingest_start_time + api_wall_time)

    tracking = False
    await tracker_task

    print(f"[DRAIN COMPLETE] Total End-to-End Drain Time: {total_e2e_drain_time:.3f} s (Wait after API: {wait_after_api_time:.3f} s)")

    # 6. Stop Worker Processes
    time.sleep(2.0)  # Let worker(s) unblock from xreadgroup and write profiling JSON
    for proc, log_f in worker_procs:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        try:
            log_f.close()
        except Exception:
            pass

    # 7. Fetch Gateway Metrics
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        gw_resp = await client.get("/v1/telemetry/profiling/gateway-metrics")
        gw_metrics = gw_resp.json()

    # 8. Load Worker Metrics
    worker_results: Dict[str, Any] = {}
    first_batch_times: List[float] = []
    last_batch_times: List[float] = []
    total_spans_persisted = 0
    total_batches_processed = 0

    combined_t3a: List[float] = []
    combined_t3b: List[float] = []
    combined_t3c: List[float] = []
    combined_t3_total: List[float] = []

    for w_name in worker_names:
        w_file = os.path.join(RESULTS_DIR, f"worker_profiling_{w_name}.json")
        if not os.path.exists(w_file):
            w_file = os.path.join(RESULTS_DIR, "worker_profiling.json")
        if os.path.exists(w_file):
            with open(w_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                worker_results[w_name] = data
                if data.get("first_batch_time"):
                    first_batch_times.append(data["first_batch_time"])
                if data.get("last_batch_time"):
                    last_batch_times.append(data["last_batch_time"])
                total_spans_persisted += data.get("spans_persisted", 0)
                total_batches_processed += data.get("messages_processed", 0)

    # Compute Worker Active Drain Time: last_batch_acked - first_batch_fetched
    if first_batch_times and last_batch_times:
        worker_active_drain_time = max(last_batch_times) - min(first_batch_times)
    else:
        worker_active_drain_time = total_e2e_drain_time

    worker_active_throughput = (
        (total_spans_persisted / worker_active_drain_time)
        if worker_active_drain_time > 0
        else 0.0
    )
    e2e_throughput = (
        (total_spans_persisted / total_e2e_drain_time)
        if total_e2e_drain_time > 0
        else 0.0
    )

    # 9. Verify Database Correctness (scoped to benchmark service_name)
    pg_check = await asyncpg.connect(settings.database_url)
    try:
        verified_db_count = await pg_check.fetchval(
            "SELECT count(*) FROM spans WHERE service_name = $1;",
            service_name,
        )
        duplicate_count = await pg_check.fetchval(
            "SELECT count(*) - count(DISTINCT (trace_id, span_id)) FROM spans WHERE service_name = $1;",
            service_name,
        )
    finally:
        await pg_check.close()

    assert verified_db_count == expected_spans, f"Expected {expected_spans} rows, found {verified_db_count}!"
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate spans!"

    # Ingestion latencies
    latencies.sort()
    n = len(latencies)
    client_p50 = calculate_percentile(latencies, 50.0)
    client_p95 = calculate_percentile(latencies, 95.0)
    client_p99 = calculate_percentile(latencies, 99.0)

    # Aggregate worker stage metrics across all active workers
    agg_stages: Dict[str, Any] = {}
    for stage_key in [
        "t_fetch",
        "t_parse",
        "t_db",
        "t_ack",
        "t_3a_pool_acquire",
        "t_3b_sql_fetch_execute",
        "t_3b_sql_execute",
        "t_3c_transaction_commit",
        "t_3c_wal_commit",
    ]:
        total_count = sum(w.get(stage_key, {}).get("count", 0) for w in worker_results.values())
        if total_count > 0:
            weighted_mean = sum(w.get(stage_key, {}).get("mean_ms", 0.0) * w.get(stage_key, {}).get("count", 0) for w in worker_results.values()) / total_count
            avg_p50 = sum(w.get(stage_key, {}).get("p50_ms", 0.0) * w.get(stage_key, {}).get("count", 0) for w in worker_results.values()) / total_count
            avg_p95 = sum(w.get(stage_key, {}).get("p95_ms", 0.0) * w.get(stage_key, {}).get("count", 0) for w in worker_results.values()) / total_count
            avg_p99 = sum(w.get(stage_key, {}).get("p99_ms", 0.0) * w.get(stage_key, {}).get("count", 0) for w in worker_results.values()) / total_count
            max_v = max((w.get(stage_key, {}).get("max_ms", 0.0) for w in worker_results.values()), default=0.0)
            agg_stages[stage_key] = {
                "count": total_count,
                "mean_ms": weighted_mean,
                "p50_ms": avg_p50,
                "p95_ms": avg_p95,
                "p99_ms": avg_p99,
                "max_ms": max_v,
            }

    # Workload balance per worker
    workload_balance: Dict[str, Any] = {}
    for w_name in worker_names:
        w_data = worker_results.get(w_name, {})
        batches = w_data.get("messages_processed", 0)
        spans = w_data.get("spans_persisted", 0)
        share_pct = (spans / total_spans_persisted * 100.0) if total_spans_persisted > 0 else 0.0
        workload_balance[w_name] = {
            "batches": batches,
            "spans": spans,
            "share_pct": round(share_pct, 2),
        }

    result_summary = {
        "run_name": run_name,
        "worker_count": len(worker_names),
        "total_requests": total_requests,
        "expected_spans": expected_spans,
        "verified_db_spans": verified_db_count,
        "duplicate_spans": duplicate_count,
        "api_wall_time_s": api_wall_time,
        "worker_active_drain_s": worker_active_drain_time,
        "total_e2e_drain_s": total_e2e_drain_time,
        "worker_active_throughput_spans_s": worker_active_throughput,
        "e2e_throughput_spans_s": e2e_throughput,
        "client_latencies_ms": {
            "mean": sum(latencies) / n if n else 0,
            "p50": client_p50,
            "p95": client_p95,
            "p99": client_p99,
        },
        "gateway_metrics": gw_metrics,
        "worker_metrics": worker_results,
        "workload_balance": workload_balance,
        "aggregate_worker_metrics": agg_stages,
        "queue_progression": queue_samples,
    }

    print(f"\n[VERIFIED] Run '{run_name}': {verified_db_count:,} / {expected_spans:,} spans in PostgreSQL (0 duplicates).")
    print(f"  Worker Active Drain Time : {worker_active_drain_time:.3f} s ({worker_active_throughput:.1f} spans/s)")
    print(f"  Total End-to-End Time   : {total_e2e_drain_time:.3f} s ({e2e_throughput:.1f} spans/s)")
    print(f"  Workload Balance         : {workload_balance}")

    return result_summary


async def main():
    print("=" * 80)
    print("MISSION: PulseOps — Day 5: 1 -> 2 -> 4 Worker Scaling Experiment")
    print("=" * 80)

    # Run 1: Single Worker Baseline
    run1 = await run_workload_run(
        run_name="Run 1 (Single Worker)",
        worker_names=["single-worker"],
        total_requests=500,
        spans_per_req=100,
        concurrency=10,
    )

    time.sleep(2.0)

    # Run 2: Dual Concurrent Workers
    run2 = await run_workload_run(
        run_name="Run 2 (Dual Workers)",
        worker_names=["worker-1", "worker-2"],
        total_requests=500,
        spans_per_req=100,
        concurrency=10,
    )

    time.sleep(2.0)

    # Run 3: Quad Concurrent Workers
    run3 = await run_workload_run(
        run_name="Run 3 (Quad Workers)",
        worker_names=["worker-1", "worker-2", "worker-3", "worker-4"],
        total_requests=500,
        spans_per_req=100,
        concurrency=10,
    )

    # Compute scaling metrics
    tp1 = run1["worker_active_throughput_spans_s"]
    tp2 = run2["worker_active_throughput_spans_s"]
    tp3 = run3["worker_active_throughput_spans_s"]

    d1 = run1["worker_active_drain_s"]
    d2 = run2["worker_active_drain_s"]
    d3 = run3["worker_active_drain_s"]

    delta_tp_1_to_2 = ((tp2 - tp1) / tp1 * 100.0) if tp1 > 0 else 0.0
    delta_tp_2_to_4 = ((tp3 - tp2) / tp2 * 100.0) if tp2 > 0 else 0.0
    delta_tp_1_to_4 = ((tp3 - tp1) / tp1 * 100.0) if tp1 > 0 else 0.0

    delta_d_1_to_2 = ((d1 - d2) / d1 * 100.0) if d1 > 0 else 0.0
    delta_d_2_to_4 = ((d2 - d3) / d2 * 100.0) if d2 > 0 else 0.0
    delta_d_1_to_4 = ((d1 - d3) / d1 * 100.0) if d1 > 0 else 0.0

    scaling_summary = {
        "throughput_delta_1_to_2_pct": round(delta_tp_1_to_2, 2),
        "throughput_delta_2_to_4_pct": round(delta_tp_2_to_4, 2),
        "throughput_delta_1_to_4_pct": round(delta_tp_1_to_4, 2),
        "drain_time_reduction_1_to_2_pct": round(delta_d_1_to_2, 2),
        "drain_time_reduction_2_to_4_pct": round(delta_d_2_to_4, 2),
        "drain_time_reduction_1_to_4_pct": round(delta_d_1_to_4, 2),
    }

    # Save Day 5 raw results
    comparison_file = os.path.join(RESULTS_DIR, "day5_worker_scaling_experiment.json")
    with open(comparison_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "metadata": {
                    "experiment": "Day 5: 1 -> 2 -> 4 Worker Scaling Experiment",
                    "workload": "500 requests x 100 spans = 50,000 spans",
                    "concurrency": 10,
                    "spans_per_request": 100,
                    "total_requests": 500,
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
                "run1_single_worker": run1,
                "run2_dual_workers": run2,
                "run3_quad_workers": run3,
                "scaling_summary": scaling_summary,
            },
            f,
            indent=2,
        )

    # Helper to extract stage stats
    def get_w_stats(run_dict, stage_key):
        agg = run_dict.get("aggregate_worker_metrics", {}).get(stage_key)
        if agg and agg.get("count", 0) > 0:
            return f"{agg['mean_ms']:.2f} ms (p95: {agg['p95_ms']:.2f})"
        return "N/A"


    # Print 3-Way Scaling Comparison Table
    print("\n" + "=" * 96)
    print("DAY 5 CONTROLLED EXPERIMENT: 1 -> 2 -> 4 WORKER SCALING COMPARISON")
    print("=" * 96)
    print(f"{'Metric':<34} | {'Run 1: 1 Worker':<16} | {'Run 2: 2 Workers':<16} | {'Run 3: 4 Workers':<16}")
    print("-" * 96)
    print(f"{'Active Consumer Workers':<34} | {'1 worker':<16} | {'2 workers':<16} | {'4 workers':<16}")
    print(f"{'Total Spans Ingested':<34} | {run1['verified_db_spans']:<16,} | {run2['verified_db_spans']:<16,} | {run3['verified_db_spans']:<16,}")
    print(f"{'Database Discrepancies / Dups':<34} | {run1['duplicate_spans']:<16} | {run2['duplicate_spans']:<16} | {run3['duplicate_spans']:<16}")
    print(f"{'API Wall Time (Enqueuing)':<34} | {run1['api_wall_time_s']:<16.3f} s | {run2['api_wall_time_s']:<16.3f} s | {run3['api_wall_time_s']:<16.3f} s")
    print(f"{'API Ingestion Latency (p50)':<34} | {run1['client_latencies_ms']['p50']:<16.2f} ms | {run2['client_latencies_ms']['p50']:<16.2f} ms | {run3['client_latencies_ms']['p50']:<16.2f} ms")
    print(f"{'API Ingestion Latency (p95)':<34} | {run1['client_latencies_ms']['p95']:<16.2f} ms | {run2['client_latencies_ms']['p95']:<16.2f} ms | {run3['client_latencies_ms']['p95']:<16.2f} ms")
    print(f"{'API Ingestion Latency (p99)':<34} | {run1['client_latencies_ms']['p99']:<16.2f} ms | {run2['client_latencies_ms']['p99']:<16.2f} ms | {run3['client_latencies_ms']['p99']:<16.2f} ms")
    print("-" * 96)
    print(f"{'Worker Active Drain Time':<34} | {run1['worker_active_drain_s']:<16.3f} s | {run2['worker_active_drain_s']:<16.3f} s | {run3['worker_active_drain_s']:<16.3f} s")
    print(f"{'Total End-to-End Drain Time':<34} | {run1['total_e2e_drain_s']:<16.3f} s | {run2['total_e2e_drain_s']:<16.3f} s | {run3['total_e2e_drain_s']:<16.3f} s")
    print(f"{'Worker Active Throughput':<34} | {run1['worker_active_throughput_spans_s']:<16.1f} s/s | {run2['worker_active_throughput_spans_s']:<16.1f} s/s | {run3['worker_active_throughput_spans_s']:<16.1f} s/s")
    print(f"{'End-to-End Throughput':<34} | {run1['e2e_throughput_spans_s']:<16.1f} s/s | {run2['e2e_throughput_spans_s']:<16.1f} s/s | {run3['e2e_throughput_spans_s']:<16.1f} s/s")

    print("\n" + "=" * 96)
    print("SCALING EFFICIENCY & SPEEDUP ANALYSIS")
    print("Formula: Efficiency = (Speedup - 1) / (Worker_Count - 1) * 100")
    print("=" * 96)
    print(f"{'Transition':<24} | {'Throughput Delta':<20} | {'Drain Time Reduction':<22} | {'Scaling Efficiency':<20}")
    print("-" * 96)
    speedup_1_to_2 = tp2 / tp1 if tp1 > 0 else 0.0
    speedup_2_to_4 = tp3 / tp2 if tp2 > 0 else 0.0
    speedup_1_to_4 = tp3 / tp1 if tp1 > 0 else 0.0
    eff_1_to_2 = ((speedup_1_to_2 - 1.0) / (2 - 1)) * 100.0
    eff_2_to_4 = ((speedup_2_to_4 - 1.0) / (2 - 1)) * 100.0  # 2x worker scaling
    eff_1_to_4 = ((speedup_1_to_4 - 1.0) / (4 - 1)) * 100.0  # 1 to 4 worker scaling
    print(f"{'1 -> 2 Workers':<24} | {delta_tp_1_to_2:+6.1f}% throughput    | {delta_d_1_to_2:6.1f}% time reduction   | {eff_1_to_2:5.1f}% of linear ideal")
    print(f"{'2 -> 4 Workers':<24} | {delta_tp_2_to_4:+6.1f}% throughput    | {delta_d_2_to_4:6.1f}% time reduction   | {eff_2_to_4:5.1f}% of linear ideal")
    print(f"{'1 -> 4 Workers (Overall)':<24} | {delta_tp_1_to_4:+6.1f}% throughput    | {delta_d_1_to_4:6.1f}% time reduction   | {eff_1_to_4:5.1f}% of linear ideal")

    print("\n" + "=" * 96)
    print("FINE-GRAINED DATABASE STAGE DISTRIBUTION (PER-BATCH TIMINGS: MEAN & P95)")
    print("=" * 96)
    print(f"{'Stage / Component':<34} | {'Run 1: 1 Worker':<18} | {'Run 2: 2 Workers':<18} | {'Run 3: 4 Workers':<18}")
    print("-" * 96)
    print(f"{'T3a: Pool Acquisition':<34} | {get_w_stats(run1, 't_3a_pool_acquire'):<18} | {get_w_stats(run2, 't_3a_pool_acquire'):<18} | {get_w_stats(run3, 't_3a_pool_acquire'):<18}")
    print(f"{'T3b: SQL Fetch/Execute Duration':<34} | {get_w_stats(run1, 't_3b_sql_fetch_execute'):<18} | {get_w_stats(run2, 't_3b_sql_fetch_execute'):<18} | {get_w_stats(run3, 't_3b_sql_fetch_execute'):<18}")
    print(f"{'T3c: Transaction Commit Duration':<34} | {get_w_stats(run1, 't_3c_transaction_commit'):<18} | {get_w_stats(run2, 't_3c_transaction_commit'):<18} | {get_w_stats(run3, 't_3c_transaction_commit'):<18}")
    print(f"{'T3: Total DB Persistence':<34} | {get_w_stats(run1, 't_db'):<18} | {get_w_stats(run2, 't_db'):<18} | {get_w_stats(run3, 't_db'):<18}")
    print(f"{'T2: In-Memory Parse (Pydantic)':<34} | {get_w_stats(run1, 't_parse'):<18} | {get_w_stats(run2, 't_parse'):<18} | {get_w_stats(run3, 't_parse'):<18}")
    print(f"{'T4: Redis Ack/Del (XACK+XDEL)':<34} | {get_w_stats(run1, 't_ack'):<18} | {get_w_stats(run2, 't_ack'):<18} | {get_w_stats(run3, 't_ack'):<18}")

    print("\n" + "=" * 96)
    print("WORKLOAD BALANCE DISTRIBUTION ACROSS CONSUMER WORKERS")
    print("=" * 96)
    for run_obj in [run1, run2, run3]:
        print(f"[{run_obj['run_name']}] (Total: {run_obj['verified_db_spans']:,} spans across {run_obj['total_requests']} batches)")
        for w_k, w_v in run_obj.get("workload_balance", {}).items():
            print(f"  • {w_k:<16}: {w_v['batches']:>3} batches ({w_v['spans']:>6,} spans, {w_v['share_pct']:>5.1f}% share)")

    print("\n" + "=" * 96)
    print(f"Results saved to {comparison_file}")
    print("=" * 96)


if __name__ == "__main__":
    asyncio.run(main())
