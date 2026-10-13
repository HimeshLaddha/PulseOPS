import argparse
import asyncio
import json
import math
import os
import platform
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# pyright: ignore [reportMissingImports]
import asyncpg
import httpx
import redis.asyncio as aioredis

# Add backend directory to sys.path if running directly
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.config import get_settings

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")

SCENARIOS: Dict[str, Dict[str, Any]] = {
    "A": {
        "name": "Scenario A: Smoke Test",
        "description": "Baseline single-span requests with concurrency 1",
        "spans_per_req": 1,
        "concurrency": 1,
        "total_requests": 100,
        "warmup_requests": 10,
    },
    "B": {
        "name": "Scenario B: Small Batches",
        "description": "Small batches (10 spans) with concurrency 1",
        "spans_per_req": 10,
        "concurrency": 1,
        "total_requests": 200,
        "warmup_requests": 10,
    },
    "C": {
        "name": "Scenario C: Medium Batches",
        "description": "Medium batches (50 spans) with concurrency 5",
        "spans_per_req": 50,
        "concurrency": 5,
        "total_requests": 500,
        "warmup_requests": 10,
    },
    "D": {
        "name": "Scenario D: Large Batches",
        "description": "Large batches (100 spans) with concurrency 10",
        "spans_per_req": 100,
        "concurrency": 10,
        "total_requests": 500,
        "warmup_requests": 10,
    },
    "E": {
        "name": "Scenario E: Stress Test",
        "description": "Large batches (100 spans) with concurrency 25 (exceeding pool max 20)",
        "spans_per_req": 100,
        "concurrency": 25,
        "total_requests": 1000,
        "warmup_requests": 10,
    },
}


def calculate_percentile(sorted_values: List[float], p: float) -> float:
    """Calculate percentile from a sorted list of floats using linear interpolation."""
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


def generate_span_batch(
    service_name: str,
    spans_count: int,
    run_id: str,
    scenario_id: str,
    worker_id: int,
    req_index: int,
) -> Dict[str, Any]:
    """Generate a batch of strictly validated, unique telemetry spans."""
    now_nano = time.time_ns()
    trace_id = uuid.uuid4().hex  # 32-char lowercase hex
    spans = []

    for i in range(spans_count):
        span_id = uuid.uuid4().hex[:16]  # 16-char lowercase hex
        start_nano = now_nano + (i * 1_000_000)
        end_nano = start_nano + 50_000_000  # 50 ms duration

        spans.append(
            {
                "span_id": span_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": f"benchmark_op_{scenario_id.lower()}",
                "start_time_unix_nano": start_nano,
                "end_time_unix_nano": end_nano,
                "status_code": "OK",
                "attributes": {
                    "benchmark.run_id": run_id,
                    "benchmark.scenario": scenario_id,
                    "worker.id": worker_id,
                    "request.index": req_index,
                    "span.index": i,
                    "http.status_code": 200,
                    "environment": "benchmark",
                },
            }
        )

    return {
        "service_name": service_name,
        "spans": spans,
    }


class BenchmarkHarness:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        run_id: Optional[str] = None,
        mode: str = "direct",
    ):
        self.base_url = base_url.rstrip("/")
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        self.mode = mode
        self.settings = get_settings()
        self.redis_client = aioredis.from_url(self.settings.redis_url, decode_responses=True)
        os.makedirs(RESULTS_DIR, exist_ok=True)

    async def close(self):
        await self.redis_client.aclose()

    async def preflight_healthcheck(self) -> Dict[str, Any]:
        """Verify API, database pool, and Redis availability prior to benchmarking."""
        print("\n" + "=" * 75)
        print(f"PRE-FLIGHT CHECK: Pinging {self.base_url}/health (Mode: {self.mode}) ...")
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.get(f"{self.base_url}/health")
                if resp.status_code != 200:
                    raise RuntimeError(
                        f"Healthcheck failed with HTTP {resp.status_code}: {resp.text}"
                    )
                data = resp.json()
                if data.get("status") != "healthy" or data.get("database") != "connected":
                    raise RuntimeError(f"API reporting degraded health: {data}")
                if self.mode == "stream" and data.get("redis") != "connected":
                    raise RuntimeError(f"API reporting Redis disconnected: {data}")
                print(f"[OK] API Health Check Passed: {data}")
                return data
            except Exception as exc:
                print(f"[FAIL] Preflight healthcheck error: {exc}")
                raise

    async def query_db_span_count(self, run_id: str, scenario_id: Optional[str] = None) -> int:
        """Query PostgreSQL directly to count spans matching benchmark.run_id."""
        conn = await asyncpg.connect(self.settings.database_url)
        try:
            if scenario_id:
                count = await conn.fetchval(
                    """
                    SELECT count(*) FROM spans
                    WHERE attributes->>'benchmark.run_id' = $1
                      AND attributes->>'benchmark.scenario' = $2;
                    """,
                    run_id,
                    scenario_id,
                )
            else:
                count = await conn.fetchval(
                    """
                    SELECT count(*) FROM spans
                    WHERE attributes->>'benchmark.run_id' = $1;
                    """,
                    run_id,
                )
            return int(count)
        finally:
            await conn.close()

    async def run_warmup(
        self, client: httpx.AsyncClient, scenario_id: str, spans_per_req: int, warmup_requests: int
    ):
        """Execute unmetered warm-up requests to prime HTTP and DB/Redis connection pools."""
        print(f"\n[WARM-UP] Executing {warmup_requests} unmetered requests (spans/req: {spans_per_req}, mode: {self.mode}) ...")
        warmup_run_id = f"{self.run_id}_warmup"
        warmup_successes = 0
        target_url = f"{self.base_url}/v1/telemetry/spans?mode={self.mode}"
        expected_status = 202 if self.mode == "stream" else 201

        for i in range(warmup_requests):
            payload = generate_span_batch(
                service_name="pulseops-warmup",
                spans_count=spans_per_req,
                run_id=warmup_run_id,
                scenario_id="WARMUP",
                worker_id=0,
                req_index=i,
            )
            try:
                resp = await client.post(target_url, json=payload)
                if resp.status_code == expected_status:
                    warmup_successes += 1
            except Exception as e:
                print(f"[WARM-UP WARNING] Request {i} encountered error: {e}")

        print(f"[WARM-UP COMPLETE] {warmup_successes}/{warmup_requests} requests succeeded. Metrics reset.\n")
        if self.mode == "stream":
            # Wait briefly for background worker to drain warmup messages before starting timed benchmark
            for _ in range(60):
                try:
                    sl = await self.redis_client.xlen(self.settings.REDIS_STREAM_KEY)
                    pi = await self.redis_client.xpending(
                        self.settings.REDIS_STREAM_KEY, self.settings.REDIS_CONSUMER_GROUP
                    )
                    p_cnt = pi.get("pending", 0) if isinstance(pi, dict) else 0
                    if sl == 0 and p_cnt == 0:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.05)

    async def execute_scenario(self, scenario_id: str) -> Dict[str, Any]:
        """Execute a single benchmark scenario and compute rigorous performance metrics."""
        spec = SCENARIOS[scenario_id]
        name = spec["name"]
        spans_per_req = spec["spans_per_req"]
        concurrency = spec["concurrency"]
        total_requests = spec["total_requests"]
        warmup_requests = spec["warmup_requests"]
        expected_spans = total_requests * spans_per_req

        print("=" * 75)
        print(f"STARTING {name.upper()} [Mode: {self.mode.upper()}]")
        print(f"  Configuration : {spans_per_req} spans/req | Concurrency: {concurrency} | Total Reqs: {total_requests}")
        print(f"  Total Spans   : {expected_spans}")
        print(f"  Run ID        : {self.run_id}")
        print("=" * 75)

        limits = httpx.Limits(
            max_keepalive_connections=max(50, concurrency * 2),
            max_connections=max(100, concurrency * 4),
        )
        timeout = httpx.Timeout(30.0, connect=10.0)
        target_url = f"{self.base_url}/v1/telemetry/spans?mode={self.mode}"
        expected_status = 202 if self.mode == "stream" else 201

        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            # 1. Warm-up Phase
            await self.run_warmup(client, scenario_id, spans_per_req, warmup_requests)

            # 2. Benchmark Measurement Setup
            semaphore = asyncio.Semaphore(concurrency)
            latencies_ms: List[float] = []
            status_codes: Dict[int, int] = {}
            client_exceptions: int = 0
            client_timeouts: int = 0
            api_accepted_total: int = 0
            lock = asyncio.Lock()

            async def worker(req_index: int, worker_id: int):
                nonlocal client_exceptions, client_timeouts, api_accepted_total
                payload = generate_span_batch(
                    service_name=f"benchmark-service-{scenario_id.lower()}",
                    spans_count=spans_per_req,
                    run_id=self.run_id,
                    scenario_id=scenario_id,
                    worker_id=worker_id,
                    req_index=req_index,
                )

                async with semaphore:
                    t_start = time.perf_counter()
                    try:
                        resp = await client.post(
                            target_url,
                            json=payload,
                        )
                        t_elapsed = (time.perf_counter() - t_start) * 1000.0  # ms
                        code = resp.status_code

                        accepted = 0
                        if code == expected_status:
                            try:
                                body = resp.json()
                                if self.mode == "stream":
                                    accepted = body.get("span_count", spans_per_req)
                                else:
                                    accepted = body.get("inserted_count", spans_per_req)
                            except Exception:
                                accepted = spans_per_req

                        async with lock:
                            latencies_ms.append(t_elapsed)
                            status_codes[code] = status_codes.get(code, 0) + 1
                            api_accepted_total += accepted

                    except httpx.TimeoutException:
                        t_elapsed = (time.perf_counter() - t_start) * 1000.0
                        async with lock:
                            client_timeouts += 1
                            latencies_ms.append(t_elapsed)
                    except Exception:
                        t_elapsed = (time.perf_counter() - t_start) * 1000.0
                        async with lock:
                            client_exceptions += 1
                            latencies_ms.append(t_elapsed)

            # 3. Timed Measurement Phase (API Enqueuing)
            wall_start = time.perf_counter()
            tasks = [worker(i, i % concurrency) for i in range(total_requests)]
            await asyncio.gather(*tasks)
            api_wall_time_s = time.perf_counter() - wall_start

        # 3b. Monitored Drain Phase (in stream mode)
        drain_phase_duration_s = 0.0
        total_pipeline_time_s = api_wall_time_s
        if self.mode == "stream":
            print("[DRAIN] Monitoring Redis stream and PostgreSQL persistence until stream is drained ...")
            drain_start = time.perf_counter()
            timeout_s = 120.0
            drained = False

            while (time.perf_counter() - drain_start) < timeout_s:
                try:
                    stream_len = await self.redis_client.xlen(self.settings.REDIS_STREAM_KEY)
                    pending_info = await self.redis_client.xpending(
                        self.settings.REDIS_STREAM_KEY, self.settings.REDIS_CONSUMER_GROUP
                    )
                    pending_count = (
                        pending_info.get("pending", 0)
                        if isinstance(pending_info, dict)
                        else (pending_info[0] if isinstance(pending_info, (list, tuple)) else 0)
                    )
                except Exception:
                    stream_len = -1
                    pending_count = -1

                current_db = await self.query_db_span_count(self.run_id, scenario_id)
                if stream_len == 0 and pending_count == 0 and current_db >= expected_spans:
                    drained = True
                    break
                await asyncio.sleep(0.05)

            drain_phase_duration_s = time.perf_counter() - drain_start
            total_pipeline_time_s = api_wall_time_s + drain_phase_duration_s
            if not drained:
                print(f"[DRAIN WARNING] Timed out waiting for stream drain after {timeout_s}s! stream_len={stream_len}, pending={pending_count}")
            else:
                print(f"[DRAIN COMPLETE] Stream drained and verified in {drain_phase_duration_s:.3f} s.")

        # 4. Latency Percentiles (API Client Perspective)
        latencies_ms.sort()
        count_latencies = len(latencies_ms)
        min_lat = min(latencies_ms) if latencies_ms else 0.0
        max_lat = max(latencies_ms) if latencies_ms else 0.0
        mean_lat = sum(latencies_ms) / count_latencies if count_latencies else 0.0
        p50 = calculate_percentile(latencies_ms, 50.0)
        p90 = calculate_percentile(latencies_ms, 90.0)
        p95 = calculate_percentile(latencies_ms, 95.0)
        p99 = calculate_percentile(latencies_ms, 99.0)

        # 5. Throughput Calculations
        req_throughput = (total_requests / api_wall_time_s) if api_wall_time_s > 0 else 0.0
        api_span_throughput = (api_accepted_total / api_wall_time_s) if api_wall_time_s > 0 else 0.0

        # 6. Database Verification (Isolated by benchmark.run_id)
        print("[VERIFICATION] Querying PostgreSQL for persisted spans with benchmark.run_id ...")
        db_persisted_count = await self.query_db_span_count(self.run_id, scenario_id)
        persisted_span_throughput = (db_persisted_count / total_pipeline_time_s) if total_pipeline_time_s > 0 else 0.0
        discrepancy = db_persisted_count - api_accepted_total
        verification_passed = (discrepancy == 0) and (db_persisted_count == expected_spans)

        # 7. Print Console Summary
        print("\n" + "-" * 75)
        print(f"BENCHMARK RESULTS: {name} [Mode: {self.mode.upper()}]")
        print("-" * 75)
        print(f"  API Ingest Wall Time   : {api_wall_time_s:.3f} s")
        if self.mode == "stream":
            print(f"  Post-Enqueue Drain     : {drain_phase_duration_s:.3f} s")
            print(f"  Total Pipeline Time    : {total_pipeline_time_s:.3f} s")
        print(f"  Completed Requests     : {total_requests}")
        print(f"  Request Throughput     : {req_throughput:.2f} req/s")
        print(f"  Telemetry Throughput   : {api_span_throughput:.2f} spans/s (API Accepted)")
        print(f"  Persisted Throughput   : {persisted_span_throughput:.2f} spans/s (DB Verified)")
        print(f"  HTTP Status Codes      : {status_codes}")
        print(f"  Client Errors          : Timeouts={client_timeouts}, Exceptions={client_exceptions}")
        print(f"  Latency (Min / Mean)   : {min_lat:.2f} ms / {mean_lat:.2f} ms")
        print(f"  Latency (p50 / p90)    : {p50:.2f} ms / {p90:.2f} ms")
        print(f"  Latency (p95 / p99)    : {p95:.2f} ms / {p99:.2f} ms")
        print(f"  Latency (Max)          : {max_lat:.2f} ms")
        print("-" * 75)
        print(f"  DB Record Verification : Run ID = {self.run_id}")
        print(f"    Expected Spans       : {expected_spans}")
        print(f"    API Reported Ingest  : {api_accepted_total}")
        print(f"    DB Actual Persisted  : {db_persisted_count}")
        print(f"    Discrepancy          : {discrepancy}")
        print(f"    Verification Status  : {'PASSED (Zero Discrepancy)' if verification_passed else 'FAILED'}")
        print("-" * 75 + "\n")

        result_payload = {
            "scenario_id": scenario_id,
            "name": name,
            "mode": self.mode,
            "spans_per_req": spans_per_req,
            "concurrency": concurrency,
            "total_requests": total_requests,
            "warmup_requests": warmup_requests,
            "wall_time_s": round(api_wall_time_s, 4),
            "drain_time_s": round(total_pipeline_time_s, 4),
            "post_enqueue_drain_s": round(drain_phase_duration_s, 4),
            "throughput": {
                "requests_per_sec": round(req_throughput, 2),
                "api_spans_per_sec": round(api_span_throughput, 2),
                "persisted_spans_per_sec": round(persisted_span_throughput, 2),
            },
            "status_code_distribution": status_codes,
            "client_errors": {
                "timeouts": client_timeouts,
                "exceptions": client_exceptions,
            },
            "latency_ms": {
                "min": round(min_lat, 2),
                "mean": round(mean_lat, 2),
                "p50": round(p50, 2),
                "p90": round(p90, 2),
                "p95": round(p95, 2),
                "p99": round(p99, 2),
                "max": round(max_lat, 2),
            },
            "db_verification": {
                "expected_spans": expected_spans,
                "api_inserted_count": api_accepted_total,
                "db_persisted_count": db_persisted_count,
                "discrepancy": discrepancy,
                "verified": verification_passed,
            },
        }

        return result_payload

    def save_artifact(self, scenarios_results: List[Dict[str, Any]]) -> str:
        """Save full machine-readable results to backend/benchmarks/results/."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_run_{self.run_id}_{timestamp}.json"
        filepath = os.path.join(RESULTS_DIR, filename)

        metadata = {
            "run_id": self.run_id,
            "mode": self.mode,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "target_base_url": self.base_url,
            "system_metadata": {
                "python_version": sys.version,
                "platform": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "cpu_count": os.cpu_count(),
            },
            "database_config": {
                "host": self.settings.POSTGRES_HOST,
                "port": self.settings.POSTGRES_PORT,
                "database": self.settings.POSTGRES_DB,
                "pool_min_size": self.settings.DB_POOL_MIN_SIZE,
                "pool_max_size": self.settings.DB_POOL_MAX_SIZE,
            },
            "scenarios": scenarios_results,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        print(f"[ARTIFACT SAVED] Results written to: {filepath}")
        return filepath


async def main():
    parser = argparse.ArgumentParser(description="PulseOps Telemetry Benchmark Harness")
    parser.add_argument(
        "--scenario",
        type=str,
        default="ALL",
        choices=["A", "B", "C", "D", "E", "ALL"],
        help="Benchmark scenario to run (default: ALL)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="stream",
        choices=["direct", "stream"],
        help="Ingestion mode: direct (PostgreSQL) or stream (Redis Streams)",
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=DEFAULT_BASE_URL,
        help=f"Target PulseOps API base URL (default: {DEFAULT_BASE_URL})",
    )
    args = parser.parse_args()

    harness = BenchmarkHarness(base_url=args.base_url, mode=args.mode)

    try:
        # Pre-flight check
        await harness.preflight_healthcheck()

        scenarios_to_run = (
            list(SCENARIOS.keys()) if args.scenario == "ALL" else [args.scenario]
        )
        results = []

        for sc_id in scenarios_to_run:
            res = await harness.execute_scenario(sc_id)
            results.append(res)

        artifact_path = harness.save_artifact(results)
        print("\n[COMPLETE] Benchmark execution finished successfully.\n")
    finally:
        await harness.close()


if __name__ == "__main__":
    asyncio.run(main())
