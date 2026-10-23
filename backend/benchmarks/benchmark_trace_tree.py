import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import httpx

# Add backend directory to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.config import get_settings
from app.db import BATCH_INSERT_QUERY, init_db_pool, get_db_pool, close_db_pool
from app.schemas import SpanModel

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
RESULTS_DIR = os.path.join(SCRIPT_DIR, "results")
DEFAULT_OUTPUT_FILE = os.path.join(RESULTS_DIR, "day8_trace_tree_benchmark.json")

# Deterministic trace IDs for 4 tiers (32 hex characters)
TIER_SPECS: Dict[str, Dict[str, Any]] = {
    "tier_1": {
        "tier_id": "tier_1",
        "name": "Tier 1: 10 Spans (Balanced Tree, Depth <= 3)",
        "trace_id": "11111111111111111111111111111111",
        "service_name": "pulseops-tier1-service",
        "span_count": 10,
        "description": "Balanced tree with 1 root, 3 branches, each branch with 2 leaves (depth = 2)",
    },
    "tier_2": {
        "tier_id": "tier_2",
        "name": "Tier 2: 100 Spans (Mixed Parallel Fan-Out)",
        "trace_id": "22222222222222222222222222222222",
        "service_name": "pulseops-tier2-service",
        "span_count": 100,
        "description": "Mixed parallel hierarchy with 1 root, 5 subsystems, sub-tasks, and parallel leaves (depth = 3)",
    },
    "tier_3": {
        "tier_id": "tier_3",
        "name": "Tier 3: 500 Spans (Wide Parallel Branches)",
        "trace_id": "33333333333333333333333333333333",
        "service_name": "pulseops-tier3-service",
        "span_count": 500,
        "description": "Wide parallel fan-out with 1 root and 499 direct parallel child spans (depth = 1)",
    },
    "tier_4": {
        "tier_id": "tier_4",
        "name": "Tier 4: 500 Spans (Degenerate Linear Chain)",
        "trace_id": "44444444444444444444444444444444",
        "service_name": "pulseops-tier4-service",
        "span_count": 500,
        "description": "Degenerate linear chain: Root -> S1 -> S2 -> ... -> S499 (depth = 499)",
    },
}

SERVER_TIMING_REGEX = re.compile(
    r"db;dur=([0-9.]+),\s*dag;dur=([0-9.]+),\s*serialize;dur=([0-9.]+)"
)


def calculate_percentile(sorted_values: List[float], p: float) -> float:
    """Calculate percentile from a sorted list of floats using linear interpolation."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(sorted_values[int(k)], 4)
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return round(d0 + d1, 4)


def generate_deterministic_spans(tier_id: str) -> List[SpanModel]:
    """
    Generate deterministic synthetic spans for the requested tier.
    All span IDs and timestamps are strictly deterministic and reproducible.
    """
    spec = TIER_SPECS[tier_id]
    trace_id = spec["trace_id"]
    service_name = spec["service_name"]
    base_time_nano = 1_700_000_000_000_000_000  # fixed epoch anchor

    spans: List[SpanModel] = []

    if tier_id == "tier_1":
        # Tier 1: 10 spans (balanced tree, depth <= 3)
        # Root (depth 0)
        # 3 children of root (depth 1)
        # 6 grandchildren (2 under each child, depth 2)
        root_span_id = "0000000000000001"
        spans.append(
            SpanModel(
                span_id=root_span_id,
                trace_id=trace_id,
                parent_span_id=None,
                operation_name="gateway_request",
                start_time_unix_nano=base_time_nano,
                end_time_unix_nano=base_time_nano + 60_000_000,  # 60 ms
                status_code="OK",
                attributes={"tier": "tier_1", "role": "root"},
            )
        )

        child_ids = ["0000000000000002", "0000000000000003", "0000000000000004"]
        for idx, cid in enumerate(child_ids):
            c_start = base_time_nano + (idx * 5_000_000) + 2_000_000
            c_end = c_start + 30_000_000
            spans.append(
                SpanModel(
                    span_id=cid,
                    trace_id=trace_id,
                    parent_span_id=root_span_id,
                    operation_name=f"branch_operation_{idx + 1}",
                    start_time_unix_nano=c_start,
                    end_time_unix_nano=c_end,
                    status_code="OK",
                    attributes={"tier": "tier_1", "role": "branch", "branch_idx": idx + 1},
                )
            )

            # 2 leaves under this child
            for leaf_idx in range(2):
                leaf_id = f"00000000000000{5 + (idx * 2) + leaf_idx:02d}"
                l_start = c_start + (leaf_idx * 5_000_000) + 1_000_000
                l_end = l_start + 12_000_000
                spans.append(
                    SpanModel(
                        span_id=leaf_id,
                        trace_id=trace_id,
                        parent_span_id=cid,
                        operation_name=f"leaf_task_{idx + 1}_{leaf_idx + 1}",
                        start_time_unix_nano=l_start,
                        end_time_unix_nano=l_end,
                        status_code="OK",
                        attributes={"tier": "tier_1", "role": "leaf", "branch_id": cid},
                    )
                )

    elif tier_id == "tier_2":
        # Tier 2: 100 spans (mixed parallel fan-out)
        # Root (1 span) -> 5 subsystems (5 spans, depth 1)
        # -> 4 intermediate tasks per subsystem (20 spans, depth 2)
        # -> 74 parallel leaf spans distributed across intermediate tasks (depth 3)
        # Total = 1 + 5 + 20 + 74 = 100 spans
        root_span_id = "0000000000000001"
        spans.append(
            SpanModel(
                span_id=root_span_id,
                trace_id=trace_id,
                parent_span_id=None,
                operation_name="composite_workflow",
                start_time_unix_nano=base_time_nano,
                end_time_unix_nano=base_time_nano + 250_000_000,  # 250 ms
                status_code="OK",
                attributes={"tier": "tier_2", "role": "root"},
            )
        )

        subsystem_ids = []
        for s_idx in range(5):
            sub_id = f"00000000000000{s_idx + 2:02d}"
            subsystem_ids.append(sub_id)
            s_start = base_time_nano + (s_idx * 10_000_000) + 5_000_000
            s_end = s_start + 180_000_000
            spans.append(
                SpanModel(
                    span_id=sub_id,
                    trace_id=trace_id,
                    parent_span_id=root_span_id,
                    operation_name=f"subsystem_manager_{s_idx + 1}",
                    start_time_unix_nano=s_start,
                    end_time_unix_nano=s_end,
                    status_code="OK",
                    attributes={"tier": "tier_2", "role": "subsystem", "sub_idx": s_idx + 1},
                )
            )

        task_ids = []
        curr_span_num = 7
        for sub_id in subsystem_ids:
            for t_idx in range(4):
                task_id = f"000000000000{curr_span_num:04d}"
                curr_span_num += 1
                task_ids.append(task_id)
                t_start = base_time_nano + 20_000_000 + (t_idx * 5_000_000)
                t_end = t_start + 120_000_000
                spans.append(
                    SpanModel(
                        span_id=task_id,
                        trace_id=trace_id,
                        parent_span_id=sub_id,
                        operation_name=f"subtask_processor_{t_idx + 1}",
                        start_time_unix_nano=t_start,
                        end_time_unix_nano=t_end,
                        status_code="OK",
                        attributes={"tier": "tier_2", "role": "task", "parent_sub": sub_id},
                    )
                )

        # Distribute remaining 74 leaves across task_ids (20 tasks)
        remaining_leaves = 100 - len(spans)
        for l_idx in range(remaining_leaves):
            parent_task_id = task_ids[l_idx % len(task_ids)]
            leaf_id = f"000000000000{curr_span_num:04d}"
            curr_span_num += 1
            l_start = base_time_nano + 40_000_000 + (l_idx * 1_500_000)
            l_end = l_start + 15_000_000
            spans.append(
                SpanModel(
                    span_id=leaf_id,
                    trace_id=trace_id,
                    parent_span_id=parent_task_id,
                    operation_name=f"leaf_computation_{l_idx + 1}",
                    start_time_unix_nano=l_start,
                    end_time_unix_nano=l_end,
                    status_code="OK",
                    attributes={"tier": "tier_2", "role": "leaf", "parent_task": parent_task_id},
                )
            )

    elif tier_id == "tier_3":
        # Tier 3: 500 spans (wide parallel branches)
        # Root (1 span) -> 499 direct parallel child spans (depth = 1)
        root_span_id = "0000000000000001"
        spans.append(
            SpanModel(
                span_id=root_span_id,
                trace_id=trace_id,
                parent_span_id=None,
                operation_name="fanout_coordinator",
                start_time_unix_nano=base_time_nano,
                end_time_unix_nano=base_time_nano + 500_000_000,  # 500 ms envelope
                status_code="OK",
                attributes={"tier": "tier_3", "role": "root"},
            )
        )

        for i in range(1, 500):
            child_id = f"000000000000{i + 1:04d}"
            c_start = base_time_nano + 5_000_000 + (i * 200_000)
            c_end = c_start + 10_000_000  # 10 ms duration
            spans.append(
                SpanModel(
                    span_id=child_id,
                    trace_id=trace_id,
                    parent_span_id=root_span_id,
                    operation_name=f"parallel_worker_span_{i}",
                    start_time_unix_nano=c_start,
                    end_time_unix_nano=c_end,
                    status_code="OK",
                    attributes={"tier": "tier_3", "role": "parallel_worker", "worker_index": i},
                )
            )

    elif tier_id == "tier_4":
        # Tier 4: 500 spans (degenerate linear chain: Root -> S1 -> ... -> S500)
        # Root -> S1 -> S2 -> ... -> S499 (depth = 499)
        root_span_id = "0000000000000001"
        spans.append(
            SpanModel(
                span_id=root_span_id,
                trace_id=trace_id,
                parent_span_id=None,
                operation_name="linear_chain_root",
                start_time_unix_nano=base_time_nano,
                end_time_unix_nano=base_time_nano + 10_000_000,
                status_code="OK",
                attributes={"tier": "tier_4", "role": "root", "chain_depth": 0},
            )
        )

        prev_span_id = root_span_id
        for i in range(1, 500):
            curr_id = f"000000000000{i + 1:04d}"
            c_start = base_time_nano + (i * 2_000_000)
            c_end = c_start + 5_000_000  # 5 ms duration
            spans.append(
                SpanModel(
                    span_id=curr_id,
                    trace_id=trace_id,
                    parent_span_id=prev_span_id,
                    operation_name=f"linear_chain_node_{i}",
                    start_time_unix_nano=c_start,
                    end_time_unix_nano=c_end,
                    status_code="OK",
                    attributes={"tier": "tier_4", "role": "linear_node", "chain_depth": i},
                )
            )
            prev_span_id = curr_id

    assert len(spans) == spec["span_count"], f"Tier {tier_id} expected {spec['span_count']} spans, got {len(spans)}"
    return spans


async def pre_populate_database(settings: Any) -> None:
    """Pre-populate PostgreSQL with deterministic synthetic traces across all 4 tiers."""
    print("\n" + "=" * 80)
    print("POPULATING DETERMINISTIC SYNTHETIC TRACES IN POSTGRESQL")
    print("=" * 80)

    conn = await asyncpg.connect(settings.database_url)
    try:
        # Clean existing tier traces
        trace_ids = [spec["trace_id"] for spec in TIER_SPECS.values()]
        deleted = await conn.execute(
            "DELETE FROM spans WHERE trace_id = ANY($1::char(32)[]);",
            trace_ids,
        )
        print(f"[CLEANUP] Deleted old synthetic benchmark traces: {deleted}")

        # Insert each tier
        for tier_id, spec in TIER_SPECS.items():
            spans = generate_deterministic_spans(tier_id)
            print(f"[SEED] Inserting {len(spans)} spans for {spec['name']} (trace_id: {spec['trace_id']}) ...")

            trace_arr = [s.trace_id for s in spans]
            span_arr = [s.span_id for s in spans]
            parent_arr = [s.parent_span_id for s in spans]
            svc_arr = [spec["service_name"] for _ in spans]
            op_arr = [s.operation_name for s in spans]
            start_arr = [s.start_time_unix_nano for s in spans]
            end_arr = [s.end_time_unix_nano for s in spans]
            status_arr = [s.status_code for s in spans]
            attrs_arr = [json.dumps(s.attributes) for s in spans]

            inserted_rows = await conn.fetch(
                BATCH_INSERT_QUERY,
                trace_arr,
                span_arr,
                parent_arr,
                svc_arr,
                op_arr,
                start_arr,
                end_arr,
                status_arr,
                attrs_arr,
            )
            print(f"       -> Persisted {len(inserted_rows)} / {spec['span_count']} rows successfully.")

        # Verification count
        print("\n[VERIFICATION] Verifying persisted span counts in PostgreSQL:")
        for tier_id, spec in TIER_SPECS.items():
            count = await conn.fetchval(
                "SELECT count(*) FROM spans WHERE trace_id = $1;",
                spec["trace_id"],
            )
            assert count == spec["span_count"], f"Mismatch for {tier_id}: expected {spec['span_count']}, got {count}"
            print(f"  - {spec['name']}: {count} / {spec['span_count']} spans verified in PostgreSQL.")
    finally:
        await conn.close()
    print("=" * 80 + "\n")


async def benchmark_tier(
    client: httpx.AsyncClient,
    base_url: str,
    tier_id: str,
    concurrency: int = 10,
    warmup_requests: int = 100,
    measured_requests: int = 500,
) -> Dict[str, Any]:
    """Execute warmup and measured read benchmark for a single tier."""
    spec = TIER_SPECS[tier_id]
    trace_id = spec["trace_id"]
    endpoint_url = f"{base_url}/v1/traces/{trace_id}/tree"

    print("=" * 80)
    print(f"BENCHMARKING {spec['name'].upper()}")
    print(f"  Endpoint        : GET {endpoint_url}")
    print(f"  Configuration   : Concurrency C={concurrency} | Warmup: {warmup_requests} | Measured: {measured_requests}")
    print(f"  Span Count      : {spec['span_count']} spans")
    print("=" * 80)

    # 1. Warm-up Phase
    print(f"[WARMUP] Executing {warmup_requests} warm-up requests ...")
    warmup_sem = asyncio.Semaphore(concurrency)

    async def single_warmup():
        async with warmup_sem:
            r = await client.get(endpoint_url)
            assert r.status_code == 200, f"Warmup request failed with HTTP {r.status_code}"

    warmup_start = time.perf_counter()
    await asyncio.gather(*[single_warmup() for _ in range(warmup_requests)])
    warmup_elapsed = time.perf_counter() - warmup_start
    print(f"[WARMUP] {warmup_requests} warm-up requests completed in {warmup_elapsed:.2f}s.\n")

    # 2. Measured Benchmark Phase
    print(f"[MEASURED] Executing {measured_requests} measured requests under C={concurrency} ...")

    client_latencies_ms: List[float] = []
    t_db_list_ms: List[float] = []
    t_dag_list_ms: List[float] = []
    t_ser_list_ms: List[float] = []
    payload_bytes_list: List[int] = []

    sem = asyncio.Semaphore(concurrency)

    async def single_measured_request():
        async with sem:
            t0 = time.perf_counter()
            resp = await client.get(endpoint_url)
            t1 = time.perf_counter()

            e2e_ms = (t1 - t0) * 1000.0
            assert resp.status_code == 200, f"Benchmark request failed with HTTP {resp.status_code}: {resp.text}"

            server_timing_header = resp.headers.get("server-timing", "")
            match = SERVER_TIMING_REGEX.search(server_timing_header)
            if not match:
                raise RuntimeError(f"Server-Timing header missing or invalid: '{server_timing_header}'")

            t_db = float(match.group(1))
            t_dag = float(match.group(2))
            t_ser = float(match.group(3))

            client_latencies_ms.append(e2e_ms)
            t_db_list_ms.append(t_db)
            t_dag_list_ms.append(t_dag)
            t_ser_list_ms.append(t_ser)
            payload_bytes_list.append(len(resp.content))

    wall_start = time.perf_counter()
    await asyncio.gather(*[single_measured_request() for _ in range(measured_requests)])
    wall_end = time.perf_counter()
    total_wall_time_s = wall_end - wall_start

    # 3. Compute Metrics
    assert len(client_latencies_ms) == measured_requests

    sorted_client = sorted(client_latencies_ms)
    sorted_db = sorted(t_db_list_ms)
    sorted_dag = sorted(t_dag_list_ms)
    sorted_ser = sorted(t_ser_list_ms)

    mean_client = sum(client_latencies_ms) / measured_requests
    p50_client = calculate_percentile(sorted_client, 50.0)
    p95_client = calculate_percentile(sorted_client, 95.0)
    p99_client = calculate_percentile(sorted_client, 99.0)

    mean_db = sum(t_db_list_ms) / measured_requests
    p95_db = calculate_percentile(sorted_db, 95.0)

    mean_dag = sum(t_dag_list_ms) / measured_requests
    p95_dag = calculate_percentile(sorted_dag, 95.0)

    mean_ser = sum(t_ser_list_ms) / measured_requests
    p95_ser = calculate_percentile(sorted_ser, 95.0)

    req_per_s = measured_requests / total_wall_time_s
    spans_per_s = (measured_requests * spec["span_count"]) / total_wall_time_s
    mean_payload_kb = (sum(payload_bytes_list) / measured_requests) / 1024.0

    result = {
        "tier_id": tier_id,
        "name": spec["name"],
        "trace_id": trace_id,
        "span_count": spec["span_count"],
        "concurrency": concurrency,
        "warmup_requests": warmup_requests,
        "measured_requests": measured_requests,
        "total_wall_time_s": round(total_wall_time_s, 4),
        "throughput": {
            "requests_per_sec": round(req_per_s, 2),
            "spans_per_sec": round(spans_per_s, 2),
        },
        "payload_size_kb": round(mean_payload_kb, 2),
        "client_latency_ms": {
            "mean": round(mean_client, 3),
            "p50": round(p50_client, 3),
            "p95": round(p95_client, 3),
            "p99": round(p99_client, 3),
            "min": round(sorted_client[0], 3),
            "max": round(sorted_client[-1], 3),
        },
        "server_timing_breakdown_ms": {
            "t_db_fetch": {
                "mean": round(mean_db, 3),
                "p95": round(p95_db, 3),
            },
            "t_dag_assembly": {
                "mean": round(mean_dag, 3),
                "p95": round(p95_dag, 3),
            },
            "t_json_serialize": {
                "mean": round(mean_ser, 3),
                "p95": round(p95_ser, 3),
            },
        },
    }

    print(f"[RESULTS: {spec['name']}]")
    print(f"  Wall Time        : {total_wall_time_s:.3f} s")
    print(f"  Throughput       : {req_per_s:.2f} req/s | {spans_per_s:.2f} spans/s")
    print(f"  Payload Size     : {mean_payload_kb:.2f} KB")
    print(f"  Client Latency   : mean={mean_client:.2f} ms | p50={p50_client:.2f} ms | p95={p95_client:.2f} ms | p99={p99_client:.2f} ms")
    print(f"  Server Breakdown :")
    print(f"    - T_db_fetch     : mean={mean_db:.3f} ms | p95={p95_db:.3f} ms")
    print(f"    - T_dag_assembly : mean={mean_dag:.3f} ms | p95={p95_dag:.3f} ms")
    print(f"    - T_serialize    : mean={mean_ser:.3f} ms | p95={p95_ser:.3f} ms\n")

    return result


async def main():
    parser = argparse.ArgumentParser(description="PulseOps Day 8: Trace Tree Read Benchmark Harness")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"PulseOps API base URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--concurrency", type=int, default=10, help="Benchmark concurrency (default: 10)")
    parser.add_argument("--warmup-requests", type=int, default=100, help="Warmup requests per tier (default: 100)")
    parser.add_argument("--measured-requests", type=int, default=500, help="Measured requests per tier (default: 500)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help=f"Output JSON path (default: {DEFAULT_OUTPUT_FILE})")
    parser.add_argument("--skip-populate", action="store_true", help="Skip database pre-population")
    args = parser.parse_args()

    settings = get_settings()

    print("\n" + "=" * 80)
    print("PULSEOPS DAY 8: TRACE QUERY BENCHMARKING & LATENCY DECOMPOSITION")
    print("=" * 80)

    # 1. Preflight Health Check
    async with httpx.AsyncClient(timeout=10.0) as check_client:
        try:
            resp = await check_client.get(f"{args.base_url}/health")
            if resp.status_code != 200:
                print(f"[ERROR] API health check failed: HTTP {resp.status_code}")
                sys.exit(1)
            health = resp.json()
            print(f"[OK] PulseOps API is healthy: {health}")
        except Exception as e:
            print(f"[ERROR] Failed to reach PulseOps API at {args.base_url}: {e}")
            sys.exit(1)

    # 2. Pre-populate synthetic traces in PostgreSQL
    if not args.skip_populate:
        await pre_populate_database(settings)

    # 3. Execute Benchmarks across all 4 tiers
    tier_results: Dict[str, Any] = {}
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=50)
    timeout = httpx.Timeout(30.0, connect=10.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as bench_client:
        for tier_id in ["tier_1", "tier_2", "tier_3", "tier_4"]:
            res = await benchmark_tier(
                client=bench_client,
                base_url=args.base_url,
                tier_id=tier_id,
                concurrency=args.concurrency,
                warmup_requests=args.warmup_requests,
                measured_requests=args.measured_requests,
            )
            tier_results[tier_id] = res

    # 4. Comparative Analysis: Tier 3 vs Tier 4
    t3 = tier_results["tier_3"]
    t4 = tier_results["tier_4"]

    t3_server_total = (
        t3["server_timing_breakdown_ms"]["t_db_fetch"]["mean"]
        + t3["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"]
        + t3["server_timing_breakdown_ms"]["t_json_serialize"]["mean"]
    )
    t4_server_total = (
        t4["server_timing_breakdown_ms"]["t_db_fetch"]["mean"]
        + t4["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"]
        + t4["server_timing_breakdown_ms"]["t_json_serialize"]["mean"]
    )

    t3_db_pct = (t3["server_timing_breakdown_ms"]["t_db_fetch"]["mean"] / t3_server_total) * 100.0
    t3_dag_pct = (t3["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"] / t3_server_total) * 100.0
    t3_ser_pct = (t3["server_timing_breakdown_ms"]["t_json_serialize"]["mean"] / t3_server_total) * 100.0

    t4_db_pct = (t4["server_timing_breakdown_ms"]["t_db_fetch"]["mean"] / t4_server_total) * 100.0
    t4_dag_pct = (t4["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"] / t4_server_total) * 100.0
    t4_ser_pct = (t4["server_timing_breakdown_ms"]["t_json_serialize"]["mean"] / t4_server_total) * 100.0

    dag_diff_ms = t4["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"] - t3["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"]
    dag_ratio = (
        t4["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"] / max(0.0001, t3["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"])
    )

    analysis_summary = {
        "tier_3_wide_500_spans": {
            "client_mean_ms": t3["client_latency_ms"]["mean"],
            "client_p95_ms": t3["client_latency_ms"]["p95"],
            "server_breakdown_mean_ms": {
                "t_db_fetch": t3["server_timing_breakdown_ms"]["t_db_fetch"]["mean"],
                "t_dag_assembly": t3["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"],
                "t_json_serialize": t3["server_timing_breakdown_ms"]["t_json_serialize"]["mean"],
            },
            "server_percentage_breakdown": {
                "db_fetch_pct": round(t3_db_pct, 2),
                "dag_assembly_pct": round(t3_dag_pct, 2),
                "json_serialize_pct": round(t3_ser_pct, 2),
            },
        },
        "tier_4_linear_500_spans": {
            "client_mean_ms": t4["client_latency_ms"]["mean"],
            "client_p95_ms": t4["client_latency_ms"]["p95"],
            "server_breakdown_mean_ms": {
                "t_db_fetch": t4["server_timing_breakdown_ms"]["t_db_fetch"]["mean"],
                "t_dag_assembly": t4["server_timing_breakdown_ms"]["t_dag_assembly"]["mean"],
                "t_json_serialize": t4["server_timing_breakdown_ms"]["t_json_serialize"]["mean"],
            },
            "server_percentage_breakdown": {
                "db_fetch_pct": round(t4_db_pct, 2),
                "dag_assembly_pct": round(t4_dag_pct, 2),
                "json_serialize_pct": round(t4_ser_pct, 2),
            },
        },
        "ancestor_walk_penalty": {
            "dag_assembly_delta_mean_ms": round(dag_diff_ms, 3),
            "dag_assembly_speedup_or_slowdown_ratio": round(dag_ratio, 2),
            "penalty_measurable": dag_diff_ms > 0.05,
        },
    }

    full_output = {
        "benchmark_metadata": {
            "name": "Day 8: Trace Read Path Benchmarking & Latency Decomposition",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "concurrency": args.concurrency,
            "warmup_requests_per_tier": args.warmup_requests,
            "measured_requests_per_tier": args.measured_requests,
            "api_base_url": args.base_url,
        },
        "tiers": tier_results,
        "comparative_analysis": analysis_summary,
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(full_output, f, indent=2)

    print("\n" + "=" * 90)
    print("DAY 8 EMPIRICAL BENCHMARK SUMMARY & LATENCY DECOMPOSITION TABLE")
    print("=" * 90)
    print(f"{'Tier':<35} | {'Spans':<5} | {'p50 (ms)':<9} | {'p95 (ms)':<9} | {'T_db (ms)':<10} | {'T_dag (ms)':<10} | {'T_ser (ms)':<10} | {'Req/s':<8} | {'Spans/s':<10}")
    print("-" * 90)

    for tid in ["tier_1", "tier_2", "tier_3", "tier_4"]:
        r = tier_results[tid]
        print(
            f"{r['name'][:35]:<35} | "
            f"{r['span_count']:<5} | "
            f"{r['client_latency_ms']['p50']:<9.2f} | "
            f"{r['client_latency_ms']['p95']:<9.2f} | "
            f"{r['server_timing_breakdown_ms']['t_db_fetch']['mean']:<10.3f} | "
            f"{r['server_timing_breakdown_ms']['t_dag_assembly']['mean']:<10.3f} | "
            f"{r['server_timing_breakdown_ms']['t_json_serialize']['mean']:<10.3f} | "
            f"{r['throughput']['requests_per_sec']:<8.1f} | "
            f"{r['throughput']['spans_per_sec']:<10.1f}"
        )

    print("=" * 90)
    print("\n[LATENCY DECOMPOSITION: TIER 3 (500 Spans Wide) vs. TIER 4 (500 Spans Linear)]")
    print(f"  Tier 3 (Wide Parallel):")
    print(f"    - PostgreSQL Fetch (T_db_fetch) : {t3['server_timing_breakdown_ms']['t_db_fetch']['mean']:.3f} ms ({t3_db_pct:.1f}%)")
    print(f"    - DAG Assembly     (T_dag_assem) : {t3['server_timing_breakdown_ms']['t_dag_assembly']['mean']:.3f} ms ({t3_dag_pct:.1f}%)")
    print(f"    - JSON Serialize   (T_serialize) : {t3['server_timing_breakdown_ms']['t_json_serialize']['mean']:.3f} ms ({t3_ser_pct:.1f}%)")
    print(f"  Tier 4 (Degenerate Linear Chain):")
    print(f"    - PostgreSQL Fetch (T_db_fetch) : {t4['server_timing_breakdown_ms']['t_db_fetch']['mean']:.3f} ms ({t4_db_pct:.1f}%)")
    print(f"    - DAG Assembly     (T_dag_assem) : {t4['server_timing_breakdown_ms']['t_dag_assembly']['mean']:.3f} ms ({t4_dag_pct:.1f}%)")
    print(f"    - JSON Serialize   (T_serialize) : {t4['server_timing_breakdown_ms']['t_json_serialize']['mean']:.3f} ms ({t4_ser_pct:.1f}%)")
    print(f"\n  Ancestor Walk Impact:")
    print(f"    - DAG Assembly delta (Tier 4 - Tier 3): {dag_diff_ms:+.3f} ms ({dag_ratio:.2f}x)")
    print(f"    - Measurable latency penalty: {analysis_summary['ancestor_walk_penalty']['penalty_measurable']}")
    print(f"  Raw results saved to: {args.output}\n")


if __name__ == "__main__":
    asyncio.run(main())
