import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Path, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.config import get_settings
from app.db import close_db_pool, get_db_pool, get_spans_by_trace, init_db_pool, insert_spans
from app.schemas import (
    TRACE_ID_REGEX,
    IngestResponse,
    SpanBatchIngestRequest,
    SpanBatchQueuedResponse,
    SpanTreeNode,
    TraceResponse,
    TraceTreeResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulseops.api")

_redis_client: Optional[aioredis.Redis] = None


class GatewayProfilingMetrics:
    def __init__(self):
        self.parse_times: list[float] = []
        self.serialize_times: list[float] = []
        self.redis_times: list[float] = []
        self.redis_conn_acquire_times: list[float] = []
        self.redis_cmd_exec_times: list[float] = []
        self.event_loop_lags: list[float] = []
        self._lag_task: Optional[asyncio.Task] = None
        self._lag_running: bool = False
        self._proc_start_times: Optional[Any] = None

    def start_monitoring(self):
        self._proc_start_times = os.times()
        if not self._lag_running:
            self._lag_running = True
      
            self.event_loop_lags.clear()
            self._lag_task = asyncio.create_task(self._sample_loop_lag())

    async def _sample_loop_lag(self):
        interval = 0.005  # 5 ms sample resolution
        try:
            while self._lag_running:
                t0 = time.perf_counter()
                await asyncio.sleep(interval)
                elapsed = time.perf_counter() - t0
                lag = max(0.0, elapsed - interval)
                self.event_loop_lags.append(lag)
        except asyncio.CancelledError:
            pass

    def stop_monitoring(self):
        self._lag_running = False
        if self._lag_task and not self._lag_task.done():
            self._lag_task.cancel()
            self._lag_task = None

    def record(
        self,
        t_parse: float,
        t_serialize: float,
        t_redis: float,
        t_conn_acquire: float = 0.0,
        t_cmd_exec: float = 0.0,
    ):
        self.parse_times.append(t_parse)
        self.serialize_times.append(t_serialize)
        self.redis_times.append(t_redis)
        self.redis_conn_acquire_times.append(t_conn_acquire)
        self.redis_cmd_exec_times.append(t_cmd_exec)

    def reset(self):
        self.parse_times.clear()
        self.serialize_times.clear()
        self.redis_times.clear()
        self.redis_conn_acquire_times.clear()
        self.redis_cmd_exec_times.clear()
        self.event_loop_lags.clear()
        self.start_monitoring()

    def get_summary(self) -> dict:
        def stats(arr):
            if not arr:
                return {"count": 0, "min_ms": 0.0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
            s = sorted(arr)
            n = len(s)
            return {
                "count": n,
                "min_ms": s[0] * 1000.0,
                "mean_ms": (sum(s) / n) * 1000.0,
                "p50_ms": s[int(n * 0.50)] * 1000.0,
                "p95_ms": s[min(int(n * 0.95), n - 1)] * 1000.0,
                "p99_ms": s[min(int(n * 0.99), n - 1)] * 1000.0,
                "max_ms": s[-1] * 1000.0,
            }

        proc_now = os.times()
        user_s = (proc_now.user - self._proc_start_times.user) if self._proc_start_times else 0.0
        sys_s = (proc_now.system - self._proc_start_times.system) if self._proc_start_times else 0.0

        return {
            "total_requests": len(self.parse_times),
            "t_api_parse": stats(self.parse_times),
            "t_api_serialize": stats(self.serialize_times),
            "t_api_redis_xadd": stats(self.redis_times),
            "t_redis_conn_acquire": stats(self.redis_conn_acquire_times),
            "t_redis_cmd_exec": stats(self.redis_cmd_exec_times),
            "event_loop_lag": stats(self.event_loop_lags),
            "api_process_cpu_s": {
                "user_cpu_s": round(user_s, 4),
                "system_cpu_s": round(sys_s, 4),
                "total_cpu_s": round(user_s + sys_s, 4),
            },
        }


gateway_profiler = GatewayProfilingMetrics()


def get_redis_client() -> aioredis.Redis:
    """Retrieve the active Redis client."""
    if _redis_client is None:
        raise RuntimeError("Redis client has not been initialized.")
    return _redis_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown hooks."""
    global _redis_client
    settings = get_settings()
    logger.info("PulseOps starting up. Initializing database pool...")
    pool = await init_db_pool(settings)
    app.state.pool = pool

    from redis.asyncio.retry import Retry
    from redis.backoff import ExponentialBackoff

    logger.info("Initializing Redis connection to %s...", settings.redis_url)
    _redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        retry_on_error=[aioredis.ConnectionError, aioredis.TimeoutError],
        retry=Retry(ExponentialBackoff(), 3),
    )
    app.state.redis = _redis_client

    logger.info("PulseOps ready to receive telemetry.")
    yield
    logger.info("PulseOps shutting down. Closing database pool and Redis connection...")
    await close_db_pool()
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
    logger.info("PulseOps shutdown complete.")


app = FastAPI(
    title="PulseOps Telemetry API",
    description="High-performance telemetry ingestion and trace query engine",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["Health"])
async def health_check():
    """Verify API, database pool, and Redis connectivity."""
    db_status = "disconnected"
    redis_status = "disconnected"
    errors = {}

    try:
        pool = get_db_pool()
        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1;")
            db_status = "connected" if val == 1 else "degraded"
    except Exception as e:
        logger.error("Healthcheck database ping failed: %s", e)
        errors["database"] = str(e)

    try:
        redis = get_redis_client()
        pong = await redis.ping()
        redis_status = "connected" if pong else "degraded"
    except Exception as e:
        logger.error("Healthcheck redis ping failed: %s", e)
        errors["redis"] = str(e)

    is_healthy = (db_status == "connected") and (redis_status == "connected")
    status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if is_healthy else "unhealthy",
            "database": db_status,
            "redis": redis_status,
            **({"errors": errors} if errors else {}),
        },
    )


@app.post(
    "/v1/telemetry/spans",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_201_CREATED: {"model": IngestResponse},
        status.HTTP_202_ACCEPTED: {"model": SpanBatchQueuedResponse},
    },
    tags=["Telemetry"],
    summary="Ingest a batch of telemetry spans",
)
async def ingest_spans(
    raw_request: Request,
    mode: Annotated[
        Optional[Literal["direct", "stream"]],
        Query(description="Ingestion mode: direct (PostgreSQL) or stream (Redis Streams)"),
    ] = None,
    stream_key: Annotated[
        Optional[str],
        Query(description="Optional destination Redis stream key override (defaults to configured REDIS_STREAM_KEY)"),
    ] = None,
):
    """
    Ingest an atomic batch of telemetry spans.
    - All-or-nothing validation (invalid batches rejected with 422 before database or Redis execution).
    - If mode is 'stream', publishes batch to Redis Stream and returns HTTP 202 Accepted.
    - If mode is 'direct', persists directly to PostgreSQL with ON CONFLICT DO NOTHING and returns HTTP 201 Created.
    """
    settings = get_settings()
    effective_mode = mode or settings.INGESTION_MODE

    # 1. Measure T_api_parse: Read raw body bytes + Pydantic V2 JSON deserialization & validation
    body_bytes = await raw_request.body()
    t_parse_start = time.perf_counter()
    try:
        request = SpanBatchIngestRequest.model_validate_json(body_bytes)
    except ValidationError as ve:
        raise RequestValidationError(ve.errors()) from ve
    except Exception as je:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {str(je)}") from je
    t_api_parse = time.perf_counter() - t_parse_start

    if effective_mode == "stream":
        target_stream = stream_key or settings.REDIS_STREAM_KEY
        t_ser_start = time.perf_counter()
        payload_json = request.model_dump_json()
        t_api_serialize = time.perf_counter() - t_ser_start

        t_redis_start = time.perf_counter()
        try:
            redis = get_redis_client()
            pool = redis.connection_pool

            # Separate connection pool acquisition latency from Redis command execution
            t_acq_start = time.perf_counter()
            conn = await pool.get_connection()
            t_conn_acquire = time.perf_counter() - t_acq_start

            t_cmd_start = time.perf_counter()
            try:
                await conn.send_command("XADD", target_stream, "*", "data", payload_json)
                stream_id = await redis.parse_response(conn, "XADD")
            finally:
                await pool.release(conn)
            t_cmd_exec = time.perf_counter() - t_cmd_start
            t_api_redis_xadd = time.perf_counter() - t_redis_start

            gateway_profiler.record(
                t_parse=t_api_parse,
                t_serialize=t_api_serialize,
                t_redis=t_api_redis_xadd,
                t_conn_acquire=t_conn_acquire,
                t_cmd_exec=t_cmd_exec,
            )

            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    "status": "queued",
                    "stream_id": stream_id,
                    "span_count": len(request.spans),
                },
            )
        except Exception as exc:
            logger.exception("Failed to publish span batch to Redis stream: %s", exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Redis stream ingestion unavailable: {str(exc)}",
            ) from exc

    # Direct persistence (Day 1 behavior)
    try:
        pool = get_db_pool()
        inserted_count = await insert_spans(
            pool=pool,
            service_name=request.service_name,
            spans=request.spans,
        )
        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"status": "accepted", "inserted_count": inserted_count},
        )
    except Exception as exc:
        logger.exception("Failed to persist span batch: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist span batch: {str(exc)}",
        ) from exc


@app.get("/v1/telemetry/profiling/gateway-metrics", tags=["Telemetry"])
async def get_gateway_profiling_metrics():
    summary = gateway_profiler.get_summary()
    logger.info("[GATEWAY PROFILER] %s", summary)
    return summary


@app.post("/v1/telemetry/profiling/reset", tags=["Telemetry"])
async def reset_gateway_profiling_metrics():
    gateway_profiler.reset()
    return {"status": "reset"}


@app.get(
    "/v1/traces/{trace_id}",
    response_model=TraceResponse,
    tags=["Telemetry"],
    summary="Retrieve all spans for a trace",
)
async def get_trace(
    trace_id: Annotated[
        str,
        Path(
            pattern=TRACE_ID_REGEX,
            description="32-character lowercase hexadecimal trace ID",
            examples=["0123456789abcdef0123456789abcdef"],
        ),
    ],
):
    """
    Retrieve all spans associated with the given trace_id.
    - Validates trace_id format (returns HTTP 422 if invalid).
    - Returns spans ordered chronologically by start_time_unix_nano ASC.
    - Returns HTTP 404 if trace does not exist.
    """
    pool = get_db_pool()
    spans = await get_spans_by_trace(pool=pool, trace_id=trace_id)

    if not spans:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Trace {trace_id} not found",
        )

    primary_service = spans[0]["service_name"]

    return TraceResponse(
        trace_id=trace_id,
        service_name=primary_service,
        span_count=len(spans),
        spans=spans,
    )


def _compute_active_execution_ms(intervals: list[tuple[int, int]]) -> float:
    """
    Compute total active execution duration in ms using interval-union merging.
    Merges overlapping and concurrent time intervals to prevent naive parallel summation.
    """
    if not intervals:
        return 0.0

    sorted_intervals = sorted(intervals, key=lambda x: (x[0], x[1]))
    merged: list[tuple[int, int]] = []
    curr_start, curr_end = sorted_intervals[0]

    for start, end in sorted_intervals[1:]:
        if start <= curr_end:
            curr_end = max(curr_end, end)
        else:
            merged.append((curr_start, curr_end))
            curr_start, curr_end = start, end

    merged.append((curr_start, curr_end))
    total_nanos = sum(end - start for start, end in merged)
    return round(total_nanos / 1_000_000.0, 6)


def _compute_longest_path_duration_ms(top_nodes: list[SpanTreeNode]) -> float:
    """
    Compute maximum cumulative duration along any directed root-to-leaf path in ms
    using iterative DFS over tree paths.
    """
    if not top_nodes:
        return 0.0

    max_path_ms = 0.0
    # Stack entries: (current_node, cumulative_duration_ms, path_visited_ids)
    stack: list[tuple[SpanTreeNode, float, set[str]]] = [
        (n, n.duration_ms, {n.span_id}) for n in top_nodes
    ]

    while stack:
        curr, acc_dur, path_set = stack.pop()
        valid_children = [c for c in curr.children if c.span_id not in path_set]

        if not valid_children:
            # Reached a leaf node or cyclic boundary
            if acc_dur > max_path_ms:
                max_path_ms = acc_dur
        else:
            for child in valid_children:
                stack.append((child, acc_dur + child.duration_ms, path_set | {child.span_id}))

    return round(max_path_ms, 6)


def _is_ancestor(
    potential_ancestor_id: str,
    start_node_id: str,
    node_map: dict[str, SpanTreeNode],
) -> bool:
    """
    Traverse parent links upward starting from start_node_id to determine if
    potential_ancestor_id is already an ancestor.
    Returns True if potential_ancestor_id is encountered, preventing cyclic graph edges.
    """
    curr_id: Optional[str] = start_node_id
    visited: set[str] = set()

    while curr_id and curr_id in node_map:
        if curr_id == potential_ancestor_id:
            return True
        if curr_id in visited:
            break
        visited.add(curr_id)
        parent_id = node_map[curr_id].parent_span_id
        if not parent_id or parent_id == curr_id:
            break
        curr_id = parent_id

    return False


def _serialize_tree_nodes_iterative(nodes: list[SpanTreeNode]) -> str:
    """
    Iterative string builder for a list of SpanTreeNodes.
    Serializes deeply nested tree hierarchies without call-stack recursion,
    guaranteeing bounded memory and eliminating Python and Pydantic recursion limits.
    """
    out: list[str] = ["["]
    for top_idx, top_node in enumerate(nodes):
        if top_idx > 0:
            out.append(",")
        stack: list[list[Any]] = [[top_node, 0]]
        while stack:
            curr, c_idx = stack[-1]
            if c_idx == 0:
                base = {
                    "span_id": curr.span_id,
                    "trace_id": curr.trace_id,
                    "parent_span_id": curr.parent_span_id,
                    "service_name": curr.service_name,
                    "operation_name": curr.operation_name,
                    "start_time_unix_nano": curr.start_time_unix_nano,
                    "end_time_unix_nano": curr.end_time_unix_nano,
                    "duration_ms": curr.duration_ms,
                    "status_code": curr.status_code,
                    "attributes": curr.attributes,
                    "depth": curr.depth,
                }
                s = json.dumps(base)[:-1]
                out.append(s + ',"children":[')
            if c_idx < len(curr.children):
                if c_idx > 0:
                    out.append(",")
                child = curr.children[c_idx]
                stack[-1][1] += 1
                stack.append([child, 0])
            else:
                out.append("]}")
                stack.pop()
    out.append("]")
    return "".join(out)


def _serialize_trace_tree_response(res: TraceTreeResponse) -> str:
    """Iteratively serialize a TraceTreeResponse to JSON string."""
    roots_json = _serialize_tree_nodes_iterative(res.roots)
    orphans_json = _serialize_tree_nodes_iterative(res.orphans)
    service_name_val = f'"{res.service_name}"' if res.service_name else "null"
    raw_crit = res.__dict__.get("critical_path_ms")
    crit_path_val = f"{raw_crit}" if raw_crit is not None else "null"
    return (
        f'{{"trace_id":{json.dumps(res.trace_id)},'
        f'"root_service":{json.dumps(res.root_service)},'
        f'"span_count":{res.span_count},'
        f'"max_depth":{res.max_depth},'
        f'"wall_clock_duration_ms":{res.wall_clock_duration_ms},'
        f'"active_execution_ms":{res.active_execution_ms},'
        f'"longest_path_duration_ms":{res.longest_path_duration_ms},'
        f'"roots":{roots_json},'
        f'"orphans":{orphans_json},'
        f'"service_name":{service_name_val},'
        f'"critical_path_ms":{crit_path_val}}}'
    )



@app.get(
    "/v1/traces/{trace_id}/tree",
    response_model=TraceTreeResponse,
    tags=["Telemetry"],
    summary="Retrieve a trace assembled as an iterative DAG / hierarchy tree",
)
async def get_trace_tree(
    trace_id: Annotated[
        str,
        Path(
            pattern=TRACE_ID_REGEX,
            description="32-character lowercase hexadecimal trace ID",
            examples=["0123456789abcdef0123456789abcdef"],
        ),
    ],
):
    """
    Retrieve all spans for a trace assembled into an iterative DAG hierarchy / tree.

    Formal Time and Space Complexity Guarantees:
    - Building the node map: O(N) time, O(N) space.
    - Cycle detection via ancestor walk: O(N * D) worst-case time (where D is tree depth),
      bounded by O(N^2) on pathological linear inputs, or O(N) with visited markers; O(N) space.
    - Interval union (active_execution_ms): O(N log N) time for sorting intervals, O(N) space.
    - Longest path calculation (longest_path_duration_ms): O(N) time on a tree/DAG without
      cross-branch cycles, O(N) space using iterative DFS stack.
    - Iterative BFS depth assignment: O(N) time, O(N) space.
    - Overall complexity: O(N log N) time for normal workloads, O(N^2) worst-case on degenerate
      linear chains, O(N) space.

    Defensive Invariants:
    - Children references form a strict acyclic forest (zero RecursionError during Pydantic serialization).
    - Self-loops (parent_span_id == span_id), 2-node mutual loops, and multi-node cycles are
      broken and routed directly to orphans.
    - Missing parent references are categorized into orphans without throwing exceptions.
    - Returns HTTP 404 if trace does not exist.
    """
    pool = get_db_pool()
    spans = await get_spans_by_trace(pool=pool, trace_id=trace_id)

    if not spans:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Trace {trace_id} not found",
        )

    # 1. Instantiate tree nodes in O(N) and collect intervals
    node_map: dict[str, SpanTreeNode] = {}
    intervals: list[tuple[int, int]] = []
    min_start = spans[0]["start_time_unix_nano"]
    max_end = spans[0]["end_time_unix_nano"]

    for s in spans:
        node = SpanTreeNode(
            span_id=s["span_id"],
            trace_id=s["trace_id"],
            parent_span_id=s["parent_span_id"],
            service_name=s["service_name"],
            operation_name=s["operation_name"],
            start_time_unix_nano=s["start_time_unix_nano"],
            end_time_unix_nano=s["end_time_unix_nano"],
            duration_ms=s["duration_ms"],
            status_code=s["status_code"],
            attributes=s.get("attributes", {}),
            depth=0,
            children=[],
        )
        node_map[node.span_id] = node
        intervals.append((s["start_time_unix_nano"], s["end_time_unix_nano"]))
        if s["start_time_unix_nano"] < min_start:
            min_start = s["start_time_unix_nano"]
        if s["end_time_unix_nano"] > max_end:
            max_end = s["end_time_unix_nano"]

    # 2. Iterative Graph Construction in O(N) with cycle prevention
    roots: list[SpanTreeNode] = []
    orphans: list[SpanTreeNode] = []

    for s in spans:
        node = node_map[s["span_id"]]
        parent_id = node.parent_span_id
        if parent_id is None:
            roots.append(node)
        elif parent_id == node.span_id:
            # Self-parent loop guard: break self-reference and route directly to orphans
            orphans.append(node)
        elif parent_id in node_map:
            if _is_ancestor(node.span_id, parent_id, node_map):
                # Cycle detected: break edge to maintain acyclic DAG and route to orphans
                orphans.append(node)
            else:
                node_map[parent_id].children.append(node)
        else:
            orphans.append(node)

    # Iterative reachability pass to catch and isolate unreached cyclic components
    reachable: set[str] = {n.span_id for n in roots + orphans}
    reach_queue = list(roots + orphans)
    q_idx = 0
    while q_idx < len(reach_queue):
        curr_reach = reach_queue[q_idx]
        q_idx += 1
        for c in curr_reach.children:
            if c.span_id not in reachable:
                reachable.add(c.span_id)
                reach_queue.append(c)

    for s in spans:
        sid = s["span_id"]
        if sid not in reachable:
            orphans.append(node_map[sid])
            reachable.add(sid)

    # 3. Iterative BFS Depth Calculation over roots and orphans
    max_depth = 0
    bfs_queue: list[SpanTreeNode] = []
    for node in roots + orphans:
        node.depth = 0
        bfs_queue.append(node)

    visited_depth: set[str] = {n.span_id for n in bfs_queue}
    bfs_idx = 0
    while bfs_idx < len(bfs_queue):
        curr_node = bfs_queue[bfs_idx]
        bfs_idx += 1
        if curr_node.depth > max_depth:
            max_depth = curr_node.depth

        for child in curr_node.children:
            if child.span_id not in visited_depth:
                visited_depth.add(child.span_id)
                child.depth = curr_node.depth + 1
                bfs_queue.append(child)

    # 4. Metric Calculations
    wall_clock_duration_ms = round((max_end - min_start) / 1_000_000.0, 6)
    active_execution_ms = _compute_active_execution_ms(intervals)
    top_nodes = roots + orphans
    longest_path_duration_ms = _compute_longest_path_duration_ms(top_nodes)

    root_service = roots[0].service_name if roots else spans[0]["service_name"]

    tree_response = TraceTreeResponse(
        trace_id=trace_id,
        root_service=root_service,
        span_count=len(spans),
        max_depth=max_depth,
        wall_clock_duration_ms=wall_clock_duration_ms,
        active_execution_ms=active_execution_ms,
        longest_path_duration_ms=longest_path_duration_ms,
        roots=roots,
        orphans=orphans,
        service_name=root_service,
        critical_path_ms=active_execution_ms,
    )

    return Response(
        content=_serialize_trace_tree_response(tree_response),
        media_type="application/json",
    )
