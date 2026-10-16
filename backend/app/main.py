import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Path, Query, Request, status
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
    TraceResponse,
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
