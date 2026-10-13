import logging
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Path, Query, status
from fastapi.responses import JSONResponse

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
    request: SpanBatchIngestRequest,
    mode: Annotated[
        Optional[Literal["direct", "stream"]],
        Query(description="Ingestion mode: direct (PostgreSQL) or stream (Redis Streams)"),
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

    if effective_mode == "stream":
        try:
            redis = get_redis_client()
            payload_json = request.model_dump_json()
            stream_id = await redis.xadd(
                settings.REDIS_STREAM_KEY,
                {"data": payload_json},
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
