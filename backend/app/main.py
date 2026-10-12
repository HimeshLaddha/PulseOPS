import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Path, status
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.db import close_db_pool, get_db_pool, get_spans_by_trace, init_db_pool, insert_spans
from app.schemas import (
    TRACE_ID_REGEX,
    IngestResponse,
    SpanBatchIngestRequest,
    TraceResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulseops.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown hooks."""
    settings = get_settings()
    logger.info("PulseOps starting up. Initializing database pool...")
    pool = await init_db_pool(settings)
    app.state.pool = pool
    logger.info("PulseOps ready to receive telemetry.")
    yield
    logger.info("PulseOps shutting down. Closing database pool...")
    await close_db_pool()
    logger.info("PulseOps shutdown complete.")


app = FastAPI(
    title="PulseOps Telemetry API",
    description="High-performance telemetry ingestion and trace query engine",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["Health"])
async def health_check():
    """Verify API and database pool availability."""
    try:
        pool = get_db_pool()
        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1;")
            db_status = "connected" if val == 1 else "degraded"
    except Exception as e:
        logger.error("Healthcheck database ping failed: %s", e)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e)},
        )

    return {"status": "healthy", "database": db_status}


@app.post(
    "/v1/telemetry/spans",
    status_code=status.HTTP_201_CREATED,
    response_model=IngestResponse,
    tags=["Telemetry"],
    summary="Ingest a batch of telemetry spans",
)
async def ingest_spans(request: SpanBatchIngestRequest):
    """
    Ingest an atomic batch of telemetry spans.
    - All-or-nothing validation (invalid batches rejected with 422 before database execution).
    - Idempotent insertion using ON CONFLICT (trace_id, span_id) DO NOTHING.
    - Returns HTTP 201 with count of newly inserted records.
    """
    try:
        pool = get_db_pool()
        inserted_count = await insert_spans(
            pool=pool,
            service_name=request.service_name,
            spans=request.spans,
        )
        return IngestResponse(status="accepted", inserted_count=inserted_count)
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

    # All spans in the trace belong to the service declared, or take root span service
    primary_service = spans[0]["service_name"]

    return TraceResponse(
        trace_id=trace_id,
        service_name=primary_service,
        span_count=len(spans),
        spans=spans,
    )
