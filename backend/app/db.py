import json
import logging
import time
from typing import Any, Optional
import asyncpg
from app.config import Settings, get_settings
from app.schemas import SpanModel

logger = logging.getLogger("pulseops.db")

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS spans (
    trace_id CHAR(32) NOT NULL,
    span_id CHAR(16) NOT NULL,
    parent_span_id CHAR(16),
    service_name VARCHAR(128) NOT NULL,
    operation_name VARCHAR(256) NOT NULL,
    start_time_unix_nano BIGINT NOT NULL,
    end_time_unix_nano BIGINT NOT NULL,
    duration_ms DOUBLE PRECISION GENERATED ALWAYS AS (
        (end_time_unix_nano - start_time_unix_nano)::double precision / 1000000.0
    ) STORED,
    status_code VARCHAR(16) NOT NULL DEFAULT 'OK',
    attributes JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans (trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_service_start ON spans (service_name, start_time_unix_nano DESC);
"""

BATCH_INSERT_QUERY = """
INSERT INTO spans (
    trace_id,
    span_id,
    parent_span_id,
    service_name,
    operation_name,
    start_time_unix_nano,
    end_time_unix_nano,
    status_code,
    attributes
)
SELECT
    x.trace_id,
    x.span_id,
    x.parent_span_id,
    x.service_name,
    x.operation_name,
    x.start_time_unix_nano,
    x.end_time_unix_nano,
    x.status_code,
    x.attributes::jsonb
FROM UNNEST(
    $1::char(32)[],
    $2::char(16)[],
    $3::char(16)[],
    $4::varchar(128)[],
    $5::varchar(256)[],
    $6::bigint[],
    $7::bigint[],
    $8::varchar(16)[],
    $9::text[]
) AS x(
    trace_id,
    span_id,
    parent_span_id,
    service_name,
    operation_name,
    start_time_unix_nano,
    end_time_unix_nano,
    status_code,
    attributes
)
ON CONFLICT (trace_id, span_id) DO NOTHING
RETURNING 1;
"""

SELECT_TRACE_QUERY = """
SELECT
    trace_id,
    span_id,
    parent_span_id,
    service_name,
    operation_name,
    start_time_unix_nano,
    end_time_unix_nano,
    duration_ms,
    status_code,
    attributes
FROM spans
WHERE trace_id = $1
ORDER BY start_time_unix_nano ASC;
"""

_pool: Optional[asyncpg.Pool] = None


async def init_db_pool(settings: Optional[Settings] = None) -> asyncpg.Pool:
    """Initialize the asyncpg connection pool and apply schema DDL."""
    global _pool
    if _pool is not None:
        return _pool

    if settings is None:
        settings = get_settings()

    logger.info("Initializing asyncpg connection pool for %s", settings.database_url)
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.DB_POOL_MIN_SIZE,
        max_size=settings.DB_POOL_MAX_SIZE,
    )

    async with _pool.acquire() as conn:
        logger.info("Executing DDL migration statements...")
        await conn.execute(DDL_STATEMENTS)
        logger.info("DDL migration completed successfully.")

    return _pool


async def close_db_pool() -> None:
    """Gracefully close the asyncpg connection pool."""
    global _pool
    if _pool is not None:
        logger.info("Closing asyncpg connection pool...")
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed.")


def get_db_pool() -> asyncpg.Pool:
    """Retrieve the active asyncpg connection pool."""
    if _pool is None:
        raise RuntimeError("Database connection pool has not been initialized.")
    return _pool


async def insert_spans(
    pool: asyncpg.Pool,
    service_name: str,
    spans: list[SpanModel],
    return_timings: bool = False,
) -> Any:
    """
    Insert a batch of spans atomically using unnested arrays with ON CONFLICT DO NOTHING.
    Returns the count of newly inserted records (int), or (count, (t_acquire, t_sql, t_commit))
    if return_timings is True.

    Timings record client-observed stage durations:
    - t_acquire: Connection pool checkout duration.
    - t_sql: Client-observed SQL query execution and fetch duration (does not isolate PostgreSQL internal execution).
    - t_commit: Client-observed transaction commit duration (does not independently measure physical WAL fsync).
    """
    if not spans:
        return (0, (0.0, 0.0, 0.0)) if return_timings else 0

    trace_ids = [s.trace_id for s in spans]
    span_ids = [s.span_id for s in spans]
    parent_span_ids = [s.parent_span_id for s in spans]
    service_names = [service_name for _ in spans]
    operation_names = [s.operation_name for s in spans]
    start_times = [s.start_time_unix_nano for s in spans]
    end_times = [s.end_time_unix_nano for s in spans]
    status_codes = [s.status_code for s in spans]
    attributes_json = [json.dumps(s.attributes) for s in spans]

    t_acq_start = time.perf_counter()
    async with pool.acquire() as conn:
        t_acquire = time.perf_counter() - t_acq_start

        tr = conn.transaction()
        await tr.start()
        try:
            # Client-observed SQL query execution and row fetch duration (conn.fetch)
            t_sql_start = time.perf_counter()
            inserted_rows = await conn.fetch(
                BATCH_INSERT_QUERY,
                trace_ids,
                span_ids,
                parent_span_ids,
                service_names,
                operation_names,
                start_times,
                end_times,
                status_codes,
                attributes_json,
            )
            t_sql = time.perf_counter() - t_sql_start

            # Client-observed transaction commit duration (tr.commit)
            t_commit_start = time.perf_counter()
            await tr.commit()
            t_commit = time.perf_counter() - t_commit_start
        except Exception:
            await tr.rollback()
            raise

        count = len(inserted_rows)
        if return_timings:
            return count, (t_acquire, t_sql, t_commit)
        return count


async def get_spans_by_trace(
    pool: asyncpg.Pool, trace_id: str, return_timing: bool = False
) -> Any:
    """Retrieve all spans for a specific trace_id ordered by start_time_unix_nano ASC."""
    async with pool.acquire() as conn:
        t0 = time.perf_counter()
        rows = await conn.fetch(SELECT_TRACE_QUERY, trace_id)
        t_db_fetch = time.perf_counter() - t0
        results = []
        for r in rows:
            attrs = r["attributes"]
            if isinstance(attrs, str):
                attrs = json.loads(attrs)
            elif attrs is None:
                attrs = {}

            results.append(
                {
                    "trace_id": r["trace_id"].strip(),
                    "span_id": r["span_id"].strip(),
                    "parent_span_id": r["parent_span_id"].strip() if r["parent_span_id"] else None,
                    "service_name": r["service_name"],
                    "operation_name": r["operation_name"],
                    "start_time_unix_nano": int(r["start_time_unix_nano"]),
                    "end_time_unix_nano": int(r["end_time_unix_nano"]),
                    "duration_ms": float(r["duration_ms"]),
                    "status_code": r["status_code"],
                    "attributes": dict(attrs),
                }
            )
        if return_timing:
            return results, t_db_fetch
        return results
