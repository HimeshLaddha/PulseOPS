import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from typing import Optional

# Ensure backend root is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from app.config import get_settings
from app.db import close_db_pool, init_db_pool, insert_spans
from app.schemas import SpanBatchIngestRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulseops.worker")


import time

class WorkerProfilingTracker:
    def __init__(self, consumer_name: str = "worker"):
        self.consumer_name = consumer_name
        self.fetch_times: list[float] = []
        self.parse_times: list[float] = []
        self.db_times: list[float] = []
        self.ack_times: list[float] = []
        self.t_3a_times: list[float] = []
        self.t_3b_times: list[float] = []
        self.t_3c_times: list[float] = []
        self.batch_counts: int = 0
        self.span_counts: int = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def record_fetch(self, duration: float):
        self.fetch_times.append(duration)

    def record_msg(
        self,
        t_parse: float,
        t_db: float,
        t_ack: float,
        spans: int,
        t_3a: float = 0.0,
        t_3b: float = 0.0,
        t_3c: float = 0.0,
    ):
        if self.start_time is None:
            self.start_time = time.perf_counter()
        self.parse_times.append(t_parse)
        self.db_times.append(t_db)
        self.ack_times.append(t_ack)
        self.t_3a_times.append(t_3a)
        self.t_3b_times.append(t_3b)
        self.t_3c_times.append(t_3c)
        self.batch_counts += 1
        self.span_counts += spans
        self.end_time = time.perf_counter()

    def get_summary(self) -> dict:
        def stats(arr):
            if not arr:
                return {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
            s = sorted(arr)
            n = len(s)
            return {
                "count": n,
                "mean_ms": (sum(s) / n) * 1000.0,
                "p50_ms": s[int(n * 0.50)] * 1000.0,
                "p95_ms": s[min(int(n * 0.95), n - 1)] * 1000.0,
                "p99_ms": s[min(int(n * 0.99), n - 1)] * 1000.0,
                "max_ms": s[-1] * 1000.0,
            }

        total_wall_s = (self.end_time - self.start_time) if (self.start_time and self.end_time) else 0.0
        msg_per_sec = (self.batch_counts / total_wall_s) if total_wall_s > 0 else 0.0
        spans_per_sec = (self.span_counts / total_wall_s) if total_wall_s > 0 else 0.0

        return {
            "consumer_name": self.consumer_name,
            "messages_processed": self.batch_counts,
            "spans_persisted": self.span_counts,
            "first_batch_time": self.start_time,
            "last_batch_time": self.end_time,
            "active_drain_seconds": total_wall_s,
            "messages_per_sec": msg_per_sec,
            "spans_per_sec": spans_per_sec,
            "t_fetch": stats(self.fetch_times),
            "t_parse": stats(self.parse_times),
            "t_db": stats(self.db_times),
            "t_ack": stats(self.ack_times),
            "t_3a_pool_acquire": stats(self.t_3a_times),
            "t_3b_sql_execute": stats(self.t_3b_times),
            "t_3c_wal_commit": stats(self.t_3c_times),
        }

    def save_json(self, file_path: str):
        try:
            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(self.get_summary(), f, indent=2)
            consumer_file = file_path.replace(".json", f"_{self.consumer_name}.json")
            with open(consumer_file, "w", encoding="utf-8") as f:
                json.dump(self.get_summary(), f, indent=2)
            logger.info("Saved profiling summary for '%s' to %s", self.consumer_name, consumer_file)
        except Exception as e:
            logger.warning("Failed to save profiling summary JSON: %s", e)

    def print_summary(self):
        s = self.get_summary()
        if s["messages_processed"] == 0:
            return
        print("\n" + "=" * 80)
        print(f"WORKER PIPELINE PROFILING BREAKDOWN ({self.consumer_name})")
        print("=" * 80)
        print(f"Total Batches Processed:  {s['messages_processed']} batches")
        print(f"Total Spans Persisted:    {s['spans_persisted']} spans")
        print(f"Active Drain Duration:    {s['active_drain_seconds']:.3f} s")
        print(f"Worker Batch Throughput:  {s['messages_per_sec']:.2f} batches/s")
        print(f"Worker Span Throughput:   {s['spans_per_sec']:.2f} spans/s")
        print("-" * 80)
        print(f"{'Stage':<28} | {'Mean (ms)':<10} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Max (ms)':<10}")
        print("-" * 80)
        for stage, key in [
            ("T1: Fetch (Redis)", "t_fetch"),
            ("T2: Parse (JSON/Model)", "t_parse"),
            ("T3: DB Total", "t_db"),
            ("  T3a: Pool Acquire", "t_3a_pool_acquire"),
            ("  T3b: SQL Execution", "t_3b_sql_execute"),
            ("  T3c: WAL Commit (fsync)", "t_3c_wal_commit"),
            ("T4: Ack/Del (Redis)", "t_ack"),
        ]:
            st = s[key]
            print(f"{stage:<28} | {st['mean_ms']:<10.2f} | {st['p50_ms']:<10.2f} | {st['p95_ms']:<10.2f} | {st['p99_ms']:<10.2f} | {st['max_ms']:<10.2f}")
        print("=" * 80 + "\n")


async def init_consumer_group(redis_client: aioredis.Redis, stream_key: str, group_name: str):
    """Ensure consumer group exists on the target stream using MKSTREAM."""
    try:
        await redis_client.xgroup_create(
            name=stream_key,
            groupname=group_name,
            id="0",
            mkstream=True,
        )
        logger.info("Created consumer group '%s' on stream '%s'.", group_name, stream_key)
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group '%s' already exists on stream '%s'.", group_name, stream_key)
        else:
            raise


async def process_batch_message(
    pool,
    redis_client: aioredis.Redis,
    stream_key: str,
    group_name: str,
    msg_id: str,
    fields: Optional[dict],
    tracker: Optional[WorkerProfilingTracker] = None,
) -> int:
    """
    Process a single stream message:
    1. Distinguish phantom deleted IDs (fields is None) -> XACK to clear PEL.
    2. Distinguish malformed stream payloads (empty dict or missing 'data') -> route to DLQ as poison pill.
    3. Parse JSON payload into SpanBatchIngestRequest (measured as T_worker_parse).
    4. Insert spans into PostgreSQL with parameterized UNNEST (measured as T_worker_db with T3a, T3b, T3c).
    5. Acknowledge with XACK and XDEL (measured as T_worker_ack).
    """
    # 1. True phantom entry: stream record was deleted before acknowledgment
    if fields is None:
        logger.warning(
            "Message %s is a phantom entry (fields is None; deleted from stream). Acknowledging to clear from PEL.",
            msg_id,
        )
        await redis_client.xack(stream_key, group_name, msg_id)
        return 0

    # 2. Malformed stream payload: dictionary is empty or missing 'data' field
    raw_data = fields.get("data") if isinstance(fields, dict) else None
    if not isinstance(fields, dict) or not fields or not raw_data:
        err_msg = (
            "Payload dictionary is empty or missing 'data' field"
            if isinstance(fields, dict)
            else f"Malformed stream message fields: expected dict, got {type(fields)}"
        )
        logger.error(
            "Poison pill detected in message %s: %s. Routing to deadletter.",
            msg_id,
            err_msg,
        )
        try:
            await redis_client.xadd(
                f"{stream_key}:deadletter",
                {"original_id": msg_id, "error": err_msg, "data": str(raw_data if raw_data is not None else fields)},
            )
        except Exception as dl_err:
            logger.critical(
                "Failed to route poison pill %s to deadletter stream: %s. Message remains un-ACKed in PEL.",
                msg_id,
                dl_err,
            )
            raise dl_err

        # Acknowledge and trim from stream ONLY after DLQ write succeeds
        await redis_client.xack(stream_key, group_name, msg_id)
        await redis_client.xdel(stream_key, msg_id)
        return 0

    # 3. Parse JSON & Pydantic V2 validation
    t_parse_start = time.perf_counter()
    try:
        data = json.loads(raw_data)
        batch = SpanBatchIngestRequest.model_validate(data)
    except Exception as exc:
        logger.error(
            "Poison pill detected in message %s: %s. Routing to deadletter.",
            msg_id,
            exc,
        )
        try:
            await redis_client.xadd(
                f"{stream_key}:deadletter",
                {"original_id": msg_id, "error": str(exc), "data": str(raw_data)},
            )
        except Exception as dl_err:
            logger.critical(
                "Failed to route poison pill %s to deadletter stream: %s. Message remains un-ACKed in PEL.",
                msg_id,
                dl_err,
            )
            raise dl_err

        # Acknowledge and trim from stream ONLY after DLQ write succeeds
        await redis_client.xack(stream_key, group_name, msg_id)
        await redis_client.xdel(stream_key, msg_id)
        return 0
    t_parse = time.perf_counter() - t_parse_start

    # 4. PostgreSQL batch insertion with fine-grained T3a, T3b, T3c breakdown
    t_db_start = time.perf_counter()
    inserted, timings = await insert_spans(
        pool=pool,
        service_name=batch.service_name,
        spans=batch.spans,
        return_timings=True,
    )
    t_db = time.perf_counter() - t_db_start
    t_3a, t_3b, t_3c = timings
    logger.info("Persisted %d spans for service '%s' (message %s).", inserted, batch.service_name, msg_id)

    # 5. Acknowledge and trim from stream strictly after DB insertion succeeds
    t_ack_start = time.perf_counter()
    await redis_client.xack(stream_key, group_name, msg_id)
    await redis_client.xdel(stream_key, msg_id)
    t_ack = time.perf_counter() - t_ack_start

    if tracker is not None:
        tracker.record_msg(
            t_parse=t_parse,
            t_db=t_db,
            t_ack=t_ack,
            spans=inserted,
            t_3a=t_3a,
            t_3b=t_3b,
            t_3c=t_3c,
        )

    return inserted


async def run_worker(
    once: bool = False,
    batch_size: int = 50,
    block_ms: int = 1000,
    consumer_name: Optional[str] = None,
    min_idle_ms: Optional[int] = None,
):
    settings = get_settings()
    effective_min_idle_ms = (
        min_idle_ms if min_idle_ms is not None else settings.REDIS_STREAM_MIN_IDLE_MS
    )
    if consumer_name is None:
        consumer_name = f"worker-{uuid.uuid4().hex[:8]}"

    logger.info(
        "Starting PulseOps ingestion worker '%s' (reclaim_idle_ms: %d)...",
        consumer_name,
        effective_min_idle_ms,
    )

    # Initialize PostgreSQL pool with graceful error handling
    pool = None
    try:
        pool = await init_db_pool(settings)
    except Exception as db_exc:
        logger.error("Could not connect to PostgreSQL: %s. Worker exiting gracefully.", db_exc)
        return

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)
    tracker = WorkerProfilingTracker(consumer_name=consumer_name)

    try:
        await init_consumer_group(redis_client, settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP)

        running = True
        total_processed = 0

        while running:
            try:
                # 1. Process pending / abandoned messages first (e.g., from crashed workers)
                try:
                    t_claim_start = time.perf_counter()
                    claim_res = await redis_client.xautoclaim(
                        name=settings.REDIS_STREAM_KEY,
                        groupname=settings.REDIS_CONSUMER_GROUP,
                        consumername=consumer_name,
                        min_idle_time=effective_min_idle_ms,
                        start_id="0-0",
                        count=batch_size,
                    )
                    t_claim = time.perf_counter() - t_claim_start
                    claimed_messages = claim_res[1] if len(claim_res) > 1 else []
                    if claimed_messages:
                        tracker.record_fetch(t_claim)

                    for item in claimed_messages:
                        if not item or not isinstance(item, (list, tuple)) or len(item) != 2:
                            continue
                        msg_id, fields = item
                        if fields is None:
                            logger.warning(
                                "Claimed message %s has None payload (deleted from stream). Acknowledging to clear from PEL.",
                                msg_id,
                            )
                            try:
                                await redis_client.xack(settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP, msg_id)
                            except Exception as ack_err:
                                logger.debug("Failed to xack phantom message %s: %s", msg_id, ack_err)
                            continue

                        try:
                            inserted = await process_batch_message(
                                pool=pool,
                                redis_client=redis_client,
                                stream_key=settings.REDIS_STREAM_KEY,
                                group_name=settings.REDIS_CONSUMER_GROUP,
                                msg_id=msg_id,
                                fields=fields,
                                tracker=tracker,
                            )
                            total_processed += 1
                        except Exception as proc_err:
                            logger.error(
                                "Failed to process claimed message %s: %s. Leaving in PEL.",
                                msg_id,
                                proc_err,
                            )
                except Exception as claim_err:
                    logger.debug("xautoclaim encountered: %s", claim_err)

                # 2. Read new messages with '>'
                t_read_start = time.perf_counter()
                response = await redis_client.xreadgroup(
                    groupname=settings.REDIS_CONSUMER_GROUP,
                    consumername=consumer_name,
                    streams={settings.REDIS_STREAM_KEY: ">"},
                    count=batch_size,
                    block=block_ms if not once else 500,
                )
                t_read = time.perf_counter() - t_read_start

                if not response and total_processed == 0 and once:
                    logger.info("No messages to process in --once mode. Exiting.")
                    break

                if not response and total_processed > 0:
                    try:
                        cur_xlen = await redis_client.xlen(settings.REDIS_STREAM_KEY)
                        if cur_xlen == 0:
                            tracker.save_json(os.path.join(BACKEND_DIR, "benchmarks", "results", "worker_profiling.json"))
                    except Exception:
                        pass

                if response:
                    tracker.record_fetch(t_read)
                    for stream_name, messages in response:
                        for item in messages:
                            if not item or not isinstance(item, (list, tuple)) or len(item) != 2:
                                continue
                            msg_id, fields = item
                            if fields is None:
                                logger.warning(
                                    "Received message %s with None payload (deleted from stream). Acknowledging to clear from PEL.",
                                    msg_id,
                                )
                                try:
                                    await redis_client.xack(settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP, msg_id)
                                except Exception:
                                    pass
                                continue

                            try:
                                inserted = await process_batch_message(
                                    pool=pool,
                                    redis_client=redis_client,
                                    stream_key=settings.REDIS_STREAM_KEY,
                                    group_name=settings.REDIS_CONSUMER_GROUP,
                                    msg_id=msg_id,
                                    fields=fields,
                                    tracker=tracker,
                                )
                                total_processed += 1
                            except Exception as proc_err:
                                logger.error(
                                    "Failed to process message %s: %s. Leaving in PEL.",
                                    msg_id,
                                    proc_err,
                                )

                # Backlog progress tracking & drain save
                if total_processed > 0:
                    try:
                        cur_xlen = await redis_client.xlen(settings.REDIS_STREAM_KEY)
                        if total_processed % 50 == 0 or cur_xlen == 0 or once:
                            elapsed = max(0.001, time.perf_counter() - (tracker.start_time or time.perf_counter()))
                            logger.info(
                                "[PROFILER] Processed: %d batches (%d spans) | Stream Backlog (xlen): %d | Rate: %.1f spans/sec",
                                tracker.batch_counts,
                                tracker.span_counts,
                                cur_xlen,
                                tracker.span_counts / elapsed,
                            )
                        if cur_xlen == 0:
                            tracker.save_json(os.path.join(BACKEND_DIR, "benchmarks", "results", "worker_profiling.json"))
                    except Exception:
                        pass

                if once:
                    logger.info("Processed %d messages in --once mode. Exiting.", total_processed)
                    break

            except asyncio.CancelledError:
                logger.info("Worker cancelled. Shutting down gracefully...")
                running = False
            except Exception as e:
                logger.warning("Error in worker loop (e.g. database disconnect): %s", e)
                if once:
                    logger.info("Exiting --once loop on error.")
                    break
                await asyncio.sleep(1)

    finally:
        tracker.print_summary()
        tracker.save_json(os.path.join(BACKEND_DIR, "benchmarks", "results", "worker_profiling.json"))
        logger.info("Closing database pool and Redis connection...")
        if pool is not None:
            await close_db_pool()
        await redis_client.aclose()
        logger.info("Worker shutdown complete.")


def main():
    parser = argparse.ArgumentParser(description="PulseOps Telemetry Stream Consumer Worker")
    parser.add_argument("--once", action="store_true", help="Process available messages once and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously in a loop (default behavior)")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of messages to read per XREADGROUP")
    parser.add_argument("--block-ms", type=int, default=1000, help="Block timeout in ms for XREADGROUP")
    parser.add_argument("--consumer-name", type=str, default=None, help="Consumer name for consumer group")
    parser.add_argument(
        "--min-idle-ms",
        type=int,
        default=None,
        help="Min idle time in ms for XAUTOCLAIM (default: from REDIS_STREAM_MIN_IDLE_MS)",
    )
    args = parser.parse_args()

    is_once = args.once and not args.loop
    asyncio.run(
        run_worker(
            once=is_once,
            batch_size=args.batch_size,
            block_ms=args.block_ms,
            consumer_name=args.consumer_name,
            min_idle_ms=args.min_idle_ms,
        )
    )


if __name__ == "__main__":
    main()
