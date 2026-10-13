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
    fields: dict,
) -> int:
    """
    Process a single stream message:
    1. Parse JSON payload into SpanBatchIngestRequest.
    2. Insert spans into PostgreSQL using parameterized UNNEST with ON CONFLICT DO NOTHING.
    3. Acknowledge with XACK only after DB commit succeeds.
    """
    raw_data = fields.get("data")
    if not raw_data:
        logger.warning("Message %s has no 'data' field. Acknowledging and discarding.", msg_id)
        await redis_client.xack(stream_key, group_name, msg_id)
        await redis_client.xdel(stream_key, msg_id)
        return 0

    try:
        data = json.loads(raw_data)
        batch = SpanBatchIngestRequest.model_validate(data)
    except Exception as exc:
        logger.error(
            "Poison pill detected in message %s: %s. Routing to deadletter and acknowledging.",
            msg_id,
            exc,
        )
        try:
            await redis_client.xadd(
                f"{stream_key}:deadletter",
                {"original_id": msg_id, "error": str(exc), "data": str(raw_data)},
            )
        except Exception as dl_err:
            logger.warning("Failed to write to deadletter stream: %s", dl_err)
        await redis_client.xack(stream_key, group_name, msg_id)
        await redis_client.xdel(stream_key, msg_id)
        return 0

    inserted = await insert_spans(pool=pool, service_name=batch.service_name, spans=batch.spans)
    logger.debug("Persisted %d spans for service '%s' (message %s).", inserted, batch.service_name, msg_id)

    # Acknowledge and trim from stream ONLY after DB insertion succeeds
    await redis_client.xack(stream_key, group_name, msg_id)
    await redis_client.xdel(stream_key, msg_id)
    return inserted


async def run_worker(
    once: bool = False,
    batch_size: int = 10,
    block_ms: int = 2000,
    consumer_name: Optional[str] = None,
):
    settings = get_settings()
    if consumer_name is None:
        consumer_name = f"worker-{uuid.uuid4().hex[:8]}"

    logger.info("Starting PulseOps ingestion worker '%s'...", consumer_name)

    # Initialize PostgreSQL pool with graceful error handling
    pool = None
    try:
        pool = await init_db_pool(settings)
    except Exception as db_exc:
        logger.error("Could not connect to PostgreSQL: %s. Worker exiting gracefully.", db_exc)
        return

    redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)

    try:
        await init_consumer_group(redis_client, settings.REDIS_STREAM_KEY, settings.REDIS_CONSUMER_GROUP)

        running = True
        total_processed = 0

        while running:
            try:
                # 1. Process pending / abandoned messages first (e.g., from crashed workers)
                try:
                    claim_res = await redis_client.xautoclaim(
                        name=settings.REDIS_STREAM_KEY,
                        groupname=settings.REDIS_CONSUMER_GROUP,
                        consumername=consumer_name,
                        min_idle_time=0,
                        start_id="0-0",
                        count=batch_size,
                    )
                    claimed_messages = claim_res[1] if len(claim_res) > 1 else []
                    for msg_id, fields in claimed_messages:
                        if fields:
                            inserted = await process_batch_message(
                                pool=pool,
                                redis_client=redis_client,
                                stream_key=settings.REDIS_STREAM_KEY,
                                group_name=settings.REDIS_CONSUMER_GROUP,
                                msg_id=msg_id,
                                fields=fields,
                            )
                            total_processed += 1
                except Exception as claim_err:
                    logger.debug("xautoclaim encountered: %s", claim_err)

                # 2. Read new messages with '>'
                response = await redis_client.xreadgroup(
                    groupname=settings.REDIS_CONSUMER_GROUP,
                    consumername=consumer_name,
                    streams={settings.REDIS_STREAM_KEY: ">"},
                    count=batch_size,
                    block=block_ms if not once else 500,
                )

                if not response and total_processed == 0 and once:
                    logger.info("No messages to process in --once mode. Exiting.")
                    break

                if response:
                    for stream_name, messages in response:
                        for msg_id, fields in messages:
                            inserted = await process_batch_message(
                                pool=pool,
                                redis_client=redis_client,
                                stream_key=settings.REDIS_STREAM_KEY,
                                group_name=settings.REDIS_CONSUMER_GROUP,
                                msg_id=msg_id,
                                fields=fields,
                            )
                            total_processed += 1

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
    args = parser.parse_args()

    is_once = args.once and not args.loop
    asyncio.run(
        run_worker(
            once=is_once,
            batch_size=args.batch_size,
            block_ms=args.block_ms,
            consumer_name=args.consumer_name,
        )
    )


if __name__ == "__main__":
    main()
