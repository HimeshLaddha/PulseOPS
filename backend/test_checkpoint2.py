import asyncio
from app.db import init_db_pool, close_db_pool
from app.schemas import SpanModel, SpanBatchIngestRequest
from pydantic import ValidationError


async def main():
    print("Testing Checkpoint 2: Schemas & Database Lifecycle...")
    
    # 1. Test Schema Validation
    print("\n--- 1. Testing Schema Validation ---")
    valid_span_data = {
        "span_id": "4a7f9b2c1d8e3f01",
        "trace_id": "0123456789abcdef0123456789abcdef",
        "parent_span_id": None,
        "operation_name": "checkout",
        "start_time_unix_nano": 1000000000,
        "end_time_unix_nano": 2000000000,
        "status_code": "OK",
        "attributes": {"http.status": 200}
    }
    span = SpanModel(**valid_span_data)
    assert span.span_id == "4a7f9b2c1d8e3f01"
    print("[OK] Valid SpanModel parsed successfully")

    # Negative test: invalid span_id length
    try:
        SpanModel(**{**valid_span_data, "span_id": "abc"})
        assert False, "Should fail on short span_id"
    except ValidationError:
        print("[OK] Invalid span_id rejected with ValidationError")

    # Negative test: invalid trace_id length
    try:
        SpanModel(**{**valid_span_data, "trace_id": "1234"})
        assert False, "Should fail on short trace_id"
    except ValidationError:
        print("[OK] Invalid trace_id rejected with ValidationError")

    # Negative test: end_time < start_time
    try:
        SpanModel(**{**valid_span_data, "start_time_unix_nano": 500, "end_time_unix_nano": 100})
        assert False, "Should fail when end_time < start_time"
    except ValidationError:
        print("[OK] end_time < start_time rejected with ValidationError")

    # Negative test: empty batch
    try:
        SpanBatchIngestRequest(service_name="payment", spans=[])
        assert False, "Should fail on empty spans list"
    except ValidationError:
        print("[OK] Empty batch rejected with ValidationError")

    # 2. Test Connection Pool & DDL Execution
    print("\n--- 2. Testing Database Pool & DDL Execution ---")
    pool = await init_db_pool()
    async with pool.acquire() as conn:
        table_name = await conn.fetchval("SELECT to_regclass('spans')::text;")
        assert table_name == "spans", f"Expected 'spans', got '{table_name}'"
        print("[OK] Table 'spans' exists:", table_name)

        columns = await conn.fetch("""
            SELECT column_name, data_type, is_generated
            FROM information_schema.columns
            WHERE table_name = 'spans'
            ORDER BY ordinal_position;
        """)
        for col in columns:
            print(f"  Column: {col['column_name']} | Type: {col['data_type']} | Generated: {col['is_generated']}")

        indices = await conn.fetch("""
            SELECT indexname FROM pg_indexes WHERE tablename = 'spans';
        """)
        idx_names = [i["indexname"] for i in indices]
        print("[OK] Indexes created:", idx_names)
        assert "spans_pkey" in idx_names
        assert "idx_spans_trace_id" in idx_names
        assert "idx_spans_service_start" in idx_names

    await close_db_pool()
    print("\n[OK] Checkpoint 2 VERIFIED: Schema rules enforced and database pool + DDL active!")


if __name__ == "__main__":
    asyncio.run(main())
