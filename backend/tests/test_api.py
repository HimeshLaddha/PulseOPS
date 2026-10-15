import os
import sys
import pytest
from fastapi.testclient import TestClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from app.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert data["redis"] == "connected"


def test_empty_batch_rejected(client):
    response = client.post("/v1/telemetry/spans", json={"service_name": "srv", "spans": []})
    assert response.status_code == 422


def test_invalid_trace_id_path_rejected(client):
    response = client.get("/v1/traces/invalid-short-id")
    assert response.status_code == 422


def test_stream_ingest_and_validation(client):
    payload = {
        "service_name": "pytest-stream-service",
        "spans": [
            {
                "span_id": "aa11bb22cc33dd44",
                "trace_id": "0123456789abcdef0123456789abcdef",
                "parent_span_id": None,
                "operation_name": "stream_test_op",
                "start_time_unix_nano": 1700000000000000000,
                "end_time_unix_nano": 1700000000050000000,
                "status_code": "OK",
                "attributes": {"stream": True},
            }
        ],
    }

    # Stream ingestion accepted
    resp = client.post("/v1/telemetry/spans?mode=stream", json=payload)
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"
    assert "stream_id" in data
    assert data["span_count"] == 1

    # Invalid batch in stream mode rejected with 422
    bad_payload = {
        "service_name": "bad",
        "spans": [{**payload["spans"][0], "trace_id": "short"}],
    }
    resp_bad = client.post("/v1/telemetry/spans?mode=stream", json=bad_payload)
    assert resp_bad.status_code == 422


def test_ingest_and_query_vertical_slice(client):
    trace_id = "0123456789abcdef0123456789abcde0"
    span_id = "1122334455667788"

    payload = {
        "service_name": "pytest-service",
        "spans": [
            {
                "span_id": span_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "root_operation",
                "start_time_unix_nano": 1700000000000000000,
                "end_time_unix_nano": 1700000000100000000,
                "status_code": "OK",
                "attributes": {"env": "test"},
            }
        ],
    }

    # Ingestion
    resp = client.post("/v1/telemetry/spans", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "accepted"

    # Query
    get_resp = client.get(f"/v1/traces/{trace_id}")
    assert get_resp.status_code == 200
    trace = get_resp.json()
    assert trace["trace_id"] == trace_id
    assert trace["span_count"] >= 1
    assert trace["spans"][0]["duration_ms"] == 100.0


def test_stream_ingest_with_custom_stream_key(client):
    custom_stream = "telemetry:stream:test_isolated"
    payload = {
        "service_name": "pytest-custom-stream",
        "spans": [
            {
                "span_id": "cc22dd33ee44ff55",
                "trace_id": "0123456789abcdef0123456789abcdef",
                "parent_span_id": None,
                "operation_name": "isolated_stream_op",
                "start_time_unix_nano": 1700000000000000000,
                "end_time_unix_nano": 1700000000050000000,
                "status_code": "OK",
                "attributes": {"isolated": True},
            }
        ],
    }

    resp = client.post(f"/v1/telemetry/spans?mode=stream&stream_key={custom_stream}", json=payload)
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"
    assert "stream_id" in data


def test_benchmark_stream_isolation(client):
    """
    Verify that benchmark stream writes and cleanups are strictly isolated
    and preserve default application stream data.
    """
    import redis
    from app.config import get_settings

    settings = get_settings()
    sync_redis = redis.from_url(settings.redis_url, decode_responses=True)
    benchmark_stream = "telemetry:stream:benchmark"
    default_stream = settings.REDIS_STREAM_KEY

    # 1. Write sentinel batch to default stream
    sentinel_payload = {
        "service_name": "app-sentinel-service",
        "spans": [
            {
                "span_id": "9988776655443322",
                "trace_id": "0123456789abcdef0123456789abcdef",
                "parent_span_id": None,
                "operation_name": "sentinel_op",
                "start_time_unix_nano": 1700000000000000000,
                "end_time_unix_nano": 1700000000050000000,
                "status_code": "OK",
                "attributes": {"sentinel": True},
            }
        ],
    }
    resp_def = client.post("/v1/telemetry/spans?mode=stream", json=sentinel_payload)
    assert resp_def.status_code == 202
    def_id = resp_def.json()["stream_id"]

    # 2. Write benchmark batch to isolated stream
    bench_payload = {
        "service_name": "bench-isolated-service",
        "spans": [
            {
                "span_id": "1122334455667788",
                "trace_id": "0123456789abcdef0123456789abcdef",
                "parent_span_id": None,
                "operation_name": "bench_op",
                "start_time_unix_nano": 1700000000000000000,
                "end_time_unix_nano": 1700000000050000000,
                "status_code": "OK",
                "attributes": {"benchmark": True},
            }
        ],
    }
    resp_bench = client.post(
        f"/v1/telemetry/spans?mode=stream&stream_key={benchmark_stream}",
        json=bench_payload,
    )
    assert resp_bench.status_code == 202
    bench_id = resp_bench.json()["stream_id"]

    # 3. Simulate benchmark cleanup strictly targeting the benchmark stream
    sync_redis.delete(benchmark_stream)
    sync_redis.delete(f"{benchmark_stream}:deadletter")
    assert sync_redis.xlen(benchmark_stream) == 0

    # 4. Verify default stream data was untouched
    default_entries = sync_redis.xrange(default_stream)
    default_entry_ids = [entry[0] for entry in default_entries]
    assert def_id in default_entry_ids, "Default application stream was modified or wiped!"

    # Clean up sentinel from default stream
    sync_redis.xdel(default_stream, def_id)
    sync_redis.close()
