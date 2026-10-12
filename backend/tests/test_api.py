import pytest
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_empty_batch_rejected(client):
    response = client.post("/v1/telemetry/spans", json={"service_name": "srv", "spans": []})
    assert response.status_code == 422


def test_invalid_trace_id_path_rejected(client):
    response = client.get("/v1/traces/invalid-short-id")
    assert response.status_code == 422


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
