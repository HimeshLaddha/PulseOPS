import os
import sys
import uuid
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


def test_sequential_linear_trace_tree(client):
    """
    Test 1 (Sequential Linear Trace): Root -> Child A -> Child B.
    Verify max_depth = 2, longest_path_duration_ms == sum(durations),
    and active_execution_ms equals sequential sum.
    """
    trace_id = uuid.uuid4().hex
    root_id = "1000000000000001"
    child_a_id = "1000000000000002"
    child_b_id = "1000000000000003"

    payload = {
        "service_name": "checkout-service",
        "spans": [
            {
                "span_id": root_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "checkout_request",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_030_000_000,  # 30 ms
                "status_code": "OK",
                "attributes": {"step": "root"},
            },
            {
                "span_id": child_a_id,
                "trace_id": trace_id,
                "parent_span_id": root_id,
                "operation_name": "authorize_payment",
                "start_time_unix_nano": 1_000_030_000_000,
                "end_time_unix_nano": 1_000_070_000_000,  # 40 ms
                "status_code": "OK",
                "attributes": {"step": "child_a"},
            },
            {
                "span_id": child_b_id,
                "trace_id": trace_id,
                "parent_span_id": child_a_id,
                "operation_name": "charge_card_gateway",
                "start_time_unix_nano": 1_000_070_000_000,
                "end_time_unix_nano": 1_000_100_000_000,  # 30 ms
                "status_code": "OK",
                "attributes": {"step": "child_b"},
            },
        ],
    }

    # Ingest direct to PostgreSQL
    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    # Query DAG Tree endpoint
    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    # Basic metadata verification
    assert tree["trace_id"] == trace_id
    assert tree["root_service"] == "checkout-service"
    assert tree["span_count"] == 3

    # Verification: max_depth = 2 (Root=0, Child A=1, Child B=2)
    assert tree["max_depth"] == 2

    # Verification: longest_path_duration_ms == sum(durations)
    durations_sum = 30.0 + 40.0 + 30.0
    assert tree["longest_path_duration_ms"] == durations_sum
    assert tree["longest_path_duration_ms"] == 100.0

    # Verification: active_execution_ms equals sequential sum
    assert tree["active_execution_ms"] == durations_sum
    assert tree["active_execution_ms"] == 100.0

    # Verification: wall_clock_duration_ms equals overall envelope
    assert tree["wall_clock_duration_ms"] == 100.0

    # Hierarchy and depth assignment validation
    assert len(tree["roots"]) == 1
    assert tree["orphans"] == []

    root_node = tree["roots"][0]
    assert root_node["span_id"] == root_id
    assert root_node["parent_span_id"] is None
    assert root_node["depth"] == 0
    assert len(root_node["children"]) == 1

    child_a_node = root_node["children"][0]
    assert child_a_node["span_id"] == child_a_id
    assert child_a_node["parent_span_id"] == root_id
    assert child_a_node["depth"] == 1
    assert len(child_a_node["children"]) == 1

    child_b_node = child_a_node["children"][0]
    assert child_b_node["span_id"] == child_b_id
    assert child_b_node["parent_span_id"] == child_a_id
    assert child_b_node["depth"] == 2
    assert child_b_node["children"] == []

    # Backwards compatibility check for existing flat trace endpoint
    flat_resp = client.get(f"/v1/traces/{trace_id}")
    assert flat_resp.status_code == 200
    flat_data = flat_resp.json()
    assert flat_data["span_count"] == 3
    assert len(flat_data["spans"]) == 3


def test_parallel_concurrent_branches_trace_tree(client):
    """
    Test 2 (Parallel Concurrent Branches): Root dispatching Child A and Child B concurrently
    with overlapping intervals.
    Verify len(children) == 2, active_execution_ms equals the merged union boundary,
    and longest_path_duration_ms equals root.duration + max(child_A, child_B).
    """
    trace_id = uuid.uuid4().hex
    root_id = "2000000000000001"
    child_a_id = "2000000000000002"
    child_b_id = "2000000000000003"

    root_duration = 20.0
    child_a_duration = 60.0
    child_b_duration = 50.0

    payload = {
        "service_name": "gateway-aggregator",
        "spans": [
            {
                "span_id": root_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "aggregate_dashboard",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_020_000_000,  # 20 ms
                "status_code": "OK",
            },
            {
                "span_id": child_a_id,
                "trace_id": trace_id,
                "parent_span_id": root_id,
                "operation_name": "fetch_user_profile",
                "start_time_unix_nano": 1_000_020_000_000,
                "end_time_unix_nano": 1_000_080_000_000,  # 60 ms duration (20ms -> 80ms)
                "status_code": "OK",
            },
            {
                "span_id": child_b_id,
                "trace_id": trace_id,
                "parent_span_id": root_id,
                "operation_name": "fetch_account_balance",
                "start_time_unix_nano": 1_000_020_000_000,
                "end_time_unix_nano": 1_000_070_000_000,  # 50 ms duration (20ms -> 70ms)
                "status_code": "OK",
            },
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["root_service"] == "gateway-aggregator"
    assert tree["span_count"] == 3
    assert tree["max_depth"] == 1
    assert tree["orphans"] == []

    # Verification: len(children) == 2
    assert len(tree["roots"]) == 1
    root_node = tree["roots"][0]
    assert len(root_node["children"]) == 2

    child_ids = [c["span_id"] for c in root_node["children"]]
    assert set(child_ids) == {child_a_id, child_b_id}
    for c in root_node["children"]:
        assert c["children"] == []
        assert c["depth"] == 1

    # Verification: active_execution_ms equals the merged union boundary (80 ms)
    # [0, 20] U [20, 80] U [20, 70] = [0, 80] -> 80 ms
    assert tree["active_execution_ms"] == 80.0

    # Verification: longest_path_duration_ms equals root.duration + max(child_A, child_B)
    expected_longest = root_duration + max(child_a_duration, child_b_duration)
    assert tree["longest_path_duration_ms"] == expected_longest
    assert tree["longest_path_duration_ms"] == 80.0

    # Contrast against naive summation
    naive_sum = root_duration + child_a_duration + child_b_duration  # 130 ms
    assert tree["active_execution_ms"] < naive_sum
    assert tree["longest_path_duration_ms"] < naive_sum


def test_orphan_and_broken_hierarchy_trace_tree(client):
    """
    Test 3 (Orphan & Broken Hierarchy): Ingest span referencing non-existent parent_span_id.
    Verify status 200, span placed in orphans, and tree returns gracefully without crashing.
    """
    trace_id = uuid.uuid4().hex
    root_id = "3000000000000001"
    orphan_id = "3000000000000002"
    non_existent_parent = "ffffffffffffffff"

    payload = {
        "service_name": "async-pipeline",
        "spans": [
            {
                "span_id": root_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "pipeline_init",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_050_000_000,  # 50 ms
                "status_code": "OK",
            },
            {
                "span_id": orphan_id,
                "trace_id": trace_id,
                "parent_span_id": non_existent_parent,
                "operation_name": "dangling_callback",
                "start_time_unix_nano": 1_000_060_000_000,
                "end_time_unix_nano": 1_000_090_000_000,  # 30 ms
                "status_code": "OK",
            },
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["span_count"] == 2
    assert tree["max_depth"] == 0

    # Verification: root placed in roots
    assert len(tree["roots"]) == 1
    assert tree["roots"][0]["span_id"] == root_id
    assert tree["roots"][0]["children"] == []
    assert tree["roots"][0]["depth"] == 0

    # Verification: span placed in orphans gracefully
    assert len(tree["orphans"]) == 1
    orphan_node = tree["orphans"][0]
    assert orphan_node["span_id"] == orphan_id
    assert orphan_node["parent_span_id"] == non_existent_parent
    assert orphan_node["operation_name"] == "dangling_callback"
    assert orphan_node["children"] == []
    assert orphan_node["depth"] == 0

    # Disjoint intervals: [0, 50] and [60, 90] -> active = 80 ms, wall clock envelope = 90 ms
    assert tree["active_execution_ms"] == 80.0
    assert tree["wall_clock_duration_ms"] == 90.0
    assert tree["longest_path_duration_ms"] == 50.0  # max(50.0, 30.0)


def test_missing_trace_returns_404(client):
    """
    Test 4 (Missing Trace): Non-existent trace_id returns HTTP 404.
    Also verifies 422 on invalid trace_id format.
    """
    non_existent_trace_id = "00000000000000000000000000000000"

    resp_404 = client.get(f"/v1/traces/{non_existent_trace_id}/tree")
    assert resp_404.status_code == 404
    assert resp_404.json()["detail"] == f"Trace {non_existent_trace_id} not found"

    # Invalid regex trace_id returns 422
    resp_422 = client.get("/v1/traces/too-short/tree")
    assert resp_422.status_code == 422
