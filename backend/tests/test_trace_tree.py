import os
import sys
import uuid
import pytest
from fastapi.testclient import TestClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR)
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

# Ensure client-side test JSON parser can handle 500+ sequentially nested test trees
sys.setrecursionlimit(max(sys.getrecursionlimit(), 5000))

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


def test_trace_tree_mutual_two_span_cycle(client):
    """
    Test verifying that a 2-span mutual cycle (A -> B -> A) is broken gracefully,
    routed to orphans without cyclic children references, and serializes cleanly
    with HTTP 200 without triggering a RecursionError.
    """
    trace_id = uuid.uuid4().hex
    span_a_id = "5000000000000001"
    span_b_id = "5000000000000002"

    payload = {
        "service_name": "cycle-service",
        "spans": [
            {
                "span_id": span_a_id,
                "trace_id": trace_id,
                "parent_span_id": span_b_id,  # A points to B
                "operation_name": "cycle_op_a",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_040_000_000,  # 40 ms
                "status_code": "OK",
            },
            {
                "span_id": span_b_id,
                "trace_id": trace_id,
                "parent_span_id": span_a_id,  # B points to A (mutual cycle)
                "operation_name": "cycle_op_b",
                "start_time_unix_nano": 1_000_020_000_000,
                "end_time_unix_nano": 1_000_060_000_000,  # 40 ms
                "status_code": "OK",
            },
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    # Request tree endpoint; must not throw RecursionError and return HTTP 200
    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["span_count"] == 2
    assert tree["roots"] == []
    assert len(tree["orphans"]) == 2

    # Verify both spans are in orphans with empty children (acyclic guarantee)
    orphan_ids = {n["span_id"] for n in tree["orphans"]}
    assert orphan_ids == {span_a_id, span_b_id}
    for n in tree["orphans"]:
        assert n["children"] == []
        assert n["depth"] == 0

    # Verify metrics computed cleanly
    assert tree["max_depth"] == 0
    # Merged union: [0, 40] U [20, 60] = [0, 60] -> 60 ms
    assert tree["active_execution_ms"] == 60.0
    assert tree["wall_clock_duration_ms"] == 60.0
    assert tree["longest_path_duration_ms"] == 40.0
    assert tree["critical_path_ms"] == 60.0


def test_trace_tree_self_parent(client):
    """
    Test Matrix 1: Self-Parent span where parent_span_id == span_id.
    Must not loop infinitely or cause RecursionError; routed directly to orphans.
    """
    trace_id = uuid.uuid4().hex
    self_span_id = "6000000000000001"

    payload = {
        "service_name": "self-loop-service",
        "spans": [
            {
                "span_id": self_span_id,
                "trace_id": trace_id,
                "parent_span_id": self_span_id,  # Self-reference
                "operation_name": "self_loop_op",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_025_000_000,  # 25 ms
                "status_code": "OK",
            }
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["span_count"] == 1
    assert tree["roots"] == []
    assert len(tree["orphans"]) == 1

    orphan_node = tree["orphans"][0]
    assert orphan_node["span_id"] == self_span_id
    assert orphan_node["parent_span_id"] == self_span_id
    assert orphan_node["children"] == []
    assert orphan_node["depth"] == 0
    assert tree["max_depth"] == 0
    assert tree["wall_clock_duration_ms"] == 25.0
    assert tree["active_execution_ms"] == 25.0
    assert tree["longest_path_duration_ms"] == 25.0


def test_trace_tree_multi_node_cycle(client):
    """
    Test Matrix 3: Multi-Node Cycle: A -> B -> C -> A.
    Cycle must be detected and broken, all cyclic nodes safely categorized
    into orphans, zero cycle in children, serializing with HTTP 200.
    """
    trace_id = uuid.uuid4().hex
    span_a_id = "6100000000000001"
    span_b_id = "6100000000000002"
    span_c_id = "6100000000000003"

    payload = {
        "service_name": "multi-cycle-service",
        "spans": [
            {
                "span_id": span_a_id,
                "trace_id": trace_id,
                "parent_span_id": span_c_id,  # A -> C
                "operation_name": "multi_cycle_a",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_030_000_000,  # 30 ms
                "status_code": "OK",
            },
            {
                "span_id": span_b_id,
                "trace_id": trace_id,
                "parent_span_id": span_a_id,  # B -> A
                "operation_name": "multi_cycle_b",
                "start_time_unix_nano": 1_000_010_000_000,
                "end_time_unix_nano": 1_000_040_000_000,  # 30 ms
                "status_code": "OK",
            },
            {
                "span_id": span_c_id,
                "trace_id": trace_id,
                "parent_span_id": span_b_id,  # C -> B
                "operation_name": "multi_cycle_c",
                "start_time_unix_nano": 1_000_020_000_000,
                "end_time_unix_nano": 1_000_050_000_000,  # 30 ms
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
    assert tree["span_count"] == 3
    assert tree["roots"] == []
    assert len(tree["orphans"]) == 3

    # All nodes are preserved in orphans without cycles in children
    orphan_ids = {n["span_id"] for n in tree["orphans"]}
    assert orphan_ids == {span_a_id, span_b_id, span_c_id}
    for n in tree["orphans"]:
        assert n["children"] == []
        assert n["depth"] == 0

    assert tree["max_depth"] == 0
    # Merged union: [0, 30] U [10, 40] U [20, 50] = [0, 50] -> 50 ms
    assert tree["active_execution_ms"] == 50.0
    assert tree["wall_clock_duration_ms"] == 50.0
    assert tree["longest_path_duration_ms"] == 30.0
    assert tree["critical_path_ms"] == 50.0


def test_trace_tree_missing_parent(client):
    """
    Test Matrix 4: Missing Parent: Span referencing a non-existent parent span ID.
    Must be routed directly to orphans without dropping spans or throwing 500 errors.
    """
    trace_id = uuid.uuid4().hex
    orphan_id = "6200000000000001"
    non_existent_parent = "eeeeeeeeeeeeeeee"

    payload = {
        "service_name": "missing-parent-service",
        "spans": [
            {
                "span_id": orphan_id,
                "trace_id": trace_id,
                "parent_span_id": non_existent_parent,
                "operation_name": "detached_span",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_020_000_000,  # 20 ms
                "status_code": "OK",
            }
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["span_count"] == 1
    assert tree["roots"] == []
    assert len(tree["orphans"]) == 1
    assert tree["orphans"][0]["span_id"] == orphan_id
    assert tree["orphans"][0]["parent_span_id"] == non_existent_parent
    assert tree["orphans"][0]["children"] == []
    assert tree["orphans"][0]["depth"] == 0


def test_trace_tree_multiple_independent_roots(client):
    """
    Test Matrix 5: Multiple Independent Roots: 3 distinct roots with parent_span_id=None,
    each with their own child subtree.
    All roots must appear in roots list at depth=0, each with their respective children at depth=1.
    """
    trace_id = uuid.uuid4().hex
    r1_id = "6300000000000001"
    c1_id = "6300000000000002"
    r2_id = "6300000000000003"
    c2_id = "6300000000000004"
    r3_id = "6300000000000005"
    c3_id = "6300000000000006"

    payload = {
        "service_name": "multi-root-service",
        "spans": [
            # Root 1 + Child 1
            {
                "span_id": r1_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "root_1",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_010_000_000,  # 10 ms
                "status_code": "OK",
            },
            {
                "span_id": c1_id,
                "trace_id": trace_id,
                "parent_span_id": r1_id,
                "operation_name": "child_1",
                "start_time_unix_nano": 1_000_010_000_000,
                "end_time_unix_nano": 1_000_020_000_000,  # 10 ms
                "status_code": "OK",
            },
            # Root 2 + Child 2
            {
                "span_id": r2_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "root_2",
                "start_time_unix_nano": 1_000_020_000_000,
                "end_time_unix_nano": 1_000_030_000_000,  # 10 ms
                "status_code": "OK",
            },
            {
                "span_id": c2_id,
                "trace_id": trace_id,
                "parent_span_id": r2_id,
                "operation_name": "child_2",
                "start_time_unix_nano": 1_000_030_000_000,
                "end_time_unix_nano": 1_000_040_000_000,  # 10 ms
                "status_code": "OK",
            },
            # Root 3 + Child 3
            {
                "span_id": r3_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "root_3",
                "start_time_unix_nano": 1_000_040_000_000,
                "end_time_unix_nano": 1_000_050_000_000,  # 10 ms
                "status_code": "OK",
            },
            {
                "span_id": c3_id,
                "trace_id": trace_id,
                "parent_span_id": r3_id,
                "operation_name": "child_3",
                "start_time_unix_nano": 1_000_050_000_000,
                "end_time_unix_nano": 1_000_060_000_000,  # 10 ms
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
    assert tree["span_count"] == 6
    assert tree["max_depth"] == 1
    assert tree["orphans"] == []

    # Exactly 3 roots, all at depth=0
    assert len(tree["roots"]) == 3
    root_ids = {r["span_id"] for r in tree["roots"]}
    assert root_ids == {r1_id, r2_id, r3_id}

    for root_node in tree["roots"]:
        assert root_node["depth"] == 0
        assert len(root_node["children"]) == 1
        child_node = root_node["children"][0]
        assert child_node["depth"] == 1
        assert child_node["children"] == []


def test_trace_tree_deeply_nested_500_spans(client):
    """
    Test Matrix 6: Deeply Nested Trace with 500 sequential child spans
    (Root -> S1 -> S2 -> ... -> S500).
    Guarantees iterative traversal achieves max_depth = 500 without call-stack
    exhaustion (RecursionError) and serializes cleanly under bounded memory.
    """
    trace_id = uuid.uuid4().hex
    root_id = "7000000000000000"

    spans = [
        {
            "span_id": root_id,
            "trace_id": trace_id,
            "parent_span_id": None,
            "operation_name": "deep_root",
            "start_time_unix_nano": 1_000_000_000_000,
            "end_time_unix_nano": 1_000_001_000_000,  # 1 ms
            "status_code": "OK",
        }
    ]

    prev_id = root_id
    base_time = 1_000_001_000_000
    for i in range(1, 501):
        curr_id = f"7{i:015d}"
        spans.append(
            {
                "span_id": curr_id,
                "trace_id": trace_id,
                "parent_span_id": prev_id,
                "operation_name": f"nested_child_{i}",
                "start_time_unix_nano": base_time + (i - 1) * 1_000_000,
                "end_time_unix_nano": base_time + i * 1_000_000,  # 1 ms each
                "status_code": "OK",
            }
        )
        prev_id = curr_id

    payload = {
        "service_name": "deep-nesting-service",
        "spans": spans,
    }

    # Ingest 501 spans in a single atomic batch
    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    # Query DAG Tree endpoint
    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200
    tree = resp.json()

    assert tree["trace_id"] == trace_id
    assert tree["span_count"] == 501
    assert tree["max_depth"] == 500
    assert len(tree["roots"]) == 1
    assert tree["orphans"] == []

    # Verify metrics: 501 spans of 1ms each in sequential order
    assert tree["wall_clock_duration_ms"] == 501.0
    assert tree["active_execution_ms"] == 501.0
    assert tree["longest_path_duration_ms"] == 501.0


def test_trace_tree_metric_semantics(client):
    """
    Test Matrix 7: Metric Semantics Verification:
    Root (0 to 100ms, dur 100ms),
    Child 1 (10 to 40ms, dur 30ms, parent=Root),
    Child 2 (30 to 80ms, dur 50ms, parent=Root).

    Assert:
    - wall_clock_duration_ms == 100.0 (overall span envelope)
    - active_execution_ms == 100.0 (all sub-spans covered by root)
    - longest_path_duration_ms == 150.0 (Root 100ms + Child 2 50ms)
    - critical_path_ms == 100.0 (backward-compatibility alias mirroring active_execution_ms)
    """
    trace_id = uuid.uuid4().hex
    root_id = "8000000000000001"
    child1_id = "8000000000000002"
    child2_id = "8000000000000003"

    payload = {
        "service_name": "metric-semantics-service",
        "spans": [
            {
                "span_id": root_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "root_operation",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_100_000_000,  # 100 ms (0 to 100ms)
                "status_code": "OK",
            },
            {
                "span_id": child1_id,
                "trace_id": trace_id,
                "parent_span_id": root_id,
                "operation_name": "child_op_1",
                "start_time_unix_nano": 1_000_010_000_000,  # 10ms
                "end_time_unix_nano": 1_000_040_000_000,    # 40ms (30ms duration)
                "status_code": "OK",
            },
            {
                "span_id": child2_id,
                "trace_id": trace_id,
                "parent_span_id": root_id,
                "operation_name": "child_op_2",
                "start_time_unix_nano": 1_000_030_000_000,  # 30ms
                "end_time_unix_nano": 1_000_080_000_000,    # 80ms (50ms duration)
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
    assert tree["span_count"] == 3
    assert tree["max_depth"] == 1
    assert len(tree["roots"]) == 1
    assert len(tree["roots"][0]["children"]) == 2

    # Metric asserts per specification
    assert tree["wall_clock_duration_ms"] == 100.0
    assert tree["active_execution_ms"] == 100.0
    assert tree["longest_path_duration_ms"] == 150.0
    assert tree["critical_path_ms"] == 100.0


def test_trace_tree_disjoint_idle_gaps(client):
    """
    Test Matrix 8: Disjoint Idle Gaps:
    Two disjoint spans: 0 to 10ms and 90 to 100ms.
    Assert wall_clock_duration_ms == 100.0 and active_execution_ms == 20.0
    (verifying that 80ms of idle window is correctly excluded).
    """
    trace_id = uuid.uuid4().hex
    span1_id = "9000000000000001"
    span2_id = "9000000000000002"

    payload = {
        "service_name": "idle-gap-service",
        "spans": [
            {
                "span_id": span1_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "early_op",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_010_000_000,  # 10 ms (0 to 10ms)
                "status_code": "OK",
            },
            {
                "span_id": span2_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "late_op",
                "start_time_unix_nano": 1_000_090_000_000,  # 90ms
                "end_time_unix_nano": 1_000_100_000_000,   # 100ms (10 ms)
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
    assert tree["wall_clock_duration_ms"] == 100.0
    assert tree["active_execution_ms"] == 20.0
    assert tree["longest_path_duration_ms"] == 10.0
    assert tree["critical_path_ms"] == 20.0


def test_trace_tree_server_timing_header(client):
    """
    Day 8 Task 1 Test: Verify Server-Timing header in GET /v1/traces/{trace_id}/tree.
    Format: Server-Timing: db;dur=<ms>, dag;dur=<ms>, serialize;dur=<ms>
    """
    import re

    trace_id = uuid.uuid4().hex
    span_id = "8888000000000001"

    payload = {
        "service_name": "timing-service",
        "spans": [
            {
                "span_id": span_id,
                "trace_id": trace_id,
                "parent_span_id": None,
                "operation_name": "timing_op",
                "start_time_unix_nano": 1_000_000_000_000,
                "end_time_unix_nano": 1_000_010_000_000,
                "status_code": "OK",
            }
        ],
    }

    ingest_resp = client.post("/v1/telemetry/spans?mode=direct", json=payload)
    assert ingest_resp.status_code == 201

    resp = client.get(f"/v1/traces/{trace_id}/tree")
    assert resp.status_code == 200

    server_timing = resp.headers.get("server-timing")
    assert server_timing is not None, "Server-Timing header missing"

    match = re.search(
        r"db;dur=([0-9.]+),\s*dag;dur=([0-9.]+),\s*serialize;dur=([0-9.]+)",
        server_timing,
    )
    assert match is not None, f"Server-Timing format invalid: {server_timing}"

    t_db = float(match.group(1))
    t_dag = float(match.group(2))
    t_ser = float(match.group(3))

    assert t_db >= 0.0
    assert t_dag >= 0.0
    assert t_ser >= 0.0

