# PulseOps

> Production Telemetry Ingestion & Distributed Trace Platform

---

## 1. Premise & Problem Statement

> **"My app is failing in prod, where do I start?"**

When production systems experience degradation, cascading errors, or sudden latency spikes, engineers often encounter fractured, delayed, or incomplete telemetry:
- Ingestion pipelines drop spans silently during bursts or crash downstream workers.
- Schemas are loose, resulting in malformed timestamps, truncated IDs, and untraceable child spans.
- Query engines return partial traces or require complex distributed joins just to reconstruct a single call path.

**PulseOps** delivers high-integrity telemetry ingestion, strict OpenTelemetry-compliant validation, decoupled asynchronous stream buffering, and instant deterministic trace retrieval.

---

## 2. Platform Architecture

PulseOps is designed with decoupled, benchmark-proven architecture:

- **Ingestion Gateway (FastAPI):** High-throughput HTTP API providing dual-mode ingestion:
  - `?mode=stream` (Default): Validates batches with all-or-nothing semantics, publishes to Redis Streams, and returns `HTTP 202 Accepted` immediately.
  - `?mode=direct`: Bypasses the message broker and directly commits to PostgreSQL with `HTTP 201 Created`.
- **Stream Buffer (Redis 7 Alpine):** Durable, append-only stream (`telemetry:stream`) with `--appendfsync everysec` for caller decoupling and outage resilience.
- **Consumer Worker (`worker.py`):** Multi-worker consumer daemon (`pulseops-ingest-group`) using `XREADGROUP` and `XAUTOCLAIM` with a 5-second active lease window, atomic dead-letter queue routing (`telemetry:stream:deadletter`), and parameterized PostgreSQL `UNNEST` batch persistence.
- **Storage Engine (PostgreSQL 16):** Fully indexed relational trace repository with stored generated `duration_ms` precision and primary-key idempotency (`ON CONFLICT DO NOTHING`).

---

## 3. Local Setup & Quickstart

### Prerequisites
- Docker and Docker Compose
- Python 3.10+

### 1. Start Infrastructure (PostgreSQL 16 + Redis 7)
```bash
docker compose up -d
```

Verify container health:
```bash
docker compose ps
```

### 2. Run Backend Gateway Locally
```bash
# Navigate to backend directory
cd backend

# Create and activate virtual environment
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS / Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Run Ingestion Consumer Worker (Continuous Mode)
In a separate terminal:
```bash
# Continuous consumer loop
python app/worker.py --loop --batch-size 50 --block-ms 1000

# Or run a single drain pass and exit
python app/worker.py --once
```

---

## 4. Key Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/telemetry/spans?mode=stream` | Ingest batch into Redis stream buffer (`HTTP 202 Accepted`) |
| `POST` | `/v1/telemetry/spans?mode=direct` | Direct persistence to PostgreSQL (`HTTP 201 Created`) |
| `GET` | `/v1/traces/{trace_id}` | Retrieve all spans for a trace, ordered chronologically (`HTTP 200 OK`) |
| `GET` | `/health` | Check API, PostgreSQL pool, and Redis connectivity |

---

## 5. Performance Benchmarks: Day 2 (Direct PG) vs. Day 3 (Async Stream)

Benchmarked on identical hardware (Intel Core i7 12-core, Windows 10, local Docker engine) across 177,100 spans with **0 data loss**:

| Metric | Scenario D: Large (100 spans / C=10) | Scenario D: Large (100 spans / C=10) | Scenario E: Stress (100 spans / C=25) | Scenario E: Stress (100 spans / C=25) |
|---|:---:|:---:|:---:|:---:|
| **Pipeline Mode** | **Direct PG (Day 2)** | **Async Stream (Day 3)** | **Direct PG (Day 2)** | **Async Stream (Day 3)** |
| **HTTP Response** | `201 Created` | `202 Accepted` | `201 Created` | `202 Accepted` |
| **API Wall Time** | 3.818 s | **3.563 s (-6.7%)** | 11.522 s | 12.764 s |
| **API Ingestion Rate** | 13,097.58 spans/s | **14,032.42 spans/s (+7.1%)** | 8,679.33 spans/s | 7,834.34 spans/s |
| **$p_{50}$ Latency** | 68.66 ms | **63.16 ms (-8.0%)** | 227.48 ms | 242.30 ms |
| **$p_{95}$ Latency** | 91.81 ms | 94.82 ms | 611.47 ms | 700.11 ms |
| **$p_{99}$ Latency** | 257.22 ms | 261.84 ms | 991.33 ms | 1,142.81 ms |
| **FastAPI DB Checkouts** | 20 (Saturated) | **0 (No DB Touch)** | 20 (Saturated) | **0 (No DB Touch)** |
| **PostgreSQL Discrepancy** | **0 / 50,000** | **0 / 50,000** | **0 / 100,000** | **0 / 100,000** |

> **Key Finding:** Asynchronous stream buffering decouples the caller from database connection starvation and storage outages. Under high client concurrency (Scenario E), CPU-bound Pydantic V2 recursive validation and JSON decoding on the single Python event loop remain the primary latency bottleneck, informing our roadmap toward multi-worker processes.

---

## 6. Running Verification Suites

```bash
# Run pytest unit & integration suite
python -m pytest tests/test_api.py -v

# Run 7-scenario crash & fault-tolerance verification suite
python tests/test_day3_fault_tolerance.py

# Run benchmark harness
python benchmarks/load_test.py --scenario ALL --mode stream
```
