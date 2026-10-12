# PulseOps

> Production Observability & Telemetry Analysis Platform — Day 1 Foundation & Vertical Slice

---

## 1. Premise & Core Problem Statement

> **"My app is failing in prod, where do I start?"**

When production systems experience degradation, cascading errors, or sudden latency spikes, engineers often encounter fractured, delayed, or incomplete telemetry:
- Ingestion pipelines drop spans silently during bursts or crash downstream workers.
- Schemas are loose, resulting in malformed timestamps, truncated IDs, and untraceable child spans.
- Query engines return partial traces or require complex distributed joins just to reconstruct a single call path.

**PulseOps** solves this foundational problem by delivering rock-solid telemetry ingestion, strict structural validation, reliable persistence, and instant deterministic trace retrieval. 

---

## 2. Day 1 Architectural Boundaries

On **Day 1**, we intentionally avoid premature distributed complexity:
- **No microservices**
- **No message queues (Kafka / RabbitMQ)**
- **No streaming engines (Redis Streams / Flink)**
- **No frontend dashboards**

Instead, Day 1 establishes a high-fidelity **vertical slice**:
1. High-throughput HTTP ingestion with batch atomicity (all-or-nothing schema validation).
2. Direct asynchronous persistence to PostgreSQL 16 with idempotent upserts (`ON CONFLICT DO NOTHING`).
3. Nanosecond-precision timestamp validation within 64-bit signed ranges.
4. Deterministic trace retrieval by `trace_id` with sub-millisecond computed durations.

Future architectural evolutions (such as decoupling ingestion with Redis Streams or worker pools) will be driven empirically on Day 2 strictly based on measured ingestion throughput and write latency.

---

## 3. Local Setup & Quickstart

### Prerequisites
- Docker and Docker Compose
- Python 3.10+ (optional if running containerized)

### 1. Start Infrastructure
Start the dedicated PostgreSQL 16 container:
```bash
docker compose up -d
```

Verify database health:
```bash
docker ps --filter name=pulseops-postgres
```

### 2. Run Backend Locally
```bash
# Navigate to backend directory
cd backend

# Create and activate virtual environment
python -m venv .venv
# On Windows PowerShell:
.venv\Scripts\Activate.ps1
# On macOS / Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run server with hot reload
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Run Backend via Docker (Alternative)
```bash
docker build -t pulseops-backend backend/
docker run -p 8000:8000 --network host pulseops-backend
```

---

## 4. Key Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/telemetry/spans` | Ingest batch of telemetry spans with strict validation |
| `GET` | `/v1/traces/{trace_id}` | Retrieve all spans for a specific trace, ordered chronologically |
| `GET` | `/health` | Check API and database connection pool health |

---

## 5. Repository Structure

```
pulseops/
├── README.md
├── AGENTS.md
├── docker-compose.yml
├── docs/
│   ├── CURRENT_STATUS.md
│   ├── DECISION_LOG.md
│   ├── SYSTEM_ARCHITECTURE.md
│   └── API_CONTRACTS.md
└── backend/
    ├── requirements.txt
    ├── Dockerfile
    └── app/
        ├── __init__.py
        ├── main.py
        ├── config.py
        ├── db.py
        └── schemas.py
```
