Markdown# Event-Driven Predictive Inventory & Automated Procurement Engine

A high-concurrency, distributed inventory platform built with **FastAPI**, **PostgreSQL**, **Redis**, and **Apache Kafka (KRaft)**. 

The system mitigates double-allocation race conditions under peak traffic using a two-tier locking strategy, streams checkout events via Kafka, computes real-time 7-day rolling burn rates, and autonomously triggers supplier Purchase Order (PO) webhooks when stock crosses predictive depletion thresholds.

---

## Key Architecture & Features

* **Sub-Millisecond Distributed Locks:** Leverages Redis key-level locking (`SETNX` with TTL) at the API boundary, paired with transactional PostgreSQL row locks (`SELECT ... FOR UPDATE`), preventing race conditions and inventory overselling.
* **Deadlock-Free Batch Transactions:** Implements deterministic lock ordering (`sorted(bin_ids)`) across multi-item checkout and restock transactions.
* **Event-Driven Architecture:** Decouples customer-facing API latency from analytical processing by publishing change events to Kafka (`inventory-checkout-events`).
* **Predictive Runout Engine:** Background asynchronous workers calculate 7-day moving burn rates:
  $$\text{Daily Burn Rate} = \frac{\sum_{t=0}^{7} \text{Usage}_t}{7}$$
  $$\text{Estimated Runout (Days)} = \frac{\text{Current Stock}}{\text{Daily Burn Rate}}$$
* **Autonomous Replenishment Lifecycle:** Evaluates runout horizons to automatically trigger `CRITICAL` or `WARNING` alerts, dispatches supplier PO webhooks via `httpx`, and resolves pending alerts upon stock check-in (`POST /checkin`).

---

## System Architecture

                              +-------------------------------------------------------+
                              |                 FastAPI Core API Layer                |
                              +-------------------------------------------------------+
                                     |                                        |
             1. Acquire Distributed  |                                        | 3. Transactional
                Lock (SETNX + TTL)   v                                        v    Row Lock (FOR UPDATE)
                                +---------+                              +--------------+
                                |  Redis  |                              |  PostgreSQL  |
                                +---------+                              +--------------+
                                                                                |
                                                                                | 4. Publish Event
                                                                                v
                                                                         +--------------+
                                                                         | Apache Kafka |
                                                                         | (KRaft Mode) |
                                                                         +--------------+
                                                                                |
                                                                                | 5. Stream Consume
                                                                                v
                                                                 +-------------------------------+
                                                                 |     Kafka Consumer Worker     |
                                                                 +-------------------------------+
                                                                    |                         |
                                        6. Compute 7-Day Burn Rate  |                         | 7. Dispatch HTTP Webhook
                                           & Persist Recommendation v                         v
                                                              +------------+            +---------------+
                                                              | PostgreSQL |            | Supplier ERP  |
                                                              | Analytics  |            |  Procurement  |
                                                              +------------+            +---------------+

---

## Technology Stack

* **Framework:** Python 3.11+, FastAPI, Uvicorn
* **Database & Driver:** PostgreSQL 15, `asyncpg` (connection-pooled binary protocol)
* **Concurrency & Caching:** Redis 7 (`redis-py`)
* **Event Streaming:** Apache Kafka 7.4+ (KRaft consensus mode, no ZooKeeper), `aiokafka`
* **Networking & Webhooks:** `httpx` (asynchronous HTTP transport)
* **Containerization:** Docker Compose

---

## Database Schema Design

### `INVENTORYBIN`
Stores bin allocations, master part references, and replenishment triggers.
```sql
CREATE TABLE "inventorybin" (
    "id" SERIAL PRIMARY KEY,
    "bincode" VARCHAR(50) UNIQUE NOT NULL,
    "itemcode" VARCHAR(50) NOT NULL,
    "currentcount" INT NOT NULL CHECK ("currentcount" >= 0),
    "minimumcount" INT NOT NULL DEFAULT 10,
    "reordercount" INT NOT NULL DEFAULT 50,
    "updatedat" TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CHECKOUT_ITEMHistorical ledger recording every stock extraction for moving-window burn-rate analysis.SQLCREATE TABLE "checkout_item" (
    "id" SERIAL PRIMARY KEY,
    "requestid" VARCHAR(100) NOT NULL,
    "inventorybinid" INT REFERENCES "inventorybin"("id"),
    "itemcode" VARCHAR(50) NOT NULL,
    "count" INT NOT NULL,
    "requester" VARCHAR(100) NOT NULL,
    "collectdate" TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX "idx_checkout_bin_date" ON "checkout_item" ("inventorybinid", "collectdate");
REORDER_RECOMMENDATIONSystem-generated alerts driven by predictive burn rates and automated PO dispatches.SQLCREATE TABLE "reorder_recommendation" (
    "id" SERIAL PRIMARY KEY,
    "inventorybinid" INT REFERENCES "inventorybin"("id"),
    "item" VARCHAR(50) NOT NULL,
    "currentstock" INT NOT NULL,
    "dailyburnrate" NUMERIC(10, 2) NOT NULL,
    "estimateddaysremaining" NUMERIC(10, 2) NOT NULL,
    "suggestedreorderqty" INT NOT NULL,
    "prioritystatus" VARCHAR(20) NOT NULL, -- 'CRITICAL', 'WARNING', 'RESOLVED'
    "createdat" TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
Quick Start & Setup
Prerequisites
Docker Desktop (v24.0+)
Python 3.10+ (for running the client test harness)
1. Clone & Spin Up Containers
PowerShell
git clone [https://github.com/](https://github.com/)<your-username>/predictive-inventory-system.git
cd predictive-inventory-system

# Build images and start all 5 microservices in detached mode
docker compose up --build -d
2. Verify Container Health
PowerShell
docker compose ps
All five containers must report running:
fba_postgres (Port 5432)
fba_redis (Port 6379)
fba_kafka (Port 9092, 29092)
fba_inventory_api (Port 8000)
fba_kafka_worker (Background process)

3. Initialize Kafka Topic (First Run Only)
PowerShell
docker exec -it fba_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic inventory-checkout-events --partitions 1 --replication-factor 1

API Reference & Core Workflows
Interactive OpenAPI docs are available at http://localhost:8000/docs.
1. Stock Checkout (POST /checkout)
Deducts inventory, acquires distributed locks, records audit ledger, and produces a Kafka event.
Bash
curl -X POST "http://localhost:8000/checkout" \
     -H "Content-Type: application/json" \
     -d '{
       "request_id": "REQ-2026-001",
       "bin_id": 1,
       "item_code": "ITEM-101",
       "count": 90,
       "requester": "Mohan"
     }'
2. Batch Checkout (POST /checkout/batch)
Multi-item extraction with deterministic lock sorting to guarantee deadlock prevention:Bashcurl -X POST "http://localhost:8000/checkout/batch" \
     -H "Content-Type: application/json" \
     -d '{
       "request_id": "REQ-BATCH-100",
       "requester": "AssemblyTeam",
       "items": [
         {"bin_id": 1, "item_code": "ITEM-101", "count": 2},
         {"bin_id": 2, "item_code": "ITEM-102", "count": 5}
       ]
     }'
3. Stock Check-In & Alert Resolution (POST /checkin)
Replenishes warehouse inventory, resolves active reorder recommendations, and emits restock audit logs:Bashcurl -X POST "http://localhost:8000/checkin" \
     -H "Content-Type: application/json" \
     -d '{
       "reference_id": "PO-SUPPLIER-8812",
       "bin_id": 1,
       "item_code": "ITEM-101",
       "count": 90,
       "received_by": "WarehouseManager"
     }'
4. Fetch Active Recommendations (GET /analytics/reorder-recommendations)
Bash
curl -X GET "http://localhost:8000/analytics/reorder-recommendations"
Automated Verification Suite
An end-to-end integration script (test_pipeline.py) validates the complete lifecycle:
Automated pre-test stock initialization
Single checkout depletion triggering critical thresholds
Redis lock collision and concurrency boundaries
Multi-item sorted batch checkout
Background consumer prediction verification
Inbound restock check-in and alert resolution
Run the test suite:
PowerShell
# Install test dependency locally
pip install httpx

# Execute test suite against live Docker stack
python test_pipeline.py
To watch event processing and webhook dispatches in real time:
PowerShell
docker logs -f fba_kafka_worker


Variable	Default Value	Description
DATABASE_URL	postgresql://fba_admin:password123@postgres:5432/fba_inventory	PostgreSQL connection string
REDIS_HOST	redis	Redis service hostname
KAFKA_SERVERS	kafka:9092	Internal Kafka broker listener
SUPPLIER_WEBHOOK_URL	https://httpbin.org/post	Outbound destination for automated PO webhooks
<img width="568" height="535" alt="image" src="https://github.com/user-attachments/assets/b98cfb04-f511-4c3d-832d-b7121c453b02" />
