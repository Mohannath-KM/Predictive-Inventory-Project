import os
import json
import asyncio
from typing import List
import redis
import asyncpg
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError

app = FastAPI(title="Predictive Inventory API")

# Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://fba_admin:password123@postgres:5432/fba_inventory")
REDIS_HOST = os.getenv("REDIS_HOST", "redis") 
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)
producer: AIOKafkaProducer = None

# --- Pydantic Request Schemas ---
# 1. Single-item checkout
class CheckoutRequest(BaseModel):
    request_id: str
    bin_id: int
    item_code: str
    count: int
    requester: str

# 2. Multi-item checkout line item
class CheckoutItemPayload(BaseModel):
    bin_id: int
    item_code: str
    count: int

# 3. Batch checkout container
class MultiCheckoutRequest(BaseModel):
    request_id: str
    requester: str
    items: List[CheckoutItemPayload]


# 1. Single-item check-in
class CheckinRequest(BaseModel):
    reference_id: str
    bin_id: int
    item_code: str
    count: int
    received_by: str

# 2. Multi-item check-in line item 
class CheckinItemPayload(BaseModel):
    bin_id: int
    item_code: str
    count: int

# 3. Batch check-in container
class MultiCheckinRequest(BaseModel):
    reference_id: str
    received_by: str
    items: List[CheckinItemPayload]

# --- Lifecycle Events ---

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for attempt in range(1, 11):
        try:
            await producer.start()
            print("Successfully connected to Apache Kafka!")
            return
        except (KafkaConnectionError, KafkaError, Exception) as e:
            print(f"Waiting for Kafka cluster (Attempt {attempt}/10)... Error: {e}")
            await asyncio.sleep(3)

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()

# --- Single Item Checkout Endpoint ---

@app.post("/checkout", status_code=status.HTTP_202_ACCEPTED)
async def checkout_stock(req: CheckoutRequest):
    lock_key = f"lock:bin:{req.bin_id}"
    acquired = redis_client.set(lock_key, "locked", nx=True, ex=5)

    if not acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Bin is currently locked by another transaction. Please retry."
        )

    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            async with conn.transaction():
                # 1. Fetch current stock from INVENTORYBIN using exact column names
                row = await conn.fetchrow(
                    'SELECT "currentcount", "minimumcount" FROM "inventorybin" WHERE "id" = $1 FOR UPDATE', 
                    req.bin_id
                )
                
                if not row:
                    raise HTTPException(status_code=404, detail=f"Bin ID {req.bin_id} not found")

                current_stock = row['currentcount']
                min_stock = row['minimumcount']

                # 2. Validate stock availability
                if current_stock < req.count:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Insufficient stock. Available: {current_stock}, Requested: {req.count}"
                    )
                
                new_stock = current_stock - req.count
                is_low_stock = new_stock <= min_stock

                # 3. Update stock in INVENTORYBIN
                await conn.execute(
                    'UPDATE "inventorybin" SET "currentcount" = $1 WHERE "id" = $2', 
                    new_stock, req.bin_id
                )

                # 4. Insert parent record in CHECKOUT_REQUEST
                checkout_req_id = await conn.fetchval(
                    '''
                    INSERT INTO "checkout_request" (
                        "requestid", "requesteruser", "checkouttype", "checkoutstate", "createdbyuserid"
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING "id"
                    ''',
                    req.request_id, req.requester, 'MANUAL', 'COMPLETED', req.requester
                )

                # 5. Insert line item record in CHECKOUT_ITEM with Foreign Keys
                await conn.execute(
                    '''
                    INSERT INTO "checkout_item" (
                        "checkoutrequestid", "item", "count", "inventorybinid"
                    ) VALUES ($1, $2, $3, $4)
                    ''',
                    checkout_req_id, req.item_code, req.count, req.bin_id
                )

        finally:
            await conn.close()

        # 6. Publish event to Kafka
        event_payload = {
            "request_id": req.request_id,
            "checkout_request_db_id": checkout_req_id,
            "bin_id": req.bin_id,
            "item_code": req.item_code,
            "count": req.count,
            "requester": req.requester,
            "remaining_stock": new_stock,
            "is_low_stock": is_low_stock
        }
        await producer.send_and_wait("inventory-checkout-events", event_payload)

        return {
            "status": "Accepted",
            "message": "Stock checkout processed successfully",
            "remaining_stock": new_stock,
            "low_stock_warning": is_low_stock
        }

    finally:
        redis_client.delete(lock_key)

# --- Batch (Multi-Item) Checkout Endpoint ---

@app.post("/checkout/batch", status_code=status.HTTP_202_ACCEPTED)
async def checkout_stock_batch(req: MultiCheckoutRequest):
    # Sort bin IDs to prevent deadlocks during distributed locking
    bin_ids = sorted([item.bin_id for item in req.items])
    locks_acquired = []

    try:
        # 1. Acquire Redis distributed locks for all bins in this batch
        for b_id in bin_ids:
            lock_key = f"lock:bin:{b_id}"
            acquired = redis_client.set(lock_key, "locked", nx=True, ex=10)
            if not acquired:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Bin {b_id} is currently locked by another transaction. Please retry."
                )
            locks_acquired.append(lock_key)

        conn = await asyncpg.connect(DB_URL)
        processed_items = []
        try:
            async with conn.transaction():
                # 2. Insert parent record in CHECKOUT_REQUEST
                checkout_req_id = await conn.fetchval(
                    '''
                    INSERT INTO "checkout_request" (
                        "requestid", "requesteruser", "checkouttype", "checkoutstate", "createdbyuserid"
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING "id"
                    ''',
                    req.request_id, req.requester, 'MANUAL', 'COMPLETED', req.requester
                )

                # 3. Process each item transactionally
                for item in req.items:
                    row = await conn.fetchrow(
                        'SELECT "currentcount", "minimumcount" FROM "inventorybin" WHERE "id" = $1 FOR UPDATE',
                        item.bin_id
                    )
                    if not row:
                        raise HTTPException(status_code=404, detail=f"Bin ID {item.bin_id} not found")

                    current_stock = row['currentcount']
                    min_stock = row['minimumcount']

                    if current_stock < item.count:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Insufficient stock for item {item.item_code} in bin {item.bin_id}. Available: {current_stock}, Requested: {item.count}"
                        )

                    new_stock = current_stock - item.count
                    is_low_stock = new_stock <= min_stock

                    # Update stock in INVENTORYBIN
                    await conn.execute(
                        'UPDATE "inventorybin" SET "currentcount" = $1 WHERE "id" = $2',
                        new_stock, item.bin_id
                    )

                    # Insert line item in CHECKOUT_ITEM
                    await conn.execute(
                        '''
                        INSERT INTO "checkout_item" (
                            "checkoutrequestid", "item", "count", "inventorybinid"
                        ) VALUES ($1, $2, $3, $4)
                        ''',
                        checkout_req_id, item.item_code, item.count, item.bin_id
                    )

                    processed_items.append({
                        "request_id": req.request_id,
                        "checkout_request_db_id": checkout_req_id,
                        "bin_id": item.bin_id,
                        "item_code": item.item_code,
                        "count": item.count,
                        "requester": req.requester,
                        "remaining_stock": new_stock,
                        "is_low_stock": is_low_stock
                    })

        finally:
            await conn.close()

        # 4. Stream events to Kafka for each item
        for event_payload in processed_items:
            await producer.send_and_wait("inventory-checkout-events", event_payload)

        return {
            "status": "Accepted",
            "message": "Batch checkout processed successfully",
            "request_id": req.request_id,
            "items_processed": len(processed_items)
        }

    finally:
        # Release all locks
        for l_key in locks_acquired:
            redis_client.delete(l_key)



@app.post("/checkin", status_code=status.HTTP_200_OK)
async def checkin_stock(req: CheckinRequest):
    """Replenishes inventory stock, updates active reorder recommendations, and emits an audit event."""
    if req.count <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Check-in quantity must be greater than zero."
        )

    lock_key = f"lock:bin:{req.bin_id}"
    acquired = redis_client.set(lock_key, "locked", nx=True, ex=5)

    if not acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Bin is currently locked by another transaction. Please retry."
        )

    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            async with conn.transaction():
                # 1. Fetch current stock with row lock
                row = await conn.fetchrow(
                    'SELECT "currentcount", "minimumcount" FROM "inventorybin" WHERE "id" = $1 FOR UPDATE',
                    req.bin_id
                )
                if not row:
                    raise HTTPException(status_code=404, detail=f"Bin ID {req.bin_id} not found")

                current_stock = row['currentcount']
                new_stock = current_stock + req.count

                # 2. Update stock count in INVENTORYBIN
                await conn.execute(
                    'UPDATE "inventorybin" SET "currentcount" = $1 WHERE "id" = $2',
                    new_stock, req.bin_id
                )

                # 3. Resolve existing active alerts in REORDER_RECOMMENDATION
                await conn.execute(
                    '''
                    UPDATE "reorder_recommendation"
                    SET "prioritystatus" = 'RESOLVED'
                    WHERE "inventorybinid" = $1 AND "prioritystatus" IN ('CRITICAL', 'WARNING')
                    ''',
                    req.bin_id
                )

        finally:
            await conn.close()

        # 4. Stream audit event to Kafka
        event_payload = {
            "event_type": "STOCK_REPLENISHED",
            "reference_id": req.reference_id,
            "bin_id": req.bin_id,
            "item_code": req.item_code,
            "added_count": req.count,
            "new_total_stock": new_stock,
            "received_by": req.received_by
        }
        await producer.send_and_wait("inventory-checkout-events", event_payload)

        return {
            "status": "Success",
            "message": f"Successfully added {req.count} units of {req.item_code}",
            "previous_stock": current_stock,
            "new_stock": new_stock
        }

    finally:
        redis_client.delete(lock_key)


@app.post("/checkin/batch", status_code=status.HTTP_200_OK)
async def checkin_stock_batch(req: MultiCheckinRequest):
    if not req.items:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item list cannot be empty."
        )

    # Sort bin IDs to prevent deadlocks across distributed transactions
    bin_ids = sorted([item.bin_id for item in req.items])
    locks_acquired = []

    try:
        # 1. Acquire Redis distributed locks for each bin
        for b_id in bin_ids:
            lock_key = f"lock:bin:{b_id}"
            acquired = redis_client.set(lock_key, "locked", nx=True, ex=10)
            if not acquired:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Bin {b_id} is currently locked by another transaction. Please retry."
                )
            locks_acquired.append(lock_key)

        conn = await asyncpg.connect(DB_URL)
        replenished_summary = []
        try:
            async with conn.transaction():
                for item in req.items:
                    if item.count <= 0:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Check-in count for bin {item.bin_id} must be greater than zero."
                        )

                    # Fetch current stock with row-level lock
                    row = await conn.fetchrow(
                        'SELECT "currentcount" FROM "inventorybin" WHERE "id" = $1 FOR UPDATE',
                        item.bin_id
                    )
                    if not row:
                        raise HTTPException(status_code=404, detail=f"Bin ID {item.bin_id} not found")

                    current_stock = row['currentcount']
                    new_stock = current_stock + item.count

                    # Update stock in INVENTORYBIN
                    await conn.execute(
                        'UPDATE "inventorybin" SET "currentcount" = $1 WHERE "id" = $2',
                        new_stock, item.bin_id
                    )

                    # Mark active recommendations as RESOLVED
                    await conn.execute(
                        '''
                        UPDATE "reorder_recommendation"
                        SET "prioritystatus" = 'RESOLVED'
                        WHERE "inventorybinid" = $1 AND "prioritystatus" IN ('CRITICAL', 'WARNING')
                        ''',
                        item.bin_id
                    )

                    replenished_summary.append({
                        "bin_id": item.bin_id,
                        "item_code": item.item_code,
                        "added_count": item.count,
                        "previous_stock": current_stock,
                        "new_stock": new_stock
                    })

        finally:
            await conn.close()

        # 2. Publish replenishment audit events to Kafka
        for item_data in replenished_summary:
            event_payload = {
                "event_type": "STOCK_REPLENISHED",
                "reference_id": req.reference_id,
                "bin_id": item_data["bin_id"],
                "item_code": item_data["item_code"],
                "added_count": item_data["added_count"],
                "new_total_stock": item_data["new_stock"],
                "received_by": req.received_by
            }
            await producer.send_and_wait("inventory-checkout-events", event_payload)

        return {
            "status": "Success",
            "message": "Batch check-in processed successfully",
            "reference_id": req.reference_id,
            "items_updated": len(replenished_summary),
            "details": replenished_summary
        }

    finally:
        # Release all acquired Redis locks
        for l_key in locks_acquired:
            redis_client.delete(l_key)
            

# --- Analytics Endpoint ---

@app.get("/analytics/reorder-recommendations")
async def get_reorder_recommendations():
    """Fetches real-time predictive reorder recommendations generated by Kafka workers."""
    conn = await asyncpg.connect(DB_URL)
    try:
        records = await conn.fetch(
            '''
            SELECT "id", "inventorybinid", "item", "currentstock",
                   "dailyburnrate", "estimateddaysremaining", 
                   "suggestedreorderqty", "prioritystatus", "createdat"
            FROM "reorder_recommendation"
            ORDER BY "createdat" DESC
            LIMIT 50
            '''
        )
        return [dict(record) for record in records]
    finally:
        await conn.close()