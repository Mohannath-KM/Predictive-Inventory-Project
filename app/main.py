import os
import json
import asyncio
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

class CheckoutRequest(BaseModel):
    request_id: str
    bin_id: int
    item_code: str
    count: int
    requester: str

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