import os
import json
import asyncio
import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition

DB_URL = os.getenv("DATABASE_URL", "postgresql://fba_admin:password123@postgres:5432/fba_inventory")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092")
KAFKA_TOPIC = "inventory-checkout-events"

async def calculate_burn_rate_and_runout(conn, bin_id: int, current_stock: int):
    query = '''
        SELECT COALESCE(SUM("count"), 0) as total_used
        FROM "checkout_item"
        WHERE "inventorybinid" = $1
          AND "collectdate" >= NOW() - INTERVAL '7 days'
    '''
    row = await conn.fetchrow(query, bin_id)
    total_used_7d = row['total_used'] if row else 0
    daily_burn_rate = round(float(total_used_7d) / 7.0, 2)

    if daily_burn_rate > 0:
        days_remaining = round(float(current_stock) / daily_burn_rate, 2)
    else:
        days_remaining = 999.0

    return daily_burn_rate, days_remaining

async def process_checkout_event(event: dict):
    bin_id = event.get("bin_id")
    item_code = event.get("item_code")
    count = event.get("count")
    remaining_stock = event.get("remaining_stock")
    request_id = event.get("request_id")

    print(f"\n⚡ [EVENT DETECTED] Request: {request_id} | Bin: {bin_id} | Item: {item_code} | Deducted: {count}", flush=True)

    conn = await asyncpg.connect(DB_URL)
    try:
        bin_info = await conn.fetchrow(
            'SELECT "minimumcount", "reordercount" FROM "inventorybin" WHERE "id" = $1', bin_id
        )
        if not bin_info:
            return

        min_stock = bin_info['minimumcount']
        reorder_qty = bin_info['reordercount']

        daily_burn, days_left = await calculate_burn_rate_and_runout(conn, bin_id, remaining_stock)
        print(f"  📊 [METRICS] Daily Burn: {daily_burn} units/day | Estimated Runout: {days_left} days", flush=True)

        if remaining_stock <= min_stock or days_left <= 2.0:
            priority = "CRITICAL"
        elif remaining_stock <= (min_stock * 1.5) or days_left <= 5.0:
            priority = "WARNING"
        else:
            priority = "NORMAL"

        if priority in ["CRITICAL", "WARNING"]:
            await conn.execute(
                '''
                INSERT INTO "reorder_recommendation" (
                    "inventorybinid", "item", "currentstock", "dailyburnrate",
                    "estimateddaysremaining", "suggestedreorderqty", "prioritystatus"
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ''',
                bin_id, item_code, remaining_stock, daily_burn, days_left, reorder_qty, priority
            )
            print(f"  🚨 [{priority} ALERT] Auto-generated replenishment recommendation: Order {reorder_qty} units of {item_code}.", flush=True)

    finally:
        await conn.close()

async def start_consumer():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset="earliest"
    )

    # Retry loop until Kafka is reachable
    while True:
        try:
            print("Connecting consumer to Kafka broker...", flush=True)
            await consumer.start()
            
            # Explicitly assign partition 0 to bypass coordinator dependency
            tp = TopicPartition(KAFKA_TOPIC, 0)
            consumer.assign([tp])
            
            print(f"🚀 Predictive Stock Engine actively listening on topic: {KAFKA_TOPIC} [Partition 0]", flush=True)
            break
        except Exception as e:
            print(f"Waiting for Kafka cluster ({e}). Retrying in 3 seconds...", flush=True)
            await asyncio.sleep(3)

    try:
        async for msg in consumer:
            await process_checkout_event(msg.value)
    except Exception as e:
        print(f"Consumer stream error: {e}", flush=True)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(start_consumer())