"""
consumer.py
-----------
Consumes e-commerce order events from Kafka, transforms/validates them,
and persists to PostgreSQL for Grafana dashboard queries.
"""

import json
import os
import time

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME   = os.getenv("TOPIC_NAME", "ecommerce-orders")
GROUP_ID     = "ecommerce-consumer-group"

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "pipeline")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "pipeline123")
PG_DB   = os.getenv("POSTGRES_DB", "ecommerce")

BATCH_SIZE = 20   # flush to DB every N messages
BATCH_TIMEOUT = 5 # or every N seconds


# ── DB Connection ─────────────────────────────────────────────────────────────
def connect_postgres(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS,
                dbname=PG_DB
            )
            conn.autocommit = False
            print(f"[Consumer] Connected to PostgreSQL at {PG_HOST}:{PG_PORT}")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[Consumer] DB not ready ({attempt}/{retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL.")


def connect_kafka(retries=15, delay=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=-1,
            )
            print(f"[Consumer] Connected to Kafka topic '{TOPIC_NAME}'")
            return consumer
        except NoBrokersAvailable:
            print(f"[Consumer] Kafka not ready ({attempt}/{retries}), retrying...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka.")


# ── Transform ─────────────────────────────────────────────────────────────────
def transform(order: dict) -> tuple:
    """Validate and extract fields for DB insert."""
    return (
        order.get("order_id"),
        order.get("customer_id"),
        order.get("product"),
        order.get("category"),
        int(order.get("quantity", 1)),
        float(order.get("unit_price", 0.0)),
        float(order.get("total_amount", 0.0)),
        order.get("payment_method"),
        order.get("status"),
        order.get("region"),
        order.get("timestamp"),
    )


# ── Flush batch to DB ─────────────────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO orders (
        order_id, customer_id, product, category,
        quantity, unit_price, total_amount,
        payment_method, status, region, event_time
    ) VALUES %s
    ON CONFLICT (order_id) DO NOTHING;
"""

def flush_batch(conn, batch: list):
    if not batch:
        return
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, batch)
    conn.commit()
    print(f"[Consumer] Flushed {len(batch)} orders to PostgreSQL.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    conn     = connect_postgres()
    consumer = connect_kafka()

    batch      = []
    last_flush = time.time()
    total      = 0

    print("[Consumer] Listening for orders...")

    for message in consumer:
        order = message.value

        try:
            row = transform(order)
            batch.append(row)
        except Exception as e:
            print(f"[Consumer] Skipping malformed record: {e}")
            continue

        now = time.time()
        if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_TIMEOUT:
            flush_batch(conn, batch)
            total += len(batch)
            batch = []
            last_flush = now
            print(f"[Consumer] Total orders persisted: {total}")


if __name__ == "__main__":
    main()
