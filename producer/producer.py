"""
producer.py
-----------
Simulates a live e-commerce order stream and publishes events to Kafka.
Each order contains: order_id, customer, product, category, quantity,
price, payment_method, status, region, and timestamp.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME        = os.getenv("TOPIC_NAME", "ecommerce-orders")
ORDERS_PER_SECOND = float(os.getenv("ORDERS_PER_SECOND", "2"))
SLEEP             = 1.0 / ORDERS_PER_SECOND

# ── Sample data ───────────────────────────────────────────────────────────────
PRODUCTS = {
    "Electronics": ["Laptop Pro 15", "Wireless Earbuds", "4K Monitor", "Mechanical Keyboard",
                    "USB-C Hub", "Webcam HD", "Smart Watch", "Portable SSD"],
    "Clothing":    ["Denim Jacket", "Running Shoes", "Cotton T-Shirt", "Winter Coat",
                    "Yoga Pants", "Formal Shirt", "Sneakers", "Hoodie"],
    "Books":       ["Clean Code", "Deep Learning Book", "Atomic Habits", "The Pragmatic Programmer",
                    "Designing Data-Intensive Apps", "Python Crash Course"],
    "Home":        ["Air Purifier", "Coffee Maker", "Desk Lamp", "Bluetooth Speaker",
                    "Robot Vacuum", "Instant Pot"],
    "Sports":      ["Yoga Mat", "Resistance Bands", "Dumbbell Set", "Cycling Gloves",
                    "Protein Powder", "Jump Rope"],
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]
STATUSES        = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"]
REGIONS         = ["North America", "Europe", "Asia Pacific", "Middle East", "South Asia", "Latin America"]

CUSTOMERS = [f"customer_{str(uuid.uuid4())[:8]}" for _ in range(200)]

STATUS_WEIGHTS = [0.10, 0.25, 0.20, 0.20, 0.15, 0.07, 0.03]

PRICE_RANGES = {
    "Electronics": (49.99, 1499.99),
    "Clothing":    (9.99,  199.99),
    "Books":       (4.99,  59.99),
    "Home":        (19.99, 499.99),
    "Sports":      (9.99,  299.99),
}


def generate_order() -> dict:
    category = random.choice(list(PRODUCTS.keys()))
    product  = random.choice(PRODUCTS[category])
    lo, hi   = PRICE_RANGES[category]
    price    = round(random.uniform(lo, hi), 2)
    qty      = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.12, 0.08, 0.05])[0]

    return {
        "order_id":       str(uuid.uuid4()),
        "customer_id":    random.choice(CUSTOMERS),
        "product":        product,
        "category":       category,
        "quantity":       qty,
        "unit_price":     price,
        "total_amount":   round(price * qty, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status":         random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "region":         random.choice(REGIONS),
        "timestamp":      datetime.utcnow().isoformat(),
    }


def connect_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[Producer] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print(f"[Producer] Broker not ready, retrying {attempt}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def main():
    producer = connect_producer()
    count = 0

    print(f"[Producer] Streaming orders to topic '{TOPIC_NAME}' at {ORDERS_PER_SECOND}/s...")

    while True:
        order = generate_order()
        producer.send(
            TOPIC_NAME,
            key=order["order_id"],
            value=order,
        )
        count += 1

        if count % 10 == 0:
            producer.flush()
            print(f"[Producer] Sent {count} orders | Latest: {order['product']} "
                  f"({order['category']}) ${order['total_amount']} [{order['status']}]")

        time.sleep(SLEEP)


if __name__ == "__main__":
    main()
