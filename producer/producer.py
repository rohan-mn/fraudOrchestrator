import os
import json
import time
import uuid
import random

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# 1) Load .env for BOOTSTRAP + TOPIC
load_dotenv()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "transactions")

print(f"[Producer] Attempting to connect to Kafka at {BOOTSTRAP}…")

# 2) Retry until we successfully construct a producer
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            retry_backoff_ms=1000,
            request_timeout_ms=10000
        )
        print("[Producer] Connected to Kafka!")
    except KafkaError as e:
        print(f"[Producer] Connection failed: {e!r}, retrying in 5s…")
        time.sleep(5)

# 3) Prepare some fake users
users = [f"user_{i}" for i in range(1, 101)]

print("[Producer] Starting to send transactions (CTRL+C to stop)…")

try:
    while True:
        txn = {
            "txn_id": str(uuid.uuid4()),
            "user_id": random.choice(users),
            "amount": round(random.random() * 50, 2),
            "timestamp": int(time.time() * 1000)
        }

        try:
            # 4) Send + block until acknowledged (or timeout)
            fut = producer.send(TOPIC, value=txn)
            fut.get(timeout=10)
            print(f"[Producer] Sent: {txn}")
        except KafkaTimeoutError as e:
            print(f"[Producer] Timeout sending, retrying in 5s… ({e})")
            time.sleep(5)
            continue
        except KafkaError as e:
            print(f"[Producer] Kafka error: {e!r}, retrying in 5s…")
            time.sleep(5)
            continue

        time.sleep(0.1)  # ~10 txns/sec

except KeyboardInterrupt:
    print("\n[Producer] Shutting down…")
    producer.close()
