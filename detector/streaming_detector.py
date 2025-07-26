import os
import json
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
import pika

# 1) Load your .env
load_dotenv()
BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RABBIT_HOST  = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_QUEUE = os.getenv("RABBITMQ_QUEUE", "fraud-review")

print(f"[Detector] connecting to Kafka at {BOOTSTRAP}")

# 2) Create a consumer without subscribing to a group
consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",   # only used for new assignment
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(b.decode("utf-8"))
)

# 3) Manually assign partition 0 of the 'transactions' topic
tp0 = TopicPartition("transactions", 0)
consumer.assign([tp0])
print(f"[Detector] assigned manually to: {tp0}")

# 4) Seek to the very beginning of that partition
consumer.seek_to_beginning(tp0)
print(f"[Detector] seeking to beginning of {tp0}")

# 5) Setup RabbitMQ
conn = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
ch   = conn.channel()
ch.queue_declare(queue=RABBIT_QUEUE, durable=True)

print("[Detector] running — flagging amount > 30…")

try:
    # 6) Poll loop
    while True:
        # fetch up to 10 messages at a time
        records = consumer.poll(timeout_ms=1000, max_records=10)
        # records is a dict: {TopicPartition: [msgs]}
        for partition, msgs in records.items():
            for msg in msgs:
                txn = msg.value
                amount = txn.get("amount", 0)
                if amount > 30:
                    body = json.dumps(txn)
                    ch.basic_publish(
                        exchange="",
                        routing_key=RABBIT_QUEUE,
                        body=body,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f"⚠️  Flagged txn {txn['txn_id']}: ${amount}")
        # (no offset commit needed for manual assign, but you could track last offset here)
        time.sleep(0.1)

except KeyboardInterrupt:
    print("\n[Detector] interrupted — shutting down…")

finally:
    consumer.close()
    conn.close()
