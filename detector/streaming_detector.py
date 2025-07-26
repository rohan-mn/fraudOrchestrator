import json, time, os
from kafka import KafkaConsumer
import pika
from dotenv import load_dotenv

# 1) Load any .env (if you want to configure hostnames, etc.)
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RABBIT_HOST     = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_QUEUE    = os.getenv("RABBIT_QUEUE", "fraud-review")

# 2) Set up Kafka consumer
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    group_id='fraud-detector',
    value_deserializer=lambda b: json.loads(b.decode())
)

# 3) Set up RabbitMQ (reuse the connection)
rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
rabbit_ch   = rabbit_conn.channel()
rabbit_ch.queue_declare(queue=RABBIT_QUEUE, durable=True)

print("Detector running — flagging amount > 30…")

for msg in consumer:
    txn = msg.value
    if txn.get('amount', 0) > 30:
        body = json.dumps(txn)
        rabbit_ch.basic_publish(
            exchange='',
            routing_key=RABBIT_QUEUE,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"⚠️  Flagged txn {txn['txn_id']}: {txn['amount']}")
    # throttle so your logs stay readable
    time.sleep(0.01)

# (on exit)
rabbit_conn.close()
consumer.close()
