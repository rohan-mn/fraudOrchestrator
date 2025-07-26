import json, time, uuid, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = [f"user_{i}" for i in range(1,101)]
while True:
    txn = {
      "txn_id": str(uuid.uuid4()),
      "user_id": random.choice(users),
      "amount": round(random.random()*50,2),
      "timestamp": int(time.time()*1000)
    }
    producer.send('transactions', value=txn)
    time.sleep(0.1)  # ~10 txns/sec
