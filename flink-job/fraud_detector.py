import json
import pika
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def publish_to_rabbitmq(msg: str):
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    ch.queue_declare(queue='fraud-review', durable=True)
    ch.basic_publish(exchange='', routing_key='fraud-review', body=msg)
    conn.close()

def main():
    # 1) Local env (no Web UI)
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    # 2) Kafka source
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'fraud-detector'
    }
    consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    stream = env.add_source(consumer)

    # 3) Filter and sink
    (
        stream
        .map(lambda x: json.loads(x))
        .filter(lambda txn: txn.get('amount', 0) > 30)
        .map(lambda txn: publish_to_rabbitmq(json.dumps(txn)))
    )

    # 4) Execute
    env.execute('Flink Fraud Detector')

if __name__ == '__main__':
    main()
