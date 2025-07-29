# Fraud Orchestrator

## Prereqs
- Docker & Docker Compose
- Python 3.8+ & pip

## Setup
1. `cp .env.example .env` & fill in your JIRA creds (optional)
2. `docker compose up -d`
3. `pip install kafka-python pika python-dotenv`

## Run
1. `python detector/streaming_detector.py`
2. `python consumer/consumer.py`
3. `python producer/producer.py`

## Dashboards
- Kafka: http://localhost:9000
- RabbitMQ: http://localhost:15672
- Jira: your‑org.atlassian.net

# steps
1)docker compose exec kafka `
  kafka-topics `
    --create `
    --topic transactions `
    --bootstrap-server kafka:9092 `
    --partitions 6 `
    --replication-factor 1

2)docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092

3)docker compose exec kafka `
  kafka-topics `
    --describe `
    --topic transactions `
    --bootstrap-server kafka:9092

4)docker compose exec kafka `
  kafka-console-consumer `
    --bootstrap-server kafka:9092 `
    --topic transactions `
    --from-beginning `
    --max-messages 5

delete a topic: 

5)docker compose exec kafka `
  kafka-topics --bootstrap-server localhost:9092 \
               --delete --topic transactions

