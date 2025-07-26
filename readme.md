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
