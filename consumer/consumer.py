import os
import json
import pika
import requests

from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# ——— 1) Load config from .env ———————————————
load_dotenv()
RABBIT_HOST   = os.getenv("RABBITMQ_HOST",  "localhost")
RABBIT_QUEUE  = os.getenv("RABBITMQ_QUEUE", "fraud-review")

JIRA_URL        = os.getenv("JIRA_URL")
JIRA_EMAIL      = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN  = os.getenv("JIRA_API_TOKEN")
JIRA_PROJECT_KEY= os.getenv("JIRA_PROJECT_KEY", "FRD")

auth    = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {'Content-Type': 'application/json'}

# ——— 2) RabbitMQ callback —————————————————————
def on_message(ch, method, props, body):
    # Decode the JSON
    try:
        txn = json.loads(body)
    except json.JSONDecodeError:
        print("[Consumer] Invalid JSON received, discarding message")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Build Jira payload
    payload = {
        "fields": {
            "project": {"key": JIRA_PROJECT_KEY},
            "summary":     f"Fraud Alert: txn_id={txn.get('txn_id')}",
            "description": f"User: {txn.get('user_id')}\n"
                           f"Amount: {txn.get('amount')}\n"
                           f"Time: {txn.get('timestamp')}",
            "issuetype": {"name": "Task"},
            "labels":    ["fraud", "automated"]
        }
    }

    # POST to Jira
    try:
        resp = requests.post(JIRA_URL, json=payload, headers=headers, auth=auth, timeout=10)
    except requests.RequestException as e:
        print(f"[Consumer] Network error posting to Jira: {e}")
        # requeue so we can retry later
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # Handle Jira response
    if resp.status_code == 201:
        # Expect JSON with {"key": "FRD-123", ...}
        try:
            data = resp.json()
            print("[Consumer] Created issue:", data.get("key"))
        except ValueError:
            # no JSON—print the raw body
            print("[Consumer] Issue created, but Jira returned non-JSON:")
            print(resp.text)
    else:
        print(f"[Consumer] Jira API error {resp.status_code}:")
        print(resp.text)

    # Acknowledge in RabbitMQ
    ch.basic_ack(delivery_tag=method.delivery_tag)

# ——— 3) Main loop ————————————————————————
if __name__ == "__main__":
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel    = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

    channel.basic_consume(
        queue=RABBIT_QUEUE,
        on_message_callback=on_message,
        auto_ack=False
    )

    print(f"[Consumer] Waiting for messages on '{RABBIT_QUEUE}'…")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[Consumer] Interrupted, shutting down…")
        channel.stop_consuming()
    finally:
        connection.close()
