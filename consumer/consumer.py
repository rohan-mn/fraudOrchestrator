import os
from dotenv import load_dotenv
import json, pika, requests
from requests.auth import HTTPBasicAuth

# 1. Load the .env file
load_dotenv()  

# 2. Read in your secrets/config
JIRA_URL        = os.getenv('JIRA_URL')
JIRA_EMAIL      = os.getenv('JIRA_EMAIL')
JIRA_API_TOKEN  = os.getenv('JIRA_API_TOKEN')
RABBIT_URL      = os.getenv('RABBITMQ_HOST', 'localhost')

# 3. Create your auth object
AUTH = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
HEADERS = {'Content-Type': 'application/json'}

def on_message(ch, method, props, body):
    txn = body.decode().split(',')
    payload = {
      "fields": {
        "project": {"key":"CRM"},
        "summary": f"Fraud Alert: txn_id={txn[0]}",
        "description": f"User: {txn[1]}\nAmount: {txn[2]}\nTime: {txn[3]}",
        "issuetype": {"name":"Task"},
        "labels": ["fraud","automated"]
      }
    }
    resp = requests.post(JIRA_URL, json=payload, headers=HEADERS, auth=AUTH)
    print("Created issue:", resp.json().get('key'))

# 4. Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(RABBIT_URL)
)
channel = connection.channel()
channel.basic_consume(
    queue='fraud-review',
    on_message_callback=on_message,
    auto_ack=True
)

print("Waiting for fraud-review messages…")
channel.start_consuming()
