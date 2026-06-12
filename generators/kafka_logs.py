from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

broker_statuses = ["HEALTHY", "WARNING", "CRITICAL"]

topics = [
    "sales-events",
    "payment-events",
    "customer-events"
]

print("Sending Kafka metrics to Kafka topic...")

while True:

    log = {
        "topic": random.choice(topics),
        "consumer_lag": random.randint(0, 5000),
        "messages_per_second": random.randint(100, 10000),
        "broker_status": random.choice(broker_statuses),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("kafka-metrics", value=log)

    print(log)

    time.sleep(4)