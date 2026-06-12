from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

dag_ids = [
    "sales_pipeline",
    "inventory_pipeline",
    "customer_pipeline",
    "payment_pipeline"
]

task_ids = [
    "extract_data",
    "transform_data",
    "load_redshift",
    "validate_data"
]

statuses = ["SUCCESS", "FAILED", "RUNNING"]

print("Sending Airflow logs to Kafka...")

while True:

    status = random.choices(
        statuses,
        weights=[70, 20, 10]
    )[0]

    log = {
        "dag_id": random.choice(dag_ids),
        "task_id": random.choice(task_ids),
        "status": status,
        "runtime_seconds": random.randint(50, 500),
        "retry_count": random.randint(0, 3),
        "owner": fake.user_name(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("airflow-logs", value=log)

    print(log)

    time.sleep(2)