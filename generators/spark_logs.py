from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

job_statuses = ["SUCCESS", "FAILED", "RUNNING"]

print("Sending Spark logs to Kafka...")

while True:

    log = {
        "job_id": f"spark_job_{random.randint(1000,9999)}",
        "executor_memory_gb": random.choice([2,4,8,16]),
        "cpu_usage_percent": random.randint(20, 95),
        "records_processed": random.randint(1000, 1000000),
        "job_status": random.choice(job_statuses),
        "runtime_seconds": random.randint(30, 1000),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("spark-logs", value=log)

    print(log)

    time.sleep(3)