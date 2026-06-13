%pip install faker

import random
import time
from datetime import datetime

from faker import Faker

topics = [
    "sales-events",
    "payment-events",
    "customer-events"
]

statuses = ["HEALTHY", "WARNING", "CRITICAL"]

while True:

    data = [{
        "topic": random.choice(topics),
        "consumer_lag": random.randint(0,5000),
        "messages_per_second": random.randint(100,10000),
        "broker_status": random.choice(statuses),
        "timestamp": str(datetime.now())
    }]

    df = spark.createDataFrame(data)

    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable("pipeline_monitoring.kafka_metrics_bronze")

    print("Kafka metric inserted")

    time.sleep(5)
