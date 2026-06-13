%pip install faker

import random
import time
from datetime import datetime

from faker import Faker

fake = Faker()

dag_ids = [
    "sales_pipeline",
    "inventory_pipeline",
    "customer_pipeline"
]

task_ids = [
    "extract",
    "transform",
    "load"
]

statuses = ["SUCCESS", "FAILED", "RUNNING"]

while True:

    data = [{
        "dag_id": random.choice(dag_ids),
        "task_id": random.choice(task_ids),
        "status": random.choice(statuses),
        "runtime_seconds": random.randint(50, 500),
        "retry_count": random.randint(0, 3),
        "owner": fake.user_name(),
        "timestamp": str(datetime.now())
    }]

    df = spark.createDataFrame(data)

    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable("pipeline_monitoring.airflow_bronze")

    print("Airflow log inserted")

    time.sleep(5)