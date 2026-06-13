%pip install faker

import random
import time
from datetime import datetime

from faker import Faker

statuses = ["SUCCESS", "FAILED", "RUNNING"]

while True:

    data = [{
        "job_id": f"spark_job_{random.randint(1000,9999)}",
        "executor_memory_gb": random.choice([2,4,8,16]),
        "cpu_usage_percent": random.randint(20,95),
        "records_processed": random.randint(1000,100000),
        "job_status": random.choice(statuses),
        "runtime_seconds": random.randint(50,800),
        "timestamp": str(datetime.now())
    }]

    df = spark.createDataFrame(data)

    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable("pipeline_monitoring.spark_bronze")

    print("Spark log inserted")

    time.sleep(5)