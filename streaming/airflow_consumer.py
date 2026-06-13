from pyspark.sql.functions import *

# Read Streaming Data From Bronze Table
df = spark.readStream.table(
    "pipeline_monitoring.airflow_bronze"
)

# Basic Transformations
processed_df = df.withColumn(
    "is_failed",
    when(col("status") == "FAILED", 1).otherwise(0)
).withColumn(
    "is_long_running",
    when(col("runtime_seconds") > 300, 1).otherwise(0)
)

# Write Stream To Silver Table
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/airflow_silver"
    ) \
    .toTable("pipeline_monitoring.airflow_silver")

query.awaitTermination()