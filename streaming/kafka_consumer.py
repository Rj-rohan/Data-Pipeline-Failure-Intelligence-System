from pyspark.sql.functions import *

# Read Streaming Data From Bronze Table
df = spark.readStream.table(
    "pipeline_monitoring.kafka_metrics_bronze"
)

# Basic Transformations
processed_df = df.withColumn(
    "high_consumer_lag",
    when(col("consumer_lag") > 3000, 1).otherwise(0)
).withColumn(
    "broker_critical",
    when(col("broker_status") == "CRITICAL", 1).otherwise(0)
)

# Write Stream To Silver Table
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/kafka_metrics_silver"
    ) \
    .toTable("pipeline_monitoring.kafka_metrics_silver")

query.awaitTermination()