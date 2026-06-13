from pyspark.sql.functions import *

# Read Streaming Data From Bronze Table
df = spark.readStream.table(
    "pipeline_monitoring.spark_bronze"
)

# Basic Transformations
processed_df = df.withColumn(
    "high_cpu_usage",
    when(col("cpu_usage_percent") > 80, 1).otherwise(0)
).withColumn(
    "long_runtime",
    when(col("runtime_seconds") > 500, 1).otherwise(0)
)

# Write Stream To Silver Table
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/spark_silver"
    ) \
    .toTable("pipeline_monitoring.spark_silver")

query.awaitTermination()