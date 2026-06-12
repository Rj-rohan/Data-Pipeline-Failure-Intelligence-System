from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("SparkConsumer") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "spark-logs") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Define Schema
schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("executor_memory_gb", IntegerType(), True),
    StructField("cpu_usage_percent", IntegerType(), True),
    StructField("records_processed", IntegerType(), True),
    StructField("job_status", StringType(), True),
    StructField("runtime_seconds", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Print Stream
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()