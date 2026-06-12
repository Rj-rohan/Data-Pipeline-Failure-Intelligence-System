from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("AirflowConsumer") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "airflow-logs") \
    .option("startingOffsets", "latest") \
    .load()

# Convert binary to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Define Schema
schema = StructType([
    StructField("dag_id", StringType(), True),
    StructField("task_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("runtime_seconds", IntegerType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("owner", StringType(), True),
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