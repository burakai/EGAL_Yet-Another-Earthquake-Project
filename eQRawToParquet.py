import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder.appName("earthquakeRawToParquet").getOrCreate()

spark

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "earthquakeRaw"

# Read from Kafka using the Kafka source
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("failOnDataLoss", "false") \
    .load()

EQ_DIR = "/home/ubuntu/parquet-files/eq/"
EQ_CKPT_DIR = "/home/ubuntu/ckpt-files/eq"

query = lines \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", EQ_DIR) \
    .option("checkpointLocation", EQ_CKPT_DIR) \
    .start()

query.awaitTermination()
