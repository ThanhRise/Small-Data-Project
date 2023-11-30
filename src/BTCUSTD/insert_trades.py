from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, LongType, BooleanType, DoubleType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "traffic_sensor"
FILE_PATH = "/data/BTCUSDT_PARQUET"

# "ID EQP" -> INT 64

SCHEMA = StructType([
    StructField("ID", LongType()),
    StructField("PRICE", DoubleType()),
    StructField("QUANTITY", DoubleType()),
    StructField("BASE QUANTITY", DoubleType()),
    StructField("TIME",  LongType()),
    StructField("IS BUYER MAKER", BooleanType()),
    StructField("IS BEST MATCH", BooleanType())
])
spark = SparkSession.builder.appName("insert trades").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # Reduce logging verbosity

# Read the parquet file write it to the topic
# We need to specify the schema in the stream
# and also convert the entries to the format (key, value)
df_traffic_stream = spark.readStream.format("parquet")\
    .schema(SCHEMA)\
    .load(FILE_PATH)\
    .withColumn("TIME", F.from_unixtime(F.col("TIME") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))\
    .withColumn("value", F.to_json(F.struct(F.col("*"))))\
    .withColumn("key", F.lit("key"))\
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))\
    .limit(500000)\

# .withColumn("TIME", F.from_unixtime(F.col("TIME"), "yyyy-MM-dd HH:mm:ss.SSS").cast("timestamp"))\

# Read the csv file write it to the topic
# We need to specify the schema in the stream
# and also convert the entries to the format (key, value)
# /src/BTCUSTD/insert_trades.py

# Write the stream to the topic
df_traffic_stream\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
    .option("topic", KAFKA_TOPIC)\
    .option("checkpointLocation", "/tmp/checkpoint")\
    .start()\
    .awaitTermination()

spark.stop()
