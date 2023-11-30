from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, LongType, BooleanType, DoubleType

StrogePath = "gs://bigdata-class-2023/Nhom21/PARQUET/Full_data"
elasticsearch_host = "http://34.36.145.14/"

spark = SparkSession.builder.appName("lan6").getOrCreate()

SCHEMA = StructType([
    StructField("ID", LongType()),
    StructField("PRICE", DoubleType()),
    StructField("QUANTITY", DoubleType()),
    StructField("BASE_QUANTITY", DoubleType()),
    StructField("TIME",  LongType()),
    StructField("IS_BUYER_MAKER", BooleanType()),
    StructField("IS_BEST_MATCH", BooleanType())
])

df_trade_stream = spark.readStream.format("parquet")\
    .schema(SCHEMA)\
    .load(StrogePath)\
    .withColumn("value", F.to_json(F.struct(F.col("*"))))\
    .withColumn("key", F.lit("key"))\
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))\
    .select(
        F.from_json(
            F.decode(F.col("value"), "iso-8859-1"),
            SCHEMA
        ).alias("value"))\
    .select("value.*")\
    .withColumn("TIME", F.from_unixtime(F.col("TIME") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))


df_trade_stream.withWatermark("TIME", "1 minute")\
    .groupBy(F.window("TIME", "1 minute"))\
    .agg(
        F.first("PRICE").alias("open"),
        F.first("TIME").alias("open_time"),
        F.max("PRICE").alias("high"),
        F.min("PRICE").alias("low"),
        F.last("PRICE").alias("close"),
        F.sum("QUANTITY").alias("volume"),
        F.last("TIME").alias("close_time"),
        F.count("ID").alias("trades"),
)\
    .writeStream.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", elasticsearch_host)\
    .option("es.port", "80")\
    .option("es.resource", "nhom21lan4")\
    .option("es.index.auto.create", "true")\
    .option("checkpointLocation", "gs://bigdata-class-2023/Nhom21/Checkpoint/Checkpoint9")\
    .outputMode("update")\
    .start()

spark.stop()
