import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)
from elasticsearch import Elasticsearch

StrogePath = "gs://bigdata-class-2023/Nhom21/PARQUET/Full_data/part-00000-e33abbfa-812f-46a8-a786-53abf915db40-c000.snappy.parquet"
elasticsearch_host = "http://34.36.145.14:80/"
ELASTICSEARCH_INDEX = "nhom21lan5"

sparks = SparkSession.builder.appName("howt").getOrCreate()

SCHEMA = StructType([
    StructField("ID", LongType()),
    StructField("PRICE", DoubleType()),
    StructField("QUANTITY", DoubleType()),
    StructField("BASE_QUANTITY", DoubleType()),
    StructField("TIME",  LongType()),
    StructField("IS_BUYER_MAKER", BooleanType()),
    StructField("IS_BEST_MATCH", BooleanType())
])


df_trade_stream = sparks.read.format("parquet")\
    .schema(SCHEMA)\
    .load(StrogePath)\
    .withColumn("TIME", F.from_unixtime(F.col("TIME") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))\
    .withColumn("TIME", F.date_format(F.col("TIME").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))\
    .withColumn("value", F.to_json(F.struct(F.col("*"))))\
    .withColumn("key", F.lit("key"))\
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))\
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))




DF = df_trade_stream.groupBy(F.window("TIME", "1 minute"))\
    .agg(
        F.first("PRICE").alias("open"),
        F.first("TIME").alias("open_time"),
        F.max("PRICE").alias("high"),
        F.min("PRICE").alias("low"),
        F.last("PRICE").alias("close"),
        F.sum("QUANTITY").alias("volume"),
        F.last("TIME").alias("close_time"),
        F.count("ID").alias("trades"),
        F.first("ID").alias("idx"),
    )

es = Elasticsearch(hosts=elasticsearch_host)

# write to elasticsearch
for row in DF.collect():
        es.index(index=ELASTICSEARCH_INDEX, document=row.asDict())




    
    