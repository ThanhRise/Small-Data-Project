import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StructField,
    StructType,
)

StrogePath = "gs://bigdata-class-2023/Nhom21/PARQUET/Full_data"
elasticsearch_host = "http://34.36.145.14/"
CHECKPOINT_LOCATION = "gs://bigdata-class-2023/Nhom21/Checkpoint/Checkpoint9"
elasticsearch_index = "nhom21lan6"

spark = SparkSession.builder.appName("calculation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

SCHEMA = StructType(
    [
        StructField("ID", LongType()),
        StructField("PRICE", DoubleType()),
        StructField("QUANTITY", DoubleType()),
        StructField("BASE_QUANTITY", DoubleType()),
        StructField("TIME", LongType()),
        StructField("IS_BUYER_MAKER", BooleanType()),
        StructField("IS_BEST_MATCH", BooleanType()),
    ]
)

spark.sparkContext.setLogLevel("WARN")

df_trade_stream = (
    spark.readStream.format("parquet")
    .schema(SCHEMA)
    .load(StrogePath)
    .withColumn("value", F.to_json(F.struct(F.col("*"))))
    .withColumn("key", F.lit("key"))
    .withColumn("value", F.encode(F.col("value"), "iso-8859-1").cast("binary"))
    .withColumn("key", F.encode(F.col("key"), "iso-8859-1").cast("binary"))
    .select(F.from_json(F.decode(F.col("value"), "iso-8859-1"), SCHEMA).alias("value")).select("value.*")
    .withColumn("TIME", F.from_unixtime(F.col("TIME") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
)


query = (
    df_trade_stream.withWatermark("TIME", "1 minute")
    .groupBy(F.window("TIME", "1 minute"))
    .agg(
        F.first("PRICE").alias("open"),
        F.first("TIME").alias("open_time"),
        F.max("PRICE").alias("high"),
        F.min("PRICE").alias("low"),
        F.last("PRICE").alias("close"),
        F.sum("QUANTITY").alias("volume"),
        F.last("TIME").alias("close_time"),
        F.count("ID").alias("trades"),
    )
    .withColumn("open_time", F.date_format("open_time", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    .withColumn("close_time", F.date_format("close_time", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
    .writeStream.format("org.elasticsearch.spark.sql")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .option("es.nodes", elasticsearch_host)
    .option("es.port", "80")
    .option("es.mapping.id", "open_time")
    .outputMode("update")
    .start(elasticsearch_index)
)

query.awaitTermination()

spark.stop()
