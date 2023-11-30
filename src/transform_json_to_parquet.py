from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, BooleanType, DoubleType

sparks = SparkSession.builder.appName('transformParquet').getOrCreate()

files = 'gs://bigdata-class-2023/Nhom21/RawData/BTCUSDT/BTCUSDT-trades-2023-05.csv'

WRITE_PATH = 'gs://bigdata-class-2023/Nhom21/PARQUET/Full_data'

SCHEMA = StructType([

    StructField("ID", LongType()),
    StructField("PRICE", DoubleType()),
    StructField("QUANTITY", DoubleType()),
    StructField("BASE_QUANTITY", DoubleType()),
    StructField("TIME",  LongType()),
    StructField("IS_BUYER_MAKER", BooleanType()),
    StructField("IS_BEST_MATCH", BooleanType())
])



sparks.read\
    .option("multiline", "true")\
    .csv(files, schema=SCHEMA)\
    .write\
    .mode('overwrite')\
    .parquet(
        WRITE_PATH
    )