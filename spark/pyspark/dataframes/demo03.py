from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("demo03") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

ratings = spark.read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv("ratings.csv")

result = ratings \
    .withColumn("rtime", col("timestamp").cast(TimestampType())) \
    .withColumn("ryear", year(col("rtime"))) \
    .groupBy("ryear").count()

result.show()
