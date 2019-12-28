from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("demo02") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Define source of data
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 2809) \
    .load()

lines.printSchema()

# Define operations
words = lines.select(explode(split(lower(col("value")), "[^a-z]")).alias("word"))\
    .groupBy("word") \
    .count()

# Define the sink (where to write result)
query = words.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .outputMode("complete") \
    .start()

# wait for query execution
query.awaitTermination()

spark.stop()
