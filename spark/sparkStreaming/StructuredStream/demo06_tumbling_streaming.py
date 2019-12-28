from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# optional  =>  config("spark.sql.streaming.schemaInference", "true")\
spark = SparkSession.builder \
    .appName("demo05") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Define source of data
ratings = spark.readStream \
    .format("csv") \
    .schema("userid INT, movieid INT, rating DOUBLE, rtime BIGINT") \
    .option("inferSchema", "false") \
    .option("header", "false") \
    .option("delimiter", ",") \
    .option("path", "./ratings_input") \
    .load() \
    .withColumn("time", col("rtime").cast("timestamp")) \
    .drop("rtime")

# Demo 01 : 1 Week
result1 = ratings \
    .groupBy(window("time", "1 week")) \
    .count() \
    .orderBy(asc("window.start"))

# Demo 02 : by movieId and window

result2 = ratings \
    .groupBy(col("movieId"), window(col("time"), "1 week")) \
    .count() \
    .orderBy(asc("window.start")) \
    .limit(10) \
    .withColumn("weekday", expr("date_format(window.start, 'EEEE')"))

result3 = ratings \
    .where(col("movieId") == 260) \
    .groupBy(col("movieId"), window(col("time"), "1 week")) \
    .count() \
    .orderBy(asc("window.start")) \
    .limit(10) \
    .withColumn("weekday", expr("date_format(window.start, 'EEEE')"))


# Define the sink (where to write result)
query1 = result1.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

query2 = result2.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

query3 = result3.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()


# wait for query execution
spark.streams.awaitAnyTermination()

spark.stop()
