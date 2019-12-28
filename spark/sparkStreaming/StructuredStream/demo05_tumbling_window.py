from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# optional  =>  config("spark.sql.streaming.schemaInference", "true")\
spark = SparkSession.builder \
    .appName("demo05") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Define source of data
ratings = spark.read \
    .format("csv") \
    .schema("userid INT, movieid INT, rating DOUBLE, rtime BIGINT") \
    .option("inferSchema", "false") \
    .option("header", "false") \
    .option("delimiter", ",") \
    .option("path", "./ratings_input") \
    .load() \
    .withColumn("time", col("rtime").cast("timestamp")) \
    .drop("rtime")

# ratings.printSchema()
# ratings.show()

# Demo 01 : 1 Week
result = ratings \
    .groupBy(window("time", "1 week")) \
    .count() \
    .orderBy(asc("window.start"))

result.printSchema()
result.show(truncate=False)

# Demo 02 : by movieId and window

result = ratings \
    .groupBy(col("movieId"), window(col("time"), "1 week")) \
    .count() \
    .orderBy(asc("window.start")) \
    .limit(10) \
    .withColumn("weekday", expr("date_format(window.start, 'EEEE')"))

result.printSchema()
result.show(truncate=False)

# Demo 03 : for movie ID 260

result = ratings \
    .where(col("movieId") == 260) \
    .groupBy(col("movieId"), window(col("time"), "1 week")) \
    .count() \
    .orderBy(asc("window.start")) \
    .limit(10) \
    .withColumn("weekday", expr("date_format(window.start, 'EEEE')"))

result.show(truncate=False)

# Define the sink (where to write result)
# query = result.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .start()


# wait for query execution
# query.awaitTermination()

spark.stop()
