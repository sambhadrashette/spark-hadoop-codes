from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# optional  =>  config("spark.sql.streaming.schemaInference", "true")\
spark = SparkSession.builder \
    .appName("demo04") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()


# Define source of data
ratings = spark.readStream \
    .format("csv") \
    .option("header", "false") \
    .option("delimiter", ",") \
    .option("inferSchema", "false") \
    .schema("userid INT, movieid INT, rating DOUBLE, rtime BIGINT")\
    .option("path", "./ratings_input") \
    .load()

ratings.printSchema()

# Define operations
result = ratings\
    .groupBy("movieid").count()\
    .orderBy(desc("count"))\
    .limit(10)

# Define the sink (where to write result)
# query = result.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .start()

query = result.writeStream \
    .format("memory") \
    .outputMode("complete") \
    .queryName("top_movies") \
    .start()

# wait for query execution
query.awaitTermination()

spark.stop()
