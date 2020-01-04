from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# optional  =>  config("spark.sql.streaming.schemaInference", "true")\
spark = SparkSession.builder \
    .appName("demo10") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Define source of data
stream = spark.readStream\
    .format("kafka") \
    .option("subscribe", "twits")\
    .option("kafka.bootstrap.servers", "172.18.5.41:9092")\
    .option("failOnDataLoss", "false")\
    .option("startingOffsets", "earliest")\
    .load()

stream.printSchema()

tweet_schema = "id STRING, time STRING, text STRING"


# result = stream \
#     .select(col("value").cast("string")) \
#     .select(from_json(col("value"), tweet_schema).alias("tweet")) \
#     .withColumn("tw_word", explode(split(lower(exp("tweet.text")), "\\s+"))) \
#     .drop("tweet")\
#     .where("tw_word LIKE '#%'")\
#     .groupBy("tw_word").count()\
#     .orderBy(desc("count"))\
#     .limit(10)

result = stream\
    .select(col("value").cast("string"))\
    .select(from_json(col("value"), tweet_schema).alias("tweet"))\
    .withColumn("tw_word", explode(split(lower(expr("tweet.text")), "\\s+")))\
    .drop("tweet")\
    .where("tw_word LIKE '#%'")\
    .groupBy("tw_word").count()\
    .orderBy(desc("count"))\
    .limit(10)

result.printSchema()

result.writeStream\
    .trigger(processingTime="5 seconds")\
    .format("console")\
    .outputMode("complete")\
    .option("truncate", "false")\
    .start()

spark.streams.awaitAnyTermination()

spark.stop()
