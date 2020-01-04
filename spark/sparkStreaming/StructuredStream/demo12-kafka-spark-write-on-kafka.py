from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("demo11") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "twits") \
    .option("kafka.bootstrap.servers", "172.18.5.41:9092") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

tweet_schema = "id STRING, time STRING, text STRING"

result = stream \
    .select(col("value").cast("string")) \
    .select(from_json(col("value"), tweet_schema).alias("tweet")) \
    .withColumn("tw_time", expr("tweet.time  / 1000").cast("timestamp")) \
    .withColumn("tw_word", explode(split(lower(expr("tweet.text")), "\\s+"))) \
    .drop("tweet") \
    .where("tw_word LIKE '#%'") \
    .withWatermark("tw_time", "20 seconds") \
    .groupBy(window("tw_time", "30 seconds", "10 seconds"), "tw_word").count() \
    .orderBy(desc("count")) \
    .limit(10)

result \
    .select(to_json(struct("window", "tw_word", "count")).alias("value")) \
    .writeStream \
    .trigger(processingTime="30 seconds") \
    .format("kafka") \
    .outputMode("complete") \
    .option("topic", "result") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/tmp/checkpoints3") \
    .start()

spark.streams.awaitAnyTermination()

spark.stop()
