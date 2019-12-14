from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("demo01") \
    .getOrCreate()

# spark2 = SparkSession.builder.getOrCreate() // will return same object

ratings = spark.read \
    .option("header", "true") \
    .csv("data/ratings.csv")

ratings.show()

ratings.printSchema()

ratings = spark.read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv("data/ratings.csv")

ratings.printSchema()

result = ratings.select("movieId", "rating") \
    .groupBy("movieId") \
    .avg("rating")\
    .sort("avg(rating)", ascending=False)


result.show()

spark.stop()
