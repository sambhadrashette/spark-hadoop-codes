from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("demo04") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

ratings = spark.read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("delimiter", "^") \
    .csv("data/movies_caret.csv")

# Genres wise movie count
result = ratings \
    .withColumn("genre", explode(split(col("genres"), "\\|"))) \
    .groupBy("genre").count()

result.show()

# Show only action movies
result = ratings \
    .withColumn("arr_genres", split(col("genres"), "\\|")) \
    .where(array_contains(col("arr_genres"), "Action")) \
    .drop("arr_genres") \
    .drop("genres") \
    .drop("movieId")

result.show(truncate=False)

spark.stop()
