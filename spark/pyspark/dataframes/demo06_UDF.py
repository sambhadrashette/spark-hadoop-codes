from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType

conf = SparkConf() \
    .setAppName("demo06") \
    .setMaster("local")

spark = SparkSession.builder.config(conf=conf).getOrCreate()


def isNotStopWord(word):
    stopWords = ["", "a", "an", "the", "of", "for", "and", "or", "to", "in", "you", "any", "by", "this", "that", "is",
                 "with", "as"]
    if ((len(word) == 0) or (word in stopWords)):
        return False
    return True


print("test -- ", isNotStopWord("test"))
print("the -- ", isNotStopWord("the"))

udfIsNotStopWord = udf(isNotStopWord, BooleanType())

df = spark.read \
    .text("/tmp/LICENSE.txt") \
    .select(explode(split(lower(col("value")), "[^a-z]")).alias("word")) \
    .where(udfIsNotStopWord(col("word"))) \
    .groupBy("word") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

df.show()

spark.stop()
