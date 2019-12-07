from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="demo01")

file = sc.textFile("/home/spark/hadoop-2.7.3/LICENSE.txt")

lines = file.map(lambda line: line.lower())
print("file RDD Partitions" , lines.getNumPartitions() )

words = lines.flatMap(lambda line: line.split())
print("word RDD Partitions" , words.getNumPartitions() )

word1s = words.map(lambda word: (word, 1))
print("word1s RDD Partitions" , word1s.getNumPartitions() )

wordcounts = word1s.reduceByKey(lambda acc, cnt: acc + cnt, numPartitions=4) # numPartitions mostly used in suffle opperation
print("wordcounts RDD Partitions" , wordcounts.getNumPartitions() )

result = wordcounts.collect()

print(result)

sc.stop()
