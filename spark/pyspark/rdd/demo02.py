from pyspark import SparkContext

sc = SparkContext(appName="demo02")

file = sc.textFile("/home/spark/hadoop-2.7.3/LICENSE.txt")
article_cnt = sc.accumulator(0)


def count_article(word):
    if word == "a" or word == "an" or word == "the":
        article_cnt.add(1)
    return word


stopwords = sc.broadcast(
    ['the', 'a', 'an', 'of', 'or', 'this', 'that', 'and', 'by', 'to', 'from', 'any', 'for', 'you', 'in', 'will',
     'shall', 'is', 'are'])
result = file.map(lambda line: line.lower()) \
    .flatMap(lambda line: line.split()) \
    .map(count_article) \
    .filter(lambda word: word not in stopwords.value) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda acc, cnt: acc + cnt) \
    .sortBy(lambda w1: w1[1], ascending=False) \
    .take(10)

for kv in result:
    print(kv)

print("number of articles: ", article_cnt.value)

sc.stop()
