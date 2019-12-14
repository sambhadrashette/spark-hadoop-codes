from pyspark import SparkContext

sc = SparkContext(appName="demo03")

books = sc.textFile("/home/spark/hadoop-2.7.3/LICENSE.txt")


def parse_book(line):
    try:
        words = line.split(",")
        return (words[3], float(words[4]))
    except:
        return ()


results1 = books.map(parse_book)\
    .filter(lambda sp: len(sp) > 0)\
    .reduceByKey(lambda acc,price: acc + price)\
    .collect()

print(results1)


results2 = books.map(parse_book)\
    .filter(lambda sp: len(sp) > 0)\
    .aggregateByKey(0, lambda x,y: x+y, lambda x,y: x+y)\
    .collect()

print(results2)

sc.stop()
