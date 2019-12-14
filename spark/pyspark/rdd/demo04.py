from pyspark import SparkContext

sc = SparkContext(appName="demo04")

def parse_movie(line):
    try:
        parts = line.split("^")
        return (int(parts[0]), parts[1])
    except:
        return ()


def parse_rating(line):
    try:
        parts = line.split(",")
        return (int(parts[1]), 1)
    except:
        return ()


movies = sc.textFile("data/movies_caret.csv")
ratings = sc.textFile("data/ratings.csv")

all_movies = movies\
    .map(parse_movie)\
    .filter(lambda t: len(t) > 0)

all_ratings = ratings\
    .map(parse_rating)\
    .filter(lambda t: len(t) > 0)\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda rc: rc[1], ascending=False)

(mid,cnt) = all_ratings.first()

top_movie = all_movies.lookup(mid)
print(mid, top_movie)

top_movies = all_ratings.join(all_movies)\
    .sortBy(lambda t: t[1][0], ascending=False)\

# top_movies.explain()

result = top_movies.take(10)

for kv in result:
     print(kv)

sc.stop()
