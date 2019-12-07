val file = sc.textFile("/home/spark/hadoop-2.7.3/LICENSE.txt")

val lines = file.map(line => line.toLowerCase)

val words = lines.flatMap(line => line.split("[^a-z]"))

val word1s = words.map(word => (word,1))

val wordcounts = word1s.reduceByKey(_ + _)

wordcounts.foreach(println)

