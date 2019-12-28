from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# step 1. Define spark streaming context.
sc = SparkContext(master="local[2]", appName="demo01")
ssc = StreamingContext(sc, 10)

# step 2. Define the input sources by creating input DStreams.
stream = ssc.socketTextStream("localhost", 2809)

# step 3. Define the streaming computations by applying transformation and output operations to DStreams.
result = stream \
    .map(lambda line: line.lower()) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, c: a + c)
result.pprint()

# step 4. Start receiving data and processing it using streamingContext.start().
ssc.start()

# step 5. Wait for the processing to be stopped (manually or due to any error) using
# streamingContext.awaitTermination(). 
ssc.awaitTermination()

# step 6. The processing can be manually stopped using streamingContext.stop().
ssc.stop()

# To run this program -- start TCP server socket
# terminal> nc -l -k 2809

# Data written on this will be received by spark appln and do word count of the same.
