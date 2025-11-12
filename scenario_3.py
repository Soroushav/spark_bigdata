from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("127.0.0.1", 9999)
# words = lines.flatMap(lambda line: line.split(" "))
words = lines.flatMap(lambda line: line.split()) \
             .filter(lambda w: len(w) > 0)

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda a, b: a + b)

wordCounts.pprint()
ssc.start()
ssc.awaitTermination()