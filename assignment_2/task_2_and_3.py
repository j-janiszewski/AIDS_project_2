from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("words_frequency")
sc = SparkContext(conf=conf)

lines =  sc.textFile("../data/Book.txt")
words = lines.flatMap(lambda x: x.split())
result = words.countByValue()
sorted_results = collections.OrderedDict(sorted(result.items(), key=lambda x:x[1]))

for key, value in sorted_results.items():
    print(f"{key} : {value}")