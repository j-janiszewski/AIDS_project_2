from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("words_frequency")
sc = SparkContext(conf=conf)

count= sc.textFile("../data/Marvel+Graph.txt")
names = sc.textFile("../data/Marvel+Names.txt")


heroes= count.flatMap(lambda x: x.split())
result = heroes.countByValue()
sorted_results = collections.OrderedDict(sorted(result.items(), key=lambda x:x[1]))

least_pop_id = next(iter(sorted_results))
most_pop_id = next(reversed(sorted_results))


split_names = names.map(lambda x: x.split(maxsplit=1))

most_pop = split_names.filter(lambda x: x[0]==most_pop_id)
print(most_pop.collect()[0])

least_pop = split_names.filter(lambda x: x[0]==least_pop_id)
print(least_pop.collect()[0])
