from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("WordFrequency")
sc = SparkContext(conf = conf)


lines = sc.textFile('customer-orders.csv')

def parse_line(line):
    customer_id, _, amount = line.split(",")
    return (customer_id, float(amount))

rdd = lines.map(parse_line)

spendings = rdd.reduceByKey(lambda x,y: x + y).sortBy(lambda x: x[1], ascending=False)

for i in spendings.collect():
    print(i)
