from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("MinTemp")
sc = SparkContext(conf = conf)

lines = sc.textFile('1800.csv')

def parse_line(line):
    (city, _, _, temp, _, _, _, _) = line.split(",")
    temperature = int(temp)/10
    return (city, temperature)

rdd = lines.filter(lambda x: x.split(",")[2] == "TMIN").map(parse_line)

min_temperatures = rdd.reduceByKey(min)

print(min_temperatures.collect())
