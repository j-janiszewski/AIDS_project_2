from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()
df = spark.read.text("../data/Book.txt")\

words = df.select(explode(split(df.value, " ")).alias("word"))

word_freq = words.groupBy("word").count()
word_freq = word_freq.sort(desc("count"))

spark.stop()