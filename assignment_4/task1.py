from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder.master("local[3]").appName("SparkSchemaDemo").getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "0.0.0.0") \
    .option("port", 9995) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()


query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination()

spark.stop()