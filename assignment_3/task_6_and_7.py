from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import explode
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("WordFrequency").getOrCreate()
df_count = spark.read.text("../data/Marvel+Graph.txt")

df_names = spark.read.text("../data/Marvel+Names.txt")
heroes = df_count.select(explode(split(df_count.value, " ")).alias("name"))

filtered_heroes = heroes.filter(heroes.name != '')
heroes_count= filtered_heroes.groupBy("name").count()
heroes_count = heroes_count.sort(desc("count"))


first_row = list(heroes_count.first())
last_row = list(heroes_count.collect()[-1])

df_split = df_names.withColumn("ID", split(df_names["value"], ' "')[0].cast("int")).withColumn("Name", split(df_names["value"], ' "')[1])

most_popular = df_split.filter(df_split.ID == first_row[0])
most_popular.show()

least_popular = df_split.filter(df_split.ID == last_row[0])
least_popular.show()


spark.stop()