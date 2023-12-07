from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[3]").appName("SparkSchemaDemo").getOrCreate()

access_lines = spark.readStream