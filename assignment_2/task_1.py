from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[3]").appName("SparkSchemaDemo").getOrCreate()


temperature_df = spark.read.format("csv").options(inferSchema=True).load("data/1800.csv")
temperature_df = temperature_df.selectExpr("_c0 as name", "_c2 as obs_type", "_c3 as temp")

temperature_df=temperature_df.filter("(name == 'ITE00100554' or name == 'EZE00100082') and obs_type == 'TMIN' ")
temperature_df.groupby("name").min("temp").show()