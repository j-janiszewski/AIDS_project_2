from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[3]").appName("SparkSchemaDemo").getOrCreate()


customer_df = spark.read.format("csv").options(inferSchema=True).load("../data/customer-orders.csv")
customer_df = customer_df.selectExpr("_c0 as id", "_c1 as prod_id", "_c2 as amount")


customer_df.groupby("id").sum("amount").show()

spark.stop()