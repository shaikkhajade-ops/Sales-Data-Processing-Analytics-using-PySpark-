from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Sales Data ETL") \
    .getOrCreate()

df = spark.read.csv(
    "data/sample_sales.csv",
    header=True,
    inferSchema=True
)

# Cleaning
df = df.dropna()

# Transformation
df = df.withColumn("total_amount", col("quantity") * col("price"))

# Aggregation
df_grouped = df.groupBy("product").sum("total_amount")

df_grouped.show()

# Save output
df_grouped.write.mode("overwrite").orc("output/sales_data")

spark.stop()
