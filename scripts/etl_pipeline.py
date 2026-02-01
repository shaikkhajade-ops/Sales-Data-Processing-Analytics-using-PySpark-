from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Sales Data ETL") \
    .getOrCreate()

# Read sample sales data
df = spark.read.csv(
    "data/sample_sales.csv",
    header=True,
    inferSchema=True
)

# Show sample records
df.show()

# Stop Spark session
spark.stop()
