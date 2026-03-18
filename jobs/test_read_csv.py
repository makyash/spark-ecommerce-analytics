

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test_read_csv") \
    .master("local[*]") \
    .getOrCreate()

print("Spark session created successfully")
print("Spark version:", spark.version)


df = spark.read.csv("data/events.csv", header=True, inferSchema=True)

print("CSV loaded successfully")
df.printSchema()
df.show(5, truncate=False)

spark.stop()
print("Spark session stopped")