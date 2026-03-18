import os
import sys

print("Step 1: Python started")
print("Python executable:", sys.executable)

python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession

print("Step 2: PySpark imported")

spark = SparkSession.builder \
    .appName("test_spark_session") \
    .master("local[1]") \
    .config("spark.pyspark.python", python_path) \
    .config("spark.pyspark.driver.python", python_path) \
    .getOrCreate()

print("Step 3: Spark session created")
print("Spark version:", spark.version)

spark.range(5).show()

print("Step 4: spark.range worked")

spark.stop()
print("Step 5: Spark session stopped")