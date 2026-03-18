from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_timestamp, sum, hour

spark = SparkSession.builder \
        .appName("small_batch_job") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

print("Spark session active:", spark.version) 

df = spark.read.csv("data/events.csv", header =True, inferSchema=True)

print("Raw schema:")
df.printSchema()

# convert event_time to timestampe for time_based analysis 
df = df.withColumn("event_time", to_timestamp(col("event_time"))) 
print("Sample data:")
df.show(5, truncate = False)

event_counts = df.groupBy("event_type").agg(
    count("*").alias("event_count")
)

print("Event Counts:")
event_counts.show()

event_counts.limit(10).write.mode("overwrite").parquet("output/event_counts")

purchase_totals = df.filter(col("event_type") == "purchase")\
                .groupBy("user_id")\
                .agg(sum("amount").alias("total_purchase_amount"))\
                .orderBy(col("total_purchase_amount").desc())

print("Purchase totals by user:")
purchase_totals.show()
purchase_totals.write.mode("overwrite").parquet("output/purchase_totals")

purchase_by_hour = df.filter(col("event_type")=="purchase")\
                    .withColumn("event_hour", hour(col("event_time")))\
                    .groupBy("event_hour")\
                    .agg(
                        count("*").alias("purchase_count"),
                        sum("amount").alias("total_purchase_amount")
                    )\
                    .orderBy("event_hour")
print("Purchases by hour:")
purchase_by_hour.show()

# Paritioned write 
purchase_by_hour.write.mode("overwrite").partitionBy("event_hour").parquet("output/purchase_by_hour") 

spark.stop() 