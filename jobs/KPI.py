# 5 Strong KPI Problem Statements

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_timestamp, sum, hour

spark = SparkSession.builder \
        .appName("KPI_job") \
        .master("local[*]") \
        .getOrCreate()

print("Spark session active:", spark.version)  

df = spark.read.csv("data/events.csv", header = True, inferSchema = True) 
# df.show(5)

df = df.withColumn("event_time", to_timestamp(col("event_time")))
# df.show(5)

#1 KPI 1:  Which products generate the most revenue? 

product_revenue = df.filter(col("event_type")=="purchase") \
                .groupBy("product_id") \
                .agg(sum("amount").alias("total_revenue")) \
                .orderBy(col("total_revenue").desc()) \
                .limit(5)
product_revenue.show()

#2 KPI 2: what percentage of users who view products actually purchase? 
views = df.filter(col("event_type")=="view").select("user_id").distinct().count()
purchases = df.filter(col("event_type")=="purchase").select("user_id").distinct().count()

conversion_rate = purchases/views
print("Conversion Rate:", conversion_rate)

#3 Which countries generate the most revenue? 

country_revenue = df.filter(col("event_type")=="purchase") \
                .groupBy("country") \
                .agg(sum("amount").alias("Total_revenue")) \
                .orderBy(col("Total_revenue").desc())\
                .limit(5)

print("Top 5 countries with most revenue:")
country_revenue.show()

spark.stop()