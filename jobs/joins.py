# Events -> Facts table
# Users -> Dimension table
# Products -> Dimension table

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum, to_date
from pyspark.sql.window import Window

spark = SparkSession.builder\
        .appName("join")\
        .master("local[*]")\
        .getOrCreate()
print("Spark session active:", spark.version)

events_df = spark.read.csv("data/events.csv", header = True, inferSchema = True)
users_df = spark.read.csv("data/users/users.csv", header = True, inferSchema = True)
products_df = spark.read.csv("data/products/products.csv", header = True, inferSchema = True)

# rename country -> events_country to avoid conflict
events_df = events_df.withColumnRenamed("country","events_country")
products_df = products_df.withColumnRenamed("category","product_category")
#events_df.show(5)

enriched_df = events_df \
            .join(users_df,"user_id", "left")\
            .join(products_df,"product_id","left")


#validate the joins
print("Number of rows for events",events_df.count())
print("Number of rows for enriched",enriched_df.count())

enriched_df.printSchema()
# Top Category per User (ADVANCED KPI) 
user_category_spend = enriched_df.filter(col("event_type")=="purchase")\
                    .groupBy("user_id","product_category")\
                    .agg(sum("amount").alias("total_spent"))

top_category_per_user = user_category_spend\
                    .withColumn("rank",row_number().over(Window.partitionBy("user_id").orderBy(col("total_spent").desc())))\
                    .filter(col("rank")==1)

top_category_per_user.show(5)

# partitioned write 
enriched_df = enriched_df.withColumn("event_date", to_date("event_time"))
enriched_df.printSchema()
enriched_df.write.mode("overwrite").partitionBy("event_date").parquet("output/enriched_events")

spark.stop()
