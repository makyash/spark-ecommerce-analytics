from pyspark.sql.functions import min, avg
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("create")\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

df = spark.read.csv("data/events.csv", header = True, inferSchema = True)

# Create Users_df
users_df = df.select("user_id","country")\
            .dropDuplicates(["user_id"])\
            .withColumnRenamed("country","User_country")

users_df = df.groupBy("user_id")\
            .agg(min("event_time").alias("signup_date"))\
            .join(users_df,"user_id")
users_df.show(5)

# users_df.coalesce(1).write.mode("overwrite")\
#         .option("header",True)\
#         .csv("data/users/")

#rename CSV part_000 -> users.csv 

# Create Products_df

products_df = df.select("product_id","category","amount")\
            .filter(df.event_type == "purchase")\
            .groupBy("product_id","category")\
            .agg(avg("amount").alias("avg_price"))

products_df.show(5)

# products_df.write.mode("overwrite").format("csv").option("header",True).save("data/products")
# Rename products/part_000 -> products.csv


spark.stop()