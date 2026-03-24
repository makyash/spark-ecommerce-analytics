# Events -> Facts table
# Users -> Dimension table
# Products -> Dimension table

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
#events_df.show(5)

enriched_df = events_df \
            .join(users_df,"user_id", "left")\
            .join(products_df,"product_id","left")


#validate the joins
print("Number of rows for events",events_df.count())
print("Number of rows for enriched",enriched_df.count())


spark.stop()