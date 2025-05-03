"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-6-complex-transformation.py
"""

# TODO: IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, round, desc,to_timestamp, year, month, dayofweek, regexp_extract, split

spark = (
    SparkSession.builder
    .getOrCreate()
)

# TODO: Dataframes de entrada
#input
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
ratings_df = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")

print(f"Loaded {restaurants_df.count()} restaurants records")
print(f"Loaded {drivers_df.count()} drivers records")
print(f"Loaded {orders_df.count()} orders records")
print(f"Loaded {ratings_df.count()} ratings records")

# TODO 1. aggregations and group by

restaurants_df.groupBy("cuisine_type")\
  .count()\
  .orderBy(desc("count"))\
  .show(5)

cuisine_stats = (
  restaurants_df.groupBy("cuisine_type")
    .agg(
        count("*").alias("count"),
        round(avg("average_rating"),2).alias("rating"),
        max("average_rating").alias("highest"),
        min("average_rating").alias("lowest")
  )
  .orderBy(desc("rating"))
)

cuisine_stats.show(5)

# TODO 2. Filtering aggregated
cuisine_stats.filter(col("count") > 50).show(5)

cuisine_stats.filter(
    (col("rating") > 4.0) &
    (col("lowest") >= 3.5) &
    (col("count") > 10) 
).orderBy(desc("rating")).show(5)

# TODO 3. Joining datasets
restaurants_df.select("cnpj", "name").show(3)
orders_df.select("order_id", "restaurant_key").show(3)

orders_rests_df = orders_df.join(
    restaurants_df,
    orders_df.restaurant_key == restaurants_df.restaurant_id,
    how="inner"
)

orders_rests_df.select(
    "order_id",
    "name",
    "cuisine_type",
    "total_amount"
).show(5)

# TODO 4. advanced functions
orders_with_dates = orders_df.select(
    "order_id",
    "order_date",
    to_timestamp("order_date").alias("timestamp")
)

orders_with_dates.show(5)

date_components = orders_with_dates.select(
    "order_id",
    "timestamp",
    year("timestamp").alias("year"),
    month("timestamp").alias("month"),
    dayofweek("timestamp").alias("day_of_week")
)

date_components.show(5)

address_extraction = restaurants_df.select(
    "name",
    "address",
    regexp_extract("address", r"([A-Za-z ]+) - [A-Z]{2}", 1).alias("extracted_city"),
    split("address", "\n").getItem(0).alias("street"),
    split("address", "\n").getItem(1).alias("city_state"),
    split("address", "\n").getItem(2).alias("postal_code")
)

address_extraction.show(5, truncate=False)
spark.stop()