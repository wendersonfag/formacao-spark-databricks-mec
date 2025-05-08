"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-8-data-delivery.py

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master local[*]
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-8-data-delivery.py
"""

# TODO: IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sqrt, to_timestamp, year, month


spark = (
    SparkSession.builder
    .getOrCreate()
)

# TODO: Variables
#input
restaurants_path = "./storage/mysql/restaurants/*.jsonl"
drivers_path = "./storage/postgres/drivers/*.jsonl"
orders_path = "./storage/kafka/orders/*.jsonl"
#output
output_path_json = "./storage/output"



restaurants_df = spark.read.json(restaurants_path)
drivers_df = spark.read.json(drivers_path)
orders_df = spark.read.json(orders_path)



restaurant_analytics = (
    restaurants_df.select(
        "name",
        "cuisine_type",
        "city",
        "country",
        "average_rating",
        "num_reviews"
    )
    .withColumn(
        "popularity_score",
        round(col("average_rating") *sqrt(col("num_reviews") / 1000), 2)
    )
)

print("Restaurant Analytics:")
restaurant_analytics.show(5)


# TODO 1. write file formats
"""
restaurant_analytics.write\
    .mode("overwrite")\
    .option("header", "true")\
    .csv(f"{output_path_json}/csv")

restaurant_analytics.write\
    .mode("overwrite")\
    .json(f"{output_path_json}/json")

restaurant_analytics.write\
    .mode("overwrite")\
    .parquet(f"{output_path_json}/parquet")

restaurant_analytics.write\
    .mode("overwrite")\
    .orc(f"{output_path_json}/orc")
"""
# TODO 2. write modes
"""
#restaurant_analytics.write.mode("errorifexists").parquet(f"{output_path_json}/errorifexists")
restaurant_analytics.write.mode("overwrite").parquet(f"{output_path_json}/overwrite")
restaurant_analytics.write.mode("append").parquet(f"{output_path_json}/append")

# TODO = file-system-level, not data-content-level
restaurant_analytics.write.mode("ignore").parquet(f"{output_path_json}/ignore")
"""

# TODO 3. compression options
"""
restaurant_analytics.write.option("compression", "gzip").parquet(f"{output_path_json}/gzip")
restaurant_analytics.write.option("compression", "snappy").parquet(f"{output_path_json}/snappy")
restaurant_analytics.write.option("compression", "zstd").parquet(f"{output_path_json}/zstd")
restaurant_analytics.write.option("compression", "none").parquet(f"{output_path_json}/none")
"""

# TODO 4. partitioning data
"""
restaurant_analytics.write.partitionBy("cuisine_type").parquet(f"{output_path_json}/by_cuisine")
#restaurant_analytics.write.partitionBy("country", "city").parquet(f"{output_path_json}/by_location")
"""
# TODO 5. controlling file sizes
"""
restaurant_analytics.write.parquet(f"{output_path_json}/default")
restaurant_analytics.repartition(2).write.parquet(f"{output_path_json}/repartitioned")
restaurant_analytics.coalesce(1).write.parquet(f"{output_path_json}/coalesced")
"""

# Todo 6. time-based partitioning
orders_with_time = (
    orders_df.withColumn(
"order_timestamp",
        to_timestamp(col("order_date"))
    )
    .withColumn(
        "year",
        year(col("order_timestamp"))
    )
    .withColumn(
        "month",
        month(col("order_timestamp"))
    )
)


orders_with_time.write\
    .partitionBy("year", "month")\
    .parquet(f"{output_path_json}/orders_by_time")

spark.stop()