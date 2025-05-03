"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-5-basic-transformation.py
"""

from pyspark.sql import SparkSession #, Functions as fn
from pyspark.sql.functions import col, upper, concat, lit, when

spark = (
    SparkSession.builder
    .getOrCreate()
)

# TODO: Parametros
#input
rest_path = "./storage/mysql/restaurants/*.jsonl"
#output

# TODO
df_rest = (
    spark.read.json(rest_path)
)

print(f"QUATIDADE DE LINHA {df_rest.count()}")
df_rest.printSchema()
df_rest.show(5)


# TODO 1. Selecting columns

df_basic_select_rest = (
    df_rest.select(
        "cuisine_type",
        "num_reviews",
        "opening_time",
        "closing_time"
    )
)

df_basic_select_rest.show(5)

df_basic_select_rest_col = (
    df_rest.select(
        col("cuisine_type"),
        col("num_reviews"),
        col("opening_time"),
        col("closing_time")
    )
)

df_basic_select_rest_col.show(5)

# TODO 2. Renaming columns

df_renamed_rest = (
    df_rest.withColumnRenamed("name", "restaurant")
    .withColumnRenamed("num_reviews", "reviews")
    .withColumnRenamed("cuisine_type", "cuisine")
    .withColumnRenamed("opening_time", "open") 
    .withColumnRenamed("closing_time", "close")

)

print(df_renamed_rest.columns)
df_renamed_rest.show(5)


# TODO 3. filtering rows
df_high_rated_rest = (
    df_rest.filter(col("num_reviews") > 1000)
)

df_high_rated_rest.select("name", "cuisine_type").show(5)

df_italian_rest = df_rest.filter(col("cuisine_type") == "Italian")

df_italian_rest.select("name", "cuisine_type").show(5)

df_most_popular = (
    df_rest.filter("num_reviews > 3000")
)

print(df_most_popular.count())

# TODO 4. Using logical operators
good_italian_rest = (
    df_rest.filter(
        (col("cuisine_type") == "Italian") &
        (col("num_reviews") > 500)
    )
)

good_italian_rest.select("name", "cuisine_type", "num_reviews").show(5)

# TODO 5. Transforming columns
df_uppercase_rest = (
    df_rest.select(
        upper(col("name")).alias("restaurant"),
        concat(col("city"), lit(", "), col("country")).alias("location"),
    )
)

df_uppercase_rest.show(5)


# TODO 6. Adding columns
df_categorized_rest = (
    df_rest.withColumn(
        "category",
        when(col("average_rating") >= 4.5, "Excellent")
        .when(col("average_rating") >= 4.0, "Very Good")
        .when(col("average_rating") >= 3.5, "Good")
        .when(col("average_rating") >= 3.0, "Average")  
        .otherwise("Poor")
    )

)

df_categorized_rest.select("name", "cuisine_type", "num_reviews", "average_rating","category").show(5)

# TODO 7. Dropping columns
df_rest_drop =(
    df_rest.drop("city", "country", "name")
)

print(df_rest_drop.columns)
df_rest_drop.show(5)

spark.stop()