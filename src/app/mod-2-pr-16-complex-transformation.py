"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/mod-2-pr-15-basic-transformation.py
"""

# TODO: IMPORTS
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .config("spark.sql.warehouse.dir", "/opt/bitnami/spark/jobs/app/warehouse")
    .getOrCreate()
)

# TODO: set config
spark.sparkContext.setLogLevel("ERROR")
spark.sql("set spark.sql.echo=true")

# TODO: read data using SQL
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW restaurants
USING json
OPTIONS (path "./storage/mysql/restaurants/*.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW drivers
USING json
OPTIONS (path "./storage/postgres/drivers/*.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW orders
USING json
OPTIONS (path "./storage/kafka/orders/*.jsonl")
""")