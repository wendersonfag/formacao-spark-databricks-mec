"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/{nome_arquivo}.py
"""

# TODO: IMPORTS
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .getOrCreate()
)





spark.stop()