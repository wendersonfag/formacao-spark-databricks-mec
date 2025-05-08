"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  /opt/bitnami/spark/jobs/app/test_write_csv.py

docker exec -it spark-master `
  /opt/bitnami/spark/bin/spark-submit `
    --master local[*] `
    --deploy-mode client `
    /opt/bitnami/spark/jobs/app/test_write_csv.py

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestWriteCSV").getOrCreate()

df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
], ["id", "name"])

df.write.mode("overwrite").csv("file:///./storage/output/csv_test/test.csv", encoding='ISO-8859-1')
spark.stop()
