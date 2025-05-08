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

# TODO 1. basic sql queries
spark.sql("""
SELECT name, cuisine_type, average_rating, city
FROM restaurants
LIMIT 5
""").show()

spark.sql("""
SELECT 
    name AS restaurant_name,
    cuisine_type AS cuisine,
    average_rating AS rating,
    num_reviews AS review_count
FROM restaurants
LIMIT 5
""").show()

spark.sql("""
SELECT 
    name,
    average_rating,
    num_reviews,
    average_rating * SQRT(num_reviews / 1000) AS popularity_score,
    CASE
        WHEN average_rating >= 4.5 THEN 'Excellent'
        WHEN average_rating >= 4.0 THEN 'Very Good'
        WHEN average_rating >= 3.5 THEN 'Good'
        ELSE 'Average or Below'
    END AS rating_category
FROM restaurants
LIMIT 5
""").show()

# TODO 2. filter with where clause
spark.sql("""
SELECT name, cuisine_type, average_rating
FROM restaurants
WHERE average_rating > 4.5
LIMIT 5
""").show()

spark.sql("""
SELECT name, cuisine_type, city, average_rating
FROM restaurants
WHERE cuisine_type = 'Italian' AND average_rating > 4.0
LIMIT 5
""").show()

spark.sql("""
SELECT name, cuisine_type, city, average_rating
FROM restaurants
WHERE cuisine_type = 'French' OR cuisine_type = 'Japanese'
LIMIT 5
""").show()

spark.sql("""
SELECT name, cuisine_type, average_rating
FROM restaurants
WHERE cuisine_type IN ('Italian', 'Chinese', 'French', 'Japanese')
LIMIT 5
""").show()

spark.sql("""
SELECT name, cuisine_type, average_rating
FROM restaurants
WHERE average_rating BETWEEN 4.0 AND 4.5
LIMIT 5
""").show()

# TODO 3. ordering with order by
spark.sql("""
SELECT name, cuisine_type, average_rating
FROM restaurants
ORDER BY average_rating DESC
LIMIT 5
""").show()

spark.sql("""
SELECT cuisine_type, city, name, average_rating
FROM restaurants
ORDER BY cuisine_type ASC, average_rating DESC
LIMIT 5
""").show()


spark.sql("""
SELECT 
    name,
    cuisine_type,
    average_rating,
    num_reviews,
    average_rating * SQRT(num_reviews / 1000) AS popularity_score
FROM restaurants
ORDER BY popularity_score DESC
LIMIT 5
""").show(truncate=False)


# TODO 4. grouping with group by
spark.sql("""
SELECT cuisine_type, COUNT(*) AS restaurant_count
FROM restaurants
GROUP BY cuisine_type
ORDER BY restaurant_count DESC
LIMIT 5
""").show()

spark.sql("""
SELECT 
    cuisine_type,
    COUNT(*) AS restaurant_count,
    ROUND(AVG(average_rating), 2) AS avg_rating,
    MAX(average_rating) AS highest_rating,
    MIN(average_rating) AS lowest_rating,
    SUM(num_reviews) AS total_reviews
FROM restaurants
GROUP BY cuisine_type
ORDER BY restaurant_count DESC
LIMIT 5
""").show()

spark.sql("""
SELECT 
    country,
    city,
    COUNT(*) AS restaurant_count,
    ROUND(AVG(average_rating), 2) AS avg_rating
FROM restaurants
GROUP BY country, city
ORDER BY restaurant_count DESC
LIMIT 5
""").show()


spark.sql("""
SELECT 
    cuisine_type,
    COUNT(*) AS restaurant_count,
    ROUND(AVG(average_rating), 2) AS avg_rating
FROM restaurants
GROUP BY cuisine_type
HAVING COUNT(*) > 5 AND AVG(average_rating) > 3.5
ORDER BY avg_rating DESC
LIMIT 5
""").show()

# TODO 5. combining sql operations
spark.sql("""
SELECT 
    cuisine_type,
    city,
    COUNT(*) AS restaurant_count,
    ROUND(AVG(average_rating), 2) AS avg_rating,
    ROUND(AVG(num_reviews), 0) AS avg_reviews
FROM restaurants
WHERE average_rating > 3.0
GROUP BY cuisine_type, city
HAVING COUNT(*) > 1
ORDER BY restaurant_count DESC, avg_rating DESC
LIMIT 10
""").show()

spark.sql("""
SELECT 
    r.cuisine_type,
    COUNT(o.order_id) AS order_count,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    ROUND(SUM(o.total_amount), 2) AS total_revenue
FROM restaurants r
JOIN orders o 
    ON r.cnpj = o.restaurant_key
GROUP BY r.cuisine_type
ORDER BY total_revenue DESC
LIMIT 5
""").show()


spark.sql("""
SELECT 
    CASE
        WHEN average_rating >= 4.5 THEN 'Excellent (4.5+)' 
        WHEN average_rating >= 4.0 THEN 'Very Good (4.0-4.4)' 
        WHEN average_rating >= 3.5 THEN 'Good (3.5-3.9)' 
        WHEN average_rating >= 3.0 THEN 'Average (3.0-3.4)' 
        ELSE 'Below Average (<3.0)' 
    END AS rating_category,
    COUNT(*) AS restaurant_count,
    ROUND(AVG(num_reviews), 0) AS avg_reviews
FROM restaurants
GROUP BY 
    CASE
        WHEN average_rating >= 4.5 THEN 'Excellent (4.5+)' 
        WHEN average_rating >= 4.0 THEN 'Very Good (4.0-4.4)' 
        WHEN average_rating >= 3.5 THEN 'Good (3.5-3.9)' 
        WHEN average_rating >= 3.0 THEN 'Average (3.0-3.4)' 
        ELSE 'Below Average (<3.0)' 
    END
ORDER BY 
    CASE 
        WHEN rating_category = 'Excellent (4.5+)' THEN 1
        WHEN rating_category = 'Very Good (4.0-4.4)' THEN 2
        WHEN rating_category = 'Good (3.5-3.9)' THEN 3
        WHEN rating_category = 'Average (3.0-3.4)' THEN 4
        ELSE 5
    END
""").show(truncate=False)


spark.stop()