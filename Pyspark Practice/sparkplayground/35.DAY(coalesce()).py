
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max, lit, coalesce
from pyspark.sql.window import Window

# Initialize SparkSession

spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = [
    ("p1", "r1", "Great product!!", 5.0, ["electronics", "gadgets"], "2024-12-01"),
    ("p1", "r2", "Too expensive.", None, ["electronics"], "2024-12-05"),
    ("p2", "r3", "Works as expected.", 4.0, ["kitchen", "home"], "2024-11-21"),
    ("p2", "r4", "Not worth the price...", None, ["home"], "2024-11-22"),
]

columns = ["product_id", "review_id", "review_text", "rating", "categories", "date"]
df = spark.createDataFrame(df, columns)

df = df.withColumn("rating_filled", coalesce(col("rating"), lit(3.0)))

df.show(40)
"""
+----------+---------+--------------------+------+--------------------+----------+-------------+
|product_id|review_id|         review_text|rating|          categories|      date|rating_filled|
+----------+---------+--------------------+------+--------------------+----------+-------------+
|        p1|       r1|     Great product!!|   5.0|[electronics, gad...|2024-12-01|          5.0|
|        p1|       r2|      Too expensive.|  NULL|       [electronics]|2024-12-05|          3.0|
|        p2|       r3|  Works as expected.|   4.0|     [kitchen, home]|2024-11-21|          4.0|
|        p2|       r4|Not worth the pri...|  NULL|              [home]|2024-11-22|          3.0|
+----------+---------+--------------------+------+--------------------+----------+-------------+

"""