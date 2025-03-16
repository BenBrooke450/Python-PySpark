

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, concat, collect_list, concat_ws, max,lit
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.withColumn("how much??", when(col("total_amount")>100,lit("Above 100")) \
              .when((col("total_amount")>75) & (col("total_amount")<100),lit("Between 75 & 100")) \
              .otherwise(when(col("total_amount")<75,lit("Below 75")))).show()

+-------+-----------+----------+----------+--------+------------+----------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|      how much??|
+-------+-----------+----------+----------+--------+------------+----------------+
|      1|          1|         1|2024-01-15|       2|       39.98|        Below 75|
|      2|          1|         3|2024-01-20|       1|       29.99|        Below 75|
|      3|          2|         2|2024-01-16|       1|       25.00|        Below 75|
|      4|          2|         4|2024-01-22|       3|       89.97|Between 75 & 100|
|      5|          3|         5|2024-01-17|       2|       49.98|        Below 75|
|      6|          4|         6|2024-01-18|       4|      119.96|       Above 100|
|      7|          4|         7|2024-01-25|       1|       15.50|        Below 75|
|      8|          5|         8|2024-01-19|       3|       66.75|        Below 75|
|      9|          6|         9|2024-01-20|       2|       40.00|        Below 75|
|     10|          7|        10|2024-01-21|       5|      110.95|       Above 100|
|     11|          8|        11|2024-01-22|       1|       20.00|        Below 75|
|     12|          9|        12|2024-01-23|       4|       79.96|Between 75 & 100|
|     13|         10|        13|2024-01-24|       2|       55.00|        Below 75|
|     14|         11|        14|2024-01-25|       1|       25.00|        Below 75|
|     15|         12|        15|2024-01-26|       3|       67.47|        Below 75|
|     16|         13|        16|2024-01-27|       2|       34.00|        Below 75|
|     17|         14|        17|2024-01-28|       1|       15.00|        Below 75|
|     18|         15|        18|2024-01-29|       4|       92.00|Between 75 & 100|
|     19|         16|        19|2024-01-30|       3|       60.00|        Below 75|
|     20|         17|        20|2024-01-31|       2|       40.00|        Below 75|
+-------+-----------+----------+----------+--------+------------+----------------+
only showing top 20 rows