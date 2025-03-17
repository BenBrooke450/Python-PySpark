

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










df.withColumn("how much??", when(col("total_amount")>100,concat(lit("Above 100"),lit(" & Quantity: "),col("quantity"))) \
              .when((col("total_amount")>75) & (col("total_amount")<100),concat(lit("Between 75 & 100"),lit(" & Quantity: "),col("quantity"))) \
              .otherwise(when(col("total_amount")<75,concat(lit("Below 75"),lit(" & Quantity: "),col("quantity"))))).show()

+-------+-----------+----------+----------+--------+------------+--------------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|          how much??|
+-------+-----------+----------+----------+--------+------------+--------------------+
|      1|          1|         1|2024-01-15|       2|       39.98|Below 75 & Quanti...|
|      2|          1|         3|2024-01-20|       1|       29.99|Below 75 & Quanti...|
|      3|          2|         2|2024-01-16|       1|       25.00|Below 75 & Quanti...|
|      4|          2|         4|2024-01-22|       3|       89.97|Between 75 & 100 ...|
|      5|          3|         5|2024-01-17|       2|       49.98|Below 75 & Quanti...|
|      6|          4|         6|2024-01-18|       4|      119.96|Above 100 & Quant...|
|      7|          4|         7|2024-01-25|       1|       15.50|Below 75 & Quanti...|
|      8|          5|         8|2024-01-19|       3|       66.75|Below 75 & Quanti...|
|      9|          6|         9|2024-01-20|       2|       40.00|Below 75 & Quanti...|
|     10|          7|        10|2024-01-21|       5|      110.95|Above 100 & Quant...|
|     11|          8|        11|2024-01-22|       1|       20.00|Below 75 & Quanti...|
|     12|          9|        12|2024-01-23|       4|       79.96|Between 75 & 100 ...|
|     13|         10|        13|2024-01-24|       2|       55.00|Below 75 & Quanti...|
|     14|         11|        14|2024-01-25|       1|       25.00|Below 75 & Quanti...|
|     15|         12|        15|2024-01-26|       3|       67.47|Below 75 & Quanti...|
|     16|         13|        16|2024-01-27|       2|       34.00|Below 75 & Quanti...|
|     17|         14|        17|2024-01-28|       1|       15.00|Below 75 & Quanti...|
|     18|         15|        18|2024-01-29|       4|       92.00|Between 75 & 100 ...|
|     19|         16|        19|2024-01-30|       3|       60.00|Below 75 & Quanti...|
|     20|         17|        20|2024-01-31|       2|       40.00|Below 75 & Quanti...|
+-------+-----------+----------+----------+--------+------------+--------------------+
only showing top 20 rows






df.withColumn("Above a 100", when(col("total_amount")>100,concat(lit("Y"))).otherwise("N")).show()
+-------+-----------+----------+----------+--------+------------+-----------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|Above a 100|
+-------+-----------+----------+----------+--------+------------+-----------+
|      1|          1|         1|2024-01-15|       2|       39.98|          N|
|      2|          1|         3|2024-01-20|       1|       29.99|          N|
|      3|          2|         2|2024-01-16|       1|       25.00|          N|
|      4|          2|         4|2024-01-22|       3|       89.97|          N|
|      5|          3|         5|2024-01-17|       2|       49.98|          N|
|      6|          4|         6|2024-01-18|       4|      119.96|          Y|
|      7|          4|         7|2024-01-25|       1|       15.50|          N|
|      8|          5|         8|2024-01-19|       3|       66.75|          N|
|      9|          6|         9|2024-01-20|       2|       40.00|          N|
|     10|          7|        10|2024-01-21|       5|      110.95|          Y|
|     11|          8|        11|2024-01-22|       1|       20.00|          N|
|     12|          9|        12|2024-01-23|       4|       79.96|          N|
|     13|         10|        13|2024-01-24|       2|       55.00|          N|
|     14|         11|        14|2024-01-25|       1|       25.00|          N|
|     15|         12|        15|2024-01-26|       3|       67.47|          N|
|     16|         13|        16|2024-01-27|       2|       34.00|          N|
|     17|         14|        17|2024-01-28|       1|       15.00|          N|
|     18|         15|        18|2024-01-29|       4|       92.00|          N|
|     19|         16|        19|2024-01-30|       3|       60.00|          N|
|     20|         17|        20|2024-01-31|       2|       40.00|          N|
+-------+-----------+----------+----------+--------+------------+-----------+
only showing top 20 rows








from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, concat, collect_list, concat_ws, max,lit, dense_rank, rank
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

x = df.withColumn("Above a 100", when(col("total_amount")>100,concat(lit("Y"))).otherwise("N")) \
      .withColumn("Rank",rank().over(Window.partitionBy().orderBy("Above a 100"))).tail(20)

df = spark.createDataFrame(x)

df.show()
+-------+-----------+----------+----------+--------+------------+-----------+----+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|Above a 100|Rank|
+-------+-----------+----------+----------+--------+------------+-----------+----+
|     34|         30|        14|2024-02-17|       3|       71.97|          N|   1|
|     35|         31|        15|2024-02-18|       2|       50.00|          N|   1|
|     36|         32|        16|2024-02-19|       1|       27.50|          N|   1|
|     37|         33|        17|2024-02-20|       2|       34.00|          N|   1|
|     38|         34|        18|2024-02-21|       4|       92.00|          N|   1|
|     39|         35|        19|2024-02-22|       3|       60.00|          N|   1|
|     40|         36|        20|2024-02-23|       1|       18.75|          N|   1|
|     41|         37|         1|2024-02-24|       2|       39.98|          N|   1|
|     42|         38|         2|2024-02-25|       1|       19.99|          N|   1|
|     43|         39|         3|2024-02-26|       4|       89.96|          N|   1|
|     44|         40|         4|2024-02-27|       2|       40.00|          N|   1|
|     45|         41|         5|2024-02-28|       3|       67.47|          N|   1|
|     46|         42|         6|2024-03-01|       1|       22.50|          N|   1|
|     47|         43|         7|2024-03-02|       2|       40.00|          N|   1|
|     48|         44|         8|2024-03-03|       3|       55.50|          N|   1|
|     50|         46|        10|2024-03-05|       1|       12.99|          N|   1|
|      6|          4|         6|2024-01-18|       4|      119.96|          Y|  47|
|     10|          7|        10|2024-01-21|       5|      110.95|          Y|  47|
|     26|         22|         6|2024-02-09|       4|      119.96|          Y|  47|
|     49|         45|         9|2024-03-04|       4|      109.96|          Y|  47|
+-------+-----------+----------+----------+--------+------------+-----------+----+










