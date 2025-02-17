from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df.withColumn("Avg_over_category",round(avg("price").over(Window.partitionBy("category")))).show()

+----------+-------------------+-----------+------+--------------+-----------------+
|product_id|       product_name|   category| price|stock_quantity|Avg_over_category|
+----------+-------------------+-----------+------+--------------+-----------------+
|       104|         Headphones|Accessories| 79.99|           200|             47.0|
|       105|         Smartwatch|Accessories|199.99|           100|             47.0|
|       109|              Mouse|Accessories| 25.99|           300|             47.0|
|       110|           Keyboard|Accessories| 45.99|           150|             47.0|
|       112|            Charger|Accessories| 19.99|           500|             47.0|
|       166|    Smartwatch Band|Accessories| 19.99|           250|             47.0|
|       167|         Phone Case|Accessories| 15.99|           300|             47.0|
|       168|      Laptop Sleeve|Accessories| 29.99|           100|             47.0|
|       172|          Mouse Pad|Accessories| 14.99|           300|             47.0|
|       181|        Car Charger|Accessories| 19.99|           250|             47.0|
|       120|         Hair Dryer|     Beauty| 49.99|           200|             46.0|
|       121|       Skincare Set|     Beauty| 89.99|           120|             46.0|
|       122|            Shampoo|     Beauty| 15.99|           250|             46.0|
|       123|        Conditioner|     Beauty| 15.99|           230|             46.0|
|       124|         Makeup Kit|     Beauty| 59.99|            90|             46.0|
|       139|Electric Toothbrush|     Beauty| 89.99|           100|             46.0|
|       140|          Face Mask|     Beauty| 25.99|           150|             46.0|
|       141|     Essential Oils|     Beauty| 39.99|           120|             46.0|
|       142|     Makeup Brushes|     Beauty| 34.99|           180|             46.0|
|       143|         Hand Cream|     Beauty| 12.99|           220|             46.0|
+----------+-------------------+-----------+------+--------------+-----------------+
only showing top 20 rows






















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df.orderBy("category","price").show()


+----------+-------------------+-----------+------+--------------+
|product_id|       product_name|   category| price|stock_quantity|
+----------+-------------------+-----------+------+--------------+
|       172|          Mouse Pad|Accessories| 14.99|           300|
|       167|         Phone Case|Accessories| 15.99|           300|
|       112|            Charger|Accessories| 19.99|           500|
|       166|    Smartwatch Band|Accessories| 19.99|           250|
|       181|        Car Charger|Accessories| 19.99|           250|
|       105|         Smartwatch|Accessories|199.99|           100|
|       109|              Mouse|Accessories| 25.99|           300|
|       168|      Laptop Sleeve|Accessories| 29.99|           100|
|       110|           Keyboard|Accessories| 45.99|           150|
|       104|         Headphones|Accessories| 79.99|           200|
|       143|         Hand Cream|     Beauty| 12.99|           220|
|       122|            Shampoo|     Beauty| 15.99|           250|
|       123|        Conditioner|     Beauty| 15.99|           230|
|       140|          Face Mask|     Beauty| 25.99|           150|
|       142|     Makeup Brushes|     Beauty| 34.99|           180|
|       141|     Essential Oils|     Beauty| 39.99|           120|
|       120|         Hair Dryer|     Beauty| 49.99|           200|
|       124|         Makeup Kit|     Beauty| 59.99|            90|
|       200|Electric Toothbrush|     Beauty| 69.99|           120|
|       121|       Skincare Set|     Beauty| 89.99|           120|
+----------+-------------------+-----------+------+--------------+
only showing top 20 rows

















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df.orderBy("category",col("price").desc())

window_spec = Window.partitionBy("category").orderBy(col("price").desc())

df= df.withColumn("row_num", row_number().over(window_spec))

df.filter(col("row_num")<5).show()


+----------+-------------------+-----------+------+--------------+-------+
|product_id|       product_name|   category| price|stock_quantity|row_num|
+----------+-------------------+-----------+------+--------------+-------+
|       104|         Headphones|Accessories| 79.99|           200|      1|
|       110|           Keyboard|Accessories| 45.99|           150|      2|
|       168|      Laptop Sleeve|Accessories| 29.99|           100|      3|
|       109|              Mouse|Accessories| 25.99|           300|      4|
|       121|       Skincare Set|     Beauty| 89.99|           120|      1|
|       139|Electric Toothbrush|     Beauty| 89.99|           100|      2|
|       200|Electric Toothbrush|     Beauty| 69.99|           120|      3|
|       124|         Makeup Kit|     Beauty| 59.99|            90|      4|
|       101|             Laptop|Electronics|999.99|            50|      1|
|       108|            Speaker|Electronics| 89.99|           120|      2|
|       113|             Webcam|Electronics| 89.99|           100|      3|
|       178|   Wireless Speaker|Electronics| 89.99|           120|      4|
|       157|             Heater|       Home| 99.99|            60|      1|
|       175|                Rug|       Home| 99.99|            50|      2|
|       133|            Blender|       Home| 89.99|           100|      3|
|       116|            Blender|       Home| 79.99|            40|      4|
|       160|      Running Shoes|     Sports| 99.99|           200|      1|
|       186|       Hiking Boots|     Sports| 89.99|            70|      2|
|       150|     Portable Grill|     Sports| 79.99|            60|      3|
|       161|         Skateboard|     Sports| 79.99|            90|      4|
+----------+-------------------+-----------+------+--------------+-------+














from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df = df.withColumn("sale_date_altered", date_format("sale_date",'dd MMM yyyy')).show()
+-------+-----------+----------+----------+--------+------------+-----------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|sale_date_altered|
+-------+-----------+----------+----------+--------+------------+-----------------+
|      1|          1|         1|2024-01-15|       2|       39.98|      15 Jan 2024|
|      2|          1|         3|2024-01-20|       1|       29.99|      20 Jan 2024|
|      3|          2|         2|2024-01-16|       1|       25.00|      16 Jan 2024|
|      4|          2|         4|2024-01-22|       3|       89.97|      22 Jan 2024|
|      5|          3|         5|2024-01-17|       2|       49.98|      17 Jan 2024|
|      6|          4|         6|2024-01-18|       4|      119.96|      18 Jan 2024|
|      7|          4|         7|2024-01-25|       1|       15.50|      25 Jan 2024|
|      8|          5|         8|2024-01-19|       3|       66.75|      19 Jan 2024|
|      9|          6|         9|2024-01-20|       2|       40.00|      20 Jan 2024|
|     10|          7|        10|2024-01-21|       5|      110.95|      21 Jan 2024|
|     11|          8|        11|2024-01-22|       1|       20.00|      22 Jan 2024|
|     12|          9|        12|2024-01-23|       4|       79.96|      23 Jan 2024|
|     13|         10|        13|2024-01-24|       2|       55.00|      24 Jan 2024|
|     14|         11|        14|2024-01-25|       1|       25.00|      25 Jan 2024|
|     15|         12|        15|2024-01-26|       3|       67.47|      26 Jan 2024|
|     16|         13|        16|2024-01-27|       2|       34.00|      27 Jan 2024|
|     17|         14|        17|2024-01-28|       1|       15.00|      28 Jan 2024|
|     18|         15|        18|2024-01-29|       4|       92.00|      29 Jan 2024|
|     19|         16|        19|2024-01-30|       3|       60.00|      30 Jan 2024|
|     20|         17|        20|2024-01-31|       2|       40.00|      31 Jan 2024|
+-------+-----------+----------+----------+--------+------------+-----------------+
only showing top 20 rows






















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df = df.withColumn("sale_date_altered", date_format("sale_date",'dd MMM yyyy'))

df.groupBy("sale_date_altered").agg(sum(col("total_amount"))).orderBy(col("sum(total_amount)").desc()).show(10)

+-----------------+-----------------+
|sale_date_altered|sum(total_amount)|
+-----------------+-----------------+
|      18 Jan 2024|           119.96|
|      09 Feb 2024|           119.96|
|      21 Jan 2024|           110.95|
|      22 Jan 2024|           109.97|
|      04 Mar 2024|           109.96|
|      11 Feb 2024|            99.95|
|      29 Jan 2024|             92.0|
|      21 Feb 2024|             92.0|
|      07 Feb 2024|            89.97|
|      26 Feb 2024|            89.96|
+-----------------+-----------------+
only showing top 10 rows



