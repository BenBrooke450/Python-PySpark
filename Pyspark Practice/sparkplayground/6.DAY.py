


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("Rank_of_product_name",row_number().over(Window.partitionBy("category").orderBy("category")))

df.show()
+----------+-------------------+-----------+------+--------------+--------------------+
|product_id|       product_name|   category| price|stock_quantity|Rank_of_product_name|
+----------+-------------------+-----------+------+--------------+--------------------+
|       104|         Headphones|Accessories| 79.99|           200|                   1|
|       105|         Smartwatch|Accessories|199.99|           100|                   2|
|       109|              Mouse|Accessories| 25.99|           300|                   3|
|       110|           Keyboard|Accessories| 45.99|           150|                   4|
|       112|            Charger|Accessories| 19.99|           500|                   5|
|       166|    Smartwatch Band|Accessories| 19.99|           250|                   6|
|       167|         Phone Case|Accessories| 15.99|           300|                   7|
|       168|      Laptop Sleeve|Accessories| 29.99|           100|                   8|
|       172|          Mouse Pad|Accessories| 14.99|           300|                   9|
|       181|        Car Charger|Accessories| 19.99|           250|                  10|
|       120|         Hair Dryer|     Beauty| 49.99|           200|                   1|
|       121|       Skincare Set|     Beauty| 89.99|           120|                   2|
|       122|            Shampoo|     Beauty| 15.99|           250|                   3|
|       123|        Conditioner|     Beauty| 15.99|           230|                   4|
|       124|         Makeup Kit|     Beauty| 59.99|            90|                   5|
|       139|Electric Toothbrush|     Beauty| 89.99|           100|                   6|
|       140|          Face Mask|     Beauty| 25.99|           150|                   7|
|       141|     Essential Oils|     Beauty| 39.99|           120|                   8|
|       142|     Makeup Brushes|     Beauty| 34.99|           180|                   9|
|       143|         Hand Cream|     Beauty| 12.99|           220|                  10|
+----------+-------------------+-----------+------+--------------+--------------------+
only showing top 20 rows









df = df.withColumn("Rank_of_product_name",row_number().over(Window.partitionBy("category").orderBy("stock_quantity")))

df.show()
+----------+-------------------+-----------+------+--------------+--------------------+
|product_id|       product_name|   category| price|stock_quantity|Rank_of_product_name|
+----------+-------------------+-----------+------+--------------+--------------------+
|       105|         Smartwatch|Accessories|199.99|           100|                   1|
|       168|      Laptop Sleeve|Accessories| 29.99|           100|                   2|
|       110|           Keyboard|Accessories| 45.99|           150|                   3|
|       104|         Headphones|Accessories| 79.99|           200|                   4|
|       166|    Smartwatch Band|Accessories| 19.99|           250|                   5|
|       181|        Car Charger|Accessories| 19.99|           250|                   6|
|       109|              Mouse|Accessories| 25.99|           300|                   7|
|       167|         Phone Case|Accessories| 15.99|           300|                   8|
|       172|          Mouse Pad|Accessories| 14.99|           300|                   9|
|       112|            Charger|Accessories| 19.99|           500|                  10|
|       139|Electric Toothbrush|     Beauty| 89.99|           100|                   1|
|       121|       Skincare Set|     Beauty| 89.99|           120|                   2|
|       141|     Essential Oils|     Beauty| 39.99|           120|                   3|
|       200|Electric Toothbrush|     Beauty| 69.99|           120|                   4|
|       140|          Face Mask|     Beauty| 25.99|           150|                   5|
|       142|     Makeup Brushes|     Beauty| 34.99|           180|                   6|
|       120|         Hair Dryer|     Beauty| 49.99|           200|                   7|
|       143|         Hand Cream|     Beauty| 12.99|           220|                   8|
|       123|        Conditioner|     Beauty| 15.99|           230|                   9|
|       122|            Shampoo|     Beauty| 15.99|           250|                  10|
+----------+-------------------+-----------+------+--------------+--------------------+
only showing top 20 rows

























from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("Rank_of_product_name",row_number().over(Window.partitionBy("category").orderBy("stock_quantity")))

df.filter(col("product_name").isin("Mouse","Mouse Pad")).show()
+----------+------------+-----------+-----+--------------+--------------------+
|product_id|product_name|   category|price|stock_quantity|Rank_of_product_name|
+----------+------------+-----------+-----+--------------+--------------------+
|       109|       Mouse|Accessories|25.99|           300|                   7|
|       172|   Mouse Pad|Accessories|14.99|           300|                   9|
+----------+------------+-----------+-----+--------------+--------------------+

















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("Rank_of_product_name",row_number().over(Window.partitionBy("category").orderBy("stock_quantity")))

df.filter(col("product_name").contains("Mouse")).show()
+----------+------------+-----------+-----+--------------+--------------------+
|product_id|product_name|   category|price|stock_quantity|Rank_of_product_name|
+----------+------------+-----------+-----+--------------+--------------------+
|       109|       Mouse|Accessories|25.99|           300|                   7|
|       172|   Mouse Pad|Accessories|14.99|           300|                   9|
+----------+------------+-----------+-----+--------------+--------------------+




















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("Rank_of_product_name",row_number().over(Window.partitionBy("category").orderBy("stock_quantity")))

df.filter(col("stock_quantity") > 150).show()
+----------+----------------+-----------+-----+--------------+--------------------+
|product_id|    product_name|   category|price|stock_quantity|Rank_of_product_name|
+----------+----------------+-----------+-----+--------------+--------------------+
|       104|      Headphones|Accessories|79.99|           200|                   4|
|       166| Smartwatch Band|Accessories|19.99|           250|                   5|
|       181|     Car Charger|Accessories|19.99|           250|                   6|
|       109|           Mouse|Accessories|25.99|           300|                   7|
|       167|      Phone Case|Accessories|15.99|           300|                   8|
|       172|       Mouse Pad|Accessories|14.99|           300|                   9|
|       112|         Charger|Accessories|19.99|           500|                  10|
|       142|  Makeup Brushes|     Beauty|34.99|           180|                   6|
|       120|      Hair Dryer|     Beauty|49.99|           200|                   7|
|       143|      Hand Cream|     Beauty|12.99|           220|                   8|
|       123|     Conditioner|     Beauty|15.99|           230|                   9|
|       122|         Shampoo|     Beauty|15.99|           250|                  10|
|       164|Wireless Charger|Electronics|39.99|           200|                   8|
|       169|         USB Hub|Electronics|24.99|           200|                   9|
|       137|      Smart Plug|Electronics|19.99|           250|                  10|
|       197|      Smart Plug|Electronics|24.99|           250|                  11|
|       135|Smart Light Bulb|       Home|29.99|           200|                  11|
|       148|    Storage Bins|       Home|24.99|           200|                  12|
|       189|       Lunch Box|       Home|19.99|           200|                  13|
|       129|Resistance Bands|     Sports|19.99|           200|                   8|
+----------+----------------+-----------+-----+--------------+--------------------+
only showing top 20 rows



