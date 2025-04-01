

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df.withColumn("Potential_value", round(col("price")*col("stock_quantity"),2))

+----------+--------------+-----------+------+--------------+---------------+
|product_id|  product_name|   category| price|stock_quantity|Potential_value|
+----------+--------------+-----------+------+--------------+---------------+
|       101|        Laptop|Electronics|999.99|            50|        49999.5|
|       102|    Smartphone|Electronics|499.99|           150|        74998.5|
|       103|        Tablet|Electronics|299.99|            75|       22499.25|
|       104|    Headphones|Accessories| 79.99|           200|        15998.0|
|       105|    Smartwatch|Accessories|199.99|           100|        19999.0|
|       106|        Camera|Electronics|649.99|            30|        19499.7|
|       107|       Printer|Electronics|129.99|            80|        10399.2|
|       108|       Speaker|Electronics| 89.99|           120|        10798.8|
|       109|         Mouse|Accessories| 25.99|           300|         7797.0|
|       110|      Keyboard|Accessories| 45.99|           150|         6898.5|
|       111|       Monitor|Electronics|299.99|            60|        17999.4|
|       112|       Charger|Accessories| 19.99|           500|         9995.0|
|       113|        Webcam|Electronics| 89.99|           100|         8999.0|
|       114|        Router|Electronics|129.99|            75|        9749.25|
|       115|     Desk Lamp|       Home| 39.99|            90|         3599.1|
|       116|       Blender|       Home| 79.99|            40|         3199.6|
|       117|       Toaster|       Home| 29.99|           110|         3298.9|
|       118|     Microwave|       Home|149.99|            50|         7499.5|
|       119|Vacuum Cleaner|       Home|199.99|            60|        11999.4|
|       120|    Hair Dryer|     Beauty| 49.99|           200|         9998.0|
+----------+--------------+-----------+------+--------------+---------------+
only showing top 20 rows






















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("Potential_value", round(col("price")*col("stock_quantity"),2))

df = df.groupBy("category").agg(max(col("product_name")),max(col("price")))

df.withColumnRenamed("max(product_name)","product_name").withColumnRenamed("max(price)","price").show()

+-----------+----------------+------+
|   category|    product_name| price|
+-----------+----------------+------+
|Accessories| Smartwatch Band| 79.99|
|     Beauty|    Skincare Set| 89.99|
|Electronics|Wireless Speaker|999.99|
|       Home|      Wall Clock| 99.99|
|     Sports|        Yoga Mat| 99.99|
+-----------+----------------+------+
















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.withColumn("price", col("price").cast(IntegerType()))

df = df.withColumn("Cheapest_price_id",row_number().over(Window.partitionBy().orderBy("price"))).show(10)
+----------+----------------+-----------+-----+--------------+-----------------+
|product_id|    product_name|   category|price|stock_quantity|Cheapest_price_id|
+----------+----------------+-----------+-----+--------------+-----------------+
|       143|      Hand Cream|     Beauty|   12|           220|                1|
|       172|       Mouse Pad|Accessories|   14|           300|                2|
|       123|     Conditioner|     Beauty|   15|           230|                3|
|       122|         Shampoo|     Beauty|   15|           250|                4|
|       167|      Phone Case|Accessories|   15|           300|                5|
|       187|       Jump Rope|     Sports|   15|           200|                6|
|       112|         Charger|Accessories|   19|           500|                7|
|       129|Resistance Bands|     Sports|   19|           200|                8|
|       137|      Smart Plug|Electronics|   19|           250|                9|
|       162|     Soccer Ball|     Sports|   19|           250|               10|
+----------+----------------+-----------+-----+--------------+-----------------+
only showing top 10 rows






df.orderBy(col("Cheapest_price_id").desc()).show(10)
+----------+--------------+-----------+-----+--------------+-----------------+
|product_id|  product_name|   category|price|stock_quantity|Cheapest_price_id|
+----------+--------------+-----------+-----+--------------+-----------------+
|       101|        Laptop|Electronics|  999|            50|              100|
|       106|        Camera|Electronics|  649|            30|               99|
|       125|     Treadmill|     Sports|  499|            40|               98|
|       102|    Smartphone|Electronics|  499|           150|               97|
|       126| Exercise Bike|     Sports|  299|            55|               96|
|       111|       Monitor|Electronics|  299|            60|               95|
|       103|        Tablet|Electronics|  299|            75|               94|
|       179| Action Camera|Electronics|  249|            50|               93|
|       195|Smart Doorbell|Electronics|  199|            50|               92|
|       146|  Air Purifier|       Home|  199|            40|               91|
+----------+--------------+-----------+-----+--------------+-----------------+
only showing top 10 rows











