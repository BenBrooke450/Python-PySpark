

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/products.csv')

df = df.filter(col("product_name").contains("Electric")) \
    .withColumn("Rank_electric",row_number().over(Window.partitionBy().orderBy("price")))

df.show()
+----------+-------------------+--------+------+--------------+-------------+
|product_id|       product_name|category| price|stock_quantity|Rank_electric|
+----------+-------------------+--------+------+--------------+-------------+
|       144|     Electric Grill|    Home|129.99|            70|            1|
|       130|    Electric Kettle|    Home| 39.99|            85|            2|
|       155|       Electric Fan|    Home| 49.99|           100|            3|
|       200|Electric Toothbrush|  Beauty| 69.99|           120|            4|
|       194|   Electric Blanket|    Home| 79.99|            70|            5|
|       139|Electric Toothbrush|  Beauty| 89.99|           100|            6|
+----------+-------------------+--------+------+--------------+-------------+


#FIND THE DIFERENCE


df = df.withColumn("price",col("price").cast(IntegerType())).orderBy("price").show()
+----------+-------------------+--------+-----+--------------+-------------+
|product_id|       product_name|category|price|stock_quantity|Rank_electric|
+----------+-------------------+--------+-----+--------------+-------------+
|       130|    Electric Kettle|    Home|   39|            85|            2|
|       155|       Electric Fan|    Home|   49|           100|            3|
|       200|Electric Toothbrush|  Beauty|   69|           120|            4|
|       194|   Electric Blanket|    Home|   79|            70|            5|
|       139|Electric Toothbrush|  Beauty|   89|           100|            6|
|       144|     Electric Grill|    Home|  129|            70|            1|
+----------+-------------------+--------+-----+--------------+-------------+





df = spark.createDataFrame(["   Spark", "Spark  ", " Spark"], "STRING")

df.select(trim("value").alias("r")).withColumn("length", length("r")).show()
+-----+------+
|    r|length|
+-----+------+
|Spark|     5|
|Spark|     5|
|Spark|     5|
+-----+------+












