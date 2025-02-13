

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower,sum

# Create a sample DataFrame


df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')





################################################################################


#Step 4. See the first 30 entries
df.show(30)
+-------+-----------+----------+----------+--------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-------+-----------+----------+----------+--------+------------+
|      1|          1|         1|2024-01-15|       2|       39.98|
|      2|          1|         3|2024-01-20|       1|       29.99|
|      3|          2|         2|2024-01-16|       1|       25.00|
|      4|          2|         4|2024-01-22|       3|       89.97|
|      5|          3|         5|2024-01-17|       2|       49.98|
|      6|          4|         6|2024-01-18|       4|      119.96|
|      7|          4|         7|2024-01-25|       1|       15.50|
|      8|          5|         8|2024-01-19|       3|       66.75|
|      9|          6|         9|2024-01-20|       2|       40.00|
|     10|          7|        10|2024-01-21|       5|      110.95|
|     11|          8|        11|2024-01-22|       1|       20.00|
|     12|          9|        12|2024-01-23|       4|       79.96|
|     13|         10|        13|2024-01-24|       2|       55.00|
|     14|         11|        14|2024-01-25|       1|       25.00|
|     15|         12|        15|2024-01-26|       3|       67.47|
|     16|         13|        16|2024-01-27|       2|       34.00|
|     17|         14|        17|2024-01-28|       1|       15.00|
|     18|         15|        18|2024-01-29|       4|       92.00|
|     19|         16|        19|2024-01-30|       3|       60.00|
|     20|         17|        20|2024-01-31|       2|       40.00|
|     21|         18|         1|2024-02-01|       1|       19.99|
|     22|         18|         2|2024-02-05|       2|       39.98|
|     23|         19|         3|2024-02-06|       1|       29.99|
|     24|         20|         4|2024-02-07|       3|       89.97|
|     25|         21|         5|2024-02-08|       2|       49.98|
|     26|         22|         6|2024-02-09|       4|      119.96|
|     27|         23|         7|2024-02-10|       1|       15.50|
|     28|         24|         8|2024-02-11|       5|       99.95|
|     29|         25|         9|2024-02-12|       2|       40.00|
|     30|         26|        10|2024-02-13|       3|       66.75|
+-------+-----------+----------+----------+--------+------------+



################################################################################

#Step 5. What is the number of observations in the dataset?
print(df.select(col("customer_id")).count())
#50

print(df.select("*").count())
#50







################################################################################

#Step 6. What is the number of columns in the dataset?
print(len(df.collect()[1][:]))
#6







################################################################################

#Step 7. Print the name of all the columns.
print(df.summary())







################################################################################



#Step 9. Which was the most-ordered item?

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower,sum
from pyspark.sql.types import IntegerType

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

from pyspark.sql.window import Window

win = Window.partitionBy("customer_id")

df = df.withColumn("amount_of_product_per_customer", sum(col("quantity")).over(win))

df.withColumn("amount_of_product_per_customer",col("amount_of_product_per_customer").cast(IntegerType())).show()

+-------+-----------+----------+----------+--------+------------+------------------------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|amount_of_product_per_customer|
+-------+-----------+----------+----------+--------+------------+------------------------------+
|      1|          1|         1|2024-01-15|       2|       39.98|                             3|
|      2|          1|         3|2024-01-20|       1|       29.99|                             3|
|     13|         10|        13|2024-01-24|       2|       55.00|                             2|
|     14|         11|        14|2024-01-25|       1|       25.00|                             1|
|     15|         12|        15|2024-01-26|       3|       67.47|                             3|
|     16|         13|        16|2024-01-27|       2|       34.00|                             2|
|     17|         14|        17|2024-01-28|       1|       15.00|                             1|
|     18|         15|        18|2024-01-29|       4|       92.00|                             4|
|     19|         16|        19|2024-01-30|       3|       60.00|                             3|
|     20|         17|        20|2024-01-31|       2|       40.00|                             2|
+-------+-----------+----------+----------+--------+------------+------------------------------+
#only showing top 10 rows





#Step 9B

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower,sum, round
from pyspark.sql.types import IntegerType

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

#Step 9. Which was the most-ordered item?

df = df.withColumn("quantity*amount",round(col("total_amount")*col("quantity"),2))

df.groupBy("customer_id").agg(round(sum("quantity*amount"),2)).orderBy("customer_id").show(50)

+-----------+------------------------------+
|customer_id|round(sum(quantity*amount), 2)|
+-----------+------------------------------+
|          1|                        109.95|
|         10|                         110.0|
|         11|                          25.0|
|         12|                        202.41|
|         13|                          68.0|
|         14|                          15.0|
|         15|                         368.0|
|         16|                         180.0|
|         17|                          80.0|
|         18|                         99.95|
|         19|                         29.99|
|          2|                        294.91|
|         20|                        269.91|
|         21|                         99.96|
|         22|                        479.84|
|         23|                          15.5|
|         24|                        499.75|
|         25|                          80.0|
|         26|                        200.25|
|         27|                        319.84|
|         28|                          90.0|
|         29|                         35.98|
|          3|                         99.96|
|         30|                        215.91|
|         31|                         100.0|
|         32|                          27.5|
|         33|                          68.0|
|         34|                         368.0|
|         35|                         180.0|
|         36|                         18.75|
|         37|                         79.96|
|         38|                         19.99|
|         39|                        359.84|
|          4|                        495.34|
|         40|                          80.0|
|         41|                        202.41|
|         42|                          22.5|
|         43|                          80.0|
|         44|                         166.5|
|         45|                        439.84|
|         46|                         12.99|
|          5|                        200.25|
|          6|                          80.0|
|          7|                        554.75|
|          8|                          20.0|
|          9|                        319.84|
+-----------+------------------------------+





#Step 9C

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower, sum, round
from pyspark.sql.types import IntegerType

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

# Step 9. Which was the most-ordered item?

print(df.schema)
"""
StructType([StructField('sale_id', StringType(), True), 
                            StructField('customer_id', StringType(), True), 
                            StructField('product_id', StringType(), True), 
                            StructField('sale_date', StringType(), True), 
                            StructField('quantity', StringType(), True), 
                            StructField('total_amount', StringType(), True)])
                            """

df = df.withColumn("quantity", col("quantity").cast(IntegerType())).withColumn("product_id'",
                                                                               col("product_id").cast(IntegerType()))

df = df.groupBy("product_id").agg(sum("quantity").alias("sum_quantity"))

df.orderBy(col("sum_quantity").desc()).show()

+----------+------------+
| product_id | sum_quantity |
+----------+------------+
| 8 | 11 |
| 6 | 9 |
| 10 | 9 |
| 18 | 8 |
| 9 | 8 |
| 4 | 8 |
| 5 | 7 |
| 3 | 6 |
| 19 | 6 |
| 12 | 6 |
| 15 | 5 |
| 11 | 5 |
| 1 | 5 |
| 7 | 4 |
| 14 | 4 |
| 2 | 4 |
| 16 | 3 |
| 17 | 3 |
| 20 | 3 |
| 13 | 3 |
+----------+------------+

################################################################################










#10

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower,sum, round, monotonically_increasing_id
from pyspark.sql.types import IntegerType
from pyspark.sql import Window as W

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df = df.withColumn("quantity",col("quantity").cast(IntegerType()))
            .withColumn("product_id",col("product_id").cast(IntegerType()))

df.select(sum(col("quantity"))).show()
+-------------+
|sum(quantity)|
+-------------+
|          117|
+-------------+


df.withColumn("all_quantity", sum("quantity").over(W.partitionBy())).show()
+-------+-----------+----------+----------+--------+------------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|all_quantity|
+-------+-----------+----------+----------+--------+------------+------------+
|      1|          1|         1|2024-01-15|       2|       39.98|         117|
|      2|          1|         3|2024-01-20|       1|       29.99|         117|
|      3|          2|         2|2024-01-16|       1|       25.00|         117|
|      4|          2|         4|2024-01-22|       3|       89.97|         117|
|      5|          3|         5|2024-01-17|       2|       49.98|         117|
|      6|          4|         6|2024-01-18|       4|      119.96|         117|
|      7|          4|         7|2024-01-25|       1|       15.50|         117|
|      8|          5|         8|2024-01-19|       3|       66.75|         117|
|      9|          6|         9|2024-01-20|       2|       40.00|         117|
|     10|          7|        10|2024-01-21|       5|      110.95|         117|
|     11|          8|        11|2024-01-22|       1|       20.00|         117|
|     12|          9|        12|2024-01-23|       4|       79.96|         117|
|     13|         10|        13|2024-01-24|       2|       55.00|         117|
|     14|         11|        14|2024-01-25|       1|       25.00|         117|
|     15|         12|        15|2024-01-26|       3|       67.47|         117|
|     16|         13|        16|2024-01-27|       2|       34.00|         117|
|     17|         14|        17|2024-01-28|       1|       15.00|         117|
|     18|         15|        18|2024-01-29|       4|       92.00|         117|
|     19|         16|        19|2024-01-30|       3|       60.00|         117|
|     20|         17|        20|2024-01-31|       2|       40.00|         117|
+-------+-----------+----------+----------+--------+------------+------------+








################################################################################






# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, when, lower,sum, round, monotonically_increasing_id, avg
from pyspark.sql.types import IntegerType
from pyspark.sql import Window as W

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df = df.withColumn("quantity",col("quantity").cast(IntegerType())).withColumn("product_id",col("product_id").cast(IntegerType()))

#11 What is the average revenue amount per order?

df.groupBy("customer_id").agg(avg("total_amount")).orderBy("customer_id").show()

+-----------+-----------------+
|customer_id|avg(total_amount)|
+-----------+-----------------+
|          1|           34.985|
|         10|             55.0|
|         11|             25.0|
|         12|            67.47|
|         13|             34.0|
|         14|             15.0|
|         15|             92.0|
|         16|             60.0|
|         17|             40.0|
|         18|           29.985|
|         19|            29.99|
|          2|           57.485|
|         20|            89.97|
|         21|            49.98|
|         22|           119.96|
|         23|             15.5|
|         24|            99.95|
|         25|             40.0|
|         26|            66.75|
|         27|            79.96|
+-----------+-----------------+
only showing top 20 rows





################################################################################


#Step 12 See the first 25 entries


df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.limit(25).show()

+-------+-----------+----------+----------+--------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-------+-----------+----------+----------+--------+------------+
|      1|          1|         1|2024-01-15|       2|       39.98|
|      2|          1|         3|2024-01-20|       1|       29.99|
|      3|          2|         2|2024-01-16|       1|       25.00|
|      4|          2|         4|2024-01-22|       3|       89.97|
|      5|          3|         5|2024-01-17|       2|       49.98|
|      6|          4|         6|2024-01-18|       4|      119.96|
|      7|          4|         7|2024-01-25|       1|       15.50|
|      8|          5|         8|2024-01-19|       3|       66.75|
|      9|          6|         9|2024-01-20|       2|       40.00|
|     10|          7|        10|2024-01-21|       5|      110.95|
+-------+-----------+----------+----------+--------+------------+


################################################################################

#Step 13 What is the number of observations in the dataset?

# Create a sample DataFrame

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

print(df.count())
#50



################################################################################

#Step 14 What is the number of columns in the dataset?

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

print(len(df.columns))
#6




################################################################################


#Step 15 Print the name of all the columns.

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

print(df.columns)
#['sale_id', 'customer_id', 'product_id', 'sale_date', 'quantity', 'total_amount']




################################################################################


#Step 16 What is the data type of each column?

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

print(df.schema)
"""
StructType([StructField('sale_id', StringType(), True),
    StructField('customer_id', StringType(), True),
     StructField('product_id', StringType(), True),
      StructField('sale_date', StringType(), True),
       StructField('quantity', StringType(), True),
        StructField('total_amount', StringType(), True)])

"""

################################################################################


#Step 17 What is the customer_id of 25th column?

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

print(df.collect()[25][:])
#('26', '22', '6', '2024-02-09', '4', '119.96')








################################################################################


#Step 18 How many products cost more than $10.00?

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.filter(col("total_amount")>30).show()
+-------+-----------+----------+----------+--------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-------+-----------+----------+----------+--------+------------+
|      1|          1|         1|2024-01-15|       2|       39.98|
|      4|          2|         4|2024-01-22|       3|       89.97|
|      5|          3|         5|2024-01-17|       2|       49.98|
|      6|          4|         6|2024-01-18|       4|      119.96|
|      8|          5|         8|2024-01-19|       3|       66.75|
|      9|          6|         9|2024-01-20|       2|       40.00|
|     10|          7|        10|2024-01-21|       5|      110.95|
|     12|          9|        12|2024-01-23|       4|       79.96|
|     13|         10|        13|2024-01-24|       2|       55.00|
|     15|         12|        15|2024-01-26|       3|       67.47|
|     16|         13|        16|2024-01-27|       2|       34.00|
|     18|         15|        18|2024-01-29|       4|       92.00|
|     19|         16|        19|2024-01-30|       3|       60.00|
|     20|         17|        20|2024-01-31|       2|       40.00|
|     22|         18|         2|2024-02-05|       2|       39.98|
|     24|         20|         4|2024-02-07|       3|       89.97|
|     25|         21|         5|2024-02-08|       2|       49.98|
|     26|         22|         6|2024-02-09|       4|      119.96|
|     28|         24|         8|2024-02-11|       5|       99.95|
|     29|         25|         9|2024-02-12|       2|       40.00|
+-------+-----------+----------+----------+--------+------------+
only showing top 20 rows





################################################################################

#Step 19 What is the price of each item?

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.select(col("product_id"),col("total_amount")/col("quantity")).show(50)

+----------+-------------------------+
|product_id|(total_amount / quantity)|
+----------+-------------------------+
|         1|                    19.99|
|         3|                    29.99|
|         2|                     25.0|
|         4|                    29.99|
|         5|                    24.99|
|         6|                    29.99|
|         7|                     15.5|
|         8|                    22.25|
|         9|                     20.0|
|        10|                    22.19|
|        11|                     20.0|
|        12|                    19.99|
|        13|                     27.5|
|        14|                     25.0|
|        15|                    22.49|












################################################################################

#Step 19






















