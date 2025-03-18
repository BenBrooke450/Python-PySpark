
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, concat, collect_list, concat_ws, max,lit, dense_rank, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df_sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df_customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id) \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city").show()
+-----------+----------+----------+--------+------------+----------+--------------+-----------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|       address|       city|
+-----------+----------+----------+--------+------------+----------+--------------+-----------+
|          1|         3|2024-01-20|       1|       29.99|      John|    123 Elm St|Springfield|
|          1|         1|2024-01-15|       2|       39.98|      John|    123 Elm St|Springfield|
|          2|         4|2024-01-22|       3|       89.97|      Emma|    456 Oak St|Centerville|
|          2|         2|2024-01-16|       1|       25.00|      Emma|    456 Oak St|Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|   789 Pine St| Greenville|
|          4|         7|2024-01-25|       1|       15.50|      Liam|  101 Maple St|  Riverside|
|          4|         6|2024-01-18|       4|      119.96|      Liam|  101 Maple St|  Riverside|
|          5|         8|2024-01-19|       3|       66.75|      Noah|  202 Birch St|   Lakeside|
|          6|         9|2024-01-20|       2|       40.00|     Alice|  303 Cedar St|    Oakland|
|          7|        10|2024-01-21|       5|      110.95|  Isabella| 404 Spruce St|      Boise|
|          8|        11|2024-01-22|       1|       20.00|     James| 505 Walnut St| Des Moines|
|          9|        12|2024-01-23|       4|       79.96|    Sophia| 606 Cherry St|     Albany|
|         10|        13|2024-01-24|       2|       55.00|     Lucas|  707 Maple St|   Portland|
|         11|        14|2024-01-25|       1|       25.00|       Mia|    808 Oak St|      Miami|
|         12|        15|2024-01-26|       3|       67.47|   William|   909 Pine St|  Nashville|
|         13|        16|2024-01-27|       2|       34.00|    Amelia|   1010 Elm St|     Denver|
|         14|        17|2024-01-28|       1|       15.00|     Ethan| 1111 Birch St|Minneapolis|
|         15|        18|2024-01-29|       4|       92.00|    Harper| 1212 Cedar St|    Seattle|
|         16|        19|2024-01-30|       3|       60.00|   Jackson|1313 Spruce St|    Atlanta|
|         17|        20|2024-01-31|       2|       40.00| Charlotte|1414 Walnut St|  San Diego|
+-----------+----------+----------+--------+------------+----------+--------------+-----------+
only showing top 20 rows





















df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id) \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|      address|       city|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|          1|         3|2024-01-20|       1|       29.99|      John|   123 Elm St|Springfield|
|          1|         1|2024-01-15|       2|       39.98|      John|   123 Elm St|Springfield|
|          2|         4|2024-01-22|       3|       89.97|      Emma|   456 Oak St|Centerville|
|          2|         2|2024-01-16|       1|       25.00|      Emma|   456 Oak St|Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|  789 Pine St| Greenville|
|          4|         7|2024-01-25|       1|       15.50|      Liam| 101 Maple St|  Riverside|
|          4|         6|2024-01-18|       4|      119.96|      Liam| 101 Maple St|  Riverside|
|          5|         8|2024-01-19|       3|       66.75|      Noah| 202 Birch St|   Lakeside|
|          6|         9|2024-01-20|       2|       40.00|     Alice| 303 Cedar St|    Oakland|
|          7|        10|2024-01-21|       5|      110.95|  Isabella|404 Spruce St|      Boise|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
only showing top 10 rows


















df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id,"outer") \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+----------+--------+------------+----------+-------------+------------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|      address|        city|
+-----------+----------+----------+--------+------------+----------+-------------+------------+
|       NULL|      NULL|      NULL|    NULL|        NULL|    Amelia|  4444 Oak St| San Antonio|
|       NULL|      NULL|      NULL|    NULL|        NULL|     James| 4545 Pine St|      Dallas|
|       NULL|      NULL|      NULL|    NULL|        NULL|     Elena|  4646 Elm St|     Detroit|
|       NULL|      NULL|      NULL|    NULL|        NULL|     Henry|4747 Birch St|Indianapolis|
|          1|         1|2024-01-15|       2|       39.98|      John|   123 Elm St| Springfield|
|          1|         3|2024-01-20|       1|       29.99|      John|   123 Elm St| Springfield|
|          2|         2|2024-01-16|       1|       25.00|      Emma|   456 Oak St| Centerville|
|          2|         4|2024-01-22|       3|       89.97|      Emma|   456 Oak St| Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|  789 Pine St|  Greenville|
|          4|         7|2024-01-25|       1|       15.50|      Liam| 101 Maple St|   Riverside|
+-----------+----------+----------+--------+------------+----------+-------------+------------+
only showing top 10 rows




















df = df_sales.join(df_customers,"cross") \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|      address|       city|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|          1|         3|2024-01-20|       1|       29.99|      John|   123 Elm St|Springfield|
|          1|         1|2024-01-15|       2|       39.98|      John|   123 Elm St|Springfield|
|          2|         4|2024-01-22|       3|       89.97|      Emma|   456 Oak St|Centerville|
|          2|         2|2024-01-16|       1|       25.00|      Emma|   456 Oak St|Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|  789 Pine St| Greenville|
|          4|         7|2024-01-25|       1|       15.50|      Liam| 101 Maple St|  Riverside|
|          4|         6|2024-01-18|       4|      119.96|      Liam| 101 Maple St|  Riverside|
|          5|         8|2024-01-19|       3|       66.75|      Noah| 202 Birch St|   Lakeside|
|          6|         9|2024-01-20|       2|       40.00|     Alice| 303 Cedar St|    Oakland|
|          7|        10|2024-01-21|       5|      110.95|  Isabella|404 Spruce St|      Boise|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
only showing top 10 rows























df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id,"right") \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+----------+--------+------------+----------+-------------+------------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|      address|        city|
+-----------+----------+----------+--------+------------+----------+-------------+------------+
|       NULL|      NULL|      NULL|    NULL|        NULL|    Amelia|  4444 Oak St| San Antonio|
|       NULL|      NULL|      NULL|    NULL|        NULL|     James| 4545 Pine St|      Dallas|
|       NULL|      NULL|      NULL|    NULL|        NULL|     Elena|  4646 Elm St|     Detroit|
|       NULL|      NULL|      NULL|    NULL|        NULL|     Henry|4747 Birch St|Indianapolis|
|          1|         3|2024-01-20|       1|       29.99|      John|   123 Elm St| Springfield|
|          1|         1|2024-01-15|       2|       39.98|      John|   123 Elm St| Springfield|
|          2|         4|2024-01-22|       3|       89.97|      Emma|   456 Oak St| Centerville|
|          2|         2|2024-01-16|       1|       25.00|      Emma|   456 Oak St| Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|  789 Pine St|  Greenville|
|          4|         7|2024-01-25|       1|       15.50|      Liam| 101 Maple St|   Riverside|
+-----------+----------+----------+--------+------------+----------+-------------+------------+
only showing top 10 rows























df = df_sales.join(df_customers, df_sales.customer_id == df_customers.customer_id,"left") \
              .select(df_sales.customer_id,"product_id","sale_date","quantity","total_amount","first_name","address","city") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|customer_id|product_id| sale_date|quantity|total_amount|first_name|      address|       city|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
|          1|         1|2024-01-15|       2|       39.98|      John|   123 Elm St|Springfield|
|          1|         3|2024-01-20|       1|       29.99|      John|   123 Elm St|Springfield|
|          2|         2|2024-01-16|       1|       25.00|      Emma|   456 Oak St|Centerville|
|          2|         4|2024-01-22|       3|       89.97|      Emma|   456 Oak St|Centerville|
|          3|         5|2024-01-17|       2|       49.98|    Olivia|  789 Pine St| Greenville|
|          4|         6|2024-01-18|       4|      119.96|      Liam| 101 Maple St|  Riverside|
|          4|         7|2024-01-25|       1|       15.50|      Liam| 101 Maple St|  Riverside|
|          5|         8|2024-01-19|       3|       66.75|      Noah| 202 Birch St|   Lakeside|
|          6|         9|2024-01-20|       2|       40.00|     Alice| 303 Cedar St|    Oakland|
|          7|        10|2024-01-21|       5|      110.95|  Isabella|404 Spruce St|      Boise|
+-----------+----------+----------+--------+------------+----------+-------------+-----------+
only showing top 10 rows
















df = df_customers.join(df_sales, df_sales.customer_id == df_customers.customer_id,"anti") \
              .withColumn("customer_id",col("customer_id").cast(IntegerType())) \
              .orderBy("customer_id").show(10)
+-----------+----------+---------+--------------------+------------+-------------+------------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|      address|        city|state|zip_code|
+-----------+----------+---------+--------------------+------------+-------------+------------+-----+--------+
|         47|    Amelia|    James|amelia.james@live...|    555-0047|  4444 Oak St| San Antonio|   TX|   78202|
|         48|     James|   Walker|james.walker@gmai...|    555-0048| 4545 Pine St|      Dallas|   TX|   75201|
|         49|     Elena|     Gray| elena.gray@zoho.com|    555-0049|  4646 Elm St|     Detroit|   MI|   48202|
|         50|     Henry| Phillips|henry.phillips@ao...|    555-0050|4747 Birch St|Indianapolis|   IN|   46202|
+-----------+----------+---------+--------------------+------------+-------------+------------+-----+--------+




