

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df_customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df_sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

#No columns mentioned to join on or what type of join
df_customers.join(df_sales).show()
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
|customer_id|first_name|last_name|               email|phone_number|   address|       city|state|zip_code|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      1|          1|         1|2024-01-15|       2|       39.98|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      2|          1|         3|2024-01-20|       1|       29.99|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      3|          2|         2|2024-01-16|       1|       25.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      4|          2|         4|2024-01-22|       3|       89.97|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      5|          3|         5|2024-01-17|       2|       49.98|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      6|          4|         6|2024-01-18|       4|      119.96|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      7|          4|         7|2024-01-25|       1|       15.50|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      8|          5|         8|2024-01-19|       3|       66.75|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      9|          6|         9|2024-01-20|       2|       40.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     10|          7|        10|2024-01-21|       5|      110.95|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     11|          8|        11|2024-01-22|       1|       20.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     12|          9|        12|2024-01-23|       4|       79.96|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     13|         10|        13|2024-01-24|       2|       55.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     14|         11|        14|2024-01-25|       1|       25.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     15|         12|        15|2024-01-26|       3|       67.47|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     16|         13|        16|2024-01-27|       2|       34.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     17|         14|        17|2024-01-28|       1|       15.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     18|         15|        18|2024-01-29|       4|       92.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     19|         16|        19|2024-01-30|       3|       60.00|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|     20|         17|        20|2024-01-31|       2|       40.00|
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
only showing top 20 rows

















# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df_customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df_sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df_customers.join(df_sales,df_customers.customer_id == df_sales.customer_id).show()
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
|customer_id|first_name|last_name|               email|phone_number|       address|       city|state|zip_code|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St|Springfield|   IL|   62701|      2|          1|         3|2024-01-20|       1|       29.99|
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St|Springfield|   IL|   62701|      1|          1|         1|2024-01-15|       2|       39.98|
|          2|      Emma|    Jones|emma.jones@webmai...|    555-0002|    456 Oak St|Centerville|   OH|   45459|      4|          2|         4|2024-01-22|       3|       89.97|
|          2|      Emma|    Jones|emma.jones@webmai...|    555-0002|    456 Oak St|Centerville|   OH|   45459|      3|          2|         2|2024-01-16|       1|       25.00|
|          3|    Olivia|    Brown|olivia.brown@outl...|    555-0003|   789 Pine St| Greenville|   SC|   29601|      5|          3|         5|2024-01-17|       2|       49.98|
|          4|      Liam|  Johnson|liam.johnson@gmai...|    555-0004|  101 Maple St|  Riverside|   CA|   92501|      7|          4|         7|2024-01-25|       1|       15.50|
|          4|      Liam|  Johnson|liam.johnson@gmai...|    555-0004|  101 Maple St|  Riverside|   CA|   92501|      6|          4|         6|2024-01-18|       4|      119.96|
|          5|      Noah| Williams|noah.williams@yah...|    555-0005|  202 Birch St|   Lakeside|   TX|   75001|      8|          5|         8|2024-01-19|       3|       66.75|
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|  303 Cedar St|    Oakland|   CA|   94601|      9|          6|         9|2024-01-20|       2|       40.00|
|          7|  Isabella|    Davis|isabella.davis@ic...|    555-0007| 404 Spruce St|      Boise|   ID|   83701|     10|          7|        10|2024-01-21|       5|      110.95|
|          8|     James| Martinez|james.martinez@li...|    555-0008| 505 Walnut St| Des Moines|   IA|   50301|     11|          8|        11|2024-01-22|       1|       20.00|
|          9|    Sophia|   Garcia|sophia.garcia@zoh...|    555-0009| 606 Cherry St|     Albany|   NY|   12201|     12|          9|        12|2024-01-23|       4|       79.96|
|         10|     Lucas|Rodriguez|lucas.rodriguez@h...|    555-0010|  707 Maple St|   Portland|   OR|   97201|     13|         10|        13|2024-01-24|       2|       55.00|
|         11|       Mia|    Lopez|  mia.lopez@mail.com|    555-0011|    808 Oak St|      Miami|   FL|   33101|     14|         11|        14|2024-01-25|       1|       25.00|
|         12|   William| Anderson|william.anderson@...|    555-0012|   909 Pine St|  Nashville|   TN|   37201|     15|         12|        15|2024-01-26|       3|       67.47|
|         13|    Amelia|   Thomas|amelia.thomas@pro...|    555-0013|   1010 Elm St|     Denver|   CO|   80201|     16|         13|        16|2024-01-27|       2|       34.00|
|         14|     Ethan|   Taylor|ethan.taylor@inbo...|    555-0014| 1111 Birch St|Minneapolis|   MN|   55401|     17|         14|        17|2024-01-28|       1|       15.00|
|         15|    Harper|  Jackson|harper.jackson@ou...|    555-0015| 1212 Cedar St|    Seattle|   WA|   98101|     18|         15|        18|2024-01-29|       4|       92.00|
|         16|   Jackson|    White|jackson.white@yma...|    555-0016|1313 Spruce St|    Atlanta|   GA|   30301|     19|         16|        19|2024-01-30|       3|       60.00|
|         17| Charlotte|   Harris|charlotte.harris@...|    555-0017|1414 Walnut St|  San Diego|   CA|   92101|     20|         17|        20|2024-01-31|       2|       40.00|
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+-------+-----------+----------+----------+--------+------------+
only showing top 20 rows















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank, sum, concat, collect_list, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df_customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
df_sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
df = df_customers.join(df_sales,df_customers.customer_id == df_sales.customer_id).drop(df_sales.customer_id)

df = df.withColumn("Who_bought_the_most", sum(col("total_amount")).over(Window.partitionBy("customer_id").orderBy("customer_id")))

df = df.withColumn("sale_date",concat_ws(",",collect_list("sale_date")
                                         .over(Window.partitionBy("customer_id").orderBy("customer_id"))))

df = df.select("customer_id","first_name","last_name","address",
               "email","city","state","sale_date","Who_bought_the_most","quantity","total_amount").show()

+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+-------------------+--------+------------+
|customer_id|first_name|last_name|       address|               email|        city|state|           sale_date|Who_bought_the_most|quantity|total_amount|
+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+-------------------+--------+------------+
|          1|      John|    Smith|    123 Elm St|john.smith@domain...| Springfield|   IL|2024-01-20,2024-0...|              69.97|       1|       29.99|
|          1|      John|    Smith|    123 Elm St|john.smith@domain...| Springfield|   IL|2024-01-20,2024-0...|              69.97|       2|       39.98|
|         10|     Lucas|Rodriguez|  707 Maple St|lucas.rodriguez@h...|    Portland|   OR|          2024-01-24|               55.0|       2|       55.00|
|         11|       Mia|    Lopez|    808 Oak St|  mia.lopez@mail.com|       Miami|   FL|          2024-01-25|               25.0|       1|       25.00|
|         12|   William| Anderson|   909 Pine St|william.anderson@...|   Nashville|   TN|          2024-01-26|              67.47|       3|       67.47|
|         13|    Amelia|   Thomas|   1010 Elm St|amelia.thomas@pro...|      Denver|   CO|          2024-01-27|               34.0|       2|       34.00|
|         14|     Ethan|   Taylor| 1111 Birch St|ethan.taylor@inbo...| Minneapolis|   MN|          2024-01-28|               15.0|       1|       15.00|
|         15|    Harper|  Jackson| 1212 Cedar St|harper.jackson@ou...|     Seattle|   WA|          2024-01-29|               92.0|       4|       92.00|
|         16|   Jackson|    White|1313 Spruce St|jackson.white@yma...|     Atlanta|   GA|          2024-01-30|               60.0|       3|       60.00|
|         17| Charlotte|   Harris|1414 Walnut St|charlotte.harris@...|   San Diego|   CA|          2024-01-31|               40.0|       2|       40.00|
|         18|    Oliver|   Martin|1515 Cherry St|oliver.martin@icl...|Indianapolis|   IN|2024-02-05,2024-0...|              59.97|       2|       39.98|
|         18|    Oliver|   Martin|1515 Cherry St|oliver.martin@icl...|Indianapolis|   IN|2024-02-05,2024-0...|              59.97|       1|       19.99|
|         19|   Madison| Thompson| 1616 Maple St|madison.thompson@...|   Charlotte|   NC|          2024-02-06|              29.99|       1|       29.99|
|          2|      Emma|    Jones|    456 Oak St|emma.jones@webmai...| Centerville|   OH|2024-01-22,2024-0...|             114.97|       3|       89.97|
|          2|      Emma|    Jones|    456 Oak St|emma.jones@webmai...| Centerville|   OH|2024-01-22,2024-0...|             114.97|       1|       25.00|
|         20|    Elijah|   Garcia|   1717 Oak St|elijah.garcia@zoh...|     Detroit|   MI|          2024-02-07|              89.97|       3|       89.97|
|         21|  Scarlett|   Wilson|  1818 Pine St|scarlett.wilson@l...|Jacksonville|   FL|          2024-02-08|              49.98|       2|       49.98|
|         22|     Henry|    Moore|   1919 Elm St|henry.moore@yahoo...|      Boston|   MA|          2024-02-09|             119.96|       4|      119.96|
|         23|    Evelyn|      Lee| 2020 Birch St|  evelyn.lee@aol.com|  Pittsburgh|   PA|          2024-02-10|               15.5|       1|       15.50|
|         24|    Daniel|   Walker| 2121 Cedar St|daniel.walker@fas...|   St. Louis|   MO|          2024-02-11|              99.95|       5|       99.95|
+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+-------------------+--------+------------+
only showing top 20 rows































from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper, rank, sum, concat, collect_list, concat_ws, max
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df_customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
df_sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
df = df_customers.join(df_sales,df_customers.customer_id == df_sales.customer_id).drop(df_sales.customer_id)

df = df.withColumn("Who_bought_the_most", sum(col("total_amount")).over(Window.partitionBy("customer_id").orderBy("customer_id")))

df = df.withColumn("sale_date",concat_ws(",",collect_list("sale_date")
                                         .over(Window.partitionBy("customer_id").orderBy("customer_id"))))

df = df.select("customer_id","first_name","last_name","address","email","city","state","sale_date","Who_bought_the_most","quantity","total_amount")

df.groupBy("customer_id","first_name","last_name","address","email","city","state","sale_date").agg(max(col("Who_bought_the_most"))).show()
+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+------------------------+
|customer_id|first_name|last_name|       address|               email|        city|state|           sale_date|max(Who_bought_the_most)|
+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+------------------------+
|          1|      John|    Smith|    123 Elm St|john.smith@domain...| Springfield|   IL|2024-01-20,2024-0...|                   69.97|
|         10|     Lucas|Rodriguez|  707 Maple St|lucas.rodriguez@h...|    Portland|   OR|          2024-01-24|                    55.0|
|         11|       Mia|    Lopez|    808 Oak St|  mia.lopez@mail.com|       Miami|   FL|          2024-01-25|                    25.0|
|         12|   William| Anderson|   909 Pine St|william.anderson@...|   Nashville|   TN|          2024-01-26|                   67.47|
|         13|    Amelia|   Thomas|   1010 Elm St|amelia.thomas@pro...|      Denver|   CO|          2024-01-27|                    34.0|
|         14|     Ethan|   Taylor| 1111 Birch St|ethan.taylor@inbo...| Minneapolis|   MN|          2024-01-28|                    15.0|
|         15|    Harper|  Jackson| 1212 Cedar St|harper.jackson@ou...|     Seattle|   WA|          2024-01-29|                    92.0|
|         16|   Jackson|    White|1313 Spruce St|jackson.white@yma...|     Atlanta|   GA|          2024-01-30|                    60.0|
|         17| Charlotte|   Harris|1414 Walnut St|charlotte.harris@...|   San Diego|   CA|          2024-01-31|                    40.0|
|         18|    Oliver|   Martin|1515 Cherry St|oliver.martin@icl...|Indianapolis|   IN|2024-02-05,2024-0...|                   59.97|
|         19|   Madison| Thompson| 1616 Maple St|madison.thompson@...|   Charlotte|   NC|          2024-02-06|                   29.99|
|          2|      Emma|    Jones|    456 Oak St|emma.jones@webmai...| Centerville|   OH|2024-01-22,2024-0...|                  114.97|
|         20|    Elijah|   Garcia|   1717 Oak St|elijah.garcia@zoh...|     Detroit|   MI|          2024-02-07|                   89.97|
|         21|  Scarlett|   Wilson|  1818 Pine St|scarlett.wilson@l...|Jacksonville|   FL|          2024-02-08|                   49.98|
|         22|     Henry|    Moore|   1919 Elm St|henry.moore@yahoo...|      Boston|   MA|          2024-02-09|                  119.96|
|         23|    Evelyn|      Lee| 2020 Birch St|  evelyn.lee@aol.com|  Pittsburgh|   PA|          2024-02-10|                    15.5|
|         24|    Daniel|   Walker| 2121 Cedar St|daniel.walker@fas...|   St. Louis|   MO|          2024-02-11|                   99.95|
|         25|    Harper|     Hall|2222 Spruce St|harper.hall@inbox...|    Honolulu|   HI|          2024-02-12|                    40.0|
|         26|   Matthew|    Allen|2323 Walnut St|matthew.allen@dom...|   Milwaukee|   WI|          2024-02-13|                   66.75|
|         27|     Sofia|    Young|2424 Cherry St|sofia.young@webma...| San Antonio|   TX|          2024-02-14|                   79.96|
+-----------+----------+---------+--------------+--------------------+------------+-----+--------------------+------------------------+
only showing top 20 rows
















