


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, round, count, row_number, concat, upper, rank, sum, concat, collect_list, concat_ws, max
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = df.withColumn("customer_id",col("customer_id").cast(IntegerType())).orderBy(col("state"))

df = df.groupBy("state").agg(round(sum(col("max(Who_bought_the_most)")),2)).show()
+-----+---------------------------------------+
|state|round(sum(max(Who_bought_the_most)), 2)|
+-----+---------------------------------------+
|   CA|                                 348.46|
|   CO|                                   74.0|
|   DC|                                   92.0|
|   FL|                                 110.96|
|   GA|                                   60.0|
|   HI|                                   40.0|
|   IA|                                   20.0|
|   ID|                                 110.95|
|   IL|                                 109.97|
|   IN|                                  59.97|
|   MA|                                 119.96|
|   MD|                                  18.75|
|   MI|                                  89.97|
|   MN|                                   15.0|
|   MO|                                  99.95|
|   MS|                                   34.0|
|   NC|                                  69.97|
|   NV|                                 112.47|
|   NY|                                 189.92|
|   OH|                                 186.94|
+-----+---------------------------------------+
only showing top 20 rows










df = df.withColumn("customer_id",col("customer_id").cast(IntegerType())).orderBy(col("state"))

df = df.withColumn("state_rank",dense_rank().over(Window.partitionBy().orderBy("state"))).show(10)

+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|customer_id|first_name|last_name|       address|               email|         city|state|           sale_date|max(Who_bought_the_most)|state_rank|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|         31|     Chloe|    Adams|   2828 Elm St| chloe.adams@aol.com|     San Jose|   CA|          2024-02-18|                    50.0|         1|
|         17| Charlotte|   Harris|1414 Walnut St|charlotte.harris@...|    San Diego|   CA|          2024-01-31|                    40.0|         1|
|          6|     Alice|   Miller|  303 Cedar St|alice.miller@aol.com|      Oakland|   CA|          2024-01-20|                    40.0|         1|
|         32|     James|    Green| 2929 Birch St|james.green@mail.com|San Francisco|   CA|          2024-02-19|                    27.5|         1|
|          4|      Liam|  Johnson|  101 Maple St|liam.johnson@gmai...|    Riverside|   CA|2024-01-25,2024-0...|      135.45999999999998|         1|
|         44|    Daniel|   Morris|4141 Walnut St|daniel.morris@zoh...|    San Diego|   CA|          2024-03-03|                    55.5|         1|
|         13|    Amelia|   Thomas|   1010 Elm St|amelia.thomas@pro...|       Denver|   CO|          2024-01-27|                    34.0|         2|
|         40|  Benjamin|    Evans|   3737 Elm St|benjamin.evans@zo...|       Denver|   CO|          2024-02-27|                    40.0|         2|
|         34|     Lucas| Mitchell|3131 Spruce St|lucas.mitchell@gm...|   Washington|   DC|          2024-02-21|                    92.0|         3|
|         29|      Nora|   Wright|   2626 Oak St|nora.wright@gmail...|      Orlando|   FL|          2024-02-16|                   35.98|         4|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+













df = df.withColumn("customer_id",col("customer_id").cast(IntegerType())).orderBy(col("state"))

df = df.withColumn("state_rank",rank().over(Window.partitionBy().orderBy("state"))).show(15)
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|customer_id|first_name|last_name|       address|               email|         city|state|           sale_date|max(Who_bought_the_most)|state_rank|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|         17| Charlotte|   Harris|1414 Walnut St|charlotte.harris@...|    San Diego|   CA|          2024-01-31|                    40.0|         1|
|         31|     Chloe|    Adams|   2828 Elm St| chloe.adams@aol.com|     San Jose|   CA|          2024-02-18|                    50.0|         1|
|         32|     James|    Green| 2929 Birch St|james.green@mail.com|San Francisco|   CA|          2024-02-19|                    27.5|         1|
|          4|      Liam|  Johnson|  101 Maple St|liam.johnson@gmai...|    Riverside|   CA|2024-01-25,2024-0...|      135.45999999999998|         1|
|         44|    Daniel|   Morris|4141 Walnut St|daniel.morris@zoh...|    San Diego|   CA|          2024-03-03|                    55.5|         1|
|          6|     Alice|   Miller|  303 Cedar St|alice.miller@aol.com|      Oakland|   CA|          2024-01-20|                    40.0|         1|
|         13|    Amelia|   Thomas|   1010 Elm St|amelia.thomas@pro...|       Denver|   CO|          2024-01-27|                    34.0|         7|
|         40|  Benjamin|    Evans|   3737 Elm St|benjamin.evans@zo...|       Denver|   CO|          2024-02-27|                    40.0|         7|
|         34|     Lucas| Mitchell|3131 Spruce St|lucas.mitchell@gm...|   Washington|   DC|          2024-02-21|                    92.0|         9|
|         11|       Mia|    Lopez|    808 Oak St|  mia.lopez@mail.com|        Miami|   FL|          2024-01-25|                    25.0|        10|
|         21|  Scarlett|   Wilson|  1818 Pine St|scarlett.wilson@l...| Jacksonville|   FL|          2024-02-08|                   49.98|        10|
|         29|      Nora|   Wright|   2626 Oak St|nora.wright@gmail...|      Orlando|   FL|          2024-02-16|                   35.98|        10|
|         16|   Jackson|    White|1313 Spruce St|jackson.white@yma...|      Atlanta|   GA|          2024-01-30|                    60.0|        13|
|         25|    Harper|     Hall|2222 Spruce St|harper.hall@inbox...|     Honolulu|   HI|          2024-02-12|                    40.0|        14|
|          8|     James| Martinez| 505 Walnut St|james.martinez@li...|   Des Moines|   IA|          2024-01-22|                    20.0|        15|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+













df = df.withColumn("customer_id",col("customer_id").cast(IntegerType())).orderBy(col("state"))

df = df.withColumn("state_rank",row_number().over(Window.partitionBy().orderBy("state"))).show(15)
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|customer_id|first_name|last_name|       address|               email|         city|state|           sale_date|max(Who_bought_the_most)|state_rank|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
|         17| Charlotte|   Harris|1414 Walnut St|charlotte.harris@...|    San Diego|   CA|          2024-01-31|                    40.0|         1|
|         31|     Chloe|    Adams|   2828 Elm St| chloe.adams@aol.com|     San Jose|   CA|          2024-02-18|                    50.0|         2|
|         32|     James|    Green| 2929 Birch St|james.green@mail.com|San Francisco|   CA|          2024-02-19|                    27.5|         3|
|          4|      Liam|  Johnson|  101 Maple St|liam.johnson@gmai...|    Riverside|   CA|2024-01-25,2024-0...|      135.45999999999998|         4|
|         44|    Daniel|   Morris|4141 Walnut St|daniel.morris@zoh...|    San Diego|   CA|          2024-03-03|                    55.5|         5|
|          6|     Alice|   Miller|  303 Cedar St|alice.miller@aol.com|      Oakland|   CA|          2024-01-20|                    40.0|         6|
|         13|    Amelia|   Thomas|   1010 Elm St|amelia.thomas@pro...|       Denver|   CO|          2024-01-27|                    34.0|         7|
|         40|  Benjamin|    Evans|   3737 Elm St|benjamin.evans@zo...|       Denver|   CO|          2024-02-27|                    40.0|         8|
|         34|     Lucas| Mitchell|3131 Spruce St|lucas.mitchell@gm...|   Washington|   DC|          2024-02-21|                    92.0|         9|
|         11|       Mia|    Lopez|    808 Oak St|  mia.lopez@mail.com|        Miami|   FL|          2024-01-25|                    25.0|        10|
|         21|  Scarlett|   Wilson|  1818 Pine St|scarlett.wilson@l...| Jacksonville|   FL|          2024-02-08|                   49.98|        11|
|         29|      Nora|   Wright|   2626 Oak St|nora.wright@gmail...|      Orlando|   FL|          2024-02-16|                   35.98|        12|
|         16|   Jackson|    White|1313 Spruce St|jackson.white@yma...|      Atlanta|   GA|          2024-01-30|                    60.0|        13|
|         25|    Harper|     Hall|2222 Spruce St|harper.hall@inbox...|     Honolulu|   HI|          2024-02-12|                    40.0|        14|
|          8|     James| Martinez| 505 Walnut St|james.martinez@li...|   Des Moines|   IA|          2024-01-22|                    20.0|        15|
+-----------+----------+---------+--------------+--------------------+-------------+-----+--------------------+------------------------+----------+
only showing top 15 rows





