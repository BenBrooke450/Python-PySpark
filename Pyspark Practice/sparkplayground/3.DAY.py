

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, round,max, row_number, desc, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

for x in df.columns:
  print(x)


"""
  sale_id
  customer_id
  product_id
  sale_date
  quantity
  total_amount
"""



















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, count
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.groupBy("customer_id").agg(count(col("customer_id"))).orderBy(col("count(customer_id)").desc()).show(10)
+-----------+------------------+
|customer_id|count(customer_id)|
+-----------+------------------+
|         18|                 2|
|          1|                 2|
|          4|                 2|
|          2|                 2|
|          7|                 1|
|         15|                 1|
|         11|                 1|
|         29|                 1|
|         42|                 1|
|          3|                 1|
+-----------+------------------+
only showing top 10 rows























from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, count, round
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df = df.withColumn("total_paid",round(col("quantity")*col("total_amount")))

df.groupBy("customer_id").agg(sum(col("total_paid"))).orderBy(col("sum(total_paid)").desc()).show()

+-----------+---------------+
|customer_id|sum(total_paid)|
+-----------+---------------+
|          7|          555.0|
|         24|          500.0|
|          4|          496.0|
|         22|          480.0|
|         45|          440.0|
|         15|          368.0|
|         34|          368.0|
|         39|          360.0|
|         27|          320.0|
|          9|          320.0|
|          2|          295.0|
|         20|          270.0|
|         30|          216.0|
|         12|          202.0|
|         41|          202.0|
|          5|          200.0|
|         26|          200.0|
|         35|          180.0|
|         16|          180.0|
|         44|          167.0|
+-----------+---------------+
only showing top 20 rows


























from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, count, round, concat
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df = df.withColumn("Name",concat(col("first_name"),lit("  "),col("last_name")))

df = df.drop(col("first_name"),col("last_name"))

df.show()
+-----------+--------------------+------------+--------------+------------+-----+--------+-----------------+
|customer_id|               email|phone_number|       address|        city|state|zip_code|             Name|
+-----------+--------------------+------------+--------------+------------+-----+--------+-----------------+
|          1|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|      John  Smith|
|          2|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459|      Emma  Jones|
|          3|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601|    Olivia  Brown|
|          4|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501|    Liam  Johnson|
|          5|noah.williams@yah...|    555-0005|  202 Birch St|    Lakeside|   TX|   75001|   Noah  Williams|
|          6|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|    Alice  Miller|
|          7|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701|  Isabella  Davis|
|          8|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|  James  Martinez|
|          9|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201|   Sophia  Garcia|
|         10|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201| Lucas  Rodriguez|
|         11|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101|       Mia  Lopez|
|         12|william.anderson@...|    555-0012|   909 Pine St|   Nashville|   TN|   37201|William  Anderson|
|         13|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201|   Amelia  Thomas|
|         14|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401|    Ethan  Taylor|
|         15|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101|  Harper  Jackson|
|         16|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301|   Jackson  White|
|         17|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101|Charlotte  Harris|
|         18|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201|   Oliver  Martin|
|         19|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201|Madison  Thompson|
|         20|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|   Elijah  Garcia|
+-----------+--------------------+------------+--------------+------------+-----+--------+-----------------+
only showing top 20 rows














