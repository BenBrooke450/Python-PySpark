

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, count, round, concat
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df.withColumn("email", when(col("state") == "TX",None).otherwise(col("email"))).show()
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|       address|        city|state|zip_code|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|
|          2|      Emma|    Jones|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459|
|          3|    Olivia|    Brown|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601|
|          4|      Liam|  Johnson|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501|
|          5|      Noah| Williams|                NULL|    555-0005|  202 Birch St|    Lakeside|   TX|   75001|
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|
|          7|  Isabella|    Davis|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701|
|          8|     James| Martinez|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|
|          9|    Sophia|   Garcia|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201|
|         10|     Lucas|Rodriguez|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201|
|         11|       Mia|    Lopez|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101|
|         12|   William| Anderson|william.anderson@...|    555-0012|   909 Pine St|   Nashville|   TN|   37201|
|         13|    Amelia|   Thomas|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201|
|         14|     Ethan|   Taylor|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401|
|         15|    Harper|  Jackson|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101|
|         16|   Jackson|    White|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301|
|         17| Charlotte|   Harris|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101|
|         18|    Oliver|   Martin|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201|
|         19|   Madison| Thompson|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201|
|         20|    Elijah|   Garcia|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
only showing top 20 rows




















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg, count, round, concat
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df = df.withColumn("email", when(col("state") == "TX",None).otherwise(col("email")))

count_of_nulls = df.filter(col("email").isNull()).count()

print(count_of_nulls)
#6




count_of_nulls.show()
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+
|customer_id|first_name|last_name|email|phone_number|       address|       city|state|zip_code|
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+
|          5|      Noah| Williams| NULL|    555-0005|  202 Birch St|   Lakeside|   TX|   75001|
|         27|     Sofia|    Young| NULL|    555-0027|2424 Cherry St|San Antonio|   TX|   78201|
|         38|     Logan|   Carter| NULL|    555-0038|   3535 Oak St|    El Paso|   TX|   79901|
|         42|    Elijah|    Young| NULL|    555-0042| 3939 Cedar St|    Houston|   TX|   77001|
|         47|    Amelia|    James| NULL|    555-0047|   4444 Oak St|San Antonio|   TX|   78202|
|         48|     James|   Walker| NULL|    555-0048|  4545 Pine St|     Dallas|   TX|   75201|
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+





count_of_nulls.withColumn("Rank", row_number().over(Window.orderBy(monotonically_increasing_id()))).show()
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+----+
|customer_id|first_name|last_name|email|phone_number|       address|       city|state|zip_code|Rank|
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+----+
|          5|      Noah| Williams| NULL|    555-0005|  202 Birch St|   Lakeside|   TX|   75001|   1|
|         27|     Sofia|    Young| NULL|    555-0027|2424 Cherry St|San Antonio|   TX|   78201|   2|
|         38|     Logan|   Carter| NULL|    555-0038|   3535 Oak St|    El Paso|   TX|   79901|   3|
|         42|    Elijah|    Young| NULL|    555-0042| 3939 Cedar St|    Houston|   TX|   77001|   4|
|         47|    Amelia|    James| NULL|    555-0047|   4444 Oak St|San Antonio|   TX|   78202|   5|
|         48|     James|   Walker| NULL|    555-0048|  4545 Pine St|     Dallas|   TX|   75201|   6|
+-----------+----------+---------+-----+------------+--------------+-----------+-----+--------+----+


















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

df = df.withColumn("State_rank", row_number().over(Window.partitionBy("state").orderBy("state")))

df.select("customer_id","first_name","last_name","city","state","State_rank").show()
+-----------+----------+---------+-------------+-----+----------+
|customer_id|first_name|last_name|         city|state|State_rank|
+-----------+----------+---------+-------------+-----+----------+
|          4|      Liam|  Johnson|    Riverside|   CA|         1|
|          6|     Alice|   Miller|      Oakland|   CA|         2|
|         17| Charlotte|   Harris|    San Diego|   CA|         3|
|         31|     Chloe|    Adams|     San Jose|   CA|         4|
|         32|     James|    Green|San Francisco|   CA|         5|
|         44|    Daniel|   Morris|    San Diego|   CA|         6|
|         13|    Amelia|   Thomas|       Denver|   CO|         1|
|         40|  Benjamin|    Evans|       Denver|   CO|         2|
|         34|     Lucas| Mitchell|   Washington|   DC|         1|
|         11|       Mia|    Lopez|        Miami|   FL|         1|
|         21|  Scarlett|   Wilson| Jacksonville|   FL|         2|
|         29|      Nora|   Wright|      Orlando|   FL|         3|
|         16|   Jackson|    White|      Atlanta|   GA|         1|
|         25|    Harper|     Hall|     Honolulu|   HI|         1|
|          8|     James| Martinez|   Des Moines|   IA|         1|
|          7|  Isabella|    Davis|        Boise|   ID|         1|
|          1|      John|    Smith|  Springfield|   IL|         1|
|         43|      Lily|   Turner|      Chicago|   IL|         2|
|         18|    Oliver|   Martin| Indianapolis|   IN|         1|
|         50|     Henry| Phillips| Indianapolis|   IN|         2|
+-----------+----------+---------+-------------+-----+----------+
only showing top 20 rows
















