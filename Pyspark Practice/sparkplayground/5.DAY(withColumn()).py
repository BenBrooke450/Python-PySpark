
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')


if "full_name" not in df.columns:
    df = df.withColumn("full_name" ,lit("TEMPLATE"))

df.select([x for x in df.columns if x != "first_name" and x != "last_name" ]).show()
+-----------+--------------------+------------+--------------+------------+-----+--------+---------+
|customer_id|               email|phone_number|       address|        city|state|zip_code|full_name|
+-----------+--------------------+------------+--------------+------------+-----+--------+---------+
|          1|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701| TEMPLATE|
|          2|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459| TEMPLATE|
|          3|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601| TEMPLATE|
|          4|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501| TEMPLATE|
|          5|noah.williams@yah...|    555-0005|  202 Birch St|    Lakeside|   TX|   75001| TEMPLATE|
|          6|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601| TEMPLATE|
|          7|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701| TEMPLATE|
|          8|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301| TEMPLATE|
|          9|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201| TEMPLATE|
|         10|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201| TEMPLATE|
|         11|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101| TEMPLATE|
|         12|william.anderson@...|    555-0012|   909 Pine St|   Nashville|   TN|   37201| TEMPLATE|
|         13|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201| TEMPLATE|
|         14|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401| TEMPLATE|
|         15|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101| TEMPLATE|
|         16|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301| TEMPLATE|
|         17|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101| TEMPLATE|
|         18|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201| TEMPLATE|
|         19|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201| TEMPLATE|
|         20|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201| TEMPLATE|
+-----------+--------------------+------------+--------------+------------+-----+--------+---------+
only showing top 20 rows





















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

for x in df.select("city").collect():
  print(x[0])

"""
Springfield
Centerville
Greenville
Riverside
Lakeside
Oakland
Boise
Des Moines
Albany
Portland
Miami
Nashville
Denver
Minneapolis
Seattle
Atlanta
San Diego
Indianapolis
Charlotte
Detroit
Jacksonville
Boston
Pittsburgh
St. Louis
Honolulu
Milwaukee
San Antonio
Las Vegas
Orlando
Columbus
San Jose
San Francisco
Jackson
Washington
Philadelphia
Baltimore
Charlotte
El Paso
Memphis
Denver
Las Vegas
Houston
Chicago
San Diego
New York
Seattle
San Antonio
Dallas
Detroit
Indianapolis
"""















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

caps_name = df.withColumn("upper_Names",concat(upper(col("first_name")),lit(" "),upper(col("last_name")))).select("upper_Names")

df = df.crossJoin(caps_name.select("upper_Names")).show()
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+----------------+
|customer_id|first_name|last_name|               email|phone_number|   address|       city|state|zip_code|     upper_Names|
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+----------------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      JOHN SMITH|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|      EMMA JONES|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|    OLIVIA BROWN|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|    LIAM JOHNSON|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   NOAH WILLIAMS|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|    ALICE MILLER|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|  ISABELLA DAVIS|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|  JAMES MARTINEZ|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   SOPHIA GARCIA|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701| LUCAS RODRIGUEZ|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|       MIA LOPEZ|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|WILLIAM ANDERSON|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   AMELIA THOMAS|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|    ETHAN TAYLOR|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|  HARPER JACKSON|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   JACKSON WHITE|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|CHARLOTTE HARRIS|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   OLIVER MARTIN|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|MADISON THOMPSON|
|          1|      John|    Smith|john.smith@domain...|    555-0001|123 Elm St|Springfield|   IL|   62701|   ELIJAH GARCIA|
+-----------+----------+---------+--------------------+------------+----------+-----------+-----+--------+----------------+

















from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, count, row_number, concat, upper
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

caps_name = df.withColumn("upper_Names",concat(upper(col("first_name")),lit(" "),upper(col("last_name")))).select("upper_Names","customer_id")

df = df.join(caps_name, "customer_id").show()
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+----------------+
|customer_id|first_name|last_name|               email|phone_number|       address|        city|state|zip_code|     upper_Names|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+----------------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|      JOHN SMITH|
|          2|      Emma|    Jones|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459|      EMMA JONES|
|          3|    Olivia|    Brown|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601|    OLIVIA BROWN|
|          4|      Liam|  Johnson|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501|    LIAM JOHNSON|
|          5|      Noah| Williams|noah.williams@yah...|    555-0005|  202 Birch St|    Lakeside|   TX|   75001|   NOAH WILLIAMS|
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|    ALICE MILLER|
|          7|  Isabella|    Davis|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701|  ISABELLA DAVIS|
|          8|     James| Martinez|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|  JAMES MARTINEZ|
|          9|    Sophia|   Garcia|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201|   SOPHIA GARCIA|
|         10|     Lucas|Rodriguez|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201| LUCAS RODRIGUEZ|
|         11|       Mia|    Lopez|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101|       MIA LOPEZ|
|         12|   William| Anderson|william.anderson@...|    555-0012|   909 Pine St|   Nashville|   TN|   37201|WILLIAM ANDERSON|
|         13|    Amelia|   Thomas|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201|   AMELIA THOMAS|
|         14|     Ethan|   Taylor|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401|    ETHAN TAYLOR|
|         15|    Harper|  Jackson|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101|  HARPER JACKSON|
|         16|   Jackson|    White|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301|   JACKSON WHITE|
|         17| Charlotte|   Harris|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101|CHARLOTTE HARRIS|
|         18|    Oliver|   Martin|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201|   OLIVER MARTIN|
|         19|   Madison| Thompson|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201|MADISON THOMPSON|
|         20|    Elijah|   Garcia|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|   ELIJAH GARCIA|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+----------------+
only showing top 20 rows



