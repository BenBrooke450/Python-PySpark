





from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, concat, collect_list, concat_ws, max,lit, dense_rank, rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

df.show()
"""
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
+-------+-----------+----------+----------+--------+------------+
only showing top 20 rows
"""






print(df.tail(5))
#[Row(sale_id='46', customer_id='42', product_id='6', sale_date='2024-03-01', quantity='1', total_amount='22.50'), Row(sale_id='47', customer_id='43', product_id='7', sale_date='2024-03-02', quantity='2', total_amount='40.00'), Row(sale_id='48', customer_id='44', product_id='8', sale_date='2024-03-03', quantity='3', total_amount='55.50'), Row(sale_id='49', customer_id='45', product_id='9', sale_date='2024-03-04', quantity='4', total_amount='109.96'), Row(sale_id='50', customer_id='46', product_id='10', sale_date='2024-03-05', quantity='1', total_amount='12.99')]











T = df.tail(5)

df = spark.createDataFrame(T)

df.show()
"""
+-------+-----------+----------+----------+--------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-------+-----------+----------+----------+--------+------------+
|     46|         42|         6|2024-03-01|       1|       22.50|
|     47|         43|         7|2024-03-02|       2|       40.00|
|     48|         44|         8|2024-03-03|       3|       55.50|
|     49|         45|         9|2024-03-04|       4|      109.96|
|     50|         46|        10|2024-03-05|       1|       12.99|
+-------+-----------+----------+----------+--------+------------+
"""











df.filter((df.customer_id < 30) | (df.quantity == 1))
"""
+-------+-----------+----------+----------+--------+------------+
|sale_id|customer_id|product_id| sale_date|quantity|total_amount|
+-------+-----------+----------+----------+--------+------------+
|      2|          1|         3|2024-01-20|       1|       29.99|
|      3|          2|         2|2024-01-16|       1|       25.00|
|      7|          4|         7|2024-01-25|       1|       15.50|
|     11|          8|        11|2024-01-22|       1|       20.00|
+-------+-----------+----------+----------+--------+------------+
"""










df.filter(col("city").like("Oak%")).show()
"""
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|     address|   city|state|zip_code|
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|303 Cedar St|Oakland|   CA|   94601|
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
"""













df.filter(col("city").contains("Oak")).show()
"""
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|     address|   city|state|zip_code|
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|303 Cedar St|Oakland|   CA|   94601|
+-----------+----------+---------+--------------------+------------+------------+-------+-----+--------+
"""











df.filter(col("state").contains("T")).show()
"""
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|       address|       city|state|zip_code|
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+
|          5|      Noah| Williams|noah.williams@yah...|    555-0005|  202 Birch St|   Lakeside|   TX|   75001|
|         12|   William| Anderson|william.anderson@...|    555-0012|   909 Pine St|  Nashville|   TN|   37201|
|         27|     Sofia|    Young|sofia.young@webma...|    555-0027|2424 Cherry St|San Antonio|   TX|   78201|
|         38|     Logan|   Carter|logan.carter@gmai...|    555-0038|   3535 Oak St|    El Paso|   TX|   79901|
|         39|      Aria|    Davis| aria.davis@live.com|    555-0039|  3636 Pine St|    Memphis|   TN|   38101|
|         42|    Elijah|    Young|elijah.young@gmai...|    555-0042| 3939 Cedar St|    Houston|   TX|   77001|
|         47|    Amelia|    James|amelia.james@live...|    555-0047|   4444 Oak St|San Antonio|   TX|   78202|
|         48|     James|   Walker|james.walker@gmai...|    555-0048|  4545 Pine St|     Dallas|   TX|   75201|
+-----------+----------+---------+--------------------+------------+--------------+-----------+-----+--------+
"""











df.filter(~col("state").isin("TX","TN")).show()
"""
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
|customer_id|first_name|last_name|               email|phone_number|       address|        city|state|zip_code|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|
|          2|      Emma|    Jones|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459|
|          3|    Olivia|    Brown|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601|
|          4|      Liam|  Johnson|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501|
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|
|          7|  Isabella|    Davis|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701|
|          8|     James| Martinez|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|
|          9|    Sophia|   Garcia|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201|
|         10|     Lucas|Rodriguez|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201|
|         11|       Mia|    Lopez|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101|
|         13|    Amelia|   Thomas|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201|
|         14|     Ethan|   Taylor|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401|
|         15|    Harper|  Jackson|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101|
|         16|   Jackson|    White|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301|
|         17| Charlotte|   Harris|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101|
|         18|    Oliver|   Martin|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201|
|         19|   Madison| Thompson|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201|
|         20|    Elijah|   Garcia|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|
|         21|  Scarlett|   Wilson|scarlett.wilson@l...|    555-0021|  1818 Pine St|Jacksonville|   FL|   32201|
|         22|     Henry|    Moore|henry.moore@yahoo...|    555-0022|   1919 Elm St|      Boston|   MA|   02101|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+
only showing top 20 rows
"""











for col in df.columns:
    if "name" in col:
        df = df.drop(col)

df.show()
"""
+-----------+--------------------+------------+--------------+------------+-----+--------+
|customer_id|               email|phone_number|       address|        city|state|zip_code|
+-----------+--------------------+------------+--------------+------------+-----+--------+
|          1|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|
|          2|emma.jones@webmai...|    555-0002|    456 Oak St| Centerville|   OH|   45459|
|          3|olivia.brown@outl...|    555-0003|   789 Pine St|  Greenville|   SC|   29601|
|          4|liam.johnson@gmai...|    555-0004|  101 Maple St|   Riverside|   CA|   92501|
|          5|noah.williams@yah...|    555-0005|  202 Birch St|    Lakeside|   TX|   75001|
|          6|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|
|          7|isabella.davis@ic...|    555-0007| 404 Spruce St|       Boise|   ID|   83701|
|          8|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|
|          9|sophia.garcia@zoh...|    555-0009| 606 Cherry St|      Albany|   NY|   12201|
|         10|lucas.rodriguez@h...|    555-0010|  707 Maple St|    Portland|   OR|   97201|
|         11|  mia.lopez@mail.com|    555-0011|    808 Oak St|       Miami|   FL|   33101|
|         12|william.anderson@...|    555-0012|   909 Pine St|   Nashville|   TN|   37201|
|         13|amelia.thomas@pro...|    555-0013|   1010 Elm St|      Denver|   CO|   80201|
|         14|ethan.taylor@inbo...|    555-0014| 1111 Birch St| Minneapolis|   MN|   55401|
|         15|harper.jackson@ou...|    555-0015| 1212 Cedar St|     Seattle|   WA|   98101|
|         16|jackson.white@yma...|    555-0016|1313 Spruce St|     Atlanta|   GA|   30301|
|         17|charlotte.harris@...|    555-0017|1414 Walnut St|   San Diego|   CA|   92101|
|         18|oliver.martin@icl...|    555-0018|1515 Cherry St|Indianapolis|   IN|   46201|
|         19|madison.thompson@...|    555-0019| 1616 Maple St|   Charlotte|   NC|   28201|
|         20|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|
+-----------+--------------------+------------+--------------+------------+-----+--------+
only showing top 20 rows
"""







for col in df.columns:
  x = df.select(col).collect()
  print(x[0])
"""
Row(customer_id='1')
Row(first_name='John')
Row(last_name='Smith')
Row(email='john.smith@domain.com')
Row(phone_number='555-0001')
Row(address='123 Elm St')
Row(city='Springfield')
Row(state='IL')
Row(zip_code='62701')
"""








for col in df.columns:
  x = df.select(col).collect()
  for y in x:
    print(y)
"""
Row(customer_id='1')
Row(customer_id='2')
Row(customer_id='3')
Row(customer_id='4')
Row(customer_id='5')
Row(customer_id='6')
Row(customer_id='7')
Row(customer_id='8')
Row(customer_id='9')
Row(customer_id='10')
Row(customer_id='11')
Row(customer_id='12')
Row(customer_id='13')
Row(customer_id='14')
Row(customer_id='15')
Row(customer_id='16')
Row(customer_id='17')
Row(customer_id='18')
Row(customer_id='19')
Row(customer_id='20')
Row(customer_id='21')
Row(customer_id='22')
Row(customer_id='23')
Row(customer_id='24')
Row(customer_id='25')
Row(customer_id='26')
Row(customer_id='27')
Row(customer_id='28')
Row(customer_id='29')
Row(customer_id='30')
Row(customer_id='31')
Row(customer_id='32')
Row(customer_id='33')
Row(customer_id='34')
Row(customer_id='35')
Row(customer_id='36')
Row(customer_id='37')
Row(customer_id='38')
Row(customer_id='39')
Row(customer_id='40')
Row(customer_id='41')
Row(customer_id='42')
Row(customer_id='43')
Row(customer_id='44')
Row(customer_id='45')
Row(customer_id='46')
Row(customer_id='47')
Row(customer_id='48')
Row(customer_id='49')
Row(customer_id='50')
Row(first_name='John')
Row(first_name='Emma')
Row(first_name='Olivia')
Row(first_name='Liam')
Row(first_name='Noah')
Row(first_name='Alice')
...
...
...
"""













q = 0
for col in df.columns:
  x = df.select(col).collect()
  for y in x:
    print(y)
    q = q + 1
    if q == 5:
      q = 0
      break
"""
Row(customer_id='1')
Row(customer_id='2')
Row(customer_id='3')
Row(customer_id='4')
Row(customer_id='5')
Row(first_name='John')
Row(first_name='Emma')
Row(first_name='Olivia')
Row(first_name='Liam')
Row(first_name='Noah')
Row(last_name='Smith')
Row(last_name='Jones')
Row(last_name='Brown')
Row(last_name='Johnson')
Row(last_name='Williams')
Row(email='john.smith@domain.com')
Row(email='emma.jones@webmail.com')
Row(email='olivia.brown@outlook.com')
Row(email='liam.johnson@gmail.com')
Row(email='noah.williams@yahoo.com')
Row(phone_number='555-0001')
Row(phone_number='555-0002')
Row(phone_number='555-0003')
Row(phone_number='555-0004')
Row(phone_number='555-0005')
Row(address='123 Elm St')
Row(address='456 Oak St')
Row(address='789 Pine St')
Row(address='101 Maple St')
Row(address='202 Birch St')
Row(city='Springfield')
Row(city='Centerville')
Row(city='Greenville')
Row(city='Riverside')
Row(city='Lakeside')
Row(state='IL')
Row(state='OH')
Row(state='SC')
Row(state='CA')
Row(state='TX')
Row(zip_code='62701')
Row(zip_code='45459')
Row(zip_code='29601')
Row(zip_code='92501')
Row(zip_code='75001')
"""











print(df.count())
#50








