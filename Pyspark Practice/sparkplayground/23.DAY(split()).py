
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, coalesce, concat_ws, concat, isnull, ifnull, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()



# Create data with some null values
data = [
    Row(A='apple-green', B='cat-meow', C='red-blue', D=None, E='high'),
    Row(A='banana-pie', B=None, C='blue', D='circle-square', E='medium'),
    Row(A=None, B='dog', C=None, D='triangle', E='low'),
    Row(A='cherry-bloom', B='elephant', C='green', D='square', E='high'),
    Row(A='date-girl', B=None, C='yellow-red', D=None, E=None),
    Row(A=None, B='fox', C='purple', D='hexagon', E='low'),
    Row(A='fig-gif', B='goat-cheese', C=None, D='octagon', E=None),
    Row(A='grape-green', B='horse', C='orange', D='rectangle', E='medium'),
    Row(A='honeydew-bear', B='iguana', C='pink', D=None, E='high'),
    Row(A=None, B='jaguar-cat', C='black-white', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df.show()
"""
+-------------+-----------+-----------+-------------+------+
|            A|          B|          C|            D|     E|
+-------------+-----------+-----------+-------------+------+
|  apple-green|   cat-meow|   red-blue|         NULL|  high|
|   banana-pie|       NULL|       blue|circle-square|medium|
|         NULL|        dog|       NULL|     triangle|   low|
| cherry-bloom|   elephant|      green|       square|  high|
|    date-girl|       NULL| yellow-red|         NULL|  NULL|
|         NULL|        fox|     purple|      hexagon|   low|
|      fig-gif|goat-cheese|       NULL|      octagon|  NULL|
|  grape-green|      horse|     orange|    rectangle|medium|
|honeydew-bear|     iguana|       pink|         NULL|  high|
|         NULL| jaguar-cat|black-white|      diamond|   low|
+-------------+-----------+-----------+-------------+------+
"""








df.withColumn("A", split(col("A"),"-").getItem(0)).show()
"""
+--------+-----------+-----------+-------------+------+
|       A|          B|          C|            D|     E|
+--------+-----------+-----------+-------------+------+
|   apple|   cat-meow|   red-blue|         NULL|  high|
|  banana|       NULL|       blue|circle-square|medium|
|    NULL|        dog|       NULL|     triangle|   low|
|  cherry|   elephant|      green|       square|  high|
|    date|       NULL| yellow-red|         NULL|  NULL|
|    NULL|        fox|     purple|      hexagon|   low|
|     fig|goat-cheese|       NULL|      octagon|  NULL|
|   grape|      horse|     orange|    rectangle|medium|
|honeydew|     iguana|       pink|         NULL|  high|
|    NULL| jaguar-cat|black-white|      diamond|   low|
+--------+-----------+-----------+-------------+------+
"""









df.withColumn("A", split(col("A"),"-").getItem(1)).show()
"""
+-----+-----------+-----------+-------------+------+
|    A|          B|          C|            D|     E|
+-----+-----------+-----------+-------------+------+
|green|   cat-meow|   red-blue|         NULL|  high|
|  pie|       NULL|       blue|circle-square|medium|
| NULL|        dog|       NULL|     triangle|   low|
|bloom|   elephant|      green|       square|  high|
| girl|       NULL| yellow-red|         NULL|  NULL|
| NULL|        fox|     purple|      hexagon|   low|
|  gif|goat-cheese|       NULL|      octagon|  NULL|
|green|      horse|     orange|    rectangle|medium|
| bear|     iguana|       pink|         NULL|  high|
| NULL| jaguar-cat|black-white|      diamond|   low|
+-----+-----------+-----------+-------------+------+
"""





df.withColumn("A", col("A")[0:3]).show()
"""
+----+-----------+-----------+-------------+------+
|   A|          B|          C|            D|     E|
+----+-----------+-----------+-------------+------+
| app|   cat-meow|   red-blue|         NULL|  high|
| ban|       NULL|       blue|circle-square|medium|
|NULL|        dog|       NULL|     triangle|   low|
| che|   elephant|      green|       square|  high|
| dat|       NULL| yellow-red|         NULL|  NULL|
|NULL|        fox|     purple|      hexagon|   low|
| fig|goat-cheese|       NULL|      octagon|  NULL|
| gra|      horse|     orange|    rectangle|medium|
| hon|     iguana|       pink|         NULL|  high|
|NULL| jaguar-cat|black-white|      diamond|   low|
+----+-----------+-----------+-------------+------+
"""








