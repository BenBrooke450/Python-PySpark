from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, coalesce, split, concat_ws, concat, isnull, ifnull, lit, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(A='apple', B='cat', C='red', D=None, E='high'),
    Row(A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(A=None, B='dog', C=None, D='triangle', E='low'),
    Row(A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(A='date', B=None, C='yellow', D=None, E=None),
    Row(A=None, B='fox', C='purple', D='hexagon', E='low'),
    Row(A='fig', B='goat', C=None, D='octagon', E=None),
    Row(A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(A='honeydew', B='iguana', C='pink', D=None, E='high'),
    Row(A=None, B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df.withColumn("Row_n", row_number().over(Window.orderBy("A"))).show()
"""
+--------+--------+------+---------+------+-----+
|       A|       B|     C|        D|     E|Row_n|
+--------+--------+------+---------+------+-----+
|    NULL|     dog|  NULL| triangle|   low|    1|
|    NULL|     fox|purple|  hexagon|   low|    2|
|    NULL|  jaguar| black|  diamond|   low|    3|
|   apple|     cat|   red|     NULL|  high|    4|
|  banana|    NULL|  blue|   circle|medium|    5|
|  cherry|elephant| green|   square|  high|    6|
|    date|    NULL|yellow|     NULL|  NULL|    7|
|     fig|    goat|  NULL|  octagon|  NULL|    8|
|   grape|   horse|orange|rectangle|medium|    9|
|honeydew|  iguana|  pink|     NULL|  high|   10|
+--------+--------+------+---------+------+-----+

"""











df.withColumn("Row_n",row_number().over(Window.partitionBy("A").orderBy("A"))).show()
"""
+------+--------+------+---------+------+-----+
|     A|       B|     C|        D|     E|Row_n|
+------+--------+------+---------+------+-----+
| apple|     cat|   red|     NULL|  high|    1|
| apple|  iguana|  pink|     NULL|  high|    2|
| apple|  jaguar| black|  diamond|   low|    3|
|banana|    NULL|  blue|   circle|medium|    1|
|cherry|     dog|  NULL| triangle|   low|    1|
|cherry|elephant| green|   square|  high|    2|
|cherry|     fox|purple|  hexagon|   low|    3|
|  date|    NULL|yellow|     NULL|  NULL|    1|
|   fig|    goat|  NULL|  octagon|  NULL|    1|
| grape|   horse|orange|rectangle|medium|    1|
+------+--------+------+---------+------+-----+
"""




















