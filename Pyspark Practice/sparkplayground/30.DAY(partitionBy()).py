
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, max, lit

from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession

spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(name = "Ben" ,A='apple', B='cat', C='red', D=None, E='high'),
    Row(name = "Ben" ,A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(name = "Ben" ,A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(name = "Ana" ,A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(name = "Ana" ,A='date', B=None, C='yellow', D=None, E='low'),
    Row(name = "Marta" ,A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(name = "Ana" ,A='fig', B='goat', C=None, D='octagon', E='high'),
    Row(name = "Ben" ,A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(name = "Marta" ,A='apple', B='iguana', C='pink', D=None, E='low'),
    Row(name = "Marta" ,A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

win = Window.partitionBy('name').orderBy('A')

df = df.withColumn(
    'drv_apple',
    when(
        max(when((col('A') == 'apple') ,lit(1)).otherwise(0))
        .over(win) == 1, 'This person apple').otherwise("They don't apple")
)

df = df.withColumn(
    'drv_high',
    when(
        max(when((col('E') == 'high') ,lit(1)).otherwise(0))
        .over(win) == 1, 'This person high').otherwise("No they don't")
)


df.show()

"""
+-----+------+--------+------+---------+------+-----------------+----------------+
| name|     A|       B|     C|        D|     E|        drv_apple|        drv_high|
+-----+------+--------+------+---------+------+-----------------+----------------+
|  Ana|cherry|elephant| green|   square|  high| They don't apple|This person high|
|  Ana|  date|    NULL|yellow|     NULL|   low| They don't apple|This person high|
|  Ana|   fig|    goat|  NULL|  octagon|  high| They don't apple|This person high|
|  Ben| apple|     cat|   red|     NULL|  high|This person apple|This person high|
|  Ben|banana|    NULL|  blue|   circle|medium|This person apple|This person high|
|  Ben|cherry|     dog|  NULL| triangle|   low|This person apple|This person high|
|  Ben| grape|   horse|orange|rectangle|medium|This person apple|This person high|
|Marta| apple|  iguana|  pink|     NULL|   low|This person apple|   No they don't|
|Marta| apple|  jaguar| black|  diamond|   low|This person apple|   No they don't|
|Marta|cherry|     fox|purple|  hexagon|   low|This person apple|   No they don't|
+-----+------+--------+------+---------+------+-----------------+----------------+
"""

















from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, max, lit, lag, concat_ws

from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession

spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(name = "Ben",A='apple', B='cat', C='red', D=None, E='high'),
    Row(name = "Ben",A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(name = "Ben",A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(name = "Ana",A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(name = "Ana",A='date', B=None, C='yellow', D=None, E='low'),
    Row(name = "Marta",A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(name = "Ana",A='fig', B='goat', C=None, D='octagon', E='high'),
    Row(name = "Ben",A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(name = "Marta",A='apple', B='iguana', C='pink', D=None, E='low'),
    Row(name = "Marta",A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

window_spec = Window.partitionBy("name").orderBy("A")

df.withColumn("prev_furit", lag("A").over(window_spec)).withColumn("prev_furit", concat_ws(" - ","name","prev_furit")).show()
"""
+-----+------+--------+------+---------+------+-------------+
| name|     A|       B|     C|        D|     E|   prev_furit|
+-----+------+--------+------+---------+------+-------------+
|  Ana|cherry|elephant| green|   square|  high|          Ana|
|  Ana|  date|    NULL|yellow|     NULL|   low| Ana - cherry|
|  Ana|   fig|    goat|  NULL|  octagon|  high|   Ana - date|
|  Ben| apple|     cat|   red|     NULL|  high|          Ben|
|  Ben|banana|    NULL|  blue|   circle|medium|  Ben - apple|
|  Ben|cherry|     dog|  NULL| triangle|   low| Ben - banana|
|  Ben| grape|   horse|orange|rectangle|medium| Ben - cherry|
|Marta| apple|  iguana|  pink|     NULL|   low|        Marta|
|Marta| apple|  jaguar| black|  diamond|   low|Marta - apple|
|Marta|cherry|     fox|purple|  hexagon|   low|Marta - apple|
+-----+------+--------+------+---------+------+-------------+
"""
















