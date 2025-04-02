

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, coalesce, concat_ws, concat
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()



# Create data with some null values
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


df.show()
"""
+--------+--------+------+---------+------+
|       A|       B|     C|        D|     E|
+--------+--------+------+---------+------+
|   apple|     cat|   red|     NULL|  high|
|  banana|    NULL|  blue|   circle|medium|
|    NULL|     dog|  NULL| triangle|   low|
|  cherry|elephant| green|   square|  high|
|    date|    NULL|yellow|     NULL|  NULL|
|    NULL|     fox|purple|  hexagon|   low|
|     fig|    goat|  NULL|  octagon|  NULL|
|   grape|   horse|orange|rectangle|medium|
|honeydew|  iguana|  pink|     NULL|  high|
|    NULL|  jaguar| black|  diamond|   low|
+--------+--------+------+---------+------+
"""










df.filter(col("A").isNull()).show()
"""
+----+------+------+--------+---+
|   A|     B|     C|       D|  E|
+----+------+------+--------+---+
|NULL|   dog|  NULL|triangle|low|
|NULL|   fox|purple| hexagon|low|
|NULL|jaguar| black| diamond|low|
+----+------+------+--------+---+
"""








df.filter(col("A").isNotNull()).show()
+--------+--------+------+---------+------+
|       A|       B|     C|        D|     E|
+--------+--------+------+---------+------+
|   apple|     cat|   red|     NULL|  high|
|  banana|    NULL|  blue|   circle|medium|
|  cherry|elephant| green|   square|  high|
|    date|    NULL|yellow|     NULL|  NULL|
|     fig|    goat|  NULL|  octagon|  NULL|
|   grape|   horse|orange|rectangle|medium|
|honeydew|  iguana|  pink|     NULL|  high|
+--------+--------+------+---------+------+













df = df.withColumn("Nulls_column",when(col("A").isNull() | col("B").isNull()
                                       | col("C").isNull() | col("D").isNull() |
                                       col("E").isNull(),"Needs_updated").otherwise("all_clear"))

df.show()
"""
+--------+--------+------+---------+------+-------------+
|       A|       B|     C|        D|     E| Nulls_column|
+--------+--------+------+---------+------+-------------+
|   apple|     cat|   red|     NULL|  high|Needs_updated|
|  banana|    NULL|  blue|   circle|medium|Needs_updated|
|    NULL|     dog|  NULL| triangle|   low|Needs_updated|
|  cherry|elephant| green|   square|  high|    all_clear|
|    date|    NULL|yellow|     NULL|  NULL|Needs_updated|
|    NULL|     fox|purple|  hexagon|   low|Needs_updated|
|     fig|    goat|  NULL|  octagon|  NULL|Needs_updated|
|   grape|   horse|orange|rectangle|medium|    all_clear|
|honeydew|  iguana|  pink|     NULL|  high|Needs_updated|
|    NULL|  jaguar| black|  diamond|   low|Needs_updated|
+--------+--------+------+---------+------+-------------+
"""












df = df.withColumn("Nulls_column",when(isnull(df["A"]),"A_Needs_updated").otherwise("A_is_all_clear"))

df.show()
"""
+--------+--------+------+---------+------+---------------+
|       A|       B|     C|        D|     E|   Nulls_column|
+--------+--------+------+---------+------+---------------+
|   apple|     cat|   red|     NULL|  high| A_is_all_clear|
|  banana|    NULL|  blue|   circle|medium| A_is_all_clear|
|    NULL|     dog|  NULL| triangle|   low|A_Needs_updated|
|  cherry|elephant| green|   square|  high| A_is_all_clear|
|    date|    NULL|yellow|     NULL|  NULL| A_is_all_clear|
|    NULL|     fox|purple|  hexagon|   low|A_Needs_updated|
|     fig|    goat|  NULL|  octagon|  NULL| A_is_all_clear|
|   grape|   horse|orange|rectangle|medium| A_is_all_clear|
|honeydew|  iguana|  pink|     NULL|  high| A_is_all_clear|
|    NULL|  jaguar| black|  diamond|   low|A_Needs_updated|
+--------+--------+------+---------+------+---------------+
"""


