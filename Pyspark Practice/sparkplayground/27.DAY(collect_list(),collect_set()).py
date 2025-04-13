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
    Row(A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(A='date', B=None, C='yellow', D=None, E=None),
    Row(A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(A='fig', B='goat', C=None, D='octagon', E=None),
    Row(A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(A='apple', B='iguana', C='pink', D=None, E='high'),
    Row(A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df.show()
"""
+-----+------+--------+------+---------+------+
| name|     A|       B|     C|        D|     E|
+-----+------+--------+------+---------+------+
|  Ben| apple|     cat|   red|     NULL|  high|
|  Ben|banana|    NULL|  blue|   circle|medium|
|  Ben|cherry|     dog|  NULL| triangle|   low|
|  Ana|cherry|elephant| green|   square|  high|
|  Ana|  date|    NULL|yellow|     NULL|  NULL|
|Marta|cherry|     fox|purple|  hexagon|   low|
|  Ana|   fig|    goat|  NULL|  octagon|  NULL|
|  Ben| grape|   horse|orange|rectangle|medium|
|Marta| apple|  iguana|  pink|     NULL|  high|
|Marta| apple|  jaguar| black|  diamond|   low|
+-----+------+--------+------+---------+------+
"""





df.groupBy("name").agg(collect_list("A")).show()
"""
+-----+--------------------+
| name|     collect_list(A)|
+-----+--------------------+
|  Ana| [cherry, date, fig]|
|  Ben|[apple, banana, c...|
|Marta|[cherry, apple, a...|
+-----+--------------------+
"""







df.withColumn("new_column",collect_list("A").over(Window.partitionBy("name"))).show()
"""
+-----+------+--------+------+---------+------+--------------------+
| name|     A|       B|     C|        D|     E|          new_column|
+-----+------+--------+------+---------+------+--------------------+
|  Ana|cherry|elephant| green|   square|  high| [cherry, date, fig]|
|  Ana|  date|    NULL|yellow|     NULL|  NULL| [cherry, date, fig]|
|  Ana|   fig|    goat|  NULL|  octagon|  NULL| [cherry, date, fig]|
|  Ben| apple|     cat|   red|     NULL|  high|[apple, banana, c...|
|  Ben|banana|    NULL|  blue|   circle|medium|[apple, banana, c...|
|  Ben|cherry|     dog|  NULL| triangle|   low|[apple, banana, c...|
|  Ben| grape|   horse|orange|rectangle|medium|[apple, banana, c...|
|Marta|cherry|     fox|purple|  hexagon|   low|[cherry, apple, a...|
|Marta| apple|  iguana|  pink|     NULL|  high|[cherry, apple, a...|
|Marta| apple|  jaguar| black|  diamond|   low|[cherry, apple, a...|
+-----+------+--------+------+---------+------+--------------------+
"""







df.groupBy("name").agg(collect_set("A")).show()
"""
+-----+--------------------+
| name|      collect_set(A)|
+-----+--------------------+
|  Ana| [cherry, fig, date]|
|  Ben|[apple, cherry, g...|
|Marta|     [apple, cherry]|
+-----+--------------------+
"""






















