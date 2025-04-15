from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, collect_set, array_join, array_sort, collect_list
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(name = "Ben",A='apple', B='cat', C='red', D=None, E='high'),
    Row(name = "Ben",A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(name = "Ben",A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(name = "Ana",A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(name = "Ana",A='date', B=None, C='yellow', D=None, E=None),
    Row(name = "Marta",A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(name = "Ana",A='fig', B='goat', C=None, D='octagon', E=None),
    Row(name = "Ben",A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(name = "Marta",A='apple', B='iguana', C='pink', D=None, E='high'),
    Row(name = "Marta",A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df.withColumn("new_column",array_join(array_sort(collect_list("A").over(Window.partitionBy("name")))," | ")).show()
"""
+-----+------+--------+------+---------+------+--------------------+
| name|     A|       B|     C|        D|     E|          new_column|
+-----+------+--------+------+---------+------+--------------------+
|  Ana|cherry|elephant| green|   square|  high| cherry | date | fig|
|  Ana|  date|    NULL|yellow|     NULL|  NULL| cherry | date | fig|
|  Ana|   fig|    goat|  NULL|  octagon|  NULL| cherry | date | fig|
|  Ben| apple|     cat|   red|     NULL|  high|apple | banana | ...|
|  Ben|banana|    NULL|  blue|   circle|medium|apple | banana | ...|
|  Ben|cherry|     dog|  NULL| triangle|   low|apple | banana | ...|
|  Ben| grape|   horse|orange|rectangle|medium|apple | banana | ...|
|Marta|cherry|     fox|purple|  hexagon|   low|apple | apple | c...|
|Marta| apple|  iguana|  pink|     NULL|  high|apple | apple | c...|
|Marta| apple|  jaguar| black|  diamond|   low|apple | apple | c...|
+-----+------+--------+------+---------+------+--------------------+
"""














from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, collect_set, array_join, array_sort, collect_list
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(name = "Ben",A='apple', B='cat', C='red', D=None, E='high'),
    Row(name = "Ben",A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(name = "Ben",A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(name = "Ana",A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(name = "Ana",A='date', B=None, C='yellow', D=None, E=None),
    Row(name = "Marta",A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(name = "Ana",A='fig', B='goat', C=None, D='octagon', E=None),
    Row(name = "Ben",A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(name = "Marta",A='apple', B='iguana', C='pink', D=None, E='high'),
    Row(name = "Marta",A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df.withColumn("new_column",array_sort(collect_list("A").over(Window.partitionBy("name")))).show()
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
|Marta|cherry|     fox|purple|  hexagon|   low|[apple, apple, ch...|
|Marta| apple|  iguana|  pink|     NULL|  high|[apple, apple, ch...|
|Marta| apple|  jaguar| black|  diamond|   low|[apple, apple, ch...|
+-----+------+--------+------+---------+------+--------------------+
"""












