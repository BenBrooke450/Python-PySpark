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

df.withColumn("arra_join",collect_list(col("A")).over(Window.partitionBy("name"))).select("name","A","B","arra_join").show()
"""
+-----+------+--------+--------------------+
| name|     A|       B|           arra_join|
+-----+------+--------+--------------------+
|  Ana|cherry|elephant| [cherry, date, fig]|
|  Ana|  date|    NULL| [cherry, date, fig]|
|  Ana|   fig|    goat| [cherry, date, fig]|
|  Ben| apple|     cat|[apple, banana, c...|
|  Ben|banana|    NULL|[apple, banana, c...|
|  Ben|cherry|     dog|[apple, banana, c...|
|  Ben| grape|   horse|[apple, banana, c...|
|Marta|cherry|     fox|[cherry, apple, a...|
|Marta| apple|  iguana|[cherry, apple, a...|
|Marta| apple|  jaguar|[cherry, apple, a...|
+-----+------+--------+--------------------+
"""









df.withColumn("concat",concat(col("A"),lit(" - "),col("B"))).select("name","A","B","concat").show()
"""
+-----+------+--------+-----------------+
| name|     A|       B|           concat|
+-----+------+--------+-----------------+
|  Ben| apple|     cat|      apple - cat|
|  Ben|banana|    NULL|             NULL|
|  Ben|cherry|     dog|     cherry - dog|
|  Ana|cherry|elephant|cherry - elephant|
|  Ana|  date|    NULL|             NULL|
|Marta|cherry|     fox|     cherry - fox|
|  Ana|   fig|    goat|       fig - goat|
|  Ben| grape|   horse|    grape - horse|
|Marta| apple|  iguana|   apple - iguana|
|Marta| apple|  jaguar|   apple - jaguar|
+-----+------+--------+-----------------+
"""








df.withColumn("concat",concat_ws(" - ",col("A"),col("B"))).select("name","A","B","concat").show()
"""
+-----+------+--------+-----------------+
| name|     A|       B|           concat|
+-----+------+--------+-----------------+
|  Ben| apple|     cat|      apple - cat|
|  Ben|banana|    NULL|           banana|
|  Ben|cherry|     dog|     cherry - dog|
|  Ana|cherry|elephant|cherry - elephant|
|  Ana|  date|    NULL|             date|
|Marta|cherry|     fox|     cherry - fox|
|  Ana|   fig|    goat|       fig - goat|
|  Ben| grape|   horse|    grape - horse|
|Marta| apple|  iguana|   apple - iguana|
|Marta| apple|  jaguar|   apple - jaguar|
+-----+------+--------+-----------------+
"""











df.withColumn("concat",concat_ws(" - ",col("A"),col("B"))).select("name","A","B","concat")

df.dropna().show()
"""
+-----+------+--------+------+---------+------+
| name|     A|       B|     C|        D|     E|
+-----+------+--------+------+---------+------+
|  Ana|cherry|elephant| green|   square|  high|
|Marta|cherry|     fox|purple|  hexagon|   low|
|  Ben| grape|   horse|orange|rectangle|medium|
|Marta| apple|  jaguar| black|  diamond|   low|
+-----+------+--------+------+---------+------+
"""












df = df.withColumn("concat_again",when(~col("concat").contains("app"),col("concat"))).show()
"""
+-----+------+--------+-----------------+-----------------+
| name|     A|       B|           concat|     concat_again|
+-----+------+--------+-----------------+-----------------+
|  Ben| apple|     cat|      apple - cat|             NULL|
|  Ben|cherry|     dog|     cherry - dog|     cherry - dog|
|  Ana|cherry|elephant|cherry - elephant|cherry - elephant|
|Marta|cherry|     fox|     cherry - fox|     cherry - fox|
|  Ana|   fig|    goat|       fig - goat|       fig - goat|
|  Ben| grape|   horse|    grape - horse|    grape - horse|
|Marta| apple|  iguana|   apple - iguana|             NULL|
|Marta| apple|  jaguar|   apple - jaguar|             NULL|
+-----+------+--------+-----------------+-----------------+
"""









df = df.withColumn("concat_again",when(col("concat").contains("jag"),"HERE")).show()
"""
+-----+------+--------+-----------------+------------+
| name|     A|       B|           concat|concat_again|
+-----+------+--------+-----------------+------------+
|  Ben| apple|     cat|      apple - cat|        NULL|
|  Ben|cherry|     dog|     cherry - dog|        NULL|
|  Ana|cherry|elephant|cherry - elephant|        NULL|
|Marta|cherry|     fox|     cherry - fox|        NULL|
|  Ana|   fig|    goat|       fig - goat|        NULL|
|  Ben| grape|   horse|    grape - horse|        NULL|
|Marta| apple|  iguana|   apple - iguana|        NULL|
|Marta| apple|  jaguar|   apple - jaguar|        HERE|
+-----+------+--------+-----------------+------------+
"""










df = df.withColumn("concat_again",when(col("concat").like("jag"),"HERE")).show()
"""
+-----+------+--------+-----------------+------------+
| name|     A|       B|           concat|concat_again|
+-----+------+--------+-----------------+------------+
|  Ben| apple|     cat|      apple - cat|        NULL|
|  Ben|cherry|     dog|     cherry - dog|        NULL|
|  Ana|cherry|elephant|cherry - elephant|        NULL|
|Marta|cherry|     fox|     cherry - fox|        NULL|
|  Ana|   fig|    goat|       fig - goat|        NULL|
|  Ben| grape|   horse|    grape - horse|        NULL|
|Marta| apple|  iguana|   apple - iguana|        NULL|
|Marta| apple|  jaguar|   apple - jaguar|        NULL|
+-----+------+--------+-----------------+------------+
"""










df = df.withColumn("concat_again",split(col("concat"),"-")).show()
"""
+-----+------+--------+-----------------+--------------------+
| name|     A|       B|           concat|        concat_again|
+-----+------+--------+-----------------+--------------------+
|  Ben| apple|     cat|      apple - cat|      [apple ,  cat]|
|  Ben|cherry|     dog|     cherry - dog|     [cherry ,  dog]|
|  Ana|cherry|elephant|cherry - elephant|[cherry ,  elephant]|
|Marta|cherry|     fox|     cherry - fox|     [cherry ,  fox]|
|  Ana|   fig|    goat|       fig - goat|       [fig ,  goat]|
|  Ben| grape|   horse|    grape - horse|    [grape ,  horse]|
|Marta| apple|  iguana|   apple - iguana|   [apple ,  iguana]|
|Marta| apple|  jaguar|   apple - jaguar|   [apple ,  jaguar]|
+-----+------+--------+-----------------+--------------------+
"""


















from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, collect_set, array_join, array_sort, collect_list, concat_ws, lit, split, concat
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

df = df.groupBy(df.name).agg(collect_list(df.A)).show()
"""
+-----+--------------------+
| name|     collect_list(A)|
+-----+--------------------+
|Marta|[cherry, apple, a...|
|  Ana| [cherry, date, fig]|
|  Ben|[apple, banana, c...|
+-----+--------------------+
"""












