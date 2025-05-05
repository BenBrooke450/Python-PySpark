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
    Row(name = "Jemma" ,A='cherry', B='jaguar', C='black', D='diamond', E='low'),
    Row(name = "Marta" ,A='apple', B='iguana', C='pink', D=None, E='low'),
    Row(name = "Marta" ,A='grape', B='jaguar', C='black', D='diamond', E='low'),
    Row(name = "Jemma" ,A='grape', B='jaguar', C='black', D='diamond', E='low'),
    Row(name = "Jemma" ,A='banana', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)

df = df.unpivot("name", ['A', 'B', 'C', 'D', 'E'], "var", "val")

df.show(40)
"""
+-----+---+---------+
| name|var|      val|
+-----+---+---------+
|  Ben|  A|    apple|
|  Ben|  B|      cat|
|  Ben|  C|      red|
|  Ben|  D|     NULL|
|  Ben|  E|     high|
|  Ben|  A|   banana|
|  Ben|  B|     NULL|
|  Ben|  C|     blue|
|  Ben|  D|   circle|
|  Ben|  E|   medium|
|  Ben|  A|   cherry|
|  Ben|  B|      dog|
|  Ben|  C|     NULL|
|  Ben|  D| triangle|
|  Ben|  E|      low|
|  Ana|  A|   cherry|
|  Ana|  B| elephant|
|  Ana|  C|    green|
|  Ana|  D|   square|
|  Ana|  E|     high|
|  Ana|  A|     date|
|  Ana|  B|     NULL|
|  Ana|  C|   yellow|
|  Ana|  D|     NULL|
|  Ana|  E|      low|
|Marta|  A|   cherry|
|Marta|  B|      fox|
|Marta|  C|   purple|
|Marta|  D|  hexagon|
|Marta|  E|      low|
|  Ana|  A|      fig|
|  Ana|  B|     goat|
|  Ana|  C|     NULL|
|  Ana|  D|  octagon|
|  Ana|  E|     high|
|  Ben|  A|    grape|
|  Ben|  B|    horse|
|  Ben|  C|   orange|
|  Ben|  D|rectangle|
|  Ben|  E|   medium|
+-----+---+---------+
"""
