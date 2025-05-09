
"""
col()	Refers to a column in a DataFrame (used for expressions and filters)
lit()	Creates a column with a constant literal value
when() / otherwise()	Used for conditional logic (like SQL CASE WHEN)
withColumn()	Creates a new column or replaces an existing one
select()	Selects columns from a DataFrame
filter() / where()	Filters rows based on a condition
groupBy()	Groups rows and allows aggregate computations
agg()	Applies aggregation functions to grouped data
orderBy() / sort()	Sorts the DataFrame by specified columns
join()	Joins two DataFrames on one or more keys
explode() transforms arrays/maps into multiple rows (useful in nested data)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("Explode Example").getOrCreate()

data = [
    ("Alice", ["apple", "banana"]),
    ("Bob", ["orange"]),
    ("Charlie", ["grape", "peach", "kiwi"])
]

df = spark.createDataFrame(data, ["name", "fruits"])
df.show(truncate=False)
"""
+-------+----------------------+
| name  | fruits               |
+-------+----------------------+
| Alice | [apple, banana]      |
| Bob   | [orange]             |
| Charlie | [grape, peach, kiwi] |
+-------+----------------------+
"""

df_exploded = df.withColumn("fruit", explode(df["fruits"]))
df_exploded.show()
"""
+-------+----------------------+--------+
| name  | fruits               | fruit  |
+-------+----------------------+--------+
| Alice | [apple, banana]      | apple  |
| Alice | [apple, banana]      | banana |
| Bob   | [orange]             | orange |
| Charlie | [grape, peach, kiwi] | grape  |
| Charlie | [grape, peach, kiwi] | peach  |
| Charlie | [grape, peach, kiwi] | kiwi   |
+-------+----------------------+--------+
"""














"""
explode_outer() works in PySpark — it's very similar to explode(), 
    but it handles null or empty arrays more gracefully by preserving 
    the row and returning a null instead of dropping it.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer

spark = SparkSession.builder.appName("Explode Outer Example").getOrCreate()

data = [
    ("Alice", ["apple", "banana"]),
    ("Bob", None),                 # Null array
    ("Charlie", []),              # Empty array
    ("David", ["kiwi"])
]

df = spark.createDataFrame(data, ["name", "fruits"])
df.show(truncate=False)

"""
+-------+----------------+
| name  | fruits         |
+-------+----------------+
| Alice | [apple, banana]|
| Bob   | null           |
| Charlie | []           |
| David | [kiwi]         |
+-------+----------------+
"""

df_exploded = df.withColumn("fruit", explode_outer(df["fruits"]))
df_exploded.show()
"""
+-------+----------------+--------+
| name  | fruits         | fruit  |
+-------+----------------+--------+
| Alice | [apple, banana]| apple  |
| Alice | [apple, banana]| banana |
| Bob   | null           | null   |
| Charlie | []           | null   |
| David | [kiwi]         | kiwi   |
+-------+----------------+--------+
"""








"""
The opposite of explode() in PySpark is:

🧲 collect_list() or collect_set()

These functions are used in combination with groupBy() to re-aggregate individual
    rows back into an array, which is the reverse of what explode() does.
"""



from pyspark.sql import functions as F

df_recombined = df_exploded.groupBy("name").agg(F.collect_list("fruit").alias("fruits"))
df_recombined.show()

"""
+-------+----------------+
| name  | fruits         |
+-------+----------------+
| Alice | [apple, banana]|
| David | [kiwi]         |
+-------+----------------+
"""


"""
Flatten arrays	explode()
Re-group to arrays	collect_list() / collect_set()

✅ collect_list() keeps duplicates

✅ collect_set() removes duplicates
"""










