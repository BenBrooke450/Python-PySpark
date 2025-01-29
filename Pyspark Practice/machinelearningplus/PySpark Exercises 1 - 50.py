




#2. How to convert the index of a PySpark DataFrame into a column? Difficulty Level: L1

"""
Hint: The PySpark DataFrame doesn’t have an explicit concept of
    an index like Pandas DataFrame. However, if you have a DataFrame
     and you’d like to add a new column that is basically a row number.

Input:


df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

df.show()

+-------+-----+
| Name|Value|
+-------+-----+
| Alice| 1|
| Bob| 2|
|Charlie| 3|
+-------+-----+


"""

from pyspark.sql.functions import rank
from pyspark.sql import Window

df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

w = Window.orderBy("value")

df = df.withColumn("index",rank().over(w))

df.show()

"""
+-------+-----+-----+
|   Name|Value|index|
+-------+-----+-----+
|  Alice|    1|    1|
|    Bob|    2|    2|
|Charlie|    3|    3|
+-------+-----+-----+
"""





###############################################################################




#3. How to combine many lists to form a PySpark DataFrame?

"""

Difficulty Level: L1

Create a PySpark DataFrame from list1 and list2

Hint: For Creating DataFrame from multiple lists, 
    first create an RDD (Resilient Distributed Dataset)
     from those lists and then convert the RDD to a DataFrame.

Input:

# Define your lists
list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]
"""














###############################################################################


#4. How to get the items of list A not present in list B?


"""
Difficulty Level: L2

Get the items of list_A not present in list_B in PySpark,
    you can use the subtract operation on RDDs (Resilient Distributed Datasets).

Input:

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
"""


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

A =[1, 2, 3, 4, 5]
A = spark.createDataFrame([(x,) for x in A],["value"])

A.show()


############

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

A =[1, 2, 3, 4, 5]
A = spark.createDataFrame([(x,) for x in A],["value"])

B = [4, 5, 6, 7, 8]
B = spark.createDataFrame([(x,) for x in B],["second_value"])

df = A.subtract(B).show()
+-----+
|value|
+-----+
|    1|
|    3|
|    2|
+-----+









###############################################################################


#5. How to get the items not common to both list A and list B?

"""
Difficulty Level: L2

Get all items of list_A and list_B not common to both.

Input:

list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
"""

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

A =[1, 2, 3, 4, 5]
A = spark.createDataFrame([(x,) for x in A],["value"])

B = [4, 5, 6, 7, 8]
B = spark.createDataFrame([(x,) for x in B],["second_value"])

# Find unique elements in df1 not in df2
unique_in_df1 = A.subtract(B)

# Find unique elements in df2 not in df1
unique_in_df2 = B.subtract(A)

# Union the results to get all unique elements
unique_elements = unique_in_df1.union(unique_in_df2)










###############################################################################


#6. How to get the items not common to both list A and list B?

"""
Compute the minimum, 25th percentile, median, 75th, and maximum of column Age

input

# Create a sample DataFrame
data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show()
+----+---+
|Name|Age|
+----+---+
| A| 10|
| B| 20|
| C| 30|
| D| 40|
| E| 50|
| F| 15|
| G| 28|
| H| 54|
| I| 41|
| J| 86|
+----+---+

"""


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
df = spark.createDataFrame(data, ["Name", "Age"])

from pyspark.sql.functions import max, col

df.select(max(col("Age"))).show()

Age = [row['Age']for row in df.select(col('Age')).collect()]

print(Age)

qs = df.approxQuantile("Age", [0.25, 0.5, 0.75], 0.0)
print(qs)









###############################################################################

#7 How to get frequency counts of unique items of a column?

"""
Difficulty Level: L1

Calculte the frequency counts of each unique value

Input

from pyspark.sql import Row

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)

# show DataFrame
df.show()
"""



# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql import Row
from pyspark.sql.functions import max, count, col

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

# create DataFrame
df = spark.createDataFrame(data)



df.groupBy(col("job")).agg(count(col("job"))).show()
+---------+----------+
|      job|count(job)|
+---------+----------+
| Engineer|         4|
|Scientist|         2|
|   Doctor|         1|
+---------+----------+



df.groupBy(col("job"),col("name")).agg(count(col("job"))).show()
+---------+----+----------+
|      job|name|count(job)|
+---------+----+----------+
| Engineer|John|         2|
|Scientist|Mary|         1|
| Engineer| Bob|         2|
|   Doctor| Sam|         1|
|Scientist| Bob|         1|
+---------+----+----------+





###############################################################################

#8 How to keep only top 2 most frequent values as it is and replace everything else as ‘Other’?

# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql import Window, types
from pyspark.sql import Row
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id,when

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Sample data
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]

df = spark.createDataFrame(data)

df = df.groupBy(col("job")).agg(count(col("job")).alias("Count"))

df = df.withColumn("row_idx",row_number().over(Window.orderBy(monotonically_increasing_id())))

df = df.withColumn("row_idx",when(col("row_idx")<3,col("row_idx")).otherwise("other")).show()

"""
+---------+-----+-------+
|      job|Count|row_idx|
+---------+-----+-------+
|Scientist|    2|      1|
| Engineer|    4|      2|
|   Doctor|    1|  other|
+---------+-----+-------+

"""







###############################################################################

#9



















