




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


+-------+-----+-----+
|   Name|Value|index|
+-------+-----+-----+
|  Alice|    1|    1|
|    Bob|    2|    2|
|Charlie|    3|    3|
+-------+-----+-----+






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


+---------+-----+-------+
|      job|Count|row_idx|
+---------+-----+-------+
|Scientist|    2|      1|
| Engineer|    4|      2|
|   Doctor|    1|  other|
+---------+-----+-------+









###############################################################################

#9 How to Drop rows with NA values specific to a particular column?


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

df.na.drop().show()


+----+-----+---+
|Name|Value| id|
+----+-----+---+
|   B|    3|456|
+----+-----+---+




###############################################################################


#10 How to rename columns of a PySpark DataFrame using two lists –  one containing the old column names and the other containing the new column names?

"""
# suppose you have the following DataFrame
df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])

# old column names
old_names = ["col1", "col2", "col3"]

# new column names
new_names = ["new_col1", "new_col2", "new_col3"]

df.show()
+----+----+----+
|col1|col2|col3|
+----+----+----+
| 1| 2| 3|
| 4| 5| 6|
+----+----+----+
"""

# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# suppose you have the following DataFrame
df = spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["col1", "col2", "col3"])

# old column names
old_names = ["col1", "col2", "col3"]

# new column names
new_names = ["new_col1", "new_col2", "new_col3"]

df.withColumnRenamed("col1", "new_col1")
    .withColumnRenamed("col2", "new_col2")
    .withColumnRenamed("col3","new_col3").show()


+--------+--------+--------+
|new_col1|new_col2|new_col3|
+--------+--------+--------+
|       1|       2|       3|
|       4|       5|       6|
+--------+--------+--------+


###############################################################################


#11. How to create contigency table?

"""
# Example DataFrame
data = [("A", "X"), ("A", "Y"), ("A", "X"), ("B", "Y"), ("B", "X"), ("C", "X"), ("C", "X"), ("C", "Y")]
df = spark.createDataFrame(data, ["category1", "category2"])

df.show()
"""

# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [("A", "X"), ("A", "Y"), ("A", "X"), ("B", "Y"), ("B", "X"), ("C", "X"), ("C", "X"), ("C", "Y")]

df = spark.createDataFrame(data, ["c1", "c2"])

df.crosstab("c1", "c2").sort("c1_c2").show()

+-----+---+---+
|c1_c2|  X|  Y|
+-----+---+---+
|    A|  2|  1|
|    B|  1|  1|
|    C|  2|  1|
+-----+---+---+




###############################################################################


#13 How to find the numbers that are multiples of 3 from a column?

"""
from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()
+---+------+
| id|random|
+---+------+
| 0| 7|
| 1| 6|
| 2| 9|
| 3| 7|
| 4| 3|
| 5| 8|
| 6| 9|
| 7| 8|
| 8| 3|
| 9| 8|
+---+------+
"""



# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

df.filter(col("random")%2==0).show()


+---+------+
| id|random|
+---+------+
|  1|     6|
+---+------+



###############################################################################


#14 How to extract items at given positions from a column?

"""
from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()

pos = [0, 4, 8, 5]
+---+------+
| id|random|
+---+------+
| 0| 7|
| 1| 6|
| 2| 9|
| 3| 7|
| 4| 3|
| 5| 8|
| 6| 9|
| 7| 8|
| 8| 3|
| 9| 8|
+---+------+
"""



# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql import Window, types
from pyspark.sql import Row
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()

pos = [0, 4, 8, 5]

print(df.collect()[0])
#Row(id=0, random=7)

print(df.collect()[4])
#Row(id=4, random=7)

print(df.collect()[8])
#Row(id=8, random=3)

print(df.collect()[5])
#Row(id=5, random=9)





###############################################################################

#15 How to stack two DataFrames vertically ?

"""
# Create DataFrame for region A
df_A = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 10), ("orange", 2, 8)], ["Name", "Col_1", "Col_2"])
df_A.show()

# Create DataFrame for region B
df_B = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 15), ("grape", 4, 6)], ["Name", "Col_1", "Col_3"])
df_B.show()
+------+-----+-----+
| Name|Col_1|Col_2|
+------+-----+-----+
| apple| 3| 5|
|banana| 1| 10|
|orange| 2| 8|
+------+-----+-----+

+------+-----+-----+
| Name|Col_1|Col_3|
+------+-----+-----+
| apple| 3| 5|
|banana| 1| 15|
| grape| 4| 6|
+------+-----+-----+
"""




# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Create DataFrame for region A
df_A = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 10), ("orange", 2, 8)], ["Name", "Col_1", "Col_2"])

# Create DataFrame for region B
df_B = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 15), ("grape", 4, 6)], ["Name", "Col_1", "Col_3"])

df_A.union(df_B).show()


+------+-----+-----+
|  Name|Col_1|Col_2|
+------+-----+-----+
| apple|    3|    5|
|banana|    1|   10|
|orange|    2|    8|
| apple|    3|    5|
|banana|    1|   15|
| grape|    4|    6|
+------+-----+-----+









###############################################################################


#17 How to convert the first character of each element in a series to uppercase?

"""
# Suppose you have the following DataFrame
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()
+-----+
| name|
+-----+
| john|
|alice|
| bob|
+-----+
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when, upper

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [("john",), ("alice",), ("bob",)]

df = spark.createDataFrame(data, ["name"])

list1 = []

for i, n in enumerate(df.collect()):
    list1.append((i + 1, n[0][0].upper() + n[0][1:]))

df = spark.createDataFrame(list1, ["number", "name"])

df.show()

+------+-----+
|number| name|
+------+-----+
|     1| John|
|     2|Alice|
|     3|  Bob|
+------+-----+





###############################################################################


#18 How to compute summary statistics for all columns in a dataframe


"""
# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()
+-------+---+------+
| name|age|salary|
+-------+---+------+
| James| 34| 55000|
|Michael| 30| 70000|
| Robert| 37| 60000|
| Maria| 29| 80000|
| Jen| 32| 65000|
+-------+---+------+
"""


# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when, upper

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.summary().show()

+-------+------+-----------------+-----------------+
|summary|  name|              age|           salary|
+-------+------+-----------------+-----------------+
|  count|     5|                5|                5|
|   mean|  NULL|             32.4|          66000.0|
| stddev|  NULL|3.209361307176242|9617.692030835671|
|    min| James|               29|            55000|
|    25%|  NULL|               30|            60000|
|    50%|  NULL|               32|            65000|
|    75%|  NULL|               34|            70000|
|    max|Robert|               37|            80000|
+-------+------+-----------------+-----------------+



###############################################################################

#19 How to calculate the number of characters in each word in a column?

"""
# Suppose you have the following DataFrame
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.show()
+-----+
| name|
+-----+
| john|
|alice|
| bob|
+-----+
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when, upper, length

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])

df.withColumn("Count",length(col("name"))).show()


+-----+-----+
| name|Count|
+-----+-----+
| john|    4|
|alice|    5|
|  bob|    3|
+-----+-----+


###############################################################################

#20 How to compute difference of differences between consecutive numbers of a column?

"""
# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()
+-------+---+------+
| name|age|salary|
+-------+---+------+
| James| 34| 55000|
|Michael| 30| 70000|
| Robert| 37| 60000|
| Maria| 29| 80000|
| Jen| 32| 65000|
+-------+---+------+
"""


# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, count, col, rank, row_number, monotonically_increasing_id, when, upper, length

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]
df = spark.createDataFrame(data, ["name", "age" , "salary"])

list1 = []

i = 0

while i < len(df.collect()) - 1:
  list1.append((df.collect()[i][0],df.collect()[i][1],
                df.select('salary').collect()[i+1][0] -  df.select('salary').collect()[i][0]))
  i = i + 1



df = spark.createDataFrame(list1, ["name", "age" , "diff_to_next_person"]).show()
+-------+---+-------------------+
|   name|age|diff_to_next_person|
+-------+---+-------------------+
|  James| 34|              15000|
|Michael| 30|             -10000|
| Robert| 37|              20000|
|  Maria| 29|             -15000|
+-------+---+-------------------+



###############################################################################


#21. How to get the day of month, week number, day of year and day of week from a date strings?


"""
data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df.show()
+----------+-----------+
|date_str_1| date_str_2|
+----------+-----------+
|2023-05-18|01 Jan 2010|
|2023-12-31|01 Jan 2010|
+----------+-----------+
"""



# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import day, month, year, col

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df = df.withColumn("day_1",day(col("date_str_1")))
df = df.withColumn("month_1",month(col("date_str_1")))
df = df.withColumn("year_1",year(col("date_str_1")))

df.show()
+----------+-----------+-----+-------+------+
|date_str_1| date_str_2|day_1|month_1|year_1|
+----------+-----------+-----+-------+------+
|2023-05-18|01 Jan 2010|   18|      5|  2023|
|2023-12-31|01 Jan 2010|   31|     12|  2023|
+----------+-----------+-----+-------+------+








# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek, col

data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

# Convert date string to date format
df = df.withColumn("date_1", to_date(df.date_str_1, 'yyyy-MM-dd'))
df = df.withColumn("date_2", to_date(df.date_str_2, 'dd MMM yyyy'))

df = df.withColumn("day_of_month", dayofmonth(df.date_1))\
.withColumn("week_number", weekofyear(df.date_1))\
.withColumn("day_of_year", dayofyear(df.date_1))\
.withColumn("day_of_week", dayofweek(df.date_1))

df.show()

+----------+-----------+----------+----------+------------+-----------+-----------+-----------+
|date_str_1| date_str_2|    date_1|    date_2|day_of_month|week_number|day_of_year|day_of_week|
+----------+-----------+----------+----------+------------+-----------+-----------+-----------+
|2023-05-18|01 Jan 2010|2023-05-18|2010-01-01|          18|         20|        138|          5|
|2023-12-31|01 Jan 2010|2023-12-31|2010-01-01|          31|         52|        365|          1|
+----------+-----------+----------+----------+------------+-----------+-----------+-----------+






###############################################################################


#22 How to convert year-month string to dates corresponding to the 4th day of the month?

"""
df = spark.createDataFrame([('Jan 2010',), ('Feb 2011',), ('Mar 2012',)], ['MonthYear'])

df.show()
+---------+
|MonthYear|
+---------+
| Jan 2010|
| Feb 2011|
| Mar 2012|
+---------+
"""

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek, col, concat, lit

df = spark.createDataFrame([('Jan 2010',), ('Feb 2011',), ('Mar 2012',)], ['MonthYear'])

df.withColumn("MonthYear",concat(lit("4"),lit(" "),col("MonthYear"))).show()


+----------+
| MonthYear|
+----------+
|4 Jan 2010|
|4 Feb 2011|
|4 Mar 2012|
+----------+





###############################################################################

#23 How to filter words that contain atleast 2 vowels from a series?

"""
df = spark.createDataFrame([('Apple',), ('Orange',), ('Plan',) , ('Python',) , ('Money',)], ['Word'])

df.show()
+------+
| Word|
+------+
| Apple|
|Orange|
| Plan|
|Python|
| Money|
+------+
"""


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek, col, concat, lit

df = spark.createDataFrame([('Apple',), ('Orange',), ('Plan',) , ('Python',) , ('Money',)], ['Word'])

list1 = []
vowls = ["a","e","i","o","u"]

for n in df.collect():
  t = 0
  for v in n[0].lower():
    if v in vowls:
      t = t + 1
      if t == 2:
        break
    elif len(n[0]) - 1 == t:
      list1.append((n[0],))
    else:
      t = t + 1

df = spark.createDataFrame(list1,["Letters_not_woth_two"]).show()



###############################################################################

#24 How to filter valid emails from a list?

"""
# Create a list
data = ['buying books at amazom.com', 'rameses@egypt.com', 'matt@t.co', 'narendra@modi.com']

# Convert the list to DataFrame
df = spark.createDataFrame(data, "string")
df.show(truncate =False)
+--------------------------+
|value |
+--------------------------+
|buying books at amazom.com|
|rameses@egypt.com |
|matt@t.co |
|narendra@modi.com |
+--------------------------+
"""


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import col, lit, contains, when

data = [('buying books at amazom.com',), ('rameses@egypt.com',), ('matt@t.co',),( 'narendra@modi.com',)]

# Convert the list to DataFrame
df = spark.createDataFrame(data, ["string"])

df.withColumn("Correct", when(col("string")
                              .contains(".com") & col("string")
                              .contains("@"),lit("correct")).otherwise(lit("incorrect"))).show()




###############################################################################

#25






















