


#51. How to impute missing values with Zero?

"""
# Suppose df is your DataFrame
df = spark.createDataFrame([(1, None), (None, 2), (3, 4), (5, None)], ["a", "b"])

df.show()

"""


# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import col, when

df = spark.createDataFrame([(1, None), (None, 2), (3, 4), (5, None)], ["a", "b"])

df.na.fill(0).show()













###################################################################

#55 How to convert a column to lower case using UDF?

"""
# Create a DataFrame to test
data = [('John Doe', 'NEW YORK'),
('Jane Doe', 'LOS ANGELES'),
('Mike Johnson', 'CHICAGO'),
('Sara Smith', 'SAN FRANCISCO')]

df = spark.createDataFrame(data, ['Name', 'City'])

"""



# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

from pyspark.sql.functions import col, when, lower

# Create a DataFrame to test
data = [('John Doe', 'NEW YORK'),
('Jane Doe', 'LOS ANGELES'),
('Mike Johnson', 'CHICAGO'),
('Sara Smith', 'SAN FRANCISCO')]

df = spark.createDataFrame(data, ['Name', 'City'])

df.withColumn("City",lower(col('City'))).show()


+------------+-------------+
|        Name|         City|
+------------+-------------+
|    John Doe|     new york|
|    Jane Doe|  los angeles|
|Mike Johnson|      chicago|
|  Sara Smith|san francisco|
+------------+-------------+





###################################################################

#63 How to convert the categorical string data into numerical data or index?

"""
# Create a sample DataFrame
data = [('cat',), ('dog',), ('mouse',), ('fish',), ('dog',), ('cat',), ('mouse',)]
df = spark.createDataFrame(data, ["animal"])

df.show()
"""










