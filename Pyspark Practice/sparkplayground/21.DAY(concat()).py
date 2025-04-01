

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, concat, collect_list, concat_ws, max,lit, dense_rank, rank, contains
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create data with some null values
data = [
    ("Alice", 30, "New York"),
    ("Bob", None, "Los Angeles"),
    (None, 25, None),
    ("Charlie", None, "Chicago")
]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
"""
+-------+----+-----------+
|   Name| Age|       City|
+-------+----+-----------+
|  Alice|  30|   New York|
|    Bob|NULL|Los Angeles|
|   NULL|  25|       NULL|
|Charlie|NULL|    Chicago|
+-------+----+-----------+
"""








df = df.withColumn("Coalesce",coalesce(df["Name"], df["Age"]))

df.show()
"""
+-------+----+-----------+--------+
|   Name| Age|       City|Coalesce|
+-------+----+-----------+--------+
|  Alice|  30|   New York|   Alice|
|    Bob|NULL|Los Angeles|     Bob|
|   NULL|  25|       NULL|      25|
|Charlie|NULL|    Chicago| Charlie|
+-------+----+-----------+--------+
"""










df = df.withColumn("Coalesce",concat(df["Name"], df["Age"]))

df.show()
"""
+-------+----+-----------+--------+
|   Name| Age|       City|Coalesce|
+-------+----+-----------+--------+
|  Alice|  30|   New York| Alice30|
|    Bob|NULL|Los Angeles|    NULL|
|   NULL|  25|       NULL|    NULL|
|Charlie|NULL|    Chicago|    NULL|
+-------+----+-----------+--------+
"""












df = df.withColumn("Concat_ws",concat_ws("-",df["Name"], df["City"]))

df.show()
"""
+-------+----+-----------+---------------+
|   Name| Age|       City|      Concat_ws|
+-------+----+-----------+---------------+
|  Alice|  30|   New York| Alice-New York|
|    Bob|NULL|Los Angeles|Bob-Los Angeles|
|   NULL|  25|       NULL|               |
|Charlie|NULL|    Chicago|Charlie-Chicago|
+-------+----+-----------+---------------+
"""



















