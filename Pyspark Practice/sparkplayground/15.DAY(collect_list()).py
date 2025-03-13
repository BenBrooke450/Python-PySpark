


df = df.withColumn("customer_id",col("customer_id").cast(IntegerType())).orderBy(col("state"))

x = df.agg(collect_list(df["customer_id"])).collect()
print(x[0][:])
([17, 31, 32, 4, 44, 6, 13, 40, 34, 11, 21, 29, 16, 25, 8, 7, 1,
            43, 18, 22, 36, 20, 14, 24, 33, 19, 37, 28, 41, 45, 9,
            2, 30, 10, 23, 35, 3, 12, 39, 27, 38, 42, 5, 15, 46, 26],)













from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

for x in df.select(df.city).collect():
  print(x[0])

Springfield
Centerville
Greenville
Riverside
Lakeside
Oakland
Boise
Des Moines
Albany
Portland
Miami
Nashville
Denver
Minneapolis
Seattle
Atlanta
San Diego
Indianapolis
Charlotte
Detroit
Jacksonville
Boston
Pittsburgh
St. Louis
Honolulu
Milwaukee
San Antonio
Las Vegas
Orlando
Columbus
San Jose
San Francisco
Jackson
Washington
Philadelphia
Baltimore
Charlotte
El Paso
Memphis
Denver
Las Vegas
Houston
Chicago
San Diego
New York
Seattle
San Antonio
Dallas
Detroit
Indianapolis
