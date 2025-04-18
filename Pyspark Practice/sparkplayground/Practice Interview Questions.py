



"""


Read data from a CSV file.
Filter out customers with a purchase amount less than 100 USD.
Further filter to include only customers aged 30 or above.
Use display(df) to show the final DataFrame.
"""


#enter the file path here
file_path = "/datasets/customers.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

df = df.filter((col("purchase_amount")>100) & (col("age") >= 30))

display(df.select("customer_id","name","purchase_amount"))


################################################################################


"""
Handling Null Values


You are provided with a dataset containing customer information. 
    The dataset may have missing values in the customer_id or email columns.
     Your task is to filter out any rows where either customer_id or email is null.
"""


# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/customers_raw.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

df = df.na.drop()

# Display the final DataFrame using the display() function.
display(df)






################################################################################


"""
Calculate Total Purchases by Customer


Given a dataset of customer purchases, your task
    is to group the data by customer and calculate
    the total purchase amount for each customer. You
     will need to group by customer_id and sum up
      the purchase_amount for each individual.

Order the result by customer_id

Use display(df) to show the final DataFrame.
"""


# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, lit, contains, when, sum, round
from pyspark.sql.types import IntegerType

#enter the file path here
file_path = "/datasets/customer_purchases.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

# Display the final DataFrame using the display() function.
df = df.groupBy("customer_id").agg(sum("purchase_amount").cast(IntegerType()).alias("total_purchase")).orderBy("customer_id")

display(df)







################################################################################


"""
Calculate Discounts on Products


You are given a dataset containing product_id, 
    product_name, original_price, and discount_percentage. 
    Your task is to compute the final price for each product 
    by applying the discount and return the product_id, 
    product_name, and final_price for each product.

The formula for calculating the final_price is: original_price * ( 1 - discount_percentage/100 )

Use display(df) to show the final DataFrame.
"""

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, lit, contains, when, sum, round

#enter the file path here
file_path = "/datasets/products.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

# Display the final DataFrame using the display() function.
df = df.withColumn("final_price",col("original_price")*(1 - col("discount_percentage")/100 ))

display(df.select("product_id","product_name","final_price"))






################################################################################

"""
Load & Transform JSON file

You are provided with a nested JSON file that
    contains customer purchase details. The JSON
    contains an array of products for each customer 
    along with details such as product name and price.

Your task is to flatten the JSON structure and 
    extract the relevant fields: customer_id, 
    order_id, product_name, and product_price. 
    You will need to explode the array of products 
    so that each product becomes a separate row.

After flattening and exploding the data, use display(df) to show the final DataFrame.
"""



# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, lit, contains, when, sum, round, explode


#enter the file path here
file_path = "/datasets/orders.json"


#read the file
df = spark.read.format('json').option("multiline", "true").load(file_path)

df = df.withColumn("products", explode("products"))

df = df.withColumn("product_name",col("products").getItem("product_name"))
        .withColumn("product_price",col("products").getItem("product_price"))

df = df.drop(col("products"))

display(df)






################################################################################








"""
Employees Earning More than Average


Write a PySpark query to retrieve employees who earn more 
    than the average salary of their respective department. 
    The query should output the employee's name, department name, and salary.

Use the provided employee and department dataframes to solve this challenge.
"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, contains, when, sum, avg
from pyspark.sql.window import Window

# Start a Spark session
spark = SparkSession.builder \
    .appName("Employees Above Average Salary") \
    .master("local[*]") \
    .getOrCreate()

# Employee DataFrame
employee_data = [
    (1, "Alice", 5000, 1),
    (2, "Bob", 7000, 2),
    (3, "Charlie", 4000, 1),
    (4, "David", 6000, 2),
    (5, "Eve", 8000, 3),
    (6, "Kev", 9000, 3),
    (7, "Mev", 10000, 3),
    (8, "Mob", 12000, 2)
]

employee_columns = ["employee_id", "employee_name", "salary", "department_id"]
emp_df = spark.createDataFrame(employee_data, employee_columns)

# Department DataFrame
department_data = [
    (1, "HR"),
    (2, "Engineering"),
    (3, "Finance")
]
department_columns = ["department_id", "department_name"]
dept_df = spark.createDataFrame(department_data, department_columns)

df = emp_df.join(dept_df,"department_id")

df = df.withColumn("Avg over each Department",
                   avg(col("salary")).over(Window.partitionBy(col("department_name"))))

df = (df.filter(col("salary")>col("Avg over each Department"))
      .select("employee_name","department_name","salary").orderBy("salary"))

display(df)


################################################################################



"""
Remove Duplicates From Dataset



Given a dataset containing user information with duplicate
    user_id values, write a PySpark query to remove duplicate 
    rows while retaining the row with the latest created_date 
    for each user_id. The result should contain the latest entry for each user.
    
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date
from pyspark.sql.functions import col, lit, contains, when, sum, avg, max

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RemoveDuplicates") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("email", StringType(), True)
])

# Sample data
data = [
    (1, "Alice", date(2023, 5, 10), "alice@example.com"),
    (1, "Alice", date(2023, 6, 15), "alice_new@example.com"),
    (2, "Bob", date(2023, 7, 1), "bob@example.com"),
    (3, "Charlie", date(2023, 5, 20), "charlie@example.com"),
    (3, "Charlie", date(2023, 6, 25), "charlie_updated@example.com"),
    (4, "David", date(2023, 8, 5), "david@example.com")
]

# Create DataFrame
user_df = spark.createDataFrame(data, schema)

df = user_df.groupBy("user_id","user_name").agg(max("created_date"),max("email"))

display(df.withColumnRenamed("max(created_date)","created_date",).withColumnRenamed("max(email)","email"))





################################################################################


"""
Word Count Program in PySpark


Write a PySpark program to count the occurrences of each word 
    in a given text file. The solution must utilize RDD transformations
     and actions for processing, and then convert the final RDD into a 
     DataFrame for output. Sort DataFrame by count in descending order.
"""

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/notes.txt"

#read the file
df = spark.read.text(file_path)

# Display the final DataFrame using the display() function.
list1 = []
for n in df.collect()[:]:
  list1.append(n[0])

list1 = " ".join(list1)
list1 = list1.split(" ")


list2=[]
for m in list1:
  list2.append((m,list1.count(m)))

set2 = set(list2)

df = spark.createDataFrame(set2,["word","count"])

display(df.orderBy("count"))





















