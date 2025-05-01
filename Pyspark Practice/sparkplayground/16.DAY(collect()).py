
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

id_list = df.select(col("customer_id")).collect()







print(id_list)
#[Row(customer_id='1'), Row(customer_id='2'), Row(customer_id='3'), Row(customer_id='4'), Row(customer_id='5'), Row(customer_id='6'), Row(customer_id='7'), Row(customer_id='8'), Row(customer_id='9'), Row(customer_id='10'), Row(customer_id='11'), Row(customer_id='12'), Row(customer_id='13'), Row(customer_id='14'), Row(customer_id='15'), Row(customer_id='16'), Row(customer_id='17'), Row(customer_id='18'), Row(customer_id='19'), Row(customer_id='20'), Row(customer_id='21'), Row(customer_id='22'), Row(customer_id='23'), Row(customer_id='24'), Row(customer_id='25'), Row(customer_id='26'), Row(customer_id='27'), Row(customer_id='28'), Row(customer_id='29'), Row(customer_id='30'), Row(customer_id='31'), Row(customer_id='32'), Row(customer_id='33'), Row(customer_id='34'), Row(customer_id='35'), Row(customer_id='36'), Row(customer_id='37'), Row(customer_id='38'), Row(customer_id='39'), Row(customer_id='40'), Row(customer_id='41'), Row(customer_id='42'), Row(customer_id='43'), Row(customer_id='44'), Row(customer_id='45'), Row(customer_id='46'), Row(customer_id='47'), Row(customer_id='48'), Row(customer_id='49'), Row(customer_id='50')]


print(id_list[0])
#Row(customer_id='1')







other_list  = []

for x in id_list:
  other_list.append(x[0])

print(other_list)
#['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50']











bigger_list = df.select("customer_id","city","state","name").collect()




for i,x in enumerate(bigger_list):
  print(x[0:2])
  if i == 10:
    break

"""
('1', 'John')
('2', 'Emma')
('3', 'Olivia')
('4', 'Liam')
('5', 'Noah')
('6', 'Alice')
('7', 'Isabella')
('8', 'James')
('9', 'Sophia')
('10', 'Lucas')
('11', 'Mia')
"""







for i,x in enumerate(bigger_list):
  print(x[:])
  if i == 10:
    break

"""
('1', 'John', 'Springfield', 'IL')
('2', 'Emma', 'Centerville', 'OH')
('3', 'Olivia', 'Greenville', 'SC')
('4', 'Liam', 'Riverside', 'CA')
('5', 'Noah', 'Lakeside', 'TX')
('6', 'Alice', 'Oakland', 'CA')
('7', 'Isabella', 'Boise', 'ID')
('8', 'James', 'Des Moines', 'IA')
('9', 'Sophia', 'Albany', 'NY')
('10', 'Lucas', 'Portland', 'OR')
('11', 'Mia', 'Miami', 'FL')
"""






bigger_list = df.select("customer_id","first_name","city","state").collect()

for i,x in enumerate(bigger_list):
  if "Liam" in x[:]:
    print("We found him, he´s at postion:",i)
    break

#We found him, he´s at postion: 3
















from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, max, lit, lag, concat_ws, collect_list

from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession

spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(name = "Ben",A='apple', B='cat', C='red', D=None, E='high'),
    Row(name = "Ben",A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(name = "Ben",A='cherry', B='dog', C=None, D='triangle', E='low'),
    Row(name = "Ana",A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(name = "Ana",A='date', B=None, C='yellow', D=None, E='low'),
    Row(name = "Marta",A='cherry', B='fox', C='purple', D='hexagon', E='low'),
    Row(name = "Ana",A='fig', B='goat', C=None, D='octagon', E='high'),
    Row(name = "Ben",A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(name = "Marta",A='apple', B='iguana', C='pink', D=None, E='low'),
    Row(name = "Marta",A='apple', B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)


df = df.withColumn("new_column",collect_list("A").over(Window.partitionBy("name")))

new_column = df.select("new_column").collect()

print(new_column)
#[Row(new_column=['cherry', 'date', 'fig']), Row(new_column=['cherry', 'date', 'fig']), Row(new_column=['cherry', 'date', 'fig']), Row(new_column=['apple', 'banana', 'cherry', 'grape']), Row(new_column=['apple', 'banana', 'cherry', 'grape']), Row(new_column=['apple', 'banana', 'cherry', 'grape']), Row(new_column=['apple', 'banana', 'cherry', 'grape']), Row(new_column=['cherry', 'apple', 'apple']), Row(new_column=['cherry', 'apple', 'apple']), Row(new_column=['cherry', 'apple', 'apple'])]










df = df.withColumn("new_column",collect_list("A").over(Window.partitionBy("name")))

new_column = df.select("new_column").collect()

df = spark.createDataFrame(new_column)

df.show()
"""
+--------------------+
|          new_column|
+--------------------+
| [cherry, date, fig]|
| [cherry, date, fig]|
| [cherry, date, fig]|
|[apple, banana, c...|
|[apple, banana, c...|
|[apple, banana, c...|
|[apple, banana, c...|
|[cherry, apple, a...|
|[cherry, apple, a...|
|[cherry, apple, a...|
+--------------------+
"""

