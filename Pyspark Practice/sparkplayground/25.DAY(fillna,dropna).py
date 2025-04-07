

# Initialize SparkSession
spark = SparkSession.builder.appName("ExtractNestedData").getOrCreate()

data = [
    Row(A='apple', B='cat', C='red', D=None, E='high'),
    Row(A='banana', B=None, C='blue', D='circle', E='medium'),
    Row(A=None, B='dog', C=None, D='triangle', E='low'),
    Row(A='cherry', B='elephant', C='green', D='square', E='high'),
    Row(A='date', B=None, C='yellow', D=None, E=None),
    Row(A=None, B='fox', C='purple', D='hexagon', E='low'),
    Row(A='fig', B='goat', C=None, D='octagon', E=None),
    Row(A='grape', B='horse', C='orange', D='rectangle', E='medium'),
    Row(A='honeydew', B='iguana', C='pink', D=None, E='high'),
    Row(A=None, B='jaguar', C='black', D='diamond', E='low')
]

df = spark.createDataFrame(data)


df.fillna("PLEASE FILL").show()
"""
+-----------+-----------+-----------+-----------+-----------+
|          A|          B|          C|          D|          E|
+-----------+-----------+-----------+-----------+-----------+
|      apple|        cat|        red|PLEASE FILL|       high|
|     banana|PLEASE FILL|       blue|     circle|     medium|
|PLEASE FILL|        dog|PLEASE FILL|   triangle|        low|
|     cherry|   elephant|      green|     square|       high|
|       date|PLEASE FILL|     yellow|PLEASE FILL|PLEASE FILL|
|PLEASE FILL|        fox|     purple|    hexagon|        low|
|        fig|       goat|PLEASE FILL|    octagon|PLEASE FILL|
|      grape|      horse|     orange|  rectangle|     medium|
|   honeydew|     iguana|       pink|PLEASE FILL|       high|
|PLEASE FILL|     jaguar|      black|    diamond|        low|
+-----------+-----------+-----------+-----------+-----------+
"""








df.dropna().show()
"""
+------+--------+------+---------+------+
|     A|       B|     C|        D|     E|
+------+--------+------+---------+------+
|cherry|elephant| green|   square|  high|
| grape|   horse|orange|rectangle|medium|
+------+--------+------+---------+------+
"""

























