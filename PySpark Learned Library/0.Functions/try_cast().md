

# **1. What is `try_cast`?**

`try_cast` is a **PySpark SQL function** introduced in Spark 3.0. Its purpose is to **safely convert a column from one data type to another** without throwing an error if the conversion fails.

* Returns `NULL` for rows where conversion fails.
* Unlike `cast()`, which will **throw an exception** if a value cannot be converted (depending on SQL configuration), `try_cast` is **safe and fail-tolerant**.

It is very useful when dealing with **dirty data**, such as strings that may contain invalid numbers or malformed dates.

---

# **2. Syntax**

```python
from pyspark.sql.functions import expr, try_cast

# Using expr
df = df.withColumn("new_col", expr("try_cast(old_col AS target_type)"))

# Using try_cast directly
df = df.withColumn("new_col", try_cast("old_col", "target_type"))
```

**Parameters:**

| Parameter     | Description                                                                             |
| ------------- | --------------------------------------------------------------------------------------- |
| `col`         | The column you want to convert (string or Column object).                               |
| `target_type` | The Spark SQL data type to cast to (e.g., `"int"`, `"double"`, `"date"`, `"timestamp"`) |

**Returns:** A column of the `target_type` or `NULL` if conversion fails.

---

# **3. Behavior**

1. **Valid conversion → returns value in target type**
2. **Invalid conversion → returns NULL**
3. **NULL input → returns NULL**

Example:

| Input   | `try_cast(col AS INT)` |
| ------- | ---------------------- |
| "123"   | 123                    |
| "12.34" | NULL                   |
| "abc"   | NULL                   |
| NULL    | NULL                   |

---

# **4. Supported Data Types**

`try_cast` supports all standard Spark SQL types:

* Numeric types: `int`, `bigint`, `float`, `double`, `decimal`
* String types: `string`, `varchar`
* Date/Time: `date`, `timestamp`
* Boolean: `boolean`

You can also use Spark SQL type objects:

```python
from pyspark.sql.types import IntegerType
df.withColumn("col_int", try_cast("col_str", IntegerType()))
```

---

# **5. Example Usage**

### **A. Convert string to integer safely**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import try_cast

spark = SparkSession.builder.getOrCreate()

data = [("123",), ("abc",), ("456",), (None,)]
df = spark.createDataFrame(data, ["num_str"])

df2 = df.withColumn("num_int", try_cast("num_str", "int"))
df2.show()
```

Output:

```
+-------+-------+
|num_str|num_int|
+-------+-------+
|    123|    123|
|    abc|   null|
|    456|    456|
|   null|   null|
+-------+-------+
```

---

### **B. Convert string to double**

```python
df2 = df.withColumn("num_double", try_cast("num_str", "double"))
df2.show()
```

* Strings that can’t convert (e.g., "abc") become NULL.
* Works for floats, decimals, etc.

---

### **C. Convert string to date**

```python
data = [("2023-01-01",), ("not_a_date",)]
df = spark.createDataFrame(data, ["date_str"])

df2 = df.withColumn("date_col", try_cast("date_str", "date"))
df2.show()
```

Output:

```
+------------+----------+
|    date_str|  date_col|
+------------+----------+
|  2023-01-01|2023-01-01|
|not_a_date  |      null|
+------------+----------+
```

---

# **6. Comparison with `cast`**

| Feature                | `cast()`                                                    | `try_cast()` |
| ---------------------- | ----------------------------------------------------------- | ------------ |
| Invalid value          | Throws error (sometimes fails silently depending on config) | Returns NULL |
| Safe for dirty data    | No                                                          | Yes          |
| Available in Spark SQL | Yes                                                         | Spark ≥3.0   |

---

# **7. Notes & Best Practices**

1. **Useful for ETL pipelines** with dirty data.
2. Can be chained with `fillna()` to replace NULLs after conversion:

```python
df.withColumn("col_int", try_cast("col_str", "int")).fillna(0, ["col_int"])
```

3. Always prefer **try_cast** when reading unknown CSV or JSON columns to avoid job failures.
4. Works with both **DataFrame API** (`withColumn`) and **SQL expressions** (`expr("try_cast(...)")`).

---

# **8. Summary**

* `try_cast` = **safe, fail-tolerant cast**.
* Returns `NULL` if conversion fails.
* Very helpful for ETL, cleaning, and preparing features for ML.
* Supported types: numeric, boolean, date/timestamp, string.
* Requires Spark **3.0 or higher**.


