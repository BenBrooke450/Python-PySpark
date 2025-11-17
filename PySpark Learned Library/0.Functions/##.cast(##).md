

# **1. What is `cast`?**

`cast` is a **PySpark SQL function** used to **convert a column from one data type to another**. Unlike `try_cast`, it **does not automatically handle invalid values**, so if a value cannot be converted, the behavior may depend on Spark’s configuration:

* By default, invalid conversions may **produce NULL** (for Spark SQL expressions).
* Some operations can throw errors depending on the data type and version.

`cast` is commonly used for **ETL transformations, feature engineering, and schema adjustments**.

---

# **2. Syntax**

```python
from pyspark.sql.functions import col

# Cast a column using withColumn
df = df.withColumn("new_col", col("old_col").cast("target_type"))
```

**Parameters:**

| Parameter        | Description                                                                        |
| ---------------- | ---------------------------------------------------------------------------------- |
| `col("old_col")` | The column you want to convert                                                     |
| `target_type`    | The target Spark SQL data type, e.g., `"int"`, `"double"`, `"date"`, `"timestamp"` |

**Returns:** A new column with the desired data type.

---

# **3. Behavior**

1. **Valid conversion → returns value in target type**
2. **Invalid conversion → typically returns NULL**
3. **NULL input → returns NULL**

Example:

| Input   | `cast("int")`           |
| ------- | ----------------------- |
| "123"   | 123                     |
| "12.34" | 12 (fraction truncated) |
| "abc"   | NULL                    |
| NULL    | NULL                    |

**Important:** `cast` will silently truncate or drop decimals for integer conversions. `try_cast` is safer when you want **explicit NULL for invalid values**.

---

# **4. Supported Data Types**

`cast` supports standard Spark SQL types:

* Numeric: `int`, `bigint`, `float`, `double`, `decimal`
* String: `string`
* Date/Time: `date`, `timestamp`
* Boolean: `boolean`

Also works with type objects:

```python
from pyspark.sql.types import IntegerType
df.withColumn("col_int", col("col_str").cast(IntegerType()))
```

---

# **5. Example Usage**

### **A. Convert string to integer**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

data = [("123",), ("abc",), ("456",), (None,)]
df = spark.createDataFrame(data, ["num_str"])

df2 = df.withColumn("num_int", col("num_str").cast("int"))
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
df2 = df.withColumn("num_double", col("num_str").cast("double"))
df2.show()
```

* "123" → 123.0
* "abc" → NULL
* Works for `float`, `decimal`, `double`

---

### **C. Convert string to date**

```python
data = [("2023-01-01",), ("not_a_date",)]
df = spark.createDataFrame(data, ["date_str"])

df2 = df.withColumn("date_col", col("date_str").cast("date"))
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

# **6. Comparison with `try_cast`**

| Feature                | `cast()`                                   | `try_cast()`                                 |
| ---------------------- | ------------------------------------------ | -------------------------------------------- |
| Safe for dirty data    | No (may throw errors depending on context) | Yes, always returns NULL if conversion fails |
| Invalid value          | NULL or exception                          | NULL                                         |
| Available in Spark SQL | Yes                                        | Spark ≥3.0                                   |

---

# **7. Notes & Best Practices**

1. `cast` is commonly used when the **data is mostly clean**, or you want standard Spark behavior for invalid values.
2. When dealing with **unknown or dirty data**, prefer `try_cast`.
3. Works with both **DataFrame API** (`col().cast()`) and **SQL expressions** (`expr("col AS target_type")`).
4. Can be chained for multiple column conversions.

---

# **8. Summary**

* `cast` = standard type conversion in PySpark
* Returns NULL for invalid conversions (or may throw exceptions in some contexts)
* Supports numeric, string, date/time, boolean
* Common for ETL, preprocessing, and feature engineering
* Less safe than `try_cast` when working with messy data

