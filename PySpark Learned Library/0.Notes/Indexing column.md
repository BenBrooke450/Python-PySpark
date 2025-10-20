

# Indexing in PySpark — Full Summary

PySpark **does not have a built-in “index”** like pandas.
But you can still create **row indices**, and you can access **columns by index position** programmatically.

Let’s break both down clearly 👇

---

## **1️⃣ Row Indexing (Creating Row Numbers or IDs)**

Since PySpark DataFrames are **distributed and unordered**, there’s no default index.
If you need one, here are the main methods:

---

### **A. `monotonically_increasing_id()`**

Adds a **unique numeric ID** column to each row.

```python
from pyspark.sql.functions import monotonically_increasing_id

df_indexed = df.withColumn("index", monotonically_increasing_id())
```

✅ Each row gets a unique 64-bit integer.
⚠️ Not sequential (due to partitions).
Example output:

| Name | Dept | index      |
| ---- | ---- | ---------- |
| John | IT   | 0          |
| Amy  | HR   | 1          |
| Max  | IT   | 8589934592 |

---

### **B. `zipWithIndex()` (Sequential Index)**

For **strictly sequential indices (0, 1, 2, 3 …)**:

```python
df_indexed = df.rdd.zipWithIndex().map(lambda x: (*x[0], x[1])).toDF(df.columns + ["index"])
```

✅ Produces sequential order
⚠️ Slower because it converts between RDD and DataFrame.

---

### **C. `row_number()` Window Function**

For **ordered** sequential numbering based on a column:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.orderBy("Salary")
df_indexed = df.withColumn("index", row_number().over(windowSpec))
```

✅ Sequential and deterministic
✅ You can order by specific columns
⚠️ More expensive for large data due to sorting.

---

## 🧮 **2️⃣ Column Indexing (Selecting Columns by Position)**

In PySpark, you can access columns by **name** or by **index position** using `df.columns`.

---

### **A. Select Columns by Index Range**

```python
X = df.select([df.columns[i] for i in range(0, 7)])
```

✅ Selects columns **0 through 6** (like slicing).
✅ Useful when you don’t want to type column names manually.

---

### **B. Select a Single Column by Index**

```python
col8 = df.select([df.columns[8]])
```

If you want it as a **Python list** (e.g., for labels `y`):

```python
y = [x[0] for x in df.select([df.columns[i] for i in [8]]).collect()]
```

🔍 Breakdown:

* `df.columns[i]` → gets column name by position (e.g., `"Salary"`).
* `df.select([...])` → selects it as a DataFrame.
* `.collect()` → brings all rows to driver (be careful with large data).
* `[x[0] for x in ...]` → extracts the column values into a flat Python list.

✅ Works well for small datasets or ML splits (X, y).
⚠️ Not scalable for big DataFrames (use `.rdd` for large ones).

---

## 🧠 **3️⃣ Accessing Data by Index or Value**

* **Row indexing** is manual (you must add your own index column).
* **Column indexing** can be done via `df.columns[i]`.
* There’s no `.iloc` or `.loc` like in pandas — you use `.filter()` or `.where()` instead.

Example:

```python
df_indexed.filter(df_indexed.index == 5).show()
```

---

## 🧾 **4️⃣ Summary Table**

| Goal                          | Method                                               | Example                                              | Notes                         |
| ----------------------------- | ---------------------------------------------------- | ---------------------------------------------------- | ----------------------------- |
| Add unique ID                 | `monotonically_increasing_id()`                      | `df.withColumn("id", monotonically_increasing_id())` | Fast, not sequential          |
| Add sequential index          | `zipWithIndex()`                                     | `df.rdd.zipWithIndex()`                              | Exact sequence, slower        |
| Ordered numbering             | `row_number()`                                       | `row_number().over(Window.orderBy("col"))`           | Deterministic, requires order |
| Select columns by index range | `df.select([df.columns[i] for i in range(a,b)])`     | Flexible for subsets                                 |                               |
| Select column as list         | `[x[0] for x in df.select(df.columns[i]).collect()]` | Good for small data                                  |                               |
| Access rows by index          | `df.filter(df.index == n)`                           | Manual filtering                                     |                               |

---

## ⚙️ **5️⃣ Key Takeaways**

✅ PySpark has **no built-in index** — you must create one manually.
✅ For **small datasets**, `.collect()` + list comprehensions are fine.
✅ For **large datasets**, avoid `.collect()` — use RDD operations or keep data distributed.
✅ Use **window functions** for ordered, sequential numbering when order matters.

