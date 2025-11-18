

# **`row_number()` in PySpark — Detailed Summary**

`row_number()` is a **window function** in PySpark that assigns a **unique, sequential integer** (1, 2, 3, …) **to each row within a window**.

You import it with:

```python
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
```

---

# 1. **What `row_number()` Does**

* Generates **sequential numbers** starting from 1
* Restarts numbering in **each partition** (if you specify `partitionBy`)
* Follows the **ordering** you define with `orderBy`
* Guarantees **unique** row numbers within each window

Example:

```python
w = Window.partitionBy("group").orderBy("score")
df.withColumn("rn", row_number().over(w))
```

Produces:

```
group | score | rn
A     | 10    | 1
A     | 30    | 2
B     |  5    | 1
B     | 20    | 2
```

---

# 2. **What It Guarantees**

### ✔ Unique numbers per partition

Within each window, every row gets a unique rank.

### ✔ Strictly sequential (1, 2, 3 …)

No jumps, no gaps.

### ✔ Deterministic *if your orderBy is deterministic*

If you order by a well-defined column (e.g., timestamp, ID), results are stable.

---

# 3. **What It Does NOT Guarantee**

### ❌ No natural order if you don't specify orderBy

If you omit `orderBy`, Spark *will refuse to run*, because row_number needs an ordering.

### ❌ Not stable when the sort column has duplicates

If two rows tie on the orderBy column and you don't provide a secondary sort, Spark may order them arbitrarily.

### ❌ Not free — row_number() usually triggers a shuffle

Any time you order by a column, Spark may need to redistribute data.

---

# 4. **Global Row Number (no partitionBy)**

If you want a single sequence for the entire DataFrame:

```python
w = Window.orderBy("some_column")
df = df.withColumn("id", row_number().over(w))
```

This gives:

```
1, 2, 3, 4, ...
```

**This is the correct way to create sequential IDs** in Spark.

---

# 5. **Common Use Cases**

### ✔ Create a global sequential ID

Often used as a safer alternative to `monotonically_increasing_id()`.

### ✔ Deduplicate rows

Classic pattern:

```python
w = Window.partitionBy("user_id").orderBy(col("timestamp").desc())
df = df.withColumn("rn", row_number().over(w)) \
       .filter("rn = 1")
```

Keeps the most recent row per user.

### ✔ Paginate / sample top-N per group

Example: top 3 movies per genre.

### ✔ Stable ordering before writing to systems that require indexing

---

# 6. **Performance Characteristics**

* Uses **windowing and sorting**, so it can be expensive.
* Usually involves a **shuffle** unless:

  * Data is already partitioned *and* sorted on the partitioning columns.

---

# 7. **`row_number()` vs `monotonically_increasing_id()`**

| Feature             | `row_number()`    | `monotonically_increasing_id()` |
| ------------------- | ----------------- | ------------------------------- |
| Sequential (1,2,3…) | ✔ Yes             | ❌ No                            |
| Ordered             | ✔ Yes             | ❌ No                            |
| Unique              | ✔ Yes             | ✔ Yes                           |
| Requires sort       | ✔ Yes             | ❌ No                            |
| Global stable ID    | ✔ Yes             | ❌ No                            |
| Fast / no shuffle   | ❌ No (needs sort) | ✔ Yes                           |

Use **row_number** when you need *sequence / ordering*.
Use **monotonically_increasing_id** when you need *unique IDs without caring about order*.

---

# **Summary (Short Version)**

**`row_number()`** assigns sequential numbers to rows **within a window** based on an **order**.
It is deterministic, produces clean 1-based sequences, and is the right tool for sequential indexing, deduplication, and top-N queries.

---

If you want, I can also give you:

* A diagram of how window partitions work
* Example notebooks showing real-world usages
* A guide to generating fast sequential IDs on massive datasets
