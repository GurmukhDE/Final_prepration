# 🚀 PySpark Day 3 — Practice Problems

> **Topics Covered:** Joins · Aggregations · Window Functions · Date & String Operations · Bonus Challenge

---

## ⚙️ Setup — Run This First

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Day3Practice").getOrCreate()

# --- Sample DataFrames ---

employees_data = [
    (1, "Alice",   "Engineering", 95000,  "2020-03-15"),
    (2, "Bob",     "Marketing",   72000,  "2019-07-01"),
    (3, "Charlie", "Engineering", 110000, "2018-01-10"),
    (4, "Diana",   "HR",          65000,  "2021-06-20"),
    (5, "Eve",     "Marketing",   80000,  "2020-11-05"),
    (6, "Frank",   "Engineering", 88000,  "2022-02-28"),
    (7, "Grace",   "HR",          70000,  "2017-09-14"),
    (8, "Hank",    "Marketing",   76000,  "2023-01-01"),
]

departments_data = [
    ("Engineering", "Alice Manager", "New York"),
    ("Marketing",   "Bob Manager",   "Chicago"),
    ("HR",          "Carol Manager", "Austin"),
    ("Finance",     "Dave Manager",  "Boston"),   # No employees in Finance
]

sales_data = [
    (1, "2024-01", 5000),
    (1, "2024-02", 7000),
    (1, "2024-03", 6500),
    (2, "2024-01", 3000),
    (2, "2024-02", 4500),
    (2, "2024-03", 4000),
    (3, "2024-01", 8000),
    (3, "2024-02", 9500),
    (3, "2024-03", 7000),
]

emp_schema   = ["emp_id", "name", "department", "salary", "hire_date"]
dept_schema  = ["department", "manager", "location"]
sales_schema = ["emp_id", "month", "sales_amount"]

employees   = spark.createDataFrame(employees_data,   emp_schema)
departments = spark.createDataFrame(departments_data, dept_schema)
sales       = spark.createDataFrame(sales_data,       sales_schema)
```

---

## 🟡 Section 1: Joins

### Q1 — Inner Join

Get all employees along with their department location.  
Show `name`, `department`, `location`.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
employees.join(departments, on="department", how="inner") \
    .select("name", "department", "location") \
    .show()
```

</details>

---

### Q2 — Anti Join / Right Join

Find departments that have **no employees**.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
# Option A — Right Join
employees.join(departments, on="department", how="right") \
    .filter(F.col("emp_id").isNull()) \
    .select("department", "location") \
    .show()

# Option B — Left Anti Join (cleaner)
departments.join(employees, on="department", how="left_anti").show()
```

</details>

---

### Q3 — Join + Aggregation

Join employees with sales and show each employee's **total sales** alongside their **salary**.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
total_sales = sales.groupBy("emp_id").agg(F.sum("sales_amount").alias("total_sales"))

employees.join(total_sales, on="emp_id", how="left") \
    .select("name", "salary", "total_sales") \
    .show()
```

</details>

---

## 🟠 Section 2: Aggregations

### Q4 — Multi-Aggregation

For each department, find the **average salary**, **max salary**, and **employee count**.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
employees.groupBy("department").agg(
    F.round(F.avg("salary"), 2).alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.count("emp_id").alias("emp_count")
).show()
```

</details>

---

### Q5 — Filter After Aggregation (HAVING equivalent)

Find departments where the **average salary > 80,000**.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
employees.groupBy("department") \
    .agg(F.avg("salary").alias("avg_salary")) \
    .filter(F.col("avg_salary") > 80000) \
    .show()
```

</details>

---

## 🔴 Section 3: Window Functions

### Q6 — Rank Within Partition

Rank employees **within each department** by salary (highest = rank 1).

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
window = Window.partitionBy("department").orderBy(F.desc("salary"))

employees.withColumn("salary_rank", F.rank().over(window)) \
    .select("name", "department", "salary", "salary_rank") \
    .show()
```

</details>

---

### Q7 — Compare to Department Average

For each employee, show their salary vs the **department average** and the **difference**.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
window = Window.partitionBy("department")

employees \
    .withColumn("dept_avg", F.round(F.avg("salary").over(window), 2)) \
    .withColumn("diff_from_avg", F.round(F.col("salary") - F.col("dept_avg"), 2)) \
    .select("name", "department", "salary", "dept_avg", "diff_from_avg") \
    .show()
```

</details>

---

### Q8 — Running Total

Calculate a **running total of sales** per employee, ordered by month.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
window = Window.partitionBy("emp_id") \
               .orderBy("month") \
               .rowsBetween(Window.unboundedPreceding, Window.currentRow)

sales.withColumn("running_total", F.sum("sales_amount").over(window)).show()
```

</details>

---

### Q9 — Top Earner Per Department

Find the **top earner per department** using `dense_rank()`.

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
window = Window.partitionBy("department").orderBy(F.desc("salary"))

employees.withColumn("rnk", F.dense_rank().over(window)) \
    .filter(F.col("rnk") == 1) \
    .drop("rnk") \
    .show()
```

</details>

---

## 🔵 Section 4: Date & String Operations

### Q10 — Years of Tenure

Calculate how many **years each employee has been at the company** (from `hire_date` to today).

```python
# Your code here
```

<details>
<summary>💡 Solution</summary>

```python
employees.withColumn(
    "years_at_company",
    F.floor(F.datediff(F.current_date(), F.to_date("hire_date")) / 365)
).select("name", "hire_date", "years_at_company").show()
```

</details>

---

## 🏁 Bonus Challenge — Sales Efficiency

> **Goal:** For each department, find the employee with the highest **sales-to-salary ratio**.

**Steps to follow:**
1. Aggregate total sales per employee from the `sales` DataFrame
2. Join with `employees`
3. Calculate `total_sales / salary` as `efficiency_ratio`
4. Use a window function to rank within department
5. Filter to rank 1 per department

```python
# Try it yourself — solution below when you're ready!
```

<details>
<summary>💡 Solution</summary>

```python
# Step 1 — Total sales per employee
total_sales = sales.groupBy("emp_id").agg(F.sum("sales_amount").alias("total_sales"))

# Step 2 & 3 — Join and compute ratio
combined = employees.join(total_sales, on="emp_id", how="left") \
    .withColumn("efficiency_ratio", F.round(F.col("total_sales") / F.col("salary"), 4))

# Step 4 — Rank within department
window = Window.partitionBy("department").orderBy(F.desc("efficiency_ratio"))

# Step 5 — Filter top 1
combined.withColumn("rnk", F.rank().over(window)) \
    .filter(F.col("rnk") == 1) \
    .select("name", "department", "salary", "total_sales", "efficiency_ratio") \
    .show()
```

</details>

---

## 📌 Concepts Quick Reference

| Topic | Key Functions |
|---|---|
| **Joins** | `inner`, `left`, `right`, `left_anti` |
| **Aggregations** | `groupBy()`, `agg()`, `sum()`, `avg()`, `count()`, `max()` |
| **Window Functions** | `rank()`, `dense_rank()`, `sum().over()`, `avg().over()` |
| **Window Spec** | `partitionBy()`, `orderBy()`, `rowsBetween()` |
| **Date Ops** | `datediff()`, `to_date()`, `current_date()` |
| **Filter Post-Agg** | `.filter()` / `.where()` on aggregated columns |

---

## 🗺️ Learning Path

```
Day 1 → RDDs, SparkContext basics
Day 2 → DataFrames, Schema, Basic transformations
Day 3 → Joins, Aggregations, Window Functions  ← You are here
Day 4 → UDFs, Broadcast Joins, Performance Tuning
Day 5 → Spark SQL, Partitioning, Caching strategies
```

---

*Happy Sparking! ⚡ Open an issue or PR if you spot improvements.*
