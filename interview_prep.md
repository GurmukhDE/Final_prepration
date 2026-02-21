# 🚀 Interview Preparation Guide
### Python | SQL | PySpark — GlobalLogic & NAB (Sr. Data Engineer)

---

## 📋 Table of Contents

- [🐍 Python Coding Questions](#-python-coding-questions)
- [🗄️ SQL Coding Questions](#️-sql-coding-questions)
- [⚡ PySpark Coding Questions](#-pyspark-coding-questions)
- [🎯 Interview Day Tips](#-interview-day-tips)

---

## 🐍 Python Coding Questions

---

### 1. Reverse a String Without Slicing

```python
def reverse_string(s):
    result = ""
    for char in s:
        result = char + result
    return result

print(reverse_string("hello"))  # olleh
```

---

### 2. Check if a String is a Palindrome

```python
def is_palindrome(s):
    s = s.lower().replace(" ", "")
    return s == s[::-1]

print(is_palindrome("racecar"))  # True
```

---

### 3. Find Duplicates in a List

```python
def find_duplicates(lst):
    seen = set()
    duplicates = set()
    for item in lst:
        if item in seen:
            duplicates.add(item)
        seen.add(item)
    return list(duplicates)

print(find_duplicates([1, 2, 3, 2, 4, 3, 5]))  # [2, 3]
```

---

### 4. Find the Second Largest Number in a List

```python
def second_largest(lst):
    unique = list(set(lst))
    unique.sort()
    return unique[-2]

print(second_largest([10, 20, 4, 45, 99]))  # 45
```

---

### 5. Count Frequency of Each Word in a String

```python
from collections import Counter

def word_frequency(s):
    words = s.lower().split()
    return dict(Counter(words))

print(word_frequency("hello world hello python"))
# {'hello': 2, 'world': 1, 'python': 1}
```

---

### 6. Flatten a Nested List

```python
def flatten(lst):
    result = []
    for item in lst:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result

print(flatten([1, [2, [3, 4], 5], 6]))  # [1, 2, 3, 4, 5, 6]
```

---

### 7. FizzBuzz

```python
def fizzbuzz(n):
    for i in range(1, n + 1):
        if i % 15 == 0:
            print("FizzBuzz")
        elif i % 3 == 0:
            print("Fizz")
        elif i % 5 == 0:
            print("Buzz")
        else:
            print(i)

fizzbuzz(20)
```

---

### 8. Fibonacci Sequence

```python
def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        print(a, end=" ")
        a, b = b, a + b

fibonacci(10)  # 0 1 1 2 3 5 8 13 21 34
```

---

### 9. Check if Two Strings are Anagrams

```python
def is_anagram(s1, s2):
    return sorted(s1.lower()) == sorted(s2.lower())

print(is_anagram("listen", "silent"))  # True
```

---

### 10. Find Missing Number in a List (1 to N)

```python
def find_missing(lst, n):
    expected = n * (n + 1) // 2
    return expected - sum(lst)

print(find_missing([1, 2, 4, 5, 6], 6))  # 3
```

---

### 11. Remove Duplicates While Maintaining Order

```python
def remove_duplicates(lst):
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]

print(remove_duplicates([1, 2, 3, 2, 4, 1, 5]))  # [1, 2, 3, 4, 5]
```

---

### 12. Lambda + Map + Filter

```python
nums = [1, 2, 3, 4, 5, 6]

# Square of even numbers
result = list(map(lambda x: x**2, filter(lambda x: x % 2 == 0, nums)))
print(result)  # [4, 16, 36]
```

---

### 13. Decorator Example

```python
def my_decorator(func):
    def wrapper(*args, **kwargs):
        print("Before function call")
        result = func(*args, **kwargs)
        print("After function call")
        return result
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()
# Before function call
# Hello!
# After function call
```

---

### 14. OOP — Class with Inheritance

```python
class Animal:
    def __init__(self, name):
        self.name = name

    def speak(self):
        return "Some sound"


class Dog(Animal):
    def speak(self):
        return f"{self.name} says Woof!"


class Cat(Animal):
    def speak(self):
        return f"{self.name} says Meow!"


d = Dog("Rex")
print(d.speak())  # Rex says Woof!

c = Cat("Whiskers")
print(c.speak())  # Whiskers says Meow!
```

---

### 15. Read a CSV and Find Max Salary using Pandas

```python
import pandas as pd

df = pd.read_csv("employees.csv")
max_salary = df["salary"].max()
top_earner = df[df["salary"] == max_salary]
print(top_earner)
```

---

### 16. Handle Exceptions Properly

```python
def divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Cannot divide by zero"
    except TypeError:
        return "Invalid input types"
    finally:
        print("Division attempted")

print(divide(10, 0))   # Cannot divide by zero
print(divide(10, 2))   # 5.0
```

---

### 17. List Comprehension with Condition

```python
# Get squares of odd numbers from 1 to 20
squares = [x**2 for x in range(1, 21) if x % 2 != 0]
print(squares)
# [1, 9, 25, 49, 81, 121, 169, 225, 289, 361]
```

---

### 18. Generator Function

```python
def number_generator(n):
    for i in range(n):
        yield i * i

gen = number_generator(5)
for val in gen:
    print(val)  # 0, 1, 4, 9, 16
```

---

### 19. Merge Two Sorted Lists

```python
def merge_sorted(l1, l2):
    result = []
    i = j = 0
    while i < len(l1) and j < len(l2):
        if l1[i] < l2[j]:
            result.append(l1[i])
            i += 1
        else:
            result.append(l2[j])
            j += 1
    result.extend(l1[i:])
    result.extend(l2[j:])
    return result

print(merge_sorted([1, 3, 5], [2, 4, 6]))  # [1, 2, 3, 4, 5, 6]
```

---

### 20. Count Vowels in a String

```python
def count_vowels(s):
    return sum(1 for char in s.lower() if char in "aeiou")

print(count_vowels("Hello World"))  # 3
```

---

## 🗄️ SQL Coding Questions

---

### 1. Find the Second Highest Salary

```sql
SELECT MAX(salary) AS second_highest
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
```

---

### 2. Find Nth Highest Salary (Using DENSE_RANK)

```sql
-- Change 3 to N for Nth highest
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked
WHERE rnk = 3;
```

---

### 3. Find Duplicate Records

```sql
SELECT email, COUNT(*) AS cnt
FROM employees
GROUP BY email
HAVING COUNT(*) > 1;
```

---

### 4. Delete Duplicate Rows Keeping One

```sql
DELETE FROM employees
WHERE id NOT IN (
    SELECT MIN(id)
    FROM employees
    GROUP BY email
);
```

---

### 5. Employees Who Earn More Than Their Manager

```sql
SELECT
    e.name    AS employee,
    e.salary  AS emp_salary,
    m.name    AS manager,
    m.salary  AS mgr_salary
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

---

### 6. Running Total Using Window Function

```sql
SELECT
    employee_id,
    salary,
    SUM(salary) OVER (ORDER BY employee_id) AS running_total
FROM employees;
```

---

### 7. Rank Employees by Salary Within Each Department

```sql
SELECT
    name,
    department,
    salary,
    RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
FROM employees;
```

> **Key difference:**
> - `RANK()` — skips numbers after a tie (1, 1, 3)
> - `DENSE_RANK()` — no gaps after a tie (1, 1, 2)
> - `ROW_NUMBER()` — always unique (1, 2, 3)

---

### 8. Find Employees Who Joined in the Last 30 Days

```sql
SELECT *
FROM employees
WHERE join_date >= CURRENT_DATE - INTERVAL '30 days';
```

---

### 9. Pivot — Count Employees Per Department Per Year

```sql
SELECT
    department,
    SUM(CASE WHEN YEAR(join_date) = 2022 THEN 1 ELSE 0 END) AS "2022",
    SUM(CASE WHEN YEAR(join_date) = 2023 THEN 1 ELSE 0 END) AS "2023",
    SUM(CASE WHEN YEAR(join_date) = 2024 THEN 1 ELSE 0 END) AS "2024"
FROM employees
GROUP BY department;
```

---

### 10. Find Departments with No Employees

```sql
SELECT d.department_name
FROM departments d
LEFT JOIN employees e ON d.id = e.department_id
WHERE e.id IS NULL;
```

---

### 11. Cumulative Distribution / Percentile

```sql
SELECT
    name,
    salary,
    PERCENT_RANK() OVER (ORDER BY salary) AS percentile_rank,
    CUME_DIST()    OVER (ORDER BY salary) AS cumulative_dist
FROM employees;
```

---

### 12. LAG and LEAD — Month Over Month Sales Comparison

```sql
SELECT
    month,
    sales,
    LAG(sales)  OVER (ORDER BY month)            AS prev_month_sales,
    LEAD(sales) OVER (ORDER BY month)            AS next_month_sales,
    sales - LAG(sales) OVER (ORDER BY month)     AS growth
FROM monthly_sales;
```

---

### 13. CTE — Top 3 Salaries Per Department

```sql
WITH ranked AS (
    SELECT
        name,
        department,
        salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
    FROM employees
)
SELECT *
FROM ranked
WHERE rnk <= 3;
```

---

### 14. Self Join — Find Employees in the Same Department

```sql
SELECT
    a.name AS emp1,
    b.name AS emp2,
    a.department
FROM employees a
JOIN employees b
    ON a.department = b.department
    AND a.id < b.id;
```

---

### 15. Find Customers Who Never Placed an Order

```sql
SELECT c.customer_name
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.id IS NULL;
```

---

### 16. Find Total Sales Per Month (Date Formatting)

```sql
SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    SUM(amount)                       AS total_sales
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY month;
```

---

### 17. Find Employees with the Same Salary

```sql
SELECT salary, COUNT(*) AS count, GROUP_CONCAT(name) AS employees
FROM employees
GROUP BY salary
HAVING COUNT(*) > 1;
```

---

### 18. Update Salary with a 10% Hike for a Department

```sql
UPDATE employees
SET salary = salary * 1.10
WHERE department = 'Engineering';
```

---

### 19. COALESCE — Handle NULL Values

```sql
SELECT
    name,
    COALESCE(phone, email, 'No contact info') AS contact
FROM employees;
```

---

### 20. Recursive CTE — Org Hierarchy

```sql
WITH RECURSIVE org_hierarchy AS (
    -- Base case: top-level manager
    SELECT id, name, manager_id, 1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case
    SELECT e.id, e.name, e.manager_id, oh.level + 1
    FROM employees e
    JOIN org_hierarchy oh ON e.manager_id = oh.id
)
SELECT * FROM org_hierarchy ORDER BY level;
```

---

## ⚡ PySpark Coding Questions

---

### 1. Create a SparkSession and DataFrame

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InterviewPrep") \
    .getOrCreate()

data = [("Alice", 30, 50000), ("Bob", 25, 40000), ("Carol", 35, 60000)]
columns = ["name", "age", "salary"]

df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()
```

---

### 2. Filter Rows

```python
# Method 1 — Column expression
df.filter(df.salary > 45000).show()

# Method 2 — String expression
df.filter("salary > 45000").show()

# Method 3 — Multiple conditions
from pyspark.sql.functions import col
df.filter((col("salary") > 40000) & (col("age") < 35)).show()
```

---

### 3. GroupBy and Aggregation

```python
from pyspark.sql.functions import avg, count, max, min, sum

df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    count("id").alias("total_employees"),
    max("salary").alias("max_salary")
).show()
```

---

### 4. Add a New Column

```python
from pyspark.sql.functions import col, lit, when

# Simple calculation
df = df.withColumn("salary_after_tax", col("salary") * 0.8)

# Conditional column
df = df.withColumn("grade",
    when(col("salary") > 55000, "A")
    .when(col("salary") > 45000, "B")
    .otherwise("C")
)

df.show()
```

---

### 5. Window Function — Rank by Salary per Department

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, desc

window_spec = Window.partitionBy("department").orderBy(desc("salary"))

df = df.withColumn("rank",        rank().over(window_spec)) \
       .withColumn("dense_rank",  dense_rank().over(window_spec)) \
       .withColumn("row_number",  row_number().over(window_spec))

df.show()
```

---

### 6. Join Two DataFrames

```python
emp_df = spark.createDataFrame([
    (1, "Alice", 10),
    (2, "Bob",   20),
    (3, "Carol", 10)
], ["id", "name", "dept_id"])

dept_df = spark.createDataFrame([
    (10, "Engineering"),
    (20, "Marketing")
], ["dept_id", "dept_name"])

# Inner Join
inner = emp_df.join(dept_df, on="dept_id", how="inner")

# Left Join
left = emp_df.join(dept_df, on="dept_id", how="left")

inner.show()
left.show()
```

---

### 7. Handle Null Values

```python
# Drop rows with any nulls
df.dropna().show()

# Drop rows where specific columns have nulls
df.dropna(subset=["salary", "name"]).show()

# Fill nulls
df.fillna({"salary": 0, "name": "Unknown"}).show()

# Replace nulls using when/otherwise
from pyspark.sql.functions import when, col
df = df.withColumn("salary", when(col("salary").isNull(), 0).otherwise(col("salary")))
```

---

### 8. Read and Write CSV / Parquet

```python
# Read CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("data.csv")

# Write as Parquet (preferred for big data)
df.write.mode("overwrite").parquet("output/data.parquet")

# Read Parquet
df2 = spark.read.parquet("output/data.parquet")

# Write partitioned Parquet
df.write.mode("overwrite").partitionBy("department").parquet("output/partitioned/")
```

---

### 9. UDF — User Defined Function

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_salary(salary):
    if salary > 50000:
        return "High"
    elif salary > 30000:
        return "Medium"
    else:
        return "Low"

# Register UDF
categorize_udf = udf(categorize_salary, StringType())

df = df.withColumn("category", categorize_udf(col("salary")))
df.show()
```

> ⚠️ **Interview Tip:** Prefer built-in PySpark functions over UDFs — built-ins are optimized by Catalyst engine and much faster. Use UDFs only when no built-in alternative exists.

---

### 10. Broadcast Join (Large + Small Table)

```python
from pyspark.sql.functions import broadcast

# Broadcast the smaller DataFrame to avoid shuffle
result = large_df.join(broadcast(small_df), on="id", how="inner")
result.show()
```

> **When to use:** When one table is small enough to fit in memory (< a few hundred MB). Avoids expensive shuffle joins.

---

### 11. Remove Duplicates

```python
# Drop all duplicate rows
df.dropDuplicates().show()

# Drop duplicates based on specific columns
df.dropDuplicates(["email"]).show()

# Count duplicates
total = df.count()
distinct = df.dropDuplicates(["email"]).count()
print(f"Duplicates: {total - distinct}")
```

---

### 12. Repartition vs Coalesce

```python
# Repartition — increases OR decreases partitions (causes full shuffle)
df = df.repartition(10)

# Repartition by column (good for partitioned writes)
df = df.repartition(10, col("department"))

# Coalesce — only REDUCES partitions (no full shuffle, more efficient)
df = df.coalesce(2)

print(df.rdd.getNumPartitions())
```

> **Key rule:** Use `coalesce` to reduce partitions (efficient). Use `repartition` to increase or when you need even distribution.

---

### 13. Cache and Persist a DataFrame

```python
# Cache (stores in memory)
df.cache()
df.count()  # Triggers the cache

# Persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
df.unpersist()
```

---

### 14. Run SQL on a DataFrame

```python
# Register as temp view
df.createOrReplaceTempView("employees")

# Run SQL
result = spark.sql("""
    SELECT
        department,
        AVG(salary)  AS avg_salary,
        COUNT(*)     AS total_employees
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")

result.show()
```

---

### 15. LAG and LEAD in PySpark

```python
from pyspark.sql.functions import lag, lead
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department").orderBy("month")

df = df.withColumn("prev_month_sales", lag("sales", 1).over(window_spec)) \
       .withColumn("next_month_sales", lead("sales", 1).over(window_spec)) \
       .withColumn("growth", col("sales") - lag("sales", 1).over(window_spec))

df.show()
```

---

### 16. Running Total in PySpark

```python
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

window_spec = Window.partitionBy("department").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

df = df.withColumn("running_total", spark_sum("sales").over(window_spec))
df.show()
```

---

### 17. Explode — Flatten Array Column

```python
from pyspark.sql.functions import explode, col

data = [("Alice", ["Python", "SQL", "Spark"]),
        ("Bob",   ["Java", "Scala"])]

df = spark.createDataFrame(data, ["name", "skills"])

df_exploded = df.withColumn("skill", explode(col("skills"))).drop("skills")
df_exploded.show()
# Each skill becomes its own row
```

---

### 18. Pivot Table in PySpark

```python
df.groupBy("department") \
  .pivot("year") \
  .agg({"salary": "avg"}) \
  .show()
```

---

### 19. Schema Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id",         IntegerType(), nullable=False),
    StructField("name",       StringType(),  nullable=True),
    StructField("salary",     DoubleType(),  nullable=True),
    StructField("department", StringType(),  nullable=True)
])

df = spark.read.schema(schema).csv("employees.csv", header=True)
df.printSchema()
```

---

### 20. Word Count (Classic PySpark Problem)

```python
from pyspark.sql.functions import split, explode, lower, col

text_df = spark.createDataFrame([
    ("Hello world hello Python",),
    ("PySpark is great and Python is fun",)
], ["text"])

word_count = text_df \
    .withColumn("word", explode(split(lower(col("text")), " "))) \
    .groupBy("word") \
    .count() \
    .orderBy("count", ascending=False)

word_count.show()
```

---

## 🎯 Interview Day Tips

### General
- Always **explain your thought process** before writing code — interviewers care about how you think.
- Start with a **brute force approach**, then optimize — shows analytical thinking.
- **Ask clarifying questions** — edge cases, data size, expected output format.

### For SQL (NAB)
- Mention **indexes and query optimization** — shows senior-level awareness.
- Know the difference between `WHERE` vs `HAVING`, `UNION` vs `UNION ALL`.
- For complex queries, always **use CTEs** — cleaner and more readable than subqueries.

### For PySpark (NAB Sr. Data Engineer)
- Prefer **built-in functions** over UDFs — Catalyst optimizer can't optimize UDFs.
- Mention **broadcast joins** for small table joins — shows performance awareness.
- Know when to use **repartition vs coalesce**.
- Be ready to discuss **data pipeline design**, **partitioning strategies**, and **handling skewed data**.

### Concepts to Mention to Impress Interviewers
- **Lazy evaluation** in PySpark — transformations are lazy, actions trigger execution.
- **DAG (Directed Acyclic Graph)** — how Spark builds execution plans.
- **Catalyst Optimizer** — Spark's query optimizer.
- **Tungsten Engine** — Spark's memory and CPU optimization engine.
- **Delta Lake / Data Lakehouse** — modern data engineering architecture.

---

> 💪 **Best of luck with GlobalLogic & NAB! You've got this!**

---

*Prepared for interview on Feb 2026 | Python • SQL • PySpark*
