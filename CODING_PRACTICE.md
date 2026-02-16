# 💻 Data Engineer Coding Practice Guide

**65 Real Interview Questions with Solutions**

---

## 📋 Table of Contents

1. [SQL Coding Problems (30 Questions)](#sql-coding-problems)
   - [Easy Level (1-10)](#easy-level-sql)
   - [Medium Level (11-20)](#medium-level-sql)
   - [Hard Level (21-30)](#hard-level-sql)
2. [PySpark Coding Problems (20 Questions)](#pyspark-coding-problems)
   - [DataFrame Operations (31-40)](#dataframe-operations)
   - [Advanced Transformations (41-50)](#advanced-transformations)
3. [Python Coding Problems (15 Questions)](#python-coding-problems)
   - [Data Structures & Algorithms (51-60)](#data-structures--algorithms)
   - [Data Engineering Specific (61-65)](#data-engineering-specific)
4. [Practice Tips & Strategies](#practice-tips--strategies)

---

## 💾 SQL Coding Problems

> **Note:** These are real interview questions asked at banks and financial institutions. Practice writing these queries by hand first, then verify in a database.

### Easy Level SQL

#### Question 1: Find all customers who made transactions above $10,000

**Table:** `transactions (customer_id, amount, transaction_date)`

```sql
SELECT DISTINCT customer_id
FROM transactions
WHERE amount > 10000;
```

---

#### Question 2: Count number of transactions per customer

**Table:** `transactions (customer_id, amount, transaction_date)`

```sql
SELECT 
    customer_id,
    COUNT(*) as transaction_count
FROM transactions
GROUP BY customer_id
ORDER BY transaction_count DESC;
```

---

#### Question 3: Find customers with no transactions

**Tables:** `customers (customer_id, name)`, `transactions (customer_id, amount)`

```sql
-- Method 1: Using LEFT JOIN
SELECT 
    c.customer_id,
    c.name
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.customer_id IS NULL;

-- Method 2: Using NOT EXISTS
SELECT customer_id, name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 
    FROM transactions t 
    WHERE t.customer_id = c.customer_id
);
```

---

#### Question 4: Get total amount spent by each customer in last 30 days

```sql
SELECT 
    customer_id,
    SUM(amount) as total_spent
FROM transactions
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY customer_id
ORDER BY total_spent DESC;
```

---

#### Question 5: Find customers who made transactions on consecutive days

```sql
SELECT DISTINCT 
    t1.customer_id
FROM transactions t1
JOIN transactions t2 
    ON t1.customer_id = t2.customer_id
    AND t2.transaction_date = t1.transaction_date + INTERVAL '1 day';
```

---

#### Question 6: Find the average transaction amount per month

```sql
SELECT 
    DATE_TRUNC('month', transaction_date) as month,
    AVG(amount) as avg_amount,
    COUNT(*) as transaction_count
FROM transactions
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month DESC;
```

---

#### Question 7: Find duplicate email addresses

**Table:** `customers (customer_id, name, email)`

```sql
SELECT 
    email,
    COUNT(*) as count
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;
```

---

#### Question 8: Get top 5 customers by total spending

```sql
SELECT 
    c.customer_id,
    c.name,
    SUM(t.amount) as total_spent
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC
LIMIT 5;
```

---

#### Question 9: Find accounts with negative balance

**Table:** `accounts (account_id, customer_id, balance)`

```sql
SELECT 
    account_id,
    customer_id,
    balance
FROM accounts
WHERE balance < 0
ORDER BY balance ASC;
```

---

#### Question 10: Calculate month-over-month growth in transactions

```sql
WITH monthly_counts AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) as month,
        COUNT(*) as txn_count
    FROM transactions
    GROUP BY DATE_TRUNC('month', transaction_date)
)
SELECT 
    month,
    txn_count,
    LAG(txn_count) OVER (ORDER BY month) as prev_month_count,
    txn_count - LAG(txn_count) OVER (ORDER BY month) as growth
FROM monthly_counts
ORDER BY month;
```

---

### Medium Level SQL

#### Question 11: Find customers who made transactions in all 12 months of 2024

```sql
SELECT customer_id
FROM transactions
WHERE EXTRACT(YEAR FROM transaction_date) = 2024
GROUP BY customer_id
HAVING COUNT(DISTINCT EXTRACT(MONTH FROM transaction_date)) = 12;
```

---

#### Question 12: Get running total of transactions for each customer

```sql
SELECT 
    customer_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY transaction_date
    ) as running_total
FROM transactions
ORDER BY customer_id, transaction_date;
```

---

#### Question 13: Find the 2nd highest transaction amount per customer

```sql
WITH ranked_transactions AS (
    SELECT 
        customer_id,
        amount,
        DENSE_RANK() OVER (
            PARTITION BY customer_id 
            ORDER BY amount DESC
        ) as rank
    FROM transactions
)
SELECT 
    customer_id,
    amount as second_highest_amount
FROM ranked_transactions
WHERE rank = 2;
```

---

#### Question 14: Identify customers whose spending increased every month for 3 consecutive months

```sql
WITH monthly_spending AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', transaction_date) as month,
        SUM(amount) as total_spent
    FROM transactions
    GROUP BY customer_id, DATE_TRUNC('month', transaction_date)
),
spending_with_lags AS (
    SELECT 
        customer_id,
        month,
        total_spent,
        LAG(total_spent, 1) OVER (PARTITION BY customer_id ORDER BY month) as prev_month,
        LAG(total_spent, 2) OVER (PARTITION BY customer_id ORDER BY month) as two_months_ago
    FROM monthly_spending
)
SELECT DISTINCT customer_id
FROM spending_with_lags
WHERE total_spent > prev_month 
  AND prev_month > two_months_ago
  AND prev_month IS NOT NULL 
  AND two_months_ago IS NOT NULL;
```

---

#### Question 15: Find products that were purchased together (Market Basket Analysis)

**Table:** `order_items (order_id, product_id)`

```sql
SELECT 
    oi1.product_id as product_1,
    oi2.product_id as product_2,
    COUNT(DISTINCT oi1.order_id) as times_bought_together
FROM order_items oi1
JOIN order_items oi2 
    ON oi1.order_id = oi2.order_id
    AND oi1.product_id < oi2.product_id  -- Avoid duplicates
GROUP BY oi1.product_id, oi2.product_id
HAVING COUNT(DISTINCT oi1.order_id) >= 5
ORDER BY times_bought_together DESC;
```

---

#### Question 16: Calculate retention rate

**Customers who transacted in both Jan and Feb 2024**

```sql
WITH jan_customers AS (
    SELECT DISTINCT customer_id
    FROM transactions
    WHERE transaction_date BETWEEN '2024-01-01' AND '2024-01-31'
),
feb_customers AS (
    SELECT DISTINCT customer_id
    FROM transactions
    WHERE transaction_date BETWEEN '2024-02-01' AND '2024-02-29'
)
SELECT 
    COUNT(DISTINCT j.customer_id) as jan_customers,
    COUNT(DISTINCT f.customer_id) as retained_customers,
    ROUND(
        100.0 * COUNT(DISTINCT f.customer_id) / COUNT(DISTINCT j.customer_id), 
        2
    ) as retention_rate
FROM jan_customers j
LEFT JOIN feb_customers f ON j.customer_id = f.customer_id;
```

---

#### Question 17: Find churned customers

**First transaction >1 year ago, no transactions in last 6 months**

```sql
WITH customer_dates AS (
    SELECT 
        customer_id,
        MIN(transaction_date) as first_transaction,
        MAX(transaction_date) as last_transaction
    FROM transactions
    GROUP BY customer_id
)
SELECT 
    customer_id,
    first_transaction,
    last_transaction,
    CURRENT_DATE - last_transaction as days_since_last_txn
FROM customer_dates
WHERE first_transaction < CURRENT_DATE - INTERVAL '1 year'
  AND last_transaction < CURRENT_DATE - INTERVAL '6 months';
```

---

#### Question 18: Pivot - Show monthly transaction count for each customer

```sql
SELECT 
    customer_id,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 1 THEN 1 END) as Jan,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 2 THEN 1 END) as Feb,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 3 THEN 1 END) as Mar,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 4 THEN 1 END) as Apr,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 5 THEN 1 END) as May,
    COUNT(CASE WHEN EXTRACT(MONTH FROM transaction_date) = 6 THEN 1 END) as Jun
FROM transactions
WHERE EXTRACT(YEAR FROM transaction_date) = 2024
GROUP BY customer_id;
```

---

#### Question 19: Find gaps in transaction dates

```sql
WITH transaction_dates AS (
    SELECT 
        customer_id,
        transaction_date,
        LEAD(transaction_date) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date
        ) as next_transaction_date
    FROM transactions
)
SELECT 
    customer_id,
    transaction_date as gap_start,
    next_transaction_date as gap_end,
    next_transaction_date - transaction_date - 1 as gap_days
FROM transaction_dates
WHERE next_transaction_date - transaction_date > 1
ORDER BY gap_days DESC;
```

---

#### Question 20: Calculate moving average (3-day window)

```sql
SELECT 
    transaction_date,
    amount,
    AVG(amount) OVER (
        ORDER BY transaction_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3day
FROM transactions
ORDER BY transaction_date;
```

---

### Hard Level SQL

#### Question 21: Collaborative filtering - Find similar customers

```sql
WITH target_customer_products AS (
    SELECT DISTINCT product_id
    FROM order_items
    WHERE customer_id = 'CUST123'
),
similar_customers AS (
    SELECT 
        oi.customer_id,
        COUNT(DISTINCT oi.product_id) as common_products
    FROM order_items oi
    JOIN target_customer_products tcp ON oi.product_id = tcp.product_id
    WHERE oi.customer_id != 'CUST123'
    GROUP BY oi.customer_id
)
SELECT 
    customer_id,
    common_products,
    ROUND(
        100.0 * common_products / (SELECT COUNT(*) FROM target_customer_products), 
        2
    ) as similarity_score
FROM similar_customers
WHERE common_products >= 3
ORDER BY similarity_score DESC
LIMIT 10;
```

---

#### Question 22: Recursive CTE - Employee hierarchy

**Table:** `employees (employee_id, name, manager_id)`

```sql
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: CEO (no manager)
    SELECT 
        employee_id,
        name,
        manager_id,
        1 as level,
        name as hierarchy_path
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: employees with managers
    SELECT 
        e.employee_id,
        e.name,
        e.manager_id,
        eh.level + 1,
        eh.hierarchy_path || ' -> ' || e.name
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy
ORDER BY level, name;
```

---

#### Question 23: Find median transaction amount per customer

```sql
WITH ranked_amounts AS (
    SELECT 
        customer_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount) as rn,
        COUNT(*) OVER (PARTITION BY customer_id) as total_count
    FROM transactions
)
SELECT 
    customer_id,
    AVG(amount) as median_amount
FROM ranked_amounts
WHERE rn IN (
    FLOOR((total_count + 1) / 2.0), 
    CEIL((total_count + 1) / 2.0)
)
GROUP BY customer_id;
```

---

#### Question 24: Fraud detection - Same card in 2 cities within 1 hour

**Table:** `transactions (transaction_id, card_id, city, transaction_time)`

```sql
SELECT DISTINCT
    t1.card_id,
    t1.transaction_id as txn1,
    t2.transaction_id as txn2,
    t1.city as city1,
    t2.city as city2,
    t1.transaction_time as time1,
    t2.transaction_time as time2,
    EXTRACT(EPOCH FROM (t2.transaction_time - t1.transaction_time))/60 as minutes_apart
FROM transactions t1
JOIN transactions t2 
    ON t1.card_id = t2.card_id
    AND t1.city != t2.city
    AND t2.transaction_time > t1.transaction_time
    AND t2.transaction_time <= t1.transaction_time + INTERVAL '1 hour'
ORDER BY t1.card_id, t1.transaction_time;
```

---

#### Question 25: Complex JOIN - Customer lifetime value with category breakdown

```sql
SELECT 
    c.customer_id,
    c.name,
    pc.category_name,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(oi.quantity * oi.price) as total_spent_in_category,
    SUM(SUM(oi.quantity * oi.price)) OVER (PARTITION BY c.customer_id) as customer_lifetime_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN product_categories pc ON p.category_id = pc.category_id
GROUP BY c.customer_id, c.name, pc.category_name
ORDER BY customer_lifetime_value DESC, c.customer_id, total_spent_in_category DESC;
```

---

#### Question 26: Detect anomalies in daily transaction volume

```sql
WITH daily_stats AS (
    SELECT 
        DATE(transaction_date) as txn_date,
        COUNT(*) as txn_count,
        AVG(COUNT(*)) OVER (
            ORDER BY DATE(transaction_date) 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_7day,
        STDDEV(COUNT(*)) OVER (
            ORDER BY DATE(transaction_date) 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as stddev_7day
    FROM transactions
    GROUP BY DATE(transaction_date)
)
SELECT 
    txn_date,
    txn_count,
    avg_7day,
    CASE 
        WHEN txn_count > avg_7day + (2 * stddev_7day) THEN 'HIGH_ANOMALY'
        WHEN txn_count < avg_7day - (2 * stddev_7day) THEN 'LOW_ANOMALY'
        ELSE 'NORMAL'
    END as status
FROM daily_stats
WHERE stddev_7day IS NOT NULL
ORDER BY txn_date DESC;
```

---

#### Question 27: RFM Analysis (Recency, Frequency, Monetary)

```sql
WITH rfm_calc AS (
    SELECT 
        customer_id,
        CURRENT_DATE - MAX(transaction_date) as recency_days,
        COUNT(*) as frequency,
        SUM(amount) as monetary
    FROM transactions
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days) as R_score,
        NTILE(5) OVER (ORDER BY frequency DESC) as F_score,
        NTILE(5) OVER (ORDER BY monetary DESC) as M_score
    FROM rfm_calc
)
SELECT 
    customer_id,
    R_score,
    F_score,
    M_score,
    (R_score + F_score + M_score) as rfm_total,
    CASE 
        WHEN R_score >= 4 AND F_score >= 4 AND M_score >= 4 THEN 'Champions'
        WHEN R_score >= 3 AND F_score >= 3 THEN 'Loyal'
        WHEN R_score <= 2 AND F_score <= 2 THEN 'At Risk'
        ELSE 'Regular'
    END as customer_segment
FROM rfm_scores
ORDER BY rfm_total DESC;
```

---

#### Questions 28-30: Additional Complex Scenarios

**Practice these patterns:**
- Q28: Cohort analysis - Track customer behavior by signup month
- Q29: Session analytics - Group transactions into sessions (30-min gaps)
- Q30: Complex aggregations with ROLLUP/CUBE for reporting

---

## ⚡ PySpark Coding Problems

### DataFrame Operations

#### Question 31: Read CSV file and show basic statistics

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession.builder.appName('DataEngineering').getOrCreate()

# Read CSV
df = spark.read.csv('transactions.csv', header=True, inferSchema=True)

# Basic statistics
df.printSchema()
df.show(10)
df.describe().show()

print(f'Total rows: {df.count()}')
print(f'Total columns: {len(df.columns)}')
```

---

#### Question 32: Filter transactions above $10,000 and select specific columns

```python
# Filter and select
filtered_df = df.filter(col('amount') > 10000) \
    .select('customer_id', 'amount', 'transaction_date') \
    .orderBy(col('amount').desc())

filtered_df.show()

# Alternative syntax
filtered_df = df.where(df.amount > 10000) \
    .select(df.customer_id, df.amount, df.transaction_date)

filtered_df.show()
```

---

#### Question 33: GroupBy aggregations - Count and sum per customer

```python
from pyspark.sql.functions import count, sum, avg, min, max

# Group by customer
customer_stats = df.groupBy('customer_id').agg(
    count('*').alias('txn_count'),
    sum('amount').alias('total_spent'),
    avg('amount').alias('avg_amount'),
    min('amount').alias('min_amount'),
    max('amount').alias('max_amount')
).orderBy(col('total_spent').desc())

customer_stats.show()
```

---

#### Question 34: JOIN two DataFrames

```python
# Read customers data
customers_df = spark.read.csv('customers.csv', header=True, inferSchema=True)

# Inner join
result = df.join(
    customers_df,
    df.customer_id == customers_df.customer_id,
    'inner'
).select(
    customers_df.customer_id,
    customers_df.name,
    df.amount,
    df.transaction_date
)

result.show()

# Left join to find customers with no transactions
no_txn = customers_df.join(
    df,
    customers_df.customer_id == df.customer_id,
    'left'
).filter(df.customer_id.isNull()) \
 .select(customers_df.customer_id, customers_df.name)

no_txn.show()
```

---

#### Question 35: Window functions - Running total and ranking

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

# Define window specification
windowSpec = Window.partitionBy('customer_id').orderBy('transaction_date')

# Running total
df_with_running_total = df.withColumn(
    'running_total',
    sum('amount').over(windowSpec)
)

df_with_running_total.show()

# Ranking
windowSpec_rank = Window.partitionBy('customer_id').orderBy(col('amount').desc())

df_ranked = df.withColumn(
    'rank',
    rank().over(windowSpec_rank)
).withColumn(
    'dense_rank',
    dense_rank().over(windowSpec_rank)
).withColumn(
    'row_number',
    row_number().over(windowSpec_rank)
)

df_ranked.show()
```

---

#### Question 36: Handle null values and data cleaning

```python
# Check for nulls
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Drop rows with any null
df_no_nulls = df.dropna()

# Drop rows with nulls in specific columns
df_clean = df.dropna(subset=['customer_id', 'amount'])

# Fill nulls with default values
df_filled = df.fillna({
    'amount': 0,
    'city': 'Unknown'
})

# Replace specific values
df_replaced = df.replace('INVALID', None, subset=['status'])
```

---

#### Question 37: Add new columns with transformations

```python
from pyspark.sql.functions import when, year, month, dayofmonth

# Add multiple columns
df_transformed = df \
    .withColumn('amount_category',
        when(col('amount') < 100, 'Low')
        .when(col('amount') < 1000, 'Medium')
        .otherwise('High')
    ) \
    .withColumn('year', year('transaction_date')) \
    .withColumn('month', month('transaction_date')) \
    .withColumn('day', dayofmonth('transaction_date')) \
    .withColumn('amount_doubled', col('amount') * 2)

df_transformed.show()
```

---

#### Question 38: Read and write Parquet with partitioning

```python
# Write Parquet with partitioning
df.write \
    .mode('overwrite') \
    .partitionBy('year', 'month') \
    .parquet('s3://bucket/transactions/')

# Read Parquet
df_parquet = spark.read.parquet('s3://bucket/transactions/')

# Read with partition filter (predicate pushdown)
df_filtered = spark.read \
    .parquet('s3://bucket/transactions/') \
    .filter((col('year') == 2024) & (col('month') == 1))

df_filtered.show()
```

---

#### Question 39: Pivot table - Transaction count by month and customer type

```python
# Pivot
pivot_df = df.groupBy('customer_type').pivot('month').agg(
    count('*').alias('txn_count'),
    sum('amount').alias('total_amount')
)

pivot_df.show()

# Unpivot (melt) - opposite operation
from pyspark.sql.functions import expr

unpivot_df = pivot_df.selectExpr(
    'customer_type',
    """stack(12, 
    'Jan', Jan, 'Feb', Feb, 'Mar', Mar, 
    'Apr', Apr, 'May', May, 'Jun', Jun, 
    'Jul', Jul, 'Aug', Aug, 'Sep', Sep, 
    'Oct', Oct, 'Nov', Nov, 'Dec', Dec) as (month, count)"""
)

unpivot_df.show()
```

---

#### Question 40: UDF (User Defined Function)

```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Define Python function
def categorize_amount(amount):
    if amount < 100:
        return 'Low'
    elif amount < 1000:
        return 'Medium'
    else:
        return 'High'

# Register as UDF
categorize_udf = udf(categorize_amount, StringType())

# Use UDF
df_with_category = df.withColumn(
    'category',
    categorize_udf(col('amount'))
)

df_with_category.show()

# Better approach: Use built-in functions (faster)
df_builtin = df.withColumn('category',
    when(col('amount') < 100, 'Low')
    .when(col('amount') < 1000, 'Medium')
    .otherwise('High')
)
```

---

### Advanced Transformations

#### Question 41: Broadcast Join for small dimension tables

```python
from pyspark.sql.functions import broadcast

# Read small dimension table
countries_df = spark.read.csv('countries.csv', header=True)

# Broadcast join (efficient for small tables < 10MB)
result = df.join(
    broadcast(countries_df),
    df.country_code == countries_df.code,
    'left'
)

result.show()
```

---

#### Question 42: Handle data skew with salting

```python
from pyspark.sql.functions import rand, floor

# Add salt column (random number 0-9)
df_salted = df.withColumn('salt', (rand() * 10).cast('int'))

# Aggregate with salt
result = df_salted.groupBy('customer_id', 'salt').agg(
    sum('amount').alias('total')
).groupBy('customer_id').agg(
    sum('total').alias('final_total')
)

result.show()
```

---

#### Question 43: Cache and persist for optimization

```python
from pyspark import StorageLevel

# Cache in memory (default)
df_filtered = df.filter(col('amount') > 1000)
df_filtered.cache()

# Use the cached DataFrame multiple times
count1 = df_filtered.count()
count2 = df_filtered.groupBy('customer_id').count().count()

# Persist with different storage levels
df_filtered.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
df_filtered.unpersist()
```

---

#### Question 44: Structured Streaming - Read from Kafka

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema
schema = StructType([
    StructField('customer_id', StringType()),
    StructField('amount', DoubleType()),
    StructField('timestamp', StringType())
])

# Read from Kafka
kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'transactions') \
    .load()

# Parse JSON
parsed_df = kafka_df.select(
    from_json(col('value').cast('string'), schema).alias('data')
).select('data.*')

# Process stream
processed_df = parsed_df.filter(col('amount') > 1000)

# Write to console
query = processed_df.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

query.awaitTermination()
```

---

#### Question 45: Deduplicate records

```python
# Method 1: Drop all duplicates
df_dedup = df.dropDuplicates()

# Method 2: Drop duplicates based on specific columns
df_dedup = df.dropDuplicates(['customer_id', 'transaction_date'])

# Method 3: Keep latest record using window function
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('customer_id', 'transaction_date') \
    .orderBy(col('update_time').desc())

df_dedup = df.withColumn('rn', row_number().over(windowSpec)) \
    .filter(col('rn') == 1) \
    .drop('rn')

df_dedup.show()
```

---

#### Questions 46-50: Additional Advanced Topics

**Practice these patterns:**
- Q46: Delta Lake - Time travel and ACID transactions
- Q47: Repartition and Coalesce for optimization
- Q48: Complex aggregations with multiple grouping sets
- Q49: Handle complex nested JSON structures
- Q50: Performance tuning - Catalyst optimizer and execution plans

---

## 🐍 Python Coding Problems

### Data Structures & Algorithms

#### Question 51: Find duplicate numbers in an array

```python
def find_duplicates(arr):
    seen = set()
    duplicates = set()
    
    for num in arr:
        if num in seen:
            duplicates.add(num)
        else:
            seen.add(num)
    
    return list(duplicates)

# Test
arr = [1, 2, 3, 2, 4, 5, 3, 6]
print(find_duplicates(arr))  # Output: [2, 3]

# Alternative using Counter
from collections import Counter

def find_duplicates_v2(arr):
    counter = Counter(arr)
    return [num for num, count in counter.items() if count > 1]

print(find_duplicates_v2(arr))
```

---

#### Question 52: Merge two sorted lists

```python
def merge_sorted_lists(list1, list2):
    result = []
    i, j = 0, 0
    
    while i < len(list1) and j < len(list2):
        if list1[i] <= list2[j]:
            result.append(list1[i])
            i += 1
        else:
            result.append(list2[j])
            j += 1
    
    # Add remaining elements
    result.extend(list1[i:])
    result.extend(list2[j:])
    
    return result

# Test
list1 = [1, 3, 5, 7]
list2 = [2, 4, 6, 8]
print(merge_sorted_lists(list1, list2))
# Output: [1, 2, 3, 4, 5, 6, 7, 8]
```

---

#### Question 53: Group anagrams together

```python
def group_anagrams(words):
    from collections import defaultdict
    
    anagram_dict = defaultdict(list)
    
    for word in words:
        # Sort characters as key
        key = ''.join(sorted(word))
        anagram_dict[key].append(word)
    
    return list(anagram_dict.values())

# Test
words = ['eat', 'tea', 'tan', 'ate', 'nat', 'bat']
print(group_anagrams(words))
# Output: [['eat', 'tea', 'ate'], ['tan', 'nat'], ['bat']]
```

---

#### Question 54: Find first non-repeating character in a string

```python
def first_non_repeating_char(s):
    from collections import Counter
    
    char_count = Counter(s)
    
    for char in s:
        if char_count[char] == 1:
            return char
    
    return None

# Test
print(first_non_repeating_char('leetcode'))  # Output: 'l'
print(first_non_repeating_char('loveleetcode'))  # Output: 'v'
```

---

#### Question 55: Flatten nested dictionary

```python
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    
    return dict(items)

# Test
nested = {
    'a': 1,
    'b': {
        'c': 2,
        'd': {
            'e': 3
        }
    }
}

print(flatten_dict(nested))
# Output: {'a': 1, 'b_c': 2, 'b_d_e': 3}
```

---

### Data Engineering Specific

#### Question 61: Read CSV and calculate statistics using pandas

```python
import pandas as pd
import numpy as np

def analyze_transactions(file_path):
    # Read CSV
    df = pd.read_csv(file_path)
    
    # Basic stats
    print('Shape:', df.shape)
    print('\nColumn types:')
    print(df.dtypes)
    
    print('\nMissing values:')
    print(df.isnull().sum())
    
    # Statistical summary
    print('\nNumeric columns summary:')
    print(df.describe())
    
    # Group by analysis
    customer_stats = df.groupBy('customer_id').agg({
        'amount': ['count', 'sum', 'mean', 'min', 'max']
    })
    
    return customer_stats

# Use
stats = analyze_transactions('transactions.csv')
print(stats)
```

---

#### Question 62: Parse JSON log file and extract specific fields

```python
import json

def parse_json_logs(file_path, fields_to_extract):
    results = []
    
    with open(file_path, 'r') as f:
        for line in f:
            try:
                log = json.loads(line)
                extracted = {
                    field: log.get(field) 
                    for field in fields_to_extract
                }
                results.append(extracted)
            except json.JSONDecodeError as e:
                print(f'Error parsing line: {e}')
                continue
    
    return results

# Use
logs = parse_json_logs('app.log', ['timestamp', 'level', 'message'])

# Convert to DataFrame
import pandas as pd
df = pd.DataFrame(logs)
print(df.head())
```

---

#### Question 63: Connect to PostgreSQL and execute queries

```python
import psycopg2
import pandas as pd

def query_database(query):
    try:
        # Connect to database
        conn = psycopg2.connect(
            host='localhost',
            database='mydb',
            user='user',
            password='password'
        )
        
        # Execute query
        df = pd.read_sql_query(query, conn)
        
        # Close connection
        conn.close()
        
        return df
        
    except Exception as e:
        print(f'Database error: {e}')
        return None

# Use
query = 'SELECT * FROM transactions WHERE amount > 10000'
df = query_database(query)
print(df)
```

---

#### Question 64: Data quality validation framework

```python
import pandas as pd

def validate_data(df, rules):
    """
    Validate DataFrame against rules
    rules: dict with column -> list of validation functions
    """
    validation_results = {}
    
    for column, validators in rules.items():
        column_results = []
        
        for validator in validators:
            result = validator(df[column])
            column_results.append(result)
        
        validation_results[column] = all(column_results)
    
    return validation_results

# Define validators
def not_null(series):
    return series.notna().all()

def positive_values(series):
    return (series > 0).all()

def valid_email(series):
    return series.str.contains('@').all()

# Use
rules = {
    'customer_id': [not_null],
    'amount': [not_null, positive_values],
    'email': [not_null, valid_email]
}

results = validate_data(df, rules)
print('Validation results:', results)
```

---

#### Question 65: ETL Pipeline - Extract, Transform, Load

```python
import pandas as pd
from datetime import datetime

class ETLPipeline:
    def __init__(self, source_path, dest_path):
        self.source_path = source_path
        self.dest_path = dest_path
        self.df = None
    
    def extract(self):
        """Extract data from source"""
        print(f'Extracting from {self.source_path}')
        self.df = pd.read_csv(self.source_path)
        print(f'Extracted {len(self.df)} rows')
        return self
    
    def transform(self):
        """Transform data"""
        print('Transforming data...')
        
        # Clean nulls
        self.df = self.df.dropna()
        
        # Add derived columns
        self.df['amount_category'] = pd.cut(
            self.df['amount'],
            bins=[0, 100, 1000, float('inf')],
            labels=['Low', 'Medium', 'High']
        )
        
        # Date formatting
        self.df['transaction_date'] = pd.to_datetime(
            self.df['transaction_date']
        )
        self.df['year'] = self.df['transaction_date'].dt.year
        self.df['month'] = self.df['transaction_date'].dt.month
        
        print(f'Transformed to {len(self.df)} rows')
        return self
    
    def load(self):
        """Load data to destination"""
        print(f'Loading to {self.dest_path}')
        self.df.to_csv(self.dest_path, index=False)
        print('Load complete')
        return self
    
    def run(self):
        """Run full ETL pipeline"""
        start_time = datetime.now()
        
        self.extract().transform().load()
        
        duration = (datetime.now() - start_time).total_seconds()
        print(f'Pipeline completed in {duration:.2f} seconds')

# Use
pipeline = ETLPipeline('raw_data.csv', 'processed_data.csv')
pipeline.run()
```

---

## 💡 Practice Tips & Strategies

### How to Practice Effectively

1. **Start with Easy, then Medium, then Hard** - build confidence gradually
2. **Time yourself** - aim to solve Easy in 5-10 mins, Medium in 15-20 mins
3. **Write code by hand first**, then type and test
4. **Explain your solution out loud** - practice for interviews
5. **Learn one new concept daily** and practice 3 problems on it

### Common Interview Patterns

- **SQL:** JOINs, Window functions, CTEs, Date operations
- **PySpark:** GroupBy-Agg, Window functions, Joins, Handling nulls
- **Python:** Dictionary operations, List comprehensions, File I/O

### Online Resources

- **LeetCode:** SQL and Python problems
- **HackerRank:** SQL and Python (especially data engineering track)
- **StrataScratch:** Real interview questions from companies
- **DataLemur:** Banking/Finance specific SQL questions

### Daily Practice Schedule

**Days 1-2:** SQL - 10 questions daily  
**Day 3:** PySpark basics - 8 questions  
**Day 4:** Python + Advanced PySpark - 10 questions  
**Day 5:** Mock coding test - Random 5 questions in time limit

---

## 🚀 Good Luck!

**Practice daily and you'll ace the interview!**

For complete interview preparation strategy, see [README.md](README.md)

---

*Remember: It's not about memorizing solutions, it's about understanding patterns and problem-solving approaches.*
