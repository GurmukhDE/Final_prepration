# 📘 Complete Data Engineering Interview Guide
### NAB (National Australia Bank) & GlobalLogic — Senior / Lead Role | 7–8+ Years Experience

---

![SQL](https://img.shields.io/badge/SQL-Snowflake%20%7C%20Redshift%20%7C%20HiveQL-blue)
![Python](https://img.shields.io/badge/Python-PySpark%20%7C%20boto3%20%7C%20pandas-yellow)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20EMR%20%7C%20Glue%20%7C%20Lambda-orange)
![Spark](https://img.shields.io/badge/Apache-Spark%20%7C%20Delta%20Lake-red)
![DevOps](https://img.shields.io/badge/DevOps-Jenkins%20%7C%20Terraform%20%7C%20Docker-lightgrey)

---

## 📋 Table of Contents

| # | Section | Key Topics |
|---|---------|------------|
| [1](#-section-1-sql-hive-ql-snowflake--redshift) | SQL, Hive-QL, Snowflake & Redshift | Window functions, SCD, partitioning, query optimization |
| [2](#-section-2-python-for-data-engineering) | Python for Data Engineering | File processing, boto3, decorators, async, coding problems |
| [3](#-section-3-pyspark--apache-spark) | PySpark & Apache Spark | DAG, skew, streaming, coding problems |
| [4](#-section-4-etl--elt-pipelines) | ETL / ELT Pipelines | CDC, idempotency, Airflow, error handling |
| [5](#-section-5-data-modeling) | Data Modeling | Star Schema, Data Vault 2.0, normalization |
| [6](#-section-6-aws-services) | AWS Services | S3, EMR, Glue, Lambda, Redshift, SNS, SQS |
| [7](#-section-7-databricks--delta-lake) | Databricks & Delta Lake | ACID, time travel, Medallion architecture |
| [8](#-section-8-hadoop-ecosystem) | Hadoop Ecosystem | HDFS, Hive, Impala |
| [9](#-section-9-cicd--devops) | CI/CD & DevOps | Jenkins, Docker, Terraform, Kubernetes, Ansible |
| [10](#-section-10-unix--shell-scripting) | Unix / Shell Scripting | Bash scripts, cron, file monitoring |
| [11](#-section-11-data-quality--observability) | Data Quality & Observability | Great Expectations, dbt tests, data contracts |
| [12](#-section-12-system-design--edge-cases) | System Design — Edge Cases | High-TPS, multi-tenant, DR, Lakehouse design |
| [13](#-section-13-program-management-globallogic-focus) | Program Management | Roadmaps, budgeting, stakeholder conflict |
| [14](#-section-14-behavioral--leadership) | Behavioral & Leadership | STAR method questions |

---

## 💡 How to Use This Guide

- **❓ Question** — The interview question
- **✅ Answer** — Detailed answer with code where applicable
- **🔁 Follow-ups** — Likely follow-up questions the interviewer will ask
- **🎯 Scenarios** — Real-world scenario / use-case questions
- **📁 Project Q&A** — Questions about YOUR experience on the topic

---

---

# 🗄️ Section 1: SQL, Hive-QL, Snowflake & Redshift

---

## 1.1 Window Functions

---

### ❓ Q1: Explain `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`. When do you use each?

**✅ Answer:**

All three are window functions that assign a numeric label to rows within a partition.

| Function | Ties | Skips Rank | Use Case |
|----------|------|------------|----------|
| `ROW_NUMBER()` | No — always unique | N/A | Get exactly 1 row per group (latest record) |
| `RANK()` | Yes — same rank | Yes — gaps after ties (1,1,3) | Leaderboards where gaps matter |
| `DENSE_RANK()` | Yes — same rank | No — continuous (1,1,2) | Top-N without gaps |

**💻 Code Example:**

```sql
-- Get the latest transaction per customer
WITH ranked AS (
    SELECT
        customer_id,
        transaction_id,
        amount,
        transaction_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY transaction_date DESC
        ) AS rn
    FROM transactions
)
SELECT * FROM ranked WHERE rn = 1;

-- Top 3 customers by spend this month (allowing ties)
WITH ranked AS (
    SELECT
        customer_id,
        SUM(amount) AS total_spend,
        DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS spend_rank
    FROM transactions
    WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY customer_id
)
SELECT * FROM ranked WHERE spend_rank <= 3;
```

**🔁 Follow-ups:**
- What is the difference between `PARTITION BY` and `GROUP BY`?
- Can you use window functions in a `WHERE` clause? *(No — use a CTE or subquery)*
- How do you deduplicate rows using `ROW_NUMBER()`?
- What is the performance impact of window functions on 1B+ rows in Snowflake vs Redshift?

**🎯 Scenarios:**
- *"You have a transactions table with millions of rows. Get the top 3 transactions by value per customer per month."*
- *"Flag the FIRST fraudulent transaction per account for fraud investigation."*
- *"Upstream systems resend data causing duplicates in the staging table. Deduplicate using SQL."*

**📁 Project Q&A:**
- Did you use window functions for SCD Type 2 tracking? Walk me through the logic.
- Have you optimised window functions over billions of rows? What approach did you use?

---

### ❓ Q2: Explain CTEs, Subqueries, and Temp Tables. When do you prefer each?

**✅ Answer:**

| Type | Materialized? | Reusable? | Indexable? | Best For |
|------|--------------|-----------|------------|----------|
| CTE | Usually not (inline) | Within same query | No | Readability, recursive queries |
| Subquery | No | No | No | Simple inline logic |
| Temp Table | Yes | Across statements | Yes | Large intermediate results, multiple reuses |

**💻 Code Example — Recursive CTE (org chart):**

```sql
-- Find all subordinates of a manager (recursive)
WITH RECURSIVE org_hierarchy AS (
    -- Base case: the manager
    SELECT employee_id, manager_id, name, 1 AS level
    FROM employees
    WHERE employee_id = 1001  -- top-level manager

    UNION ALL

    -- Recursive case: direct reports
    SELECT e.employee_id, e.manager_id, e.name, h.level + 1
    FROM employees e
    INNER JOIN org_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM org_hierarchy ORDER BY level;
```

**💻 Code Example — Temp Table (better performance for multi-step ETL in Redshift):**

```sql
-- Step 1: Pre-compute and materialize
CREATE TEMP TABLE active_customers AS
SELECT customer_id, SUM(amount) AS total_spend
FROM transactions
WHERE status = 'COMPLETED'
AND transaction_date >= DATEADD('month', -3, CURRENT_DATE)
GROUP BY customer_id;

-- Analyze for Redshift optimizer to use statistics
ANALYZE active_customers;

-- Step 2: Reuse the temp table multiple times
SELECT c.segment, AVG(ac.total_spend)
FROM active_customers ac
JOIN customers c USING (customer_id)
GROUP BY c.segment;
```

**🔁 Follow-ups:**
- In Snowflake, are CTEs always inlined or can they be materialized?
- What is a `MATERIALIZED CTE` and when does Snowflake decide to materialize?
- What is the difference between a temp table and a table variable in SQL Server?
- Why might a CTE perform differently in Redshift vs Snowflake?

**🎯 Scenarios:**
- *"You have a 5-step transformation pipeline. Would you use nested CTEs or temp tables in Redshift?"*
- *"Your recursive org hierarchy CTE is timing out at 10+ levels. How do you optimize it?"*

---

### ❓ Q3: Implement SCD Type 2 in SQL using MERGE

**✅ Answer:**

SCD Type 2 adds a new row when a dimension attribute changes, preserving full history.

**💻 Code — Snowflake MERGE for SCD Type 2:**

```sql
-- Target: dim_customer with history tracking
-- Columns: surrogate_key, customer_id, email, address, 
--          effective_date, expiry_date, is_current, record_hash

-- Step 1: Expire old records that have changed
MERGE INTO dim_customer AS target
USING (
    -- Staging with hash of tracked columns
    SELECT
        customer_id,
        email,
        address,
        MD5(CONCAT(email, '|', COALESCE(address, ''))) AS record_hash
    FROM staging_customer
) AS source
ON (target.customer_id = source.customer_id AND target.is_current = TRUE)
WHEN MATCHED AND target.record_hash <> source.record_hash THEN
    UPDATE SET
        target.expiry_date  = CURRENT_DATE - 1,
        target.is_current   = FALSE;

-- Step 2: Insert new/changed records
INSERT INTO dim_customer (
    customer_id, email, address,
    effective_date, expiry_date, is_current, record_hash
)
SELECT
    s.customer_id, s.email, s.address,
    CURRENT_DATE, '9999-12-31', TRUE,
    MD5(CONCAT(s.email, '|', COALESCE(s.address, '')))
FROM staging_customer s
LEFT JOIN dim_customer d
    ON s.customer_id = d.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL  -- new customer
   OR MD5(CONCAT(s.email, '|', COALESCE(s.address, ''))) <> d.record_hash; -- changed

-- Query: What was the customer's address on 2023-06-15?
SELECT * FROM dim_customer
WHERE customer_id = 42
  AND '2023-06-15' BETWEEN effective_date AND expiry_date;
```

**🔁 Follow-ups:**
- How do you handle late-arriving dimension changes in SCD Type 2?
- What is the difference between SCD Type 2 and Data Vault Satellites?
- How do you implement SCD Type 2 in PySpark (without SQL MERGE)?
- How do you prevent duplicate `is_current = TRUE` records during concurrent pipeline runs?

**🎯 Scenarios:**
- *"Customer addresses change frequently. Design an SCD Type 2 that also handles source corrections (Type 1 override within Type 2 history)."*
- *"You need to rebuild historical snapshots for audit — query the dimension state at any past date."*

---

## 1.2 Query Optimization & Execution Plans

---

### ❓ Q4: How do you identify and fix a slow query in Snowflake, Redshift, and Oracle?

**✅ Answer:**

**Snowflake:**
```sql
-- Step 1: Check Query Profile in Snowflake UI (most detailed)
-- Step 2: Find problematic queries
SELECT query_id, query_text, execution_time, bytes_scanned, partitions_scanned, partitions_total
FROM snowflake.account_usage.query_history
WHERE execution_status = 'SUCCESS'
  AND start_time >= DATEADD('hour', -24, CURRENT_TIMESTAMP)
ORDER BY execution_time DESC
LIMIT 20;

-- Step 3: Check partition pruning ratio
-- If partitions_scanned / partitions_total is high → add clustering key

-- Step 4: Check for spills (memory overflow to disk)
-- Look for "Remote Disk I/O" in Query Profile → increase warehouse size or optimize query
```

**Redshift:**
```sql
-- Check execution plan
EXPLAIN SELECT customer_id, SUM(amount)
FROM transactions
WHERE transaction_date >= '2024-01-01'
GROUP BY customer_id;
-- Look for: DS_BCAST_INNER (bad DISTKEY), Nested Loop (missing join column index)

-- Find slow queries and their bottlenecks
SELECT q.query, q.elapsed, s.step, s.rows, s.bytes, s.label
FROM stl_query q
JOIN svl_query_summary s ON q.query = s.query
WHERE q.userid > 1
ORDER BY q.elapsed DESC
LIMIT 10;

-- Check table skew (DISTKEY effectiveness)
SELECT slice, COUNT(*) AS row_count
FROM stv_blocklist
WHERE tbl = (SELECT id FROM stv_tbl_perm WHERE name = 'transactions' LIMIT 1)
GROUP BY slice
ORDER BY row_count DESC;
```

**Oracle:**
```sql
-- Get execution plan with actuals
EXPLAIN PLAN FOR
SELECT customer_id, SUM(amount) FROM transactions
WHERE transaction_date >= DATE '2024-01-01'
GROUP BY customer_id;

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(FORMAT => 'ALL'));

-- Gather fresh statistics (fixes bad cardinality estimates)
EXEC DBMS_STATS.GATHER_TABLE_STATS('SCHEMA_NAME', 'TRANSACTIONS', CASCADE => TRUE);

-- Create index on frequently filtered column
CREATE INDEX idx_txn_date ON transactions(transaction_date);
```

**🔁 Follow-ups:**
- What is cardinality and how does the query optimizer use it?
- What is the difference between hash join, merge join, and nested loop join?
- How do you force a specific join type in Snowflake vs Oracle?
- What is a bind variable and how does it affect Oracle's shared pool?

**🎯 Scenarios:**
- *"A report that ran in 5 seconds now takes 8 minutes after a nightly data load. Statistics have not been updated. Fix it."*
- *"You discover an N+1 correlated subquery problem — for each row in table A, it hits table B with 100M rows. Rewrite it."*

```sql
-- N+1 PROBLEM (slow - runs subquery once per row in orders)
SELECT o.order_id,
       (SELECT c.customer_name FROM customers c WHERE c.id = o.customer_id) AS name
FROM orders o;

-- FIXED (single join)
SELECT o.order_id, c.customer_name
FROM orders o
JOIN customers c ON c.id = o.customer_id;
```

---

## 1.3 Hive-QL Specific

---

### ❓ Q5: Key differences between Hive-QL and standard SQL. What are Hive's performance pitfalls?

**✅ Answer:**

**Key Differences:**

```sql
-- ❌ Hive does NOT support UPDATE/DELETE on non-ACID tables
-- You must overwrite the entire partition

-- WRONG in Hive (non-ACID table)
UPDATE customers SET email = 'new@email.com' WHERE id = 1;

-- CORRECT in Hive — overwrite the partition
INSERT OVERWRITE TABLE customers PARTITION (region='AU')
SELECT id, CASE WHEN id=1 THEN 'new@email.com' ELSE email END, region
FROM customers WHERE region = 'AU';

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
```

**Hive Sort Variants (frequently asked!):**

```sql
-- ORDER BY: Global sort — all data goes to ONE reducer (slow on large data)
SELECT * FROM transactions ORDER BY amount DESC;

-- SORT BY: Sorts within each reducer (faster, not globally sorted)
SELECT * FROM transactions SORT BY amount DESC;

-- DISTRIBUTE BY: Controls which reducer gets which rows (by hash)
SELECT * FROM transactions DISTRIBUTE BY customer_id SORT BY amount;

-- CLUSTER BY: DISTRIBUTE BY + SORT BY on same column (shorthand)
SELECT * FROM transactions CLUSTER BY customer_id;
```

**Performance Optimizations:**

```sql
-- Enable vectorized execution (significant speedup with ORC)
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;

-- Enable Cost-Based Optimizer
SET hive.cbo.enable = true;
SET hive.compute.query.using.stats = true;

-- Fix data skew with skew join optimization
SET hive.optimize.skewjoin = true;
SET hive.skewjoin.key = 100000;  -- threshold for skew detection

-- Merge small files at output
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.merge.size.per.task = 256000000;  -- 256MB target
```

**🔁 Follow-ups:**
- What file formats are supported in Hive? Why is ORC preferred over Text/CSV?
- How does Tez execution engine differ from MapReduce in Hive?
- What is ACID in Hive? Which storage format is required?
- How do you handle small file problems in HDFS/Hive?

**🎯 Scenarios:**
- *"A Hive query runs for 2 hours on a 500GB table. Walk through your optimization process."*
- *"A Hive job fails — 3 reducers finish in 2 minutes but one runs for 45 minutes. Fix the skew."*

---

## 1.4 Advanced SQL Coding Problems

---

### ❓ Q6: SQL Coding — Find gaps in a sequence / running totals / pivoting

**💻 Running Total (Cumulative Sum):**

```sql
-- Running total of transaction amounts per customer per day
SELECT
    customer_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM transactions
ORDER BY customer_id, transaction_date;
```

**💻 Find Missing Dates (Date Gaps):**

```sql
-- Generate all dates in range, find which are missing in transactions
WITH date_spine AS (
    SELECT DATEADD('day', seq4(), '2024-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 365))  -- Snowflake syntax
),
txn_dates AS (
    SELECT DISTINCT DATE(transaction_date) AS txn_dt FROM transactions
)
SELECT d.dt AS missing_date
FROM date_spine d
LEFT JOIN txn_dates t ON d.dt = t.txn_dt
WHERE t.txn_dt IS NULL;
```

**💻 Pivot Table (Dynamic Aggregation):**

```sql
-- Sales by product type per month (static pivot — Snowflake)
SELECT *
FROM (
    SELECT DATE_TRUNC('month', sale_date) AS month, product_type, amount
    FROM sales
)
PIVOT (SUM(amount) FOR product_type IN ('LOAN', 'DEPOSIT', 'CREDIT_CARD', 'INSURANCE'))
ORDER BY month;
```

**💻 Sessionization (Group events into sessions):**

```sql
-- Group user events into sessions (gap > 30 min = new session)
WITH with_gap AS (
    SELECT
        user_id,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time,
        CASE
            WHEN DATEDIFF('minute',
                LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
                event_time) > 30
            OR LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) IS NULL
            THEN 1 ELSE 0
        END AS is_new_session
    FROM user_events
),
with_session_id AS (
    SELECT *,
           SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
    FROM with_gap
)
SELECT user_id, session_id,
       MIN(event_time) AS session_start,
       MAX(event_time) AS session_end,
       COUNT(*) AS event_count
FROM with_session_id
GROUP BY user_id, session_id;
```

**💻 Median Calculation:**

```sql
-- Median transaction amount per customer (Snowflake)
SELECT customer_id, MEDIAN(amount) AS median_amount
FROM transactions
GROUP BY customer_id;

-- Median without MEDIAN() function (works in all SQL dialects)
SELECT customer_id,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
FROM transactions
GROUP BY customer_id;
```

---

---

# 🐍 Section 2: Python for Data Engineering

---

## 2.1 Large File & Memory-Efficient Processing

---

### ❓ Q7: How do you process a 50GB CSV file in Python without loading it into memory?

**✅ Answer:**

**💻 Approach 1 — Pandas Chunking:**

```python
import pandas as pd
from pathlib import Path

def process_large_csv(filepath: str, chunksize: int = 100_000) -> None:
    """Stream-process a large CSV in chunks."""
    total_rows = 0
    results = []

    for chunk in pd.read_csv(filepath, chunksize=chunksize, low_memory=False):
        # Apply transformations on each chunk
        chunk = chunk[chunk['status'] == 'ACTIVE']
        chunk['amount_aud'] = chunk['amount'] * chunk['fx_rate']

        # Aggregate within chunk
        agg = chunk.groupby('customer_id')['amount_aud'].sum()
        results.append(agg)
        total_rows += len(chunk)

    # Combine all chunk results
    final = pd.concat(results).groupby(level=0).sum()
    print(f"Processed {total_rows:,} rows")
    return final
```

**💻 Approach 2 — Pure Generator (most memory-efficient):**

```python
import csv
from typing import Generator, Iterator

def csv_row_generator(filepath: str) -> Generator[dict, None, None]:
    """Yield one CSV row at a time — constant memory usage."""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def stream_to_s3(filepath: str, bucket: str, key: str) -> None:
    """Read CSV and write transformed Parquet directly to S3."""
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io

    s3 = boto3.client('s3')
    buffer = []

    for i, row in enumerate(csv_row_generator(filepath)):
        # Transform row
        row['amount'] = float(row['amount'])
        row['processed_at'] = pd.Timestamp.now().isoformat()
        buffer.append(row)

        # Write batch of 10K rows to S3
        if len(buffer) == 10_000:
            write_batch_to_s3(s3, buffer, bucket, f"{key}/part_{i//10000:05d}.parquet")
            buffer.clear()

    # Write remaining rows
    if buffer:
        write_batch_to_s3(s3, buffer, bucket, f"{key}/part_final.parquet")
```

**💻 Approach 3 — Dask (parallel, distributed):**

```python
import dask.dataframe as dd

def process_with_dask(filepath: str) -> None:
    """Process large file using Dask for parallel execution."""
    df = dd.read_csv(filepath, blocksize="64MB")

    # Lazy computation graph built here
    result = (
        df[df['status'] == 'ACTIVE']
        .assign(amount_aud=df['amount'] * df['fx_rate'])
        .groupby('customer_id')['amount_aud']
        .sum()
    )

    # Trigger actual computation
    final = result.compute()
    final.to_parquet('output/', write_metadata_file=True)
```

**🔁 Follow-ups:**
- What is the difference between a generator and an iterator?
- How does Python's GIL affect data processing? How do you work around it?
- When would you use `multiprocessing` vs `concurrent.futures` vs `asyncio`?

**🎯 Scenarios:**
- *"Ingest 200 CSV files (each ~1GB) from S3 daily. Design the pipeline with error handling and retry."*
- *"A CSV file arrives with inconsistent encoding (UTF-8, Latin-1). Handle this in production."*

```python
def read_csv_with_encoding_detection(filepath: str) -> pd.DataFrame:
    """Auto-detect and handle encoding issues."""
    import chardet

    with open(filepath, 'rb') as f:
        raw = f.read(100_000)  # Read first 100KB for detection
        result = chardet.detect(raw)
        detected_encoding = result['encoding']

    try:
        return pd.read_csv(filepath, encoding=detected_encoding)
    except (UnicodeDecodeError, LookupError):
        # Fallback: replace undecodable chars
        return pd.read_csv(filepath, encoding='utf-8', errors='replace')
```

---

## 2.2 Decorators, Context Managers & Design Patterns

---

### ❓ Q8: Explain decorators and context managers. How are they used in data engineering?

**💻 Retry Decorator with Exponential Backoff:**

```python
import functools
import time
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """Decorator: retry a function with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    wait_time = delay * (backoff ** (attempt - 1))
                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for "
                        f"{func.__name__}: {e}. Retrying in {wait_time:.1f}s"
                    )
                    if attempt < max_attempts:
                        time.sleep(wait_time)
            raise last_exception
        return wrapper
    return decorator


# Usage
@retry(max_attempts=5, delay=2.0, exceptions=(ConnectionError, TimeoutError))
def fetch_from_api(endpoint: str) -> dict:
    """Fetch data from an external API."""
    import requests
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    return response.json()
```

**💻 Timer + Logger Decorator:**

```python
import time
import logging
from functools import wraps

def pipeline_step(step_name: str = None):
    """Decorator: log execution time and errors for pipeline steps."""
    def decorator(func):
        name = step_name or func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            logger.info(f"[START] {name}")
            try:
                result = func(*args, **kwargs)
                elapsed = time.monotonic() - start
                logger.info(f"[SUCCESS] {name} completed in {elapsed:.2f}s")
                return result
            except Exception as e:
                elapsed = time.monotonic() - start
                logger.error(f"[FAILED] {name} failed after {elapsed:.2f}s: {e}")
                raise
        return wrapper
    return decorator


# Usage in ETL pipeline
@pipeline_step("Extract from Oracle")
def extract_data(connection_str: str) -> pd.DataFrame:
    pass

@pipeline_step("Transform and validate")
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    pass
```

**💻 Context Manager for Database Connections:**

```python
from contextlib import contextmanager
import psycopg2

@contextmanager
def get_db_connection(host: str, db: str, user: str, password: str):
    """Context manager: ensure connection always closed, transaction always committed or rolled back."""
    conn = None
    try:
        conn = psycopg2.connect(host=host, database=db, user=user, password=password)
        yield conn
        conn.commit()
        logger.info("Transaction committed")
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Transaction rolled back due to: {e}")
        raise
    finally:
        if conn:
            conn.close()


# Usage
with get_db_connection(host='localhost', db='warehouse', user='etl', password='secret') as conn:
    cursor = conn.cursor()
    cursor.execute("INSERT INTO audit_log (event, ts) VALUES (%s, NOW())", ('ETL_START',))
    # Automatically committed on success, rolled back on exception, always closed
```

**💻 Singleton Pattern for Connection Pool:**

```python
import threading

class DatabaseConnectionPool:
    """Thread-safe singleton connection pool."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, max_connections: int = 5):
        if not self._initialized:
            from queue import Queue
            self._pool = Queue(maxsize=max_connections)
            self._max_connections = max_connections
            self._initialized = True

    @contextmanager
    def get_connection(self):
        conn = self._pool.get(block=True, timeout=30)
        try:
            yield conn
        finally:
            self._pool.put(conn)  # Return to pool
```

**🔁 Follow-ups:**
- What is `functools.lru_cache`? How does it help in data pipelines?
- What is the difference between `@staticmethod`, `@classmethod`, and instance methods?
- How do you implement a circuit breaker pattern in Python?

---

## 2.3 AWS with boto3

---

### ❓ Q9: Design a Python-based S3 file watcher that triggers ETL and notifies on completion

**💻 Complete Production Pattern:**

```python
import boto3
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    source_bucket: str
    source_prefix: str
    glue_job_name: str
    sns_topic_arn: str
    sqs_queue_url: str
    processed_prefix: str = "processed/"
    dead_letter_prefix: str = "failed/"


class S3ETLOrchestrator:
    """Monitors S3 for new files and orchestrates ETL pipeline."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.s3 = boto3.client('s3')
        self.glue = boto3.client('glue')
        self.sns = boto3.client('sns')
        self.sqs = boto3.client('sqs')

    @retry(max_attempts=3, delay=2.0, exceptions=(Exception,))
    def list_unprocessed_files(self) -> list[str]:
        """List files in source prefix that haven't been processed."""
        paginator = self.s3.get_paginator('list_objects_v2')
        keys = []

        for page in paginator.paginate(
            Bucket=self.config.source_bucket,
            Prefix=self.config.source_prefix
        ):
            for obj in page.get('Contents', []):
                if not obj['Key'].endswith('/'):  # Skip folder markers
                    keys.append(obj['Key'])
        return keys

    def trigger_glue_job(self, s3_key: str) -> str:
        """Start a Glue job and return the run ID."""
        s3_uri = f"s3://{self.config.source_bucket}/{s3_key}"
        response = self.glue.start_job_run(
            JobName=self.config.glue_job_name,
            Arguments={
                '--source_path': s3_uri,
                '--job_run_id': f"run_{int(time.time())}",
            }
        )
        run_id = response['JobRunId']
        logger.info(f"Glue job started: {run_id} for file: {s3_key}")
        return run_id

    def wait_for_glue_job(self, run_id: str, poll_interval: int = 30) -> str:
        """Poll Glue job until completion. Returns final status."""
        terminal_states = {'SUCCEEDED', 'FAILED', 'ERROR', 'TIMEOUT', 'STOPPED'}

        while True:
            response = self.glue.get_job_run(
                JobName=self.config.glue_job_name,
                RunId=run_id
            )
            state = response['JobRun']['JobRunState']
            logger.info(f"Glue job {run_id} state: {state}")

            if state in terminal_states:
                return state
            time.sleep(poll_interval)

    def archive_file(self, s3_key: str, success: bool) -> None:
        """Move processed file to processed or failed prefix."""
        filename = s3_key.split('/')[-1]
        dest_prefix = self.config.processed_prefix if success else self.config.dead_letter_prefix
        dest_key = f"{dest_prefix}{filename}"

        self.s3.copy_object(
            Bucket=self.config.source_bucket,
            CopySource={'Bucket': self.config.source_bucket, 'Key': s3_key},
            Key=dest_key
        )
        self.s3.delete_object(Bucket=self.config.source_bucket, Key=s3_key)
        logger.info(f"Archived {s3_key} → {dest_key}")

    def send_notification(self, message: str, subject: str) -> None:
        """Send SNS notification."""
        self.sns.publish(
            TopicArn=self.config.sns_topic_arn,
            Subject=subject,
            Message=message
        )

    def process_file(self, s3_key: str) -> None:
        """Full pipeline for one file: trigger → wait → archive → notify."""
        try:
            run_id = self.trigger_glue_job(s3_key)
            status = self.wait_for_glue_job(run_id)

            if status == 'SUCCEEDED':
                self.archive_file(s3_key, success=True)
                self.send_notification(
                    message=json.dumps({'file': s3_key, 'run_id': run_id, 'status': 'SUCCESS'}),
                    subject=f"ETL SUCCESS: {s3_key}"
                )
            else:
                self.archive_file(s3_key, success=False)
                self.send_notification(
                    message=json.dumps({'file': s3_key, 'run_id': run_id, 'status': 'FAILED'}),
                    subject=f"ETL FAILED: {s3_key}"
                )
        except Exception as e:
            logger.error(f"Pipeline error for {s3_key}: {e}")
            self.archive_file(s3_key, success=False)
            self.send_notification(
                message=f"EXCEPTION: {s3_key}\n{str(e)}",
                subject=f"ETL EXCEPTION: {s3_key}"
            )
            raise


# Lambda handler (event-driven via S3 trigger)
def lambda_handler(event: dict, context) -> dict:
    config = PipelineConfig(
        source_bucket='nab-data-lake',
        source_prefix='raw/transactions/',
        glue_job_name='transactions-etl',
        sns_topic_arn='arn:aws:sns:ap-southeast-2:123456789:etl-alerts',
        sqs_queue_url='https://sqs.ap-southeast-2.amazonaws.com/123456789/etl-queue'
    )
    orchestrator = S3ETLOrchestrator(config)

    for record in event.get('Records', []):
        s3_key = record['s3']['object']['key']
        orchestrator.process_file(s3_key)

    return {'statusCode': 200, 'body': 'Processed'}
```

**🔁 Follow-ups:**
- What is the difference between `boto3.client` and `boto3.resource`?
- How do you handle AWS credentials securely? What is the credential chain?
- How do you use S3 Transfer Manager for large file uploads (multipart)?
- What is AWS Secrets Manager and how do you integrate it into a Python pipeline?

---

## 2.4 Python Coding Problems

---

### ❓ Q10: Python Coding — Common Data Engineering Patterns

**💻 Problem 1: Flatten nested JSON (common for API ingestion):**

```python
from typing import Any

def flatten_json(obj: dict, parent_key: str = '', sep: str = '.') -> dict:
    """
    Flatten a nested JSON/dict into a single-level dict.
    {'a': {'b': {'c': 1}}} → {'a.b.c': 1}
    """
    items = {}
    for key, value in obj.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    items.update(flatten_json(item, f"{new_key}[{i}]", sep))
                else:
                    items[f"{new_key}[{i}]"] = item
        else:
            items[new_key] = value
    return items

# Test
nested = {'customer': {'id': 1, 'address': {'city': 'Sydney', 'postcode': '2000'}}}
print(flatten_json(nested))
# {'customer.id': 1, 'customer.address.city': 'Sydney', 'customer.address.postcode': '2000'}
```

**💻 Problem 2: Watermark-based incremental extraction:**

```python
import sqlite3
import json
from datetime import datetime
from pathlib import Path

WATERMARK_FILE = Path("watermarks.json")

def load_watermarks() -> dict:
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}

def save_watermark(source_table: str, watermark: str) -> None:
    marks = load_watermarks()
    marks[source_table] = watermark
    WATERMARK_FILE.write_text(json.dumps(marks, indent=2))

def extract_incremental(conn, table: str, watermark_col: str) -> tuple[list, str]:
    """Extract rows added/updated since last watermark."""
    marks = load_watermarks()
    last_mark = marks.get(table, '1970-01-01 00:00:00')

    cursor = conn.execute(
        f"SELECT * FROM {table} WHERE {watermark_col} > ? ORDER BY {watermark_col}",
        (last_mark,)
    )
    rows = cursor.fetchall()
    new_mark = rows[-1][-1] if rows else last_mark  # last value of watermark column
    return rows, new_mark
```

**💻 Problem 3: Parallel S3 file download with rate limiting:**

```python
import concurrent.futures
import boto3
from pathlib import Path

def download_s3_file(args: tuple) -> tuple[str, bool]:
    """Download a single S3 file. Returns (key, success)."""
    bucket, key, local_dir = args
    s3 = boto3.client('s3')
    local_path = Path(local_dir) / Path(key).name
    try:
        s3.download_file(bucket, key, str(local_path))
        return key, True
    except Exception as e:
        logger.error(f"Failed to download {key}: {e}")
        return key, False


def parallel_s3_download(
    bucket: str,
    keys: list[str],
    local_dir: str,
    max_workers: int = 5
) -> dict[str, bool]:
    """Download multiple S3 files in parallel (max 5 at a time)."""
    args = [(bucket, key, local_dir) for key in keys]
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_s3_file, arg): arg for arg in args}
        for future in concurrent.futures.as_completed(futures):
            key, success = future.result()
            results[key] = success

    failed = [k for k, v in results.items() if not v]
    if failed:
        logger.warning(f"{len(failed)} files failed to download: {failed}")
    return results
```

---

---

# ⚡ Section 3: PySpark & Apache Spark

---

## 3.1 Spark Architecture & Execution

---

### ❓ Q11: Explain Spark's execution model — DAG, stages, tasks, Driver vs Executor

**✅ Answer:**

```
Job (triggered by action)
  └── Stage 1 (narrow transformations — no shuffle)
        ├── Task 1 (Partition 1)
        ├── Task 2 (Partition 2)
        └── Task N (Partition N)
  └── [SHUFFLE BOUNDARY]
  └── Stage 2 (after shuffle)
        ├── Task 1
        └── Task N

Narrow transformations (same stage): map, filter, flatMap, union, select
Wide transformations (new stage):     groupBy, join, distinct, repartition, sort
```

**💻 Spark Configuration Tuning:**

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NAB-Transactions-ETL")
    # Memory settings
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")  # Off-heap for shuffle buffers
    .config("spark.driver.memory", "4g")
    # Parallelism
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")  # Tune for data size (default 200)
    # Adaptive Query Execution (Spark 3+)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    # Dynamic allocation (EMR)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "50")
    .getOrCreate()
)
```

**🔁 Follow-ups:**
- What is the difference between `cache()` and `persist()`? What storage levels exist?
- What causes OOM errors in the Spark executor vs driver? How do you fix each?
- What is speculative execution and when do you enable it?
- How do you choose the number of partitions for a Spark job?

---

## 3.2 Data Skew & Optimization

---

### ❓ Q12: What is data skew in Spark? How do you detect and fix it?

**💻 Detection:**

```python
# Check partition size distribution before processing
df = spark.read.parquet("s3://bucket/transactions/")

# Count rows per partition
from pyspark.sql.functions import spark_partition_id

df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)

# Check key distribution (find skewed keys)
df.groupBy("customer_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(10)
```

**💻 Fix 1 — Salting (most universal):**

```python
from pyspark.sql import functions as F

def salt_join(left_df, right_df, join_key: str, salt_buckets: int = 100):
    """Join with salting to distribute skewed keys."""
    # Add salt to left (large) table
    left_salted = left_df.withColumn(
        "salt", (F.rand() * salt_buckets).cast("int")
    ).withColumn(
        "salted_key", F.concat(F.col(join_key), F.lit("_"), F.col("salt").cast("string"))
    )

    # Explode right (small/dimension) table for all salt values
    salt_array = F.array([F.lit(i) for i in range(salt_buckets)])
    right_exploded = right_df.withColumn("salt", F.explode(salt_array)) \
        .withColumn(
            "salted_key",
            F.concat(F.col(join_key), F.lit("_"), F.col("salt").cast("string"))
        )

    return left_salted.join(right_exploded, "salted_key", "left") \
                      .drop("salt", "salted_key")


# Usage: customer_id 'CASH' represents 40% of all transactions
result = salt_join(transactions_df, customers_df, "customer_id", salt_buckets=50)
```

**💻 Fix 2 — Broadcast Join (small dimension table):**

```python
from pyspark.sql.functions import broadcast

# Broadcasting the smaller table eliminates shuffle entirely
result = large_transactions.join(
    broadcast(small_customers),  # Must fit in executor memory (~200MB default)
    on="customer_id",
    how="left"
)

# Manually set broadcast threshold (default 10MB, increase if needed)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(200 * 1024 * 1024))  # 200MB
```

**💻 Fix 3 — Split and union (skewed key isolated):**

```python
# Separate the whale (bot user with 50M events) from normal users
SKEWED_KEY = "BOT_USER_001"

normal_df = df.filter(F.col("user_id") != SKEWED_KEY)
skewed_df = df.filter(F.col("user_id") == SKEWED_KEY)

# Process normally
normal_result = normal_df.join(dim_df, "user_id", "left")

# Process skewed with more partitions
skewed_result = skewed_df.repartition(200) \
                          .join(broadcast(dim_df), "user_id", "left")

final = normal_result.union(skewed_result)
```

**🔁 Follow-ups:**
- What is AQE (Adaptive Query Execution)? How does Spark 3 handle skew automatically?
- What is the difference between `repartition()` and `coalesce()`?
- What is a broadcast variable vs accumulator?

---

## 3.3 PySpark Coding Problems

---

### ❓ Q13: PySpark Coding — Transformations, Aggregations, SCD Type 2

**💻 Complex Aggregation with Window Functions:**

```python
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("ETL").getOrCreate()

df = spark.read.parquet("s3://bucket/transactions/")

# Window: 7-day rolling average per customer
w_7day = Window.partitionBy("customer_id") \
    .orderBy(F.col("transaction_date").cast("long")) \
    .rangeBetween(-7 * 86400, 0)  # 7 days in seconds

# Window: running total per customer per year
w_running = Window.partitionBy("customer_id", F.year("transaction_date")) \
    .orderBy("transaction_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = df.withColumn("rolling_7d_avg", F.avg("amount").over(w_7day)) \
           .withColumn("ytd_total", F.sum("amount").over(w_running)) \
           .withColumn("txn_rank",
               F.dense_rank().over(
                   Window.partitionBy("customer_id")
                         .orderBy(F.col("amount").desc())
               ))
result.show()
```

**💻 SCD Type 2 in PySpark (without SQL MERGE):**

```python
from pyspark.sql import functions as F, DataFrame

def apply_scd2(
    target_df: DataFrame,
    source_df: DataFrame,
    business_key: str,
    tracked_cols: list[str]
) -> DataFrame:
    """
    Apply SCD Type 2 logic in PySpark.
    Returns the new complete target DataFrame.
    """
    today = F.current_date()
    far_future = F.lit("9999-12-31").cast("date")

    # Hash of tracked columns to detect changes
    hash_expr = F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in tracked_cols]))

    source_hashed = source_df.withColumn("source_hash", hash_expr)
    target_hashed = target_df.withColumn("target_hash", hash_expr)

    # Records that haven't changed — keep as-is
    unchanged = target_hashed.join(
        source_hashed, on=business_key, how="inner"
    ).filter(F.col("target_hash") == F.col("source_hash")) \
     .select(target_df.columns)

    # Records that have changed — expire the old version
    expired = target_hashed.join(
        source_hashed, on=business_key, how="inner"
    ).filter(
        (F.col("target_hash") != F.col("source_hash")) &
        (target_df["is_current"] == True)
    ).select(target_df.columns) \
     .withColumn("expiry_date", today) \
     .withColumn("is_current", F.lit(False))

    # New versions for changed records + completely new records
    changed_keys = expired.select(business_key)
    new_in_source = source_df.join(
        target_df.select(business_key).distinct(),
        on=business_key, how="left_anti"  # not in target at all
    )
    changed_source = source_df.join(changed_keys, on=business_key, how="inner")
    new_versions = changed_source.union(new_in_source) \
        .withColumn("effective_date", today) \
        .withColumn("expiry_date", far_future) \
        .withColumn("is_current", F.lit(True))

    return unchanged.union(expired).union(new_versions)
```

**💻 Read from multiple sources and merge:**

```python
def ingest_and_merge(
    spark: SparkSession,
    jdbc_url: str,
    s3_path: str,
    output_path: str
) -> None:
    """Read from Oracle DB and S3 flat files, merge, deduplicate, and write to Delta."""

    # Source 1: Oracle database (JDBC)
    oracle_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "(SELECT * FROM transactions WHERE updated_at > SYSDATE - 1) t")
        .option("user", "etl_user")
        .option("password", get_secret("oracle-password"))
        .option("numPartitions", 20)
        .option("partitionColumn", "transaction_id")
        .option("lowerBound", 1)
        .option("upperBound", 1_000_000)
        .load()
    )

    # Source 2: S3 flat files (CSV)
    flat_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(s3_path)
    )

    # Align schemas
    flat_df = flat_df.select(oracle_df.columns)

    # Union and deduplicate (keep latest by updated_at)
    w = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())
    merged = (
        oracle_df.union(flat_df)
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Write to Delta Lake
    (
        merged.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .partitionBy("transaction_year", "transaction_month")
        .save(output_path)
    )
    print(f"Written {merged.count():,} records to Delta")
```

**🎯 Scenarios:**
- *"Process a Kafka stream of bank transactions, compute 5-minute windowed fraud scores per customer, and write to Delta Lake with exactly-once semantics."*

```python
from pyspark.sql.functions import window, from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("event_time", TimestampType())

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "10 minutes")
)

windowed = (
    stream_df
    .groupBy(window("event_time", "5 minutes", "1 minute"), "customer_id")
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount")
    )
    .withColumn("fraud_score",
        F.when(F.col("txn_count") > 10, F.lit(0.9))
         .when(F.col("total_amount") > 10000, F.lit(0.7))
         .otherwise(F.lit(0.1))
    )
)

query = (
    windowed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://bucket/checkpoints/fraud/")
    .start("s3://bucket/fraud_scores/")
)
query.awaitTermination()
```

---

---

# 🔄 Section 4: ETL / ELT Pipelines

---

### ❓ Q14: Design a production-grade, idempotent ETL pipeline with full error handling

**💻 Complete Pipeline Class:**

```python
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib

class PipelineStatus(Enum):
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"

@dataclass
class PipelineRun:
    pipeline_name: str
    execution_date: str
    run_id: str = field(default_factory=lambda: hashlib.md5(
        f"{datetime.now().isoformat()}".encode()
    ).hexdigest()[:8])
    status: PipelineStatus = PipelineStatus.RUNNING
    rows_extracted: int = 0
    rows_loaded: int = 0
    error_message: str = None

class IdempotentETLPipeline:
    """
    Idempotent ETL: running multiple times produces same result.
    Key principle: use execution_date as idempotency key.
    """
    def __init__(self, spark, config: dict):
        self.spark = spark
        self.config = config
        self.audit_table = config['audit_table']

    def check_already_processed(self, execution_date: str) -> bool:
        """Check if this date was already successfully processed."""
        result = self.spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {self.audit_table}
            WHERE execution_date = '{execution_date}'
              AND status = 'SUCCESS'
        """).collect()[0]['cnt']
        return result > 0

    def extract(self, execution_date: str) -> DataFrame:
        """Extract data for a specific date (idempotent — always same result for same date)."""
        return self.spark.read.format("jdbc") \
            .option("url", self.config['jdbc_url']) \
            .option("dbtable", f"""(
                SELECT * FROM source_table
                WHERE DATE(created_at) = '{execution_date}'
            ) t""") \
            .load()

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business rules."""
        return (
            df.filter(F.col("amount") > 0)
              .withColumn("amount_aud", F.col("amount") * F.col("fx_rate"))
              .withColumn("processed_at", F.current_timestamp())
              .dropDuplicates(["transaction_id"])
        )

    def load(self, df: DataFrame, execution_date: str) -> int:
        """Overwrite the partition for this date (idempotent overwrite)."""
        df.write.format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"DATE(created_at) = '{execution_date}'") \
            .save(self.config['target_path'])
        return df.count()

    def validate(self, source_count: int, target_count: int, threshold: float = 0.99) -> bool:
        """Validate load completeness."""
        ratio = target_count / source_count if source_count > 0 else 0
        if ratio < threshold:
            raise ValueError(f"Load completeness {ratio:.2%} below threshold {threshold:.2%}")
        return True

    def run(self, execution_date: str) -> PipelineRun:
        run = PipelineRun(
            pipeline_name=self.config['name'],
            execution_date=execution_date
        )

        # Idempotency check — skip if already done
        if self.check_already_processed(execution_date):
            run.status = PipelineStatus.SKIPPED
            print(f"Skipping {execution_date} — already processed")
            return run

        try:
            raw_df = self.extract(execution_date)
            run.rows_extracted = raw_df.cache().count()

            transformed_df = self.transform(raw_df)
            run.rows_loaded = self.load(transformed_df, execution_date)

            self.validate(run.rows_extracted, run.rows_loaded)
            run.status = PipelineStatus.SUCCESS

        except Exception as e:
            run.status = PipelineStatus.FAILED
            run.error_message = str(e)
            raise
        finally:
            self._write_audit(run)

        return run

    def _write_audit(self, run: PipelineRun) -> None:
        """Write pipeline run metadata to audit table."""
        audit_data = [(
            run.run_id, run.pipeline_name, run.execution_date,
            run.status.value, run.rows_extracted, run.rows_loaded,
            run.error_message, datetime.now().isoformat()
        )]
        audit_df = self.spark.createDataFrame(
            audit_data,
            ["run_id","pipeline_name","execution_date","status",
             "rows_extracted","rows_loaded","error_message","logged_at"]
        )
        audit_df.write.format("delta").mode("append").save(self.config['audit_path'])
```

**🔁 Follow-ups:**
- What is CDC (Change Data Capture)? How does it differ from watermark-based extraction?
- What tools implement CDC? (Debezium, AWS DMS, Oracle GoldenGate)
- How do you implement exactly-once vs at-least-once semantics?
- How do you handle schema evolution in a pipeline?

---

---

# 🏗️ Section 5: Data Modeling

---

### ❓ Q15: Compare Star Schema vs Data Vault 2.0. When do you choose each?

**✅ Answer:**

```
STAR SCHEMA (Kimball)
─────────────────────
        DIM_DATE
           │
DIM_CUSTOMER ─── FACT_TRANSACTIONS ─── DIM_PRODUCT
           │
        DIM_BRANCH

Pros:  Simple, fast queries, BI-tool friendly, easy for analysts
Cons:  Rigid, hard to add new entities, poor auditability
Use:   Data Marts, stable reporting domains

DATA VAULT 2.0
──────────────
HUB_CUSTOMER ─── LINK_CUSTOMER_ACCOUNT ─── HUB_ACCOUNT
     │                                           │
SAT_CUSTOMER_DETAILS                    SAT_ACCOUNT_DETAILS
(tracks all history per attribute group)

Pros:  Highly auditable, handles multi-source integration,
       flexible, every row has load_date + record_source
Cons:  Complex queries (many joins), harder for analysts,
       more tables
Use:   Banking/insurance (NAB), regulatory compliance,
       multi-source integration, frequent schema changes
```

**💻 Data Vault DDL Example:**

```sql
-- HUB: Business key only
CREATE TABLE HUB_CUSTOMER (
    hub_customer_hk  CHAR(32)     PRIMARY KEY,  -- MD5 of business key
    customer_id      VARCHAR(50)  NOT NULL,
    load_date        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source    VARCHAR(100) NOT NULL  -- which system loaded this
);

-- SATELLITE: Descriptive attributes with full history
CREATE TABLE SAT_CUSTOMER_DETAILS (
    hub_customer_hk  CHAR(32)     NOT NULL REFERENCES HUB_CUSTOMER,
    load_date        TIMESTAMP    NOT NULL,
    load_end_date    TIMESTAMP,              -- NULL = current record
    record_source    VARCHAR(100) NOT NULL,
    hash_diff        CHAR(32)     NOT NULL,  -- MD5 of all attribute values
    email            VARCHAR(200),
    address          VARCHAR(500),
    credit_score     INTEGER,
    PRIMARY KEY (hub_customer_hk, load_date)
);

-- LINK: Relationship between Hubs (no attributes, just keys)
CREATE TABLE LINK_CUSTOMER_ACCOUNT (
    link_customer_account_hk  CHAR(32) PRIMARY KEY,
    hub_customer_hk           CHAR(32) NOT NULL REFERENCES HUB_CUSTOMER,
    hub_account_hk            CHAR(32) NOT NULL REFERENCES HUB_ACCOUNT,
    load_date                 TIMESTAMP NOT NULL,
    record_source             VARCHAR(100) NOT NULL
);
```

**🔁 Follow-ups:**
- What is a Point-in-Time (PIT) table in Data Vault and why is it critical for performance?
- What is a Business Vault? How does it differ from Raw Vault?
- How do you handle multi-source customer integration in Data Vault? (Same-as Link)
- How do you convert a Data Vault model to a Star Schema Information Mart?

**🎯 Scenarios:**
- *"NAB has customer data from 5 systems (CRM, Core Banking, Digital, Cards, Loans) with different customer IDs. Design the Data Vault Hub."*
- *"A new regulation requires tracking every change to a customer's financial profile. Which model do you choose?"*

---

---

# ☁️ Section 6: AWS Services

---

### ❓ Q16: Compare AWS Glue vs EMR vs Lambda for ETL. When do you choose each?

| Dimension | Lambda | Glue | EMR |
|-----------|--------|------|-----|
| Runtime limit | 15 min | Unlimited | Unlimited |
| Max memory | 10 GB | ~320 GB | Cluster-dependent |
| Cold start | Yes (~1s) | Yes (1-5 min) | Yes (5-10 min) |
| Server management | None | None | Some (cluster config) |
| Cost model | Per request | Per DPU-hour | Per instance-hour |
| Best for | Triggers, routing, small transforms | Moderate ETL, catalog | Large-scale, optimised Spark |
| Spark version control | N/A | Limited | Full control |
| Custom libraries | Limited | Via wheel files | Full control |

---

### ❓ Q17: Design a serverless event-driven pipeline (S3 → Lambda → Glue → Redshift → SNS)

```
S3 Upload
   │ (S3 Event Notification)
   ▼
SQS Queue ← (decouples S3 and Lambda; handles burst)
   │
   ▼
Lambda (validates file, triggers Glue)
   │
   ▼
Glue Job (PySpark ETL: S3 → Transform → Redshift)
   │
   ├── SUCCESS → SNS "ETL Complete" → Email / Slack
   └── FAILURE → SNS "ETL Failed" → PagerDuty + Dead Letter S3
```

```python
# Lambda handler
import boto3
import json

glue = boto3.client('glue')
sns = boto3.client('sns')

def lambda_handler(event, context):
    for record in event['Records']:
        # SQS wraps the S3 event
        body = json.loads(record['body'])
        s3_key = body['Records'][0]['s3']['object']['key']

        # Basic validation
        if not s3_key.endswith('.csv'):
            print(f"Skipping non-CSV file: {s3_key}")
            continue

        # Trigger Glue job
        response = glue.start_job_run(
            JobName='transactions-etl',
            Arguments={
                '--source_key': s3_key,
                '--target_schema': 'dw',
                '--target_table': 'transactions_staging',
            }
        )
        print(f"Glue job started: {response['JobRunId']} for {s3_key}")
```

---

---

# 🏆 Section 7: Databricks & Delta Lake

---

### ❓ Q18: Delta Lake ACID, Time Travel, MERGE — with code

**💻 MERGE (Upsert) Pattern:**

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://bucket/gold/customers/")

# Upsert: insert new, update changed
delta_table.alias("target").merge(
    source=staging_df.alias("source"),
    condition="target.customer_id = source.customer_id"
).whenMatchedUpdate(
    condition="target.email <> source.email OR target.address <> source.address",
    set={
        "email":          "source.email",
        "address":        "source.address",
        "updated_at":     "source.updated_at",
    }
).whenNotMatchedInsertAll() \
 .execute()
```

**💻 Time Travel:**

```python
# Query historical version
df_v5 = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("s3://bucket/gold/customers/")

# Query by timestamp
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-06-01") \
    .load("s3://bucket/gold/customers/")

# Rollback to previous version (restore)
delta_table.restoreToVersion(5)

# Show full history
delta_table.history().show(truncate=False)
```

**💻 Medallion Architecture (Bronze → Silver → Gold):**

```python
# BRONZE: Raw ingestion — no transformation, preserve exactly as received
def ingest_to_bronze(spark, source_path: str, bronze_path: str) -> None:
    (
        spark.read.option("inferSchema", False)  # Keep all as string
             .csv(source_path, header=True)
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
             .write.format("delta")
             .mode("append")
             .partitionBy("_ingested_date")
             .save(bronze_path)
    )

# SILVER: Clean, type, deduplicate, validate
def bronze_to_silver(spark, bronze_path: str, silver_path: str) -> None:
    bronze_df = spark.read.format("delta").load(bronze_path) \
        .filter(F.col("_ingested_date") == F.current_date())

    silver_df = (
        bronze_df
        .withColumn("amount", F.col("amount").cast("decimal(18,4)"))
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("amount") > 0)
        .dropDuplicates(["transaction_id"])
    )

    # Delta Live Tables quality expectations (DLT syntax)
    # @dlt.expect_or_drop("valid_amount", "amount > 0")
    # @dlt.expect_or_fail("not_null_id", "transaction_id IS NOT NULL")

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "transaction_date = current_date()")
        .save(silver_path)
    )

# GOLD: Business aggregations for BI
def silver_to_gold(spark, silver_path: str, gold_path: str) -> None:
    (
        spark.read.format("delta").load(silver_path)
        .groupBy("customer_id", F.month("transaction_date").alias("month"))
        .agg(
            F.sum("amount").alias("monthly_spend"),
            F.count("*").alias("txn_count"),
            F.avg("amount").alias("avg_txn_amount")
        )
        .write.format("delta")
        .mode("overwrite")
        .save(gold_path)
    )
```

---

---

# 🔧 Section 9: CI/CD & DevOps

---

### ❓ Q19: Jenkins Pipeline for Data Engineering (Declarative)

```groovy
// Jenkinsfile
pipeline {
    agent any

    environment {
        AWS_REGION       = 'ap-southeast-2'
        ECR_REGISTRY     = '123456789.dkr.ecr.ap-southeast-2.amazonaws.com'
        IMAGE_NAME       = 'nab-etl-pipeline'
        PYTHON_VERSION   = '3.10'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    pip install -r requirements-dev.txt
                    pytest tests/unit/ \
                        --junitxml=reports/unit-tests.xml \
                        --cov=src \
                        --cov-report=xml:reports/coverage.xml \
                        --cov-fail-under=80
                '''
            }
            post {
                always {
                    junit 'reports/unit-tests.xml'
                    cobertura coberturaReportFile: 'reports/coverage.xml'
                }
            }
        }

        stage('Data Quality Tests') {
            steps {
                sh '''
                    python -m great_expectations checkpoint run transactions_checkpoint
                '''
            }
        }

        stage('Build Docker Image') {
            steps {
                sh '''
                    docker build \
                        --build-arg BUILD_NUMBER=${BUILD_NUMBER} \
                        -t ${IMAGE_NAME}:${BUILD_NUMBER} \
                        -t ${IMAGE_NAME}:latest .
                '''
            }
        }

        stage('Push to ECR') {
            steps {
                withAWS(region: "${AWS_REGION}") {
                    sh '''
                        aws ecr get-login-password | docker login --username AWS --password-stdin ${ECR_REGISTRY}
                        docker tag ${IMAGE_NAME}:${BUILD_NUMBER} ${ECR_REGISTRY}/${IMAGE_NAME}:${BUILD_NUMBER}
                        docker push ${ECR_REGISTRY}/${IMAGE_NAME}:${BUILD_NUMBER}
                    '''
                }
            }
        }

        stage('Deploy to Staging') {
            steps {
                sh '''
                    cd terraform/environments/staging
                    terraform init
                    terraform plan -var="image_tag=${BUILD_NUMBER}" -out=tfplan
                    terraform apply tfplan
                '''
            }
        }

        stage('Integration Tests') {
            steps {
                sh 'pytest tests/integration/ --junitxml=reports/integration-tests.xml'
            }
        }

        stage('Deploy to Production') {
            when { branch 'main' }
            steps {
                input message: 'Deploy to Production?', ok: 'Deploy'
                sh '''
                    cd terraform/environments/production
                    terraform init
                    terraform apply -var="image_tag=${BUILD_NUMBER}" -auto-approve
                '''
            }
        }
    }

    post {
        failure {
            slackSend channel: '#data-engineering-alerts',
                      color: 'danger',
                      message: "Build FAILED: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
        }
        success {
            slackSend channel: '#data-engineering-alerts',
                      color: 'good',
                      message: "Build SUCCEEDED: ${env.JOB_NAME} #${env.BUILD_NUMBER}"
        }
    }
}
```

---

---

# 🐚 Section 10: Unix / Shell Scripting

---

### ❓ Q20: Shell script — directory monitor, parallel S3 downloads, log parsing

**💻 Production File Monitor:**

```bash
#!/bin/bash
# monitor_and_trigger.sh — Watch directory, trigger ETL, log & alert

set -euo pipefail  # Exit on error, undefined var, pipe failure

WATCH_DIR="/data/inbound"
PROCESSED_DIR="/data/processed"
FAILED_DIR="/data/failed"
LOG_FILE="/var/log/etl_monitor.log"
SNS_TOPIC="arn:aws:sns:ap-southeast-2:123456789:etl-alerts"
ETL_SCRIPT="/opt/etl/run_etl.py"
LOCK_FILE="/tmp/etl_monitor.lock"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$1] $2" | tee -a "$LOG_FILE"; }

# Prevent multiple instances
exec 200>"$LOCK_FILE"
flock -n 200 || { log "WARN" "Another instance is running. Exiting."; exit 0; }

mkdir -p "$PROCESSED_DIR" "$FAILED_DIR"
log "INFO" "Monitor started. Watching: $WATCH_DIR"

while true; do
    for file in "$WATCH_DIR"/*.{csv,parquet,json} 2>/dev/null; do
        # Skip if no files match glob
        [[ -f "$file" ]] || continue

        filename=$(basename "$file")
        log "INFO" "Processing: $filename"

        # Run ETL and capture exit code
        if python3 "$ETL_SCRIPT" --file "$file" \
                                  --output-table "transactions_staging" \
                                  >> "$LOG_FILE" 2>&1; then
            dest="$PROCESSED_DIR/${filename%.csv}.$(date +%Y%m%d_%H%M%S).csv"
            mv "$file" "$dest"
            log "INFO" "SUCCESS: $filename → $dest"
        else
            exit_code=$?
            dest="$FAILED_DIR/${filename%.csv}.$(date +%Y%m%d_%H%M%S).FAILED"
            mv "$file" "$dest"
            log "ERROR" "FAILED (exit $exit_code): $filename"
            aws sns publish \
                --topic-arn "$SNS_TOPIC" \
                --subject "ETL FAILURE: $filename" \
                --message "File: $filename failed with exit code $exit_code. Check $LOG_FILE" \
                > /dev/null
        fi
    done
    sleep 30
done
```

**💻 Parallel S3 Download:**

```bash
#!/bin/bash
# parallel_s3_download.sh — Download S3 files in parallel (N at a time)

set -euo pipefail

BUCKET="nab-data-lake"
PREFIX="raw/transactions/2024-01-01/"
LOCAL_DIR="/tmp/downloads"
MAX_PARALLEL=5

mkdir -p "$LOCAL_DIR"

# List all files and download in parallel
aws s3 ls "s3://${BUCKET}/${PREFIX}" --recursive \
    | awk '{print $4}' \
    | xargs -P "$MAX_PARALLEL" -I{} bash -c '
        key="{}"
        filename=$(basename "$key")
        echo "Downloading: $key"
        if aws s3 cp "s3://'"$BUCKET"'/$key" "'"$LOCAL_DIR"'/$filename" --quiet; then
            echo "SUCCESS: $filename"
        else
            echo "FAILED: $filename" >&2
        fi
    '

echo "Download complete. Files in $LOCAL_DIR:"
ls -lh "$LOCAL_DIR"
```

**💻 Log parsing with Unix tools:**

```bash
#!/bin/bash
# Parse ETL log: count errors by type, show top 10

LOG_FILE="/var/log/etl_monitor.log"

echo "=== Error Summary (last 24h) ==="
grep "\[ERROR\]" "$LOG_FILE" \
    | awk '{print $4}' \               # Extract timestamp
    | awk -F'T' '$1 >= "'$(date -d '24 hours ago' +%Y-%m-%d)'"' \  # Last 24h
    | sort | uniq -c | sort -rn | head -10

echo ""
echo "=== Files Processed Today ==="
grep "SUCCESS" "$LOG_FILE" \
    | grep "$(date +%Y-%m-%d)" \
    | wc -l

echo ""
echo "=== Failed Files Today ==="
grep "FAILED" "$LOG_FILE" \
    | grep "$(date +%Y-%m-%d)" \
    | awk '{print $NF}'  # Print filename (last field)
```

---

---

# 🛡️ Section 11: Data Quality & Observability

---

### ❓ Q21: Implement data quality checks using Great Expectations and dbt

**💻 Great Expectations:**

```python
import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint

def validate_transactions(df: pd.DataFrame) -> bool:
    """Run data quality suite on transactions DataFrame."""
    ge_df = ge.from_pandas(df)

    results = ge_df.expect_compound_columns_to_be_unique(["transaction_id"])
    results = ge_df.expect_column_values_to_not_be_null("transaction_id")
    results = ge_df.expect_column_values_to_not_be_null("customer_id")
    results = ge_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=1_000_000)
    results = ge_df.expect_column_values_to_be_in_set(
        "currency", value_set=["AUD", "USD", "EUR", "GBP"]
    )
    results = ge_df.expect_column_values_to_match_regex(
        "transaction_date", r"^\d{4}-\d{2}-\d{2}$"
    )
    results = ge_df.expect_column_pair_values_to_be_equal(
        "source_amount_aud",
        "calculated_amount_aud"
    )

    # Row count check (drift detection)
    results = ge_df.expect_table_row_count_to_be_between(
        min_value=1_000, max_value=10_000_000
    )

    validation_results = ge_df.validate()
    success = validation_results["success"]

    if not success:
        failed_checks = [
            r for r in validation_results["results"] if not r["success"]
        ]
        for check in failed_checks:
            logger.error(f"Quality check FAILED: {check['expectation_config']['expectation_type']}")

    return success
```

**💻 dbt Tests (schema.yml):**

```yaml
# models/schema.yml
version: 2

models:
  - name: fact_transactions
    description: "Cleaned and validated transactions"
    columns:
      - name: transaction_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
      - name: currency
        tests:
          - accepted_values:
              values: ['AUD', 'USD', 'EUR', 'GBP']

  # Custom SQL test
  - name: fact_transactions
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('stg_transactions')
```

---

---

# 🏛️ Section 12: System Design — Edge Cases

---

### ❓ Q22: Design a 50,000 TPS real-time financial transaction system with fraud detection

```
                    ┌─────────────────────────────────────────────────┐
                    │           INGESTION LAYER                        │
  Bank API          │  API Gateway → Kafka (50 partitions, 3 replicas) │
  Mobile App   ───► │  Topic: raw-transactions                         │
  ATM              │  Throughput: 50K TPS × 500B = 25MB/s             │
                    └──────────────────┬──────────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────────────┐
                    │         REAL-TIME FRAUD DETECTION                │
                    │  Apache Flink (CEP — Complex Event Processing)   │
                    │  Rules: velocity, geo-anomaly, amount threshold  │
                    │  ML: SageMaker endpoint (<20ms inference)        │
                    │  Target: <100ms end-to-end latency               │
                    └─────────────┬────────────────┬──────────────────┘
                                  │                │
                           APPROVE              DECLINE/REVIEW
                                  │                │
                    ┌─────────────▼──┐   ┌────────▼───────────┐
                    │  Cassandra     │   │  Fraud Case DB      │
                    │  (hot writes)  │   │  (PostgreSQL)        │
                    └───────┬────────┘   └─────────────────────┘
                            │ (Kafka CDC)
                    ┌───────▼────────────────────────────────────────┐
                    │           BATCH ANALYTICS LAYER                  │
                    │  S3 (Delta Lake) ← Kafka Connect (Parquet)      │
                    │  EMR/Databricks for daily aggregations           │
                    │  Redshift for BI / Power BI dashboards          │
                    └────────────────────────────────────────────────┘
```

**Edge Cases to address:**

| Scenario | Risk | Solution |
|----------|------|----------|
| Clock skew between systems | Wrong event ordering | Use event time (Kafka header), not processing time |
| Fraud model endpoint down | All transactions blocked or all approved | Circuit breaker: fail open (approve + flag for review) |
| 10x peak load (Black Friday) | Kafka consumer lag, Flink backpressure | Kafka partition auto-scale, Flink reactive mode |
| Duplicate transaction (retry) | Double charge | Idempotency key in message header, dedup in Cassandra |
| APRA 7-year retention | Storage cost | S3 Intelligent-Tiering + Glacier after 90 days + Object Lock (WORM) |
| Network partition | Lost transactions | Local cache + SQS retry queue per region |

---

### ❓ Q23: Design a Disaster Recovery strategy (RPO=1hr, RTO=4hr)

```
PRIMARY REGION (ap-southeast-2 Sydney)
├── Redshift (hourly snapshots → cross-region copy)
├── S3 Delta Lake (cross-region replication — near real-time)
├── RDS metadata DB (continuous WAL streaming to standby)
├── Airflow on EKS (metadata DB replicated)
└── Kafka (MirrorMaker2 to DR region)

DR REGION (ap-southeast-1 Singapore)
├── Redshift (snapshot restore target — < 1hr behind)
├── S3 replica (CRR enabled — < 15 min lag)
├── RDS standby (promote in < 10 min)
├── Airflow standby (cold — spin up via Terraform)
└── Kafka replica

DR RUNBOOK:
T+0:00  Detect failure via CloudWatch alarms
T+0:30  Trigger DR plan, start Terraform apply in DR region
T+1:30  Restore Redshift from latest snapshot (within RPO)
T+2:00  Validate data completeness and quality checks
T+3:00  Start Airflow pipelines in DR, replay from S3 raw zone
T+3:30  Switch Route53 DNS to DR endpoints
T+4:00  Notify stakeholders — DR complete ✓
```

---

---

# 📊 Section 13: Program Management (GlobalLogic Focus)

---

### ❓ Q24: How do you manage delivery for 3 parallel workstreams competing for the same 2 senior engineers?

**✅ Answer — Structured Approach:**

```
1. ASSESS — Build a capacity vs demand matrix
   ┌──────────────┬────────────┬──────────┬──────────┐
   │ Workstream   │ Effort     │ Priority │ Deadline │
   ├──────────────┼────────────┼──────────┼──────────┤
   │ Core Banking │ 8 weeks    │ P1 (Rev) │ Q4       │
   │ Reporting    │ 4 weeks    │ P2 (Reg) │ Q1       │
   │ ML Platform  │ 6 weeks    │ P3 (Inno)│ Q2       │
   └──────────────┴────────────┴──────────┴──────────┘

2. SEQUENCE — Serialize where possible, identify true parallelism
3. NEGOTIATE — Present options with trade-offs to stakeholders (not just problems)
4. COMMUNICATE — Weekly status, RAID log, transparent risk register
5. PROTECT — Shield engineers from context-switching; one primary assignment each
```

**🔁 Follow-ups:**
- How do you handle a stakeholder who constantly changes requirements mid-sprint?
- How do you communicate a delivery risk to executive stakeholders?
- How do you define "done" for a data engineering pipeline?
- How do you measure team velocity and use it for forecasting?

**🎯 Scenarios:**
- *"A vendor is 6 weeks behind. This threatens your Q4 commitment. How do you respond?"*
- *"Two executives want contradictory features from the same pipeline. Who decides?"*
- *"A new urgent executive request arrives when all engineers are at 100% capacity. What do you do?"*

---

---

# 🎯 Section 14: Behavioral & Leadership

---

> Use the **STAR Method**: **S**ituation → **T**ask → **A**ction → **R**esult (quantify whenever possible)

| # | Question | What Interviewers Are Evaluating |
|---|----------|----------------------------------|
| 1 | *Tell me about a time you delivered a complex data project under tight deadlines.* | Prioritisation, communication, trade-offs, delivery |
| 2 | *Describe a situation where you disagreed with a technical decision made by leadership.* | Professional courage, evidence-based thinking, collaboration |
| 3 | *Tell me about a time you mentored a junior engineer. What was the outcome?* | Leadership, empathy, coaching ability |
| 4 | *Describe a major production incident you handled.* | Calm under pressure, triage, communication, post-mortem |
| 5 | *Tell me about a time you had to learn a new technology quickly.* | Learning agility, self-direction, delivery under uncertainty |
| 6 | *How did you influence a non-technical stakeholder to approve a significant infrastructure change?* | Communication, business case building, stakeholder management |
| 7 | *Tell me about a pipeline you built that failed in production and impacted the business.* | Accountability, root cause analysis, prevention |
| 8 | *Describe a time you had conflicting priorities and how you managed them.* | Decision-making, communication, priority frameworks |

---

---

# 📌 Quick Reference: JD Coverage Map

| Technology | NAB JD | GlobalLogic JD | Section |
|------------|--------|----------------|---------|
| SQL (Snowflake, Redshift, Hive-QL) | ✅ Essential | ✅ Proficient | §1 |
| Python | ✅ Essential | ✅ Strong hands-on | §2 |
| PySpark / Spark | ✅ Essential | ✅ Data Engineering | §3 |
| ETL/ELT Pipeline Design | ✅ Essential | ✅ Design & Monitor | §4 |
| Data Modeling (Star / Data Vault) | ✅ Essential | ✅ Expertise | §5 |
| AWS (S3, EMR, Glue, Lambda, Redshift) | ✅ Essential | Familiarity | §6 |
| Databricks / Delta Lake | ✅ Essential | Data Engineering | §7 |
| Hadoop (HDFS, Hive, Impala) | ✅ Essential | Background | §8 |
| CI/CD (Jenkins, Docker, Terraform, K8s) | ✅ Essential | Best Practices | §9 |
| Unix / Shell Scripting | ✅ Essential | ✅ Linux environments | §10 |
| Data Quality & Testing | ✅ Expected | ✅ Reliability | §11 |
| System Design (Edge Cases) | ✅ Expected | ✅ Architecture | §12 |
| Program / Stakeholder Management | Nice to have | ✅ Essential | §13 |
| Behavioral / Leadership | ✅ Expected | ✅ Executive comms | §14 |

---

## 🚀 Preparation Tips

1. **For every project** — Prepare a 2-minute summary: *what it was, what YOU built, what challenges you solved, what the outcome was.*
2. **Have 3 system design examples ready** — A pipeline you designed end-to-end, a performance problem you solved, a failure you handled.
3. **Know your numbers** — Data volumes, row counts, latency SLAs, team sizes, cost savings.
4. **Code fluency** — Be ready to write SQL window functions, PySpark transformations, and Python ETL code on a whiteboard or in a live coding tool.
5. **For NAB** — Financial services context matters. Know about regulatory requirements, data governance, auditability, and the Data Vault approach.
6. **For GlobalLogic** — Leadership and program management is equally weighted with technical skills. Prepare examples of leading teams, managing budgets, and presenting to executives.

---

*Prepared for NAB & GlobalLogic Data Engineering Interview — Senior / Lead Role | 7–8+ Years Experience*
