# 🎯 dunnhumby Senior Big Data Engineer — Complete Interview Prep

> **Role:** Senior Big Data Engineer | **Process:** 2 Technical Rounds + 1 Hiring Manager Round  
> **Stack:** Python · PySpark · Spark · Kafka · Airflow · AWS · Azure · Delta Lake · dbt · SQL  
> **Total Questions:** 210 curated Q&As with follow-up grilling questions

---

## 📋 Table of Contents

1. [🏗️ Projects Deep Dive](#-projects-deep-dive)
2. [🐍 Python & PySpark](#-python--pyspark-30-questions)
3. [🗄️ Advanced SQL](#️-advanced-sql-30-questions)
4. [☁️ Cloud — AWS & Azure](#️-cloud--aws--azure-30-questions)
5. [🔄 End-to-End Pipeline Architecture](#-end-to-end-pipeline-architecture-30-questions)
6. [📐 Schema Management & Data Quality](#-schema-management--data-quality-30-questions)
7. [💥 OOM & Data Skew](#-oom--data-skew-30-questions)
8. [🏛️ Architectural Problem-Solving Scenarios](#️-architectural-problem-solving-scenarios-30-questions)

---

# 🏗️ Projects Deep Dive

> These are your 5 flagship projects. Know every line. Be ready for 20 minutes of grilling on each.

---

## Project 1: Real-Time Transaction Anomaly Detection Pipeline

**Company:** Bank of America | **Duration:** 8 months | **Team:** 6 engineers (you as lead DE)

### Overview
Designed and built an end-to-end real-time pipeline to detect anomalous transactions across 12 million daily active accounts. Replaced a legacy batch detection system (24-hour lag) with a streaming pipeline delivering alerts within 90 seconds of transaction occurrence.

### Architecture
```
Card Transactions (Kafka) 
    → Azure Event Hubs (ingestion buffer, 32 partitions)
    → Azure Databricks Structured Streaming (feature extraction + rule engine)
    → Delta Lake (Bronze/Silver/Gold medallion)
    → Azure ML (anomaly scoring model, <50ms inference)
    → Azure Service Bus (alert dispatch)
    → Downstream: Case Management System + Mobile Notifications
```

### Tech Stack
- **Ingestion:** Apache Kafka → Azure Event Hubs (Kafka-compatible)
- **Processing:** PySpark Structured Streaming on Databricks (auto-scaling clusters)
- **Storage:** Azure Data Lake Storage Gen2 + Delta Lake
- **Orchestration:** Apache Airflow (Azure Managed Airflow / MWAA)
- **ML Serving:** Azure ML real-time endpoints
- **Monitoring:** Azure Monitor + custom Databricks dashboards

### Key Decisions & Trade-offs
- **Why Structured Streaming over Flink?** Team had deep PySpark expertise; Structured Streaming met the 90-second SLA requirement. Flink would have been chosen if sub-5-second latency was required.
- **Why Delta Lake over plain Parquet?** Needed ACID transactions for concurrent writes from streaming and batch jobs; schema enforcement; time travel for debugging.
- **Why Event Hubs over Kafka directly?** Managed service reduces operational overhead; Kafka-compatible API meant zero code changes.
- **Watermark strategy:** 10-minute watermark to handle late-arriving events from international transactions with network delays.

### Results
- Reduced fraud detection lag from 24 hours → 90 seconds
- Caught 34% more fraud cases in the first quarter post-launch
- Processed 180,000 transactions/minute at peak
- Saved ~$18M annually in fraud losses (estimated)
- Pipeline availability: 99.97% over 12 months

### Challenges Solved
- **Data skew:** High-value customer segment had 40x more transactions per customer_id → solved with AQE + salting
- **OOM on join:** Enriching transactions with 2 TB customer profile table → solved with broadcast join on pre-filtered subset + Delta caching
- **Schema evolution:** Source card system added 3 new fields mid-project → Delta Lake mergeSchema + Schema Registry handled transparently
- **Exactly-once:** Implemented idempotent writes using transaction_id as Delta merge key

---

## Project 2: Enterprise Data Lakehouse Migration (Hadoop → Azure)

**Company:** Bank of America | **Duration:** 14 months | **Team:** 12 engineers (you as senior DE)

### Overview
Led the technical migration of 8 PB of data and 400+ Hive/Oozie/MapReduce pipelines from an on-premises Hadoop cluster to Azure Data Lake + Databricks. Zero data loss, zero business disruption during cutover.

### Architecture
```
Legacy: HDFS + Hive + Oozie + MapReduce + HBase
    ↓ Migration
Modern: ADLS Gen2 + Delta Lake + Apache Spark + Airflow + Cosmos DB
```

### Migration Strategy
1. **Assessment phase (Month 1–2):** Catalogued all 400 jobs, classified by criticality, dependency mapping
2. **Parallel run phase (Month 3–8):** Run legacy and new pipelines simultaneously, reconcile outputs daily
3. **Cutover phase (Month 9–12):** Migrate traffic job by job, feature-flag controlled
4. **Decommission phase (Month 13–14):** Validate, decommission Hadoop cluster

### Key Technical Decisions
- **Hive → Delta Lake:** Converted all Hive external tables to Delta format; used CONVERT TO DELTA where possible
- **Oozie → Airflow:** Rewrote 400 Oozie workflows as Airflow DAGs; built a translator script for simple linear workflows
- **MapReduce → PySpark:** All MapReduce jobs rewritten in PySpark; 60% reduction in code size
- **HBase → Cosmos DB:** Low-latency key-value lookups migrated to Azure Cosmos DB (same API compatibility)

### Results
- Migrated 8 PB of data with zero data loss
- 400 pipelines migrated; average execution time reduced by 68%
- Infrastructure cost reduced by 42% (cloud vs. on-prem TCO)
- Hadoop cluster decommissioned on schedule

### Challenges Solved
- **Schema drift:** 140 Hive tables had undocumented schema changes over 5 years → built automated schema comparison tool
- **Oozie coordinator complexity:** Time-based triggers rewritten as Airflow sensors with proper dependency chains
- **Small files problem:** Hadoop had 50M+ small files → Delta Lake OPTIMIZE + ZORDER consolidated to 2M files, 30x faster reads

---

## Project 3: Customer 360 Data Platform

**Company:** Bank of America | **Duration:** 10 months | **Team:** 8 engineers (you as lead DE)

### Overview
Built a unified Customer 360 data platform aggregating data from 14 source systems (core banking, credit cards, mortgages, investments, CRM, digital channels) to create a single authoritative customer view. Used by 3,000 analysts and 12 downstream ML models.

### Architecture
```
14 Source Systems (CDC via Debezium + batch files)
    → AWS S3 (Raw Landing Zone)
    → AWS Glue (initial schema validation)
    → Apache Airflow on MWAA (orchestration)
    → PySpark on EMR (transformation layer)
    → Redshift (serving layer — star schema)
    → dbt (transformation + testing)
    → Tableau / Power BI (analytics)
    → Feature Store (ML consumption)
```

### Data Model
- **Hub entities:** Customer, Account, Product, Branch
- **Fact tables:** daily_transactions, monthly_balances, product_holdings
- **SCD Type 2 dimensions:** dim_customer (address, employment status, risk tier change history)
- **Update frequency:** Core facts daily; streaming events near-real-time via micro-batch

### Key Decisions
- **Why AWS for this project?** Existing enterprise AWS agreement; Redshift Spectrum allowed querying S3 directly without full load
- **Why dbt?** 3,000 analyst users; needed lineage, documentation, and test coverage for every model; dbt's DAG model fit perfectly
- **Why SCD Type 2?** Regulatory requirement to know customer risk tier at the exact time of a historical decision (Basel III compliance)
- **Why CDC over full load?** 14 source systems; full daily load was causing source database strain; log-based CDC (Debezium) had zero source impact

### Results
- Reduced analyst query time from 4 hours (joining raw tables) to <2 minutes (pre-built Gold layer)
- 100% of 14 source systems integrated with documented lineage
- dbt test coverage: 94% of all models have at least one data quality test
- Enabled 12 ML models consuming feature store data

### Challenges Solved
- **Identity resolution:** Same customer had different IDs across 14 systems → built deterministic + probabilistic matching using name/DOB/address/SSN hash
- **Late-arriving CDC events:** Debezium sometimes delivered events out of order → watermark-based deduplication with 30-minute window
- **Regulatory compliance:** PII fields masked at Silver layer; analysts only see tokenised identifiers; audit log of all data access

---

## Project 4: Regulatory Reporting Automation Pipeline

**Company:** Bank of America | **Duration:** 6 months | **Team:** 4 engineers (you as lead DE)

### Overview
Automated 23 manual regulatory reports (Fed Reserve, OCC, FDIC) that were previously produced by a team of 8 analysts running Excel macros. Built a fully automated, auditable, idempotent pipeline with complete data lineage from raw source to submitted report.

### Architecture
```
Source Systems (Operational DBs on AWS RDS + S3 files)
    → AWS Glue ETL (ingestion + validation)
    → S3 Data Lake (Raw + Transformed)
    → PySpark on EMR (report computation)
    → Great Expectations (data quality gates)
    → Redshift (report staging)
    → Custom Python report generator (PDF/XBRL/CSV output)
    → Secure FTP → Regulatory portal submission
```

### Key Features
- **Idempotency:** Every pipeline run tagged with reporting_period + version; re-running produces identical output
- **Audit trail:** Every row transformation logged with source record hash, transformation applied, and timestamp
- **Reconciliation:** Automated comparison between pipeline output and prior manual submission for first 3 months
- **Data lineage:** Apache Atlas integration — regulators can see exactly which source record produced each reported figure

### Results
- Eliminated 8 FTE analyst hours per reporting cycle
- Reporting cycle reduced from 5 days → 4 hours
- Zero errors in 18 months of production (vs. 3-4 material errors per year manually)
- Successfully passed 2 regulatory examinations with full lineage documentation

---

## Project 5: Self-Serve Analytics Platform (Internal Data Marketplace)

**Company:** Bank of America | **Duration:** 7 months | **Team:** 5 engineers (you as senior DE)

### Overview
Built an internal data marketplace allowing business teams to discover, request access to, and consume certified data products without involving the data engineering team. Reduced data access request fulfilment from 3 weeks → same day for pre-approved datasets.

### Architecture
```
Data Producers → Delta Lake (certified data products)
    → OpenMetadata (data catalog — auto-discovery via connectors)
    → Apache Ranger (access control — row/column level security)
    → Databricks SQL (query engine)
    → dbt (transformation + documentation)
    → Internal Web Portal (data marketplace UI)
    → Slack Bot (access request workflow)
```

### Key Features
- **Data contracts:** Each dataset has a published schema, SLA, owner, and quality score
- **Automated onboarding:** New datasets auto-catalogued via OpenMetadata Spark connector
- **Policy enforcement:** Column masking for PII; row-level security by business unit
- **Quality scoring:** Every dataset has a freshness score, completeness score, and dbt test pass rate

### Results
- 3,200 datasets catalogued and searchable
- 847 active users in first 6 months
- Average data access fulfilment: 4 hours (was 3 weeks)
- 60% reduction in ad-hoc data engineering requests

---

# 🐍 Python & PySpark (30 Questions)

> ⚠️ Interviewers WILL ask you to code. Practice typing these answers, not just reading them.

---

### Q1. What is the difference between a generator and a list comprehension? When do you use each in a data pipeline?

**Answer:**
A list comprehension evaluates eagerly — it builds the entire list in memory at once. A generator is lazy — it yields one item at a time without storing all values in memory simultaneously.

```python
# List comprehension — all 10M records loaded into memory at once
records = [transform(row) for row in source_data]  # ❌ for large datasets

# Generator — processes one record at a time
def record_generator(source_data):
    for row in source_data:
        yield transform(row)  # ✅ memory efficient

# In a pipeline — chain generators
def pipeline(filepath):
    raw = read_file(filepath)          # generator
    parsed = parse_rows(raw)           # generator  
    validated = validate(parsed)       # generator
    for record in validated:           # only materialises one at a time
        write_to_db(record)
```

In data engineering, use generators when processing files larger than available memory, streaming records from APIs, or building multi-stage transformation pipelines. Use lists when you need random access, multiple passes over data, or the dataset fits comfortably in memory.

**Follow-up 1:** *"Can you make a generator reusable?"*
No — a generator is exhausted after one pass. To reuse, wrap it in a function and call again, or convert to a list (if memory allows). This is a common pitfall when you try to iterate a generator twice.

**Follow-up 2:** *"What's a generator expression vs. a generator function?"*
A generator expression is `(x for x in range(10))` — syntactic sugar, no `yield`. A generator function uses `yield` and can have complex logic, multiple yield points, and state. For pipelines, generator functions are far more powerful.

**Follow-up 3:** *"In PySpark, does this concept apply?"*
Not directly — PySpark's lazy evaluation is Spark's own DAG mechanism, not Python generators. However, Python generators are still useful for feeding data into Spark via `spark.createDataFrame(generator, schema)` for streaming test data.

---

### Q2. Write a decorator that adds retry logic with exponential backoff to any function.

**Answer:**
```python
import time
import functools
import logging
from typing import Type, Tuple

def retry(
    max_attempts: int = 3,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    base_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = base_delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == max_attempts:
                        logging.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    sleep_time = min(delay, max_delay)
                    logging.warning(f"{func.__name__} attempt {attempt} failed: {e}. Retrying in {sleep_time}s")
                    time.sleep(sleep_time)
                    delay *= backoff_factor
        return wrapper
    return decorator

# Usage
@retry(max_attempts=5, exceptions=(ConnectionError, TimeoutError), base_delay=2.0)
def fetch_api_data(endpoint: str) -> dict:
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    return response.json()
```

**Follow-up 1:** *"What about jitter? Why is it important?"*
Without jitter, multiple pipeline workers retrying simultaneously cause a "thundering herd" — they all hit the failing service at the same time, making it worse. Add `time.sleep(sleep_time + random.uniform(0, 1))` to spread retries.

**Follow-up 2:** *"How would you make this work with async functions?"*
Replace `time.sleep` with `await asyncio.sleep`, and wrap with `async def wrapper(*args, **kwargs)`. The `@functools.wraps` still applies.

**Follow-up 3:** *"Would you use this in a Spark pipeline?"*
Not on the executor side — Spark has its own retry mechanism (`spark.task.maxFailures`). Use this for driver-side API calls, database connections, or calls to external services from the driver.

---

### Q3. Explain PySpark's lazy evaluation. How does it affect pipeline design?

**Answer:**
PySpark builds a DAG (Directed Acyclic Graph) of transformations but does NOT execute them until an action is called. Transformations like `filter()`, `select()`, `join()`, `groupBy()` are lazy. Actions like `show()`, `collect()`, `count()`, `write()` trigger execution.

```python
# Nothing executes here — just builds the DAG
df = spark.read.parquet("s3://bucket/transactions/")
df_filtered = df.filter(df.amount > 1000)
df_enriched = df_filtered.join(customers_df, "customer_id")
df_agg = df_enriched.groupBy("customer_id").agg(sum("amount").alias("total"))

# THIS triggers execution of the entire chain
df_agg.write.parquet("s3://bucket/output/")  # Action
```

**Why it matters for pipeline design:**
1. **Optimization:** Catalyst Optimizer can reorder, push down predicates, and eliminate unnecessary columns before execution
2. **Multiple actions = multiple DAG executions:** If you call `count()` then `write()`, Spark runs the whole pipeline twice — use `.cache()` to avoid this
3. **Debugging is harder:** Errors surface at action time, not where the transformation was written

**Follow-up 1:** *"Give me an example where lazy evaluation causes a subtle bug."*
```python
# Bug: df_with_new_col is computed TWICE — once for count, once for write
df_with_new_col = df.withColumn("new_col", expensive_udf(df.col1))
count = df_with_new_col.count()    # executes full DAG
df_with_new_col.write.parquet(...) # executes full DAG AGAIN

# Fix: cache before multiple actions
df_with_new_col.cache()
count = df_with_new_col.count()
df_with_new_col.write.parquet(...)
df_with_new_col.unpersist()
```

**Follow-up 2:** *"What's the difference between cache() and persist()?"*
`cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`. `persist()` lets you specify the storage level: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `OFF_HEAP`. For large datasets that don't fit in memory, `MEMORY_AND_DISK` is the safe default. `OFF_HEAP` avoids JVM GC pressure for very large cached datasets.

---

### Q4. What is the Catalyst Optimizer? Name 3 optimizations it performs automatically.

**Answer:**
Catalyst is Spark SQL's query optimization framework. It takes your logical plan (what you asked for) and produces an optimized physical plan (how Spark will actually execute it).

**3 key automatic optimizations:**

1. **Predicate Pushdown:** Moves `filter()` operations as close to the data source as possible, so Parquet/Delta files are skipped before data enters the pipeline.
```python
# You write:
df.join(other_df, "id").filter(df.date > "2024-01-01")
# Catalyst rewrites to:
df.filter(df.date > "2024-01-01").join(other_df, "id")  # filter first, join less data
```

2. **Column Pruning:** Reads only the columns actually used from Parquet files (columnar format). If you select 3 of 50 columns, Catalyst tells the Parquet reader to skip the other 47.

3. **Constant Folding:** Evaluates constant expressions at compile time rather than per-row.
```python
df.filter(df.amount > 100 * 10)  # Catalyst computes 100*10=1000 once, not per row
```

**Follow-up 1:** *"Can Catalyst always optimize correctly?"*
No. It can't push predicates through certain operations (e.g., through a Python UDF — Catalyst treats UDFs as black boxes). If you use a Python UDF as a filter condition, no predicate pushdown occurs. Use native Spark functions where possible.

**Follow-up 2:** *"How do you see what Catalyst is doing?"*
```python
df.explain(True)  # Shows: Parsed → Analyzed → Optimized → Physical plans
```

---

### Q5. How do you handle data skew in a PySpark join? Explain with code.

**Answer:**
Data skew occurs when a small number of keys contain a disproportionate share of rows, causing those tasks to run much longer than others.

**Technique 1: Salting (most universal)**
```python
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

SALT_FACTOR = 20

def salt_join(large_df: DataFrame, small_df: DataFrame, join_key: str) -> DataFrame:
    # Add random salt to large table
    large_salted = large_df.withColumn(
        "salted_key",
        F.concat(F.col(join_key).cast("string"), F.lit("_"), 
                 (F.rand() * SALT_FACTOR).cast("int").cast("string"))
    )
    
    # Explode small table to match all salt values
    small_exploded = small_df.withColumn(
        "salt_range",
        F.array([F.lit(i) for i in range(SALT_FACTOR)])
    ).withColumn(
        "salt_val", F.explode(F.col("salt_range"))
    ).withColumn(
        "salted_key",
        F.concat(F.col(join_key).cast("string"), F.lit("_"), 
                 F.col("salt_val").cast("string"))
    ).drop("salt_range", "salt_val")
    
    # Join on salted key
    result = large_salted.join(small_exploded, "salted_key").drop("salted_key")
    return result
```

**Technique 2: AQE (Adaptive Query Execution) — zero code change**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE automatically detects and splits skewed partitions at runtime
```

**Technique 3: Broadcast Join — for small tables**
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "customer_id")
# Sends entire small_df to every executor — no shuffle needed
```

**Follow-up 1:** *"What are the downsides of salting?"*
- Increases small table size by SALT_FACTOR (e.g., 20x) — only viable if small table is truly small
- Adds complexity to debugging
- If SALT_FACTOR is too low, skew remains; if too high, small table blows up memory

**Follow-up 2:** *"How do you detect skew before it causes issues?"*
```python
# Check partition sizes
df.rdd.glom().map(len).collect()  # Number of rows per partition
# Or: run a sample, check key distribution
df.groupBy("join_key").count().orderBy(F.col("count").desc()).show(20)
```

**Follow-up 3:** *"AQE's skewJoin — does it always work?"*
No. AQE can't fix skew in a streaming job, can't fix skew caused by a Python UDF (opaque to Catalyst), and requires `spark.sql.adaptive.enabled = true`. It only kicks in above a skew threshold configurable via `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`.

---

### Q6. Explain PySpark window functions with an example. What are common mistakes?

**Answer:**
Window functions perform calculations across a set of rows related to the current row, without collapsing rows (unlike groupBy).

```python
from pyspark.sql import Window
import pyspark.sql.functions as F

# Define the window
window_spec = Window.partitionBy("customer_id").orderBy("purchase_date")

df = df.withColumn("row_num", F.row_number().over(window_spec)) \
       .withColumn("running_total", F.sum("amount").over(window_spec)) \
       .withColumn("prev_purchase_amount", F.lag("amount", 1).over(window_spec)) \
       .withColumn("rank", F.rank().over(window_spec.orderBy(F.col("amount").desc())))

# Rolling 7-day window (range-based)
window_7d = Window.partitionBy("customer_id") \
                  .orderBy(F.col("purchase_date").cast("long")) \
                  .rangeBetween(-7 * 86400, 0)  # 7 days in seconds

df = df.withColumn("rolling_7d_spend", F.sum("amount").over(window_7d))
```

**Common mistakes:**

1. **Missing `orderBy` for rank functions:** `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()` ALL require `orderBy`. Without it, results are non-deterministic.

2. **Row-based vs. range-based frames:**
```python
# rowsBetween: relative row positions
Window.orderBy("date").rowsBetween(-6, 0)  # last 7 rows by count

# rangeBetween: relative value distance  
Window.orderBy(col("date").cast("long")).rangeBetween(-6*86400, 0)  # last 7 days by time
# These are NOT equivalent if data has irregular intervals
```

3. **Full shuffle:** Every window with a different `partitionBy` key causes a separate shuffle. Combine windows with the same partition key.

**Follow-up 1:** *"What's the difference between rank() and dense_rank()?"*
- `rank()`: gaps in ranking after ties. e.g., 1, 2, 2, 4
- `dense_rank()`: no gaps. e.g., 1, 2, 2, 3
- `row_number()`: always unique, breaks ties arbitrarily

**Follow-up 2:** *"What happens to window functions on unbounded partitions?"*
If `partitionBy` has very few distinct values (e.g., `country`), all rows in one country go to one partition → OOM. Always partition on a high-cardinality column.

---

### Q7. What is the difference between repartition() and coalesce()?

**Answer:**

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| **Direction** | Increase or decrease | Decrease only |
| **Shuffle** | Full shuffle | No shuffle (combines existing partitions) |
| **Data distribution** | Even (hash-based) | Uneven possible |
| **Use case** | Before a skewed join; increasing parallelism | Reducing partitions before write; avoiding small files |
| **Performance** | Expensive | Cheap |

```python
# Before a join where you need more parallelism
df = df.repartition(200, "customer_id")  # repartition by key = collocate related data

# Before writing to reduce small files
df.coalesce(10).write.parquet("s3://bucket/output/")

# Special: repartitionByRange for range-partitioned data
df = df.repartitionByRange(100, "purchase_date")
```

**Follow-up 1:** *"What's the risk of using coalesce to go from 500 to 5 partitions?"*
Coalesce combines partitions without shuffling — but some executors end up with 100x more data than others. This creates downstream skew. For significant reduction, `repartition()` gives better balance at the cost of a shuffle.

**Follow-up 2:** *"What's spark.sql.shuffle.partitions and what's the right value?"*
Default is 200 — designed for small clusters. For large jobs: `number_of_partitions = total_data_size_GB / target_partition_size_MB`. Target partition size is 128 MB–200 MB. For a 1 TB dataset: 1024 GB / 0.128 GB ≈ 8,000 partitions.

---

### Q8. What is a Pandas UDF and how does it differ from a standard UDF?

**Answer:**
```python
# Standard UDF — row-by-row, Python object serialization overhead
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def standard_udf(value: float) -> float:
    return value * 1.1  # 5-10x slower than native Spark

# Pandas UDF (vectorized) — batched execution using Arrow serialization
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(DoubleType())
def pandas_vectorized_udf(values: pd.Series) -> pd.Series:
    return values * 1.1  # Near-native speed; batched via Apache Arrow

# Pandas UDF for grouped operations
from pyspark.sql.functions import PandasUDFType

@pandas_udf("customer_id string, total double", PandasUDFType.GROUPED_MAP)
def compute_customer_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    return pdf.groupby("customer_id").agg({"amount": "sum"}).reset_index()

df.groupBy("customer_id").apply(compute_customer_stats)
```

**Performance comparison:**
- Standard UDF: rows serialized one-by-one via Pickle → Python → result back → 5-10x overhead
- Pandas UDF: entire batches serialized via Apache Arrow (columnar binary format) → 10-100x faster than standard UDF
- Native Spark function: fastest — no Python serialization at all

**Decision rule:** Native Spark → Pandas UDF → Standard UDF (last resort)

**Follow-up 1:** *"When would you still use a standard UDF?"*
When the logic can't be vectorized (e.g., stateful processing of one row at a time, calling a Python library that doesn't support vectorized input like a custom tokenizer).

**Follow-up 2:** *"What is Apache Arrow and why does it make Pandas UDFs fast?"*
Arrow is a columnar in-memory data format shared between JVM and Python processes. It avoids row-by-row serialization by transferring entire columns as contiguous memory buffers — dramatically reducing the JVM↔Python boundary cost.

---

### Q9. How does Spark Structured Streaming work? Explain watermarks.

**Answer:**
Structured Streaming treats a stream as an unbounded table. Each micro-batch appends rows to this table. You write the same DataFrame API against it.

```python
# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse and transform
df_parsed = df_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# Apply watermark — tolerate events up to 10 minutes late
df_watermarked = df_parsed.withWatermark("event_time", "10 minutes")

# Aggregate in 5-minute tumbling windows
df_agg = df_watermarked.groupBy(
    F.window("event_time", "5 minutes"),
    "store_id"
).agg(F.sum("amount").alias("window_revenue"))

# Write to Delta with checkpointing
df_agg.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/revenue/") \
    .start("s3://bucket/delta/revenue/")
```

**Watermarks explained:**
A watermark tells Spark "I'm willing to wait X time for late data." The watermark advances with the maximum observed event time minus the delay threshold. Events older than the watermark are dropped from state. Without watermark, state grows indefinitely → OOM.

**Follow-up 1:** *"What's the difference between output modes: append, update, complete?"*
- `append`: Only new rows added since last trigger (requires watermark for aggregations)
- `complete`: Rewrite entire result table each trigger — only for small aggregation results
- `update`: Only changed rows since last trigger

**Follow-up 2:** *"What happens to a record that arrives after the watermark?"*
It's silently dropped. This is by design — you trade completeness for bounded state. If you need zero data loss, increase watermark tolerance or use a separate late-data reconciliation batch job.

**Follow-up 3:** *"How does checkpointing provide fault tolerance?"*
Checkpoint stores: (1) the current streaming query state, (2) Kafka offsets processed, (3) partial aggregation state. On restart, Spark reads the checkpoint and resumes from exactly where it left off — providing at-least-once delivery. Combined with idempotent writes (Delta upsert), you achieve exactly-once.

---

### Q10. How do you handle out-of-memory errors in PySpark? Full diagnostic process.

**Answer:**
OOM in Spark can occur on executor OR driver. The diagnosis path is different for each.

**Executor OOM:**
1. Check Spark UI → Stages tab → failed tasks → executor logs → look for `java.lang.OutOfMemoryError: Java heap space` or `GC overhead limit exceeded`
2. Check if it's a skewed partition (one task running much longer/larger)
3. Check if broadcast join threshold is too high

**Driver OOM:**
1. Look for `collect()`, `toPandas()`, `broadcast()` of large objects
2. Check if DAG is too deeply nested

**Fixes:**
```python
# 1. Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")  # for non-JVM memory (Python workers)

# 2. Tune memory fractions
spark.conf.set("spark.memory.fraction", "0.8")          # JVM heap for Spark
spark.conf.set("spark.memory.storageFraction", "0.3")   # of above, for caching

# 3. Fix shuffle partitions — more, smaller partitions
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# 4. Replace collect() with iterative write
df.write.parquet("s3://output/")  # Never collect() large DataFrames

# 5. Enable off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")

# 6. Fix broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")  # lower it

# 7. Repartition before heavy operations
df = df.repartition(500)  # more even distribution
```

**Follow-up 1:** *"What's `spark.executor.memoryOverhead` for?"*
It covers non-JVM memory: Python worker processes (for UDFs), native libraries, OS overhead. Default is `max(executor_memory * 0.1, 384MB)`. If using many Python UDFs or Pandas UDFs, increase this significantly.

---

### Q11. Explain Delta Lake and why it's better than plain Parquet for production pipelines.

**Answer:**
Delta Lake adds an ACID transaction layer on top of Parquet files via a JSON transaction log (`_delta_log`).

**Key advantages over plain Parquet:**

```python
# 1. ACID Transactions — concurrent reads and writes are safe
# Two jobs can write to the same Delta table simultaneously
# If one fails midway, the table is not corrupted

# 2. Schema Enforcement — rejects writes that don't match schema
df_wrong_schema.write.format("delta").save("s3://table/")
# AnalysisException: A schema mismatch detected — write is rejected

# 3. Schema Evolution — opt-in schema changes
df_new_col.write.format("delta").option("mergeSchema", "true").save("s3://table/")

# 4. Time Travel — query any historical version
df_yesterday = spark.read.format("delta").option("versionAsOf", 5).load("s3://table/")
df_before = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("s3://table/")

# 5. MERGE (Upsert) — critical for CDC and deduplication
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://table/customers/")
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# 6. OPTIMIZE + ZORDER — compact small files and co-locate data
delta_table.optimize().executeZOrderBy("customer_id", "purchase_date")

# 7. VACUUM — remove old versions to save storage
delta_table.vacuum(168)  # Remove files older than 168 hours (7 days)
```

**Follow-up 1:** *"How does the Delta transaction log work?"*
Every write creates a new JSON file in `_delta_log/`. Each log entry records: what files were added, what files were removed, schema, statistics. Reading the log gives you the current table state. Time travel reads a subset of log entries.

**Follow-up 2:** *"Delta vs. Iceberg vs. Hudi — when do you choose each?"*
- **Delta Lake:** Best with Databricks ecosystem; best Spark integration; Unity Catalog governance
- **Apache Iceberg:** Best for multi-engine environments (Spark + Flink + Trino + Athena); better partition evolution
- **Apache Hudi:** Best for high-frequency upserts (record-level indexing); common on AWS EMR

---

### Q12-15: Quick-Fire Coding Questions

**Q12. Write a PySpark job to deduplicate records keeping the latest by timestamp.**
```python
from pyspark.sql import Window
import pyspark.sql.functions as F

window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

df_deduped = df.withColumn("rn", F.row_number().over(window)) \
               .filter(F.col("rn") == 1) \
               .drop("rn")
```

**Q13. How do you read only the last 7 days of a partitioned Delta table efficiently?**
```python
df = spark.read.format("delta").load("s3://bucket/transactions/") \
    .filter(F.col("partition_date") >= F.date_sub(F.current_date(), 7))
# Partition pruning ensures only 7 directories are read — no full table scan
```

**Q14. Write a function to detect schema drift between two DataFrames.**
```python
def detect_schema_drift(expected_df, actual_df):
    expected_cols = {f.name: f.dataType for f in expected_df.schema.fields}
    actual_cols = {f.name: f.dataType for f in actual_df.schema.fields}
    
    missing = set(expected_cols) - set(actual_cols)
    new_cols = set(actual_cols) - set(expected_cols)
    type_changes = {
        col: (expected_cols[col], actual_cols[col])
        for col in expected_cols & actual_cols
        if expected_cols[col] != actual_cols[col]
    }
    return {"missing_columns": missing, "new_columns": new_cols, "type_changes": type_changes}
```

**Q15. Explain broadcast join — when to use and when NOT to use.**
```python
# USE when one side < ~200 MB
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_lookup_df), "product_id")

# DO NOT USE when:
# - Small table > 200MB (executor OOM)
# - Running in streaming mode (broadcast not supported)
# - Small table is actually a filtered version of a large table (Spark can't always determine size)

# Force disable if AQE is incorrectly auto-broadcasting:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

---

### Q16. What is the difference between mapPartitions and map in PySpark?

**Answer:**
```python
# map: called once per ROW — function initialisation overhead per row
df.rdd.map(lambda row: expensive_init() + process(row))  # expensive_init() called N times

# mapPartitions: called once per PARTITION — initialise once, process all rows
def process_partition(rows):
    connection = create_db_connection()  # initialised ONCE per partition
    for row in rows:
        yield process_with_connection(connection, row)
    connection.close()

df.rdd.mapPartitions(process_partition)  # connection created P times (P = num partitions)
```

Use `mapPartitions` when:
- Initialising expensive resources (DB connections, ML model loading, HTTP session)
- The init cost amortises well across many rows
- You need batch-style processing within a partition

**Follow-up:** *"What's the risk of mapPartitions?"*
Memory: you hold all rows in a partition in the iterator. If partitions are large, be careful not to materialise them all at once — always use `yield` (generator pattern), never `return list(rows)`.

---

### Q17. How do you tune Spark for a job that reads 10 TB of Parquet data?

**Answer:**
```python
# Configuration checklist for large Parquet jobs:

# 1. Partition tuning — target 128-256 MB per task
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256 MB
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")       # 4 MB

# 2. AQE — let Spark adapt at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256mb")

# 3. Shuffle partitions — for 10 TB: ~10000 * 1024 / 256 ≈ 40,000 tasks
spark.conf.set("spark.sql.shuffle.partitions", "40000")

# 4. Enable vectorized Parquet reader
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")

# 5. Column pruning and predicate pushdown — already auto, but verify
df.explain()  # Check for "PushedFilters" in physical plan

# 6. Partitioned writes — match partition columns to common query patterns
df.write.partitionBy("year", "month", "day").parquet("s3://output/")

# 7. Dynamic partition overwrite — prevent deleting other partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

---

### Q18-20: PySpark Architecture Questions

**Q18. Explain Spark's execution hierarchy: Job → Stage → Task.**

A **Job** is triggered by an action (`write`, `count`, `collect`). Each job is divided into **Stages** — a stage boundary occurs at every wide transformation (shuffle). Within a stage, each **Task** processes one partition of data. Tasks run in parallel across executor cores.

Example: `df.groupBy("x").agg(sum("y")).write()` creates 2 stages:
- Stage 1: Read + partial aggregation (map side) — N tasks (one per input partition)
- Stage 2 (after shuffle): Final aggregation + write — M tasks (one per shuffle partition)

**Follow-up:** *"What causes a stage boundary?"*
Any wide transformation that requires a shuffle: `groupBy`, `join` (sort-merge), `distinct`, `repartition`, `orderBy`. Narrow transformations (`filter`, `select`, `map`) don't create stage boundaries.

---

**Q19. What is speculative execution in Spark and when does it help/hurt?**

Speculative execution (`spark.speculation = true`) detects "straggler" tasks running significantly slower than their peers and launches duplicate copies of them on other executors. The first one to finish wins; the other is killed.

**Helps:** When straggler is caused by a bad node (hardware issue, noisy neighbour)
**Hurts:** When straggler is caused by data skew — launching a duplicate doesn't help and wastes resources. Also bad for non-idempotent writes.

---

**Q20. How do you unit test PySpark code?**
```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_deduplication(spark):
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("ingested_at", StringType())
    ])
    input_data = [("e1", 100.0, "2024-01-02"), ("e1", 100.0, "2024-01-01")]
    input_df = spark.createDataFrame(input_data, schema)
    
    result = deduplicate(input_df)  # your function
    
    assert result.count() == 1
    assert result.collect()[0]["ingested_at"] == "2024-01-02"
```

---

### Q21-30: Rapid-Fire PySpark

**Q21. What is the difference between `explain()` modes?**
- `explain()`: Physical plan only
- `explain("extended")`: Parsed → Analyzed → Optimized → Physical
- `explain("codegen")`: Shows generated Java code
- `explain("cost")`: Shows row count/size estimates
- `explain("formatted")`: Human-readable formatted output ← most useful in practice

**Q22. When would you use foreachBatch in Structured Streaming?**
When you need to write to a sink that doesn't have native Structured Streaming support, or when you want to apply batch DataFrame operations (like MERGE) to each micro-batch:
```python
def write_to_delta_upsert(batch_df, batch_id):
    delta_table.alias("t").merge(batch_df.alias("s"), "t.id = s.id") \
               .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

stream.writeStream.foreachBatch(write_to_delta_upsert).start()
```

**Q23. What is dynamic partition pruning (DPP)?**
DPP allows Spark to skip partitions in a fact table based on a filter on a dimension table at runtime. Example: filtering transactions for a specific store — Spark first evaluates the store filter, gets matching store_ids, then prunes transaction partitions to only those store_ids. Requires `spark.sql.optimizer.dynamicPartitionPruning.enabled = true` (default: true in Spark 3+).

**Q24. How does Spark handle NULL values in joins?**
NULLs do NOT match NULLs in Spark join conditions (SQL standard behaviour). A row with `NULL` join key will NOT match another row with `NULL` join key. To match NULLs: use `df1.join(df2, df1.key.eqNullSafe(df2.key))` which treats NULL = NULL as true.

**Q25. What's the difference between `union` and `unionByName`?**
- `union`: merges by column position — dangerous if column order differs
- `unionByName`: merges by column name — safe when schemas may differ in order
- `unionByName(df1, df2, allowMissingColumns=True)`: fills missing columns with NULL — useful for schema evolution

**Q26. How do you write idempotent PySpark pipelines?**
Three patterns:
1. **Overwrite with partition:** `df.write.mode("overwrite").partitionBy("date").parquet(path)` with dynamic overwrite mode
2. **Delta MERGE:** Use upsert with a unique key — same data written twice = same result
3. **Write-then-swap:** Write to temp path, validate, then atomic move/rename to final path

**Q27. What is checkpointing vs. caching in Spark?**
- **Caching:** Stores RDD/DataFrame in memory/disk within the Spark application. Lost if application restarts. Used for reuse within a job.
- **Checkpointing:** Writes RDD to a reliable distributed store (HDFS/S3). Truncates DAG lineage. Survives application restart. Used for very long DAG chains or streaming fault tolerance.

**Q28. Explain Spark's sort-merge join.**
For large-large joins: (1) Both sides shuffle by join key into same partitions, (2) Each partition is sorted by join key, (3) Two-pointer merge of sorted arrays. Spills to disk if partition doesn't fit in memory. AQE can switch to broadcast join if one side turns out small after filtering.

**Q29. What is `spark.sql.autoBroadcastJoinThreshold` and what's a safe value?**
It's the maximum table size that Spark will automatically broadcast. Default: 10 MB. Safe production range: 50 MB–200 MB. Above 200 MB, you risk executor OOM. Set to `-1` to disable auto-broadcast entirely and control it manually.

**Q30. How do you read from Kafka in PySpark and handle schema evolution in the payload?**
```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "topic") \
    .option("startingOffsets", "latest") \
    .load()

# Value is binary — cast and parse with flexible schema
df_parsed = df.select(
    F.from_json(
        F.col("value").cast("string"),
        schema,
        options={"mode": "PERMISSIVE",       # don't fail on bad records
                 "columnNameOfCorruptRecord": "_corrupt_record"}
    ).alias("data")
).select("data.*")

# Route corrupt records to DLQ
df_good = df_parsed.filter(F.col("_corrupt_record").isNull())
df_bad = df_parsed.filter(F.col("_corrupt_record").isNotNull())
```

---

# 🗄️ Advanced SQL (30 Questions)

---

### Q1. What is the difference between INNER JOIN, LEFT JOIN, and LEFT ANTI JOIN? When do you use each?

**Answer:**

| Join Type | Returns | Use Case |
|-----------|---------|----------|
| INNER JOIN | Only rows with matches in BOTH tables | Get transactions with valid customer data |
| LEFT JOIN | All left rows + matched right (NULL where no match) | Get all customers, whether or not they transacted |
| LEFT ANTI JOIN | Left rows with NO match in right | Find customers who haven't transacted |
| RIGHT JOIN | Rarely used — rewrite as LEFT JOIN instead | — |
| FULL OUTER JOIN | All rows from both, NULL where no match | Reconciliation between two systems |
| CROSS JOIN | Cartesian product | Date spine × region matrix |

```sql
-- LEFT ANTI JOIN — customers who never purchased
SELECT c.customer_id, c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM transactions t WHERE t.customer_id = c.customer_id
);
-- OR equivalently:
SELECT c.*
FROM customers c
LEFT JOIN transactions t ON c.customer_id = t.customer_id
WHERE t.customer_id IS NULL;
```

**Follow-up 1:** *"NOT EXISTS vs LEFT JOIN WHERE NULL — which is faster?"*
In most modern query optimisers (Redshift, Spark SQL, PostgreSQL), they produce identical plans. NOT EXISTS reads more naturally. On older systems, NOT EXISTS short-circuits on first match; LEFT JOIN must complete the join first.

**Follow-up 2:** *"What's a SEMI JOIN?"*
Returns rows from the left table that have at least one match in the right table — but doesn't duplicate rows (unlike INNER JOIN). `EXISTS` produces a semi join:
```sql
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.customer_id = c.customer_id)
```

---

### Q2. Write a SQL query to find customers who purchased in 3 consecutive months.

**Answer:**
```sql
WITH monthly_purchases AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', purchase_date) AS purchase_month
    FROM transactions
    GROUP BY 1, 2  -- deduplicate: one row per customer per month
),
with_lag AS (
    SELECT
        customer_id,
        purchase_month,
        LAG(purchase_month, 1) OVER (PARTITION BY customer_id ORDER BY purchase_month) AS prev_1,
        LAG(purchase_month, 2) OVER (PARTITION BY customer_id ORDER BY purchase_month) AS prev_2
    FROM monthly_purchases
)
SELECT DISTINCT customer_id
FROM with_lag
WHERE 
    purchase_month = prev_1 + INTERVAL '1 month'
    AND prev_1 = prev_2 + INTERVAL '1 month';
```

**Follow-up 1:** *"What if you need 3+ consecutive months (not exactly 3)?"*
Use the gaps-and-islands technique:
```sql
WITH monthly AS (
    SELECT customer_id, DATE_TRUNC('month', purchase_date) AS m FROM transactions GROUP BY 1, 2
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY m) AS rn,
           m - (ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY m) * INTERVAL '1 month') AS grp
    FROM monthly
)
SELECT customer_id, MIN(m) AS streak_start, MAX(m) AS streak_end, COUNT(*) AS streak_length
FROM ranked
GROUP BY customer_id, grp
HAVING COUNT(*) >= 3;
```

---

### Q3. Explain window functions in SQL. What's the difference between RANK, DENSE_RANK, and ROW_NUMBER?

**Answer:**
```sql
SELECT 
    customer_id,
    amount,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS row_num,
    -- Always unique: 1,2,3,4 even for ties
    
    RANK()       OVER (PARTITION BY category ORDER BY amount DESC) AS rank_val,
    -- Gaps after ties: 1,2,2,4 (skips 3)
    
    DENSE_RANK() OVER (PARTITION BY category ORDER BY amount DESC) AS dense_rank_val,
    -- No gaps: 1,2,2,3

    NTILE(4)     OVER (PARTITION BY category ORDER BY amount DESC) AS quartile,
    -- Divide into N equal buckets
    
    LAG(amount, 1)  OVER (PARTITION BY customer_id ORDER BY purchase_date) AS prev_amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY purchase_date) AS next_amount,

    SUM(amount) OVER (PARTITION BY customer_id ORDER BY purchase_date 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,

    AVG(amount) OVER (PARTITION BY customer_id ORDER BY purchase_date 
                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
FROM transactions;
```

**Follow-up 1:** *"What's ROWS BETWEEN vs RANGE BETWEEN?"*
- `ROWS BETWEEN`: physically counts rows — `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` = last 7 rows
- `RANGE BETWEEN`: uses value distance — `RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW` = last 7 days by date value. If multiple rows have the same date, RANGE includes all of them in the window.

---

### Q4. Write a query to calculate month-over-month revenue growth percentage.

**Answer:**
```sql
WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', purchase_date) AS month,
        SUM(amount) AS revenue
    FROM transactions
    GROUP BY 1
),
with_growth AS (
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
        ROUND(
            100.0 * (revenue - LAG(revenue) OVER (ORDER BY month)) 
            / NULLIF(LAG(revenue) OVER (ORDER BY month), 0),
            2
        ) AS mom_growth_pct
    FROM monthly_revenue
)
SELECT * FROM with_growth ORDER BY month;
```

**Follow-up 1:** *"Why NULLIF in the denominator?"*
Division by zero. If previous month revenue is 0, `NULLIF(0, 0)` returns NULL, making the growth NULL rather than throwing an error.

**Follow-up 2:** *"How would you also calculate YoY growth in the same query?"*
```sql
LAG(revenue, 12) OVER (ORDER BY month) AS prev_year_revenue,
100.0 * (revenue - LAG(revenue, 12) OVER (ORDER BY month)) 
/ NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0) AS yoy_growth_pct
```

---

### Q5. What is a CTE (Common Table Expression) and how does it differ from a subquery?

**Answer:**
A CTE is a named temporary result set defined at the top of a query with `WITH`. A subquery is an inline query within another query.

```sql
-- Subquery approach — harder to read, can't reuse
SELECT * FROM (
    SELECT customer_id, COUNT(*) as purchase_count
    FROM (
        SELECT * FROM transactions WHERE amount > 100
    ) filtered
    GROUP BY 1
) counted
WHERE purchase_count > 5;

-- CTE approach — readable, reusable
WITH filtered_transactions AS (
    SELECT * FROM transactions WHERE amount > 100
),
purchase_counts AS (
    SELECT customer_id, COUNT(*) AS purchase_count
    FROM filtered_transactions
    GROUP BY 1
)
SELECT * FROM purchase_counts WHERE purchase_count > 5;
```

**Performance difference:** In most modern databases (Spark SQL, PostgreSQL 12+, Redshift), CTEs are optimised the same as subqueries — the optimiser inlines them. However, in PostgreSQL before v12, CTEs were "optimisation fences" — materialised as temp tables, blocking predicate pushdown. Always check your engine.

**Follow-up 1:** *"When would you use a temp table over a CTE?"*
When you need to: (1) reference the result multiple times in the same session, (2) add an index to the intermediate result, (3) the intermediate result is very large and you want it materialised once.

---

### Q6. Write a recursive CTE to traverse an org hierarchy.

**Answer:**
```sql
-- Find all direct and indirect reports of manager_id = 100
WITH RECURSIVE org_tree AS (
    -- Base case: direct reports
    SELECT 
        employee_id,
        manager_id,
        name,
        1 AS depth
    FROM employees
    WHERE manager_id = 100
    
    UNION ALL
    
    -- Recursive case: reports of reports
    SELECT 
        e.employee_id,
        e.manager_id,
        e.name,
        ot.depth + 1
    FROM employees e
    INNER JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT * FROM org_tree ORDER BY depth, name;
```

**Follow-up 1:** *"What prevents infinite recursion?"*
The recursion terminates when the recursive query returns no rows — i.e., when there are no more employees with a `manager_id` matching an `employee_id` in the current set. You can also add `WHERE depth < 10` as a safety guard.

**Follow-up 2:** *"Does Spark SQL support recursive CTEs?"*
No — as of Spark 3.x, recursive CTEs are NOT supported. This is a known limitation. Workarounds: use an iterative PySpark loop that stops when no new rows are added, or use a graph library like GraphFrames.

---

### Q7. Explain GROUPING SETS, ROLLUP, and CUBE.

**Answer:**
```sql
-- Regular GROUP BY: one combination
SELECT region, product, SUM(revenue) FROM sales GROUP BY region, product;

-- GROUPING SETS: specify exactly which combinations you want
SELECT region, product, SUM(revenue) 
FROM sales 
GROUP BY GROUPING SETS (
    (region, product),  -- subtotal by region+product
    (region),           -- subtotal by region only
    ()                  -- grand total
);

-- ROLLUP: hierarchical subtotals (subset of GROUPING SETS)
-- Generates: (region, product), (region), ()
SELECT region, product, SUM(revenue) FROM sales GROUP BY ROLLUP(region, product);

-- CUBE: all possible combinations
-- Generates: (region, product), (region), (product), ()
SELECT region, product, SUM(revenue) FROM sales GROUP BY CUBE(region, product);

-- Identify which row is a subtotal with GROUPING()
SELECT 
    CASE WHEN GROUPING(region) = 1 THEN 'ALL REGIONS' ELSE region END AS region,
    CASE WHEN GROUPING(product) = 1 THEN 'ALL PRODUCTS' ELSE product END AS product,
    SUM(revenue)
FROM sales 
GROUP BY ROLLUP(region, product);
```

**Follow-up:** *"When would you use this in a data pipeline context?"*
When building a pre-aggregated summary table for BI tools that need region totals, product totals, AND overall totals — instead of running 3 separate queries, one ROLLUP does it all in one pass.

---

### Q8. Write a query to solve the "gaps and islands" problem.

**Answer:**
Find consecutive date ranges where a user was active (no gap of more than 1 day):

```sql
WITH daily_activity AS (
    SELECT DISTINCT user_id, activity_date FROM user_events
),
with_island_id AS (
    SELECT 
        user_id,
        activity_date,
        -- If previous date is exactly 1 day ago, same island; otherwise new island
        activity_date - ROW_NUMBER() OVER (
            PARTITION BY user_id ORDER BY activity_date
        ) * INTERVAL '1 day' AS island_id
        -- Trick: for consecutive dates, this expression is constant
    FROM daily_activity
)
SELECT
    user_id,
    MIN(activity_date) AS streak_start,
    MAX(activity_date) AS streak_end,
    COUNT(*) AS streak_length_days
FROM with_island_id
GROUP BY user_id, island_id
ORDER BY user_id, streak_start;
```

**Follow-up:** *"Explain why `activity_date - ROW_NUMBER() * interval` identifies islands."*
For consecutive dates, `date` increases by 1 and `ROW_NUMBER` increases by 1 — so their difference is constant. When there's a gap, `date` jumps by more than 1 but `ROW_NUMBER` still increments by 1 — the difference changes, creating a new island identifier.

---

### Q9. How do you implement SCD Type 2 in SQL?

**Answer:**
SCD Type 2 keeps full history by adding a new row for each change, with effective/expiry dates.

```sql
-- Schema
CREATE TABLE dim_customer (
    surrogate_key    BIGINT IDENTITY,
    customer_id      VARCHAR(50),
    name             VARCHAR(100),
    risk_tier        VARCHAR(20),
    effective_date   DATE,
    expiry_date      DATE DEFAULT '9999-12-31',
    is_current       BOOLEAN DEFAULT TRUE
);

-- Query: get current state
SELECT * FROM dim_customer WHERE is_current = TRUE;

-- Query: get state as of a specific date
SELECT * FROM dim_customer 
WHERE customer_id = 'C001'
  AND effective_date <= '2023-06-01' 
  AND expiry_date > '2023-06-01';

-- SCD2 merge logic (on change detection):
-- 1. Expire old current record
UPDATE dim_customer 
SET expiry_date = CURRENT_DATE - 1, is_current = FALSE
WHERE customer_id = :new_customer_id 
  AND is_current = TRUE
  AND risk_tier != :new_risk_tier;  -- only if something changed

-- 2. Insert new current record
INSERT INTO dim_customer (customer_id, name, risk_tier, effective_date, is_current)
SELECT :customer_id, :name, :new_risk_tier, CURRENT_DATE, TRUE
WHERE EXISTS (
    SELECT 1 FROM dim_customer 
    WHERE customer_id = :customer_id AND is_current = FALSE AND expiry_date = CURRENT_DATE - 1
);
```

**Follow-up 1:** *"How do you handle SCD2 in PySpark / Delta Lake?"*
```python
# Use Delta MERGE with SCD2 logic
deltaTable.alias("target").merge(
    source_df.alias("source"), "target.customer_id = source.customer_id AND target.is_current = true"
).whenMatchedUpdate(
    condition="target.risk_tier != source.risk_tier",
    set={"is_current": "false", "expiry_date": "current_date()"}
).whenNotMatchedInsertAll().execute()
# Then insert new current records for updated rows
```

---

### Q10. Write a query to find the second highest salary per department without using LIMIT/TOP.

**Answer:**
```sql
-- Method 1: DENSE_RANK
SELECT department_id, employee_id, salary
FROM (
    SELECT 
        department_id, employee_id, salary,
        DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rnk
    FROM employees
) ranked
WHERE rnk = 2;

-- Method 2: Correlated subquery (less efficient but classic interview answer)
SELECT department_id, MAX(salary) AS second_highest_salary
FROM employees e1
WHERE salary < (
    SELECT MAX(salary) FROM employees e2 
    WHERE e2.department_id = e1.department_id
)
GROUP BY department_id;
```

**Follow-up:** *"What if there are fewer than 2 distinct salaries in a department?"*
Method 1 returns no row for that department (correct — there is no second highest). Method 2 also returns no row. Make sure to handle this in application logic.

---

### Q11-20: Advanced SQL Quick-Fire

**Q11. What's the difference between WHERE and HAVING?**
`WHERE` filters rows BEFORE aggregation. `HAVING` filters groups AFTER aggregation. You can't use aggregate functions in WHERE.
```sql
-- Wrong: WHERE COUNT(*) > 5  ← SyntaxError
SELECT customer_id, COUNT(*) AS cnt
FROM transactions
WHERE amount > 100          -- filter rows first
GROUP BY customer_id
HAVING COUNT(*) > 5;        -- then filter groups
```

**Q12. Explain LATERAL JOIN with an example.**
```sql
-- LATERAL allows the right side to reference columns from the left side
-- Useful for: top-N per group, calling table-valued functions per row

SELECT c.customer_id, recent.purchase_date, recent.amount
FROM customers c
CROSS JOIN LATERAL (
    SELECT purchase_date, amount
    FROM transactions t
    WHERE t.customer_id = c.customer_id
    ORDER BY purchase_date DESC
    LIMIT 3
) recent;
-- This is much more efficient than the ROW_NUMBER window approach for top-N when N is small
```

**Q13. What is a non-equi join? Give a real use case.**
```sql
-- Non-equi: join condition is not = but a range
-- Use case: assign a customer to a fee tier based on their balance

SELECT c.customer_id, c.balance, t.tier_name, t.fee_rate
FROM customers c
JOIN fee_tiers t
    ON c.balance >= t.min_balance 
   AND c.balance < t.max_balance;

-- Another use case: slow lookup (date in range)
SELECT t.transaction_id, t.amount, fx.exchange_rate
FROM transactions t
JOIN fx_rates fx
    ON t.currency = fx.currency
   AND t.transaction_date BETWEEN fx.valid_from AND fx.valid_to;
```

**Q14. How do you detect and remove duplicate rows in SQL?**
```sql
-- Detect
SELECT event_id, COUNT(*) FROM events GROUP BY event_id HAVING COUNT(*) > 1;

-- Remove with ROW_NUMBER (works in most DBs)
DELETE FROM events
WHERE rowid NOT IN (
    SELECT MIN(rowid) FROM events GROUP BY event_id
);

-- Or using CTE (more readable)
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
    FROM events
)
DELETE FROM events WHERE event_id IN (SELECT event_id FROM deduped WHERE rn > 1);
```

**Q15. Write a query to pivot rows to columns (conditional aggregation).**
```sql
-- Transactions by payment type per customer (rows → columns)
SELECT 
    customer_id,
    SUM(CASE WHEN payment_type = 'CARD'   THEN amount ELSE 0 END) AS card_spend,
    SUM(CASE WHEN payment_type = 'CASH'   THEN amount ELSE 0 END) AS cash_spend,
    SUM(CASE WHEN payment_type = 'ONLINE' THEN amount ELSE 0 END) AS online_spend
FROM transactions
GROUP BY customer_id;
```

**Q16. What is query optimisation? How do you diagnose a slow SQL query?**
Steps:
1. `EXPLAIN` / `EXPLAIN ANALYZE` — look at row estimates, actual rows, cost, seq scan vs. index scan
2. Check for missing indexes on join keys and filter columns
3. Check for implicit type casts that prevent index use
4. Look for `SELECT *` pulling unnecessary columns
5. Check for functions on indexed columns in WHERE: `WHERE YEAR(date) = 2024` breaks index; use `WHERE date >= '2024-01-01'`
6. Check for N+1 query patterns (correlated subqueries evaluated per row)
7. Check statistics freshness — stale stats cause bad plan choices

**Q17. What is the difference between UNION and UNION ALL?**
`UNION` removes duplicates (sorts + deduplicates = expensive). `UNION ALL` keeps all rows (no dedup = fast). Always use `UNION ALL` unless you specifically need deduplication — most pipelines use `UNION ALL` because data is already deduplicated upstream.

**Q18. How do you calculate a retention cohort in SQL?**
```sql
WITH first_purchase AS (
    SELECT customer_id, MIN(purchase_date) AS cohort_date FROM transactions GROUP BY 1
),
purchases_with_cohort AS (
    SELECT t.customer_id, f.cohort_date,
           DATEDIFF('month', f.cohort_date, t.purchase_date) AS months_since_first
    FROM transactions t
    JOIN first_purchase f ON t.customer_id = f.customer_id
),
cohort_sizes AS (
    SELECT cohort_date, COUNT(DISTINCT customer_id) AS cohort_size FROM first_purchase GROUP BY 1
)
SELECT 
    p.cohort_date,
    p.months_since_first,
    COUNT(DISTINCT p.customer_id) AS retained,
    cs.cohort_size,
    ROUND(100.0 * COUNT(DISTINCT p.customer_id) / cs.cohort_size, 1) AS retention_pct
FROM purchases_with_cohort p
JOIN cohort_sizes cs ON p.cohort_date = cs.cohort_date
GROUP BY 1, 2, cs.cohort_size
ORDER BY 1, 2;
```

**Q19. What is the N+1 query problem?**
When you execute 1 query to get N rows, then execute N more queries (one per row) to fetch related data. In SQL pipelines: avoid correlated subqueries that reference the outer row — rewrite as a JOIN or window function. In ORM contexts: use eager loading.

**Q20. How does indexing work and what are the trade-offs?**
A B-tree index stores a sorted copy of the indexed column with pointers to the actual rows, enabling O(log n) lookups. Trade-offs:
- **Reads faster:** point lookups and range scans
- **Writes slower:** index must be updated on every INSERT/UPDATE/DELETE
- **Storage:** indexes consume space (can be 20-50% of table size)
- **Cardinality matters:** Low-cardinality columns (boolean, status) rarely benefit from B-tree index; consider bitmap indexes (Redshift, Oracle) instead.

---

### Q21-30: SQL Scenario Questions

**Q21. Find all products bought together in the same order (market basket analysis).**
```sql
SELECT 
    a.product_id AS product_1,
    b.product_id AS product_2,
    COUNT(DISTINCT a.order_id) AS co_purchase_count
FROM order_items a
JOIN order_items b 
    ON a.order_id = b.order_id 
    AND a.product_id < b.product_id  -- prevent duplicates (A,B) and (B,A)
GROUP BY 1, 2
ORDER BY 3 DESC;
```

**Q22. Find customers who churned (bought in Q1 but not Q2).**
```sql
SELECT DISTINCT q1.customer_id
FROM (SELECT DISTINCT customer_id FROM transactions WHERE purchase_date BETWEEN '2024-01-01' AND '2024-03-31') q1
LEFT JOIN (SELECT DISTINCT customer_id FROM transactions WHERE purchase_date BETWEEN '2024-04-01' AND '2024-06-30') q2
    ON q1.customer_id = q2.customer_id
WHERE q2.customer_id IS NULL;
```

**Q23. Calculate running total and flag when it exceeds a threshold.**
```sql
SELECT *, 
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS running_total,
       CASE WHEN SUM(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date) > 10000 
            THEN 'EXCEEDED' ELSE 'OK' END AS status
FROM transactions;
```

**Q24. What is a covering index?**
An index that contains all columns needed to satisfy a query — no heap access needed. If a query does `SELECT name, email FROM users WHERE user_id = 123` and there's an index on `(user_id, name, email)`, the database reads only the index, not the table (index-only scan).

**Q25. How do you handle time zones in SQL date arithmetic?**
Always store timestamps in UTC. Convert to local time only at display layer: `CONVERT_TIMEZONE('UTC', 'America/New_York', ts)` (Redshift). For date partitioning, be careful — a UTC midnight timestamp may belong to the previous day in a local time zone.

**Q26. What's the difference between TRUNCATE and DELETE?**
- `DELETE`: row-by-row removal, logged, can be rolled back, fires triggers, allows WHERE clause
- `TRUNCATE`: drops and recreates the table structure, minimally logged, cannot be rolled back in most DBs, much faster, no WHERE clause

**Q27. Explain window function FIRST_VALUE and LAST_VALUE. Common pitfall?**
```sql
-- LAST_VALUE pitfall: default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- So LAST_VALUE gives the current row's value, not the partition's last
SELECT *, 
    FIRST_VALUE(amount) OVER w AS first_amount,
    LAST_VALUE(amount)  OVER (
        PARTITION BY customer_id ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING  -- FIX
    ) AS last_amount
FROM transactions
WINDOW w AS (PARTITION BY customer_id ORDER BY purchase_date);
```

**Q28. Write a query to find the median salary per department in Spark SQL.**
```sql
SELECT department_id, 
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees
GROUP BY department_id;
-- In Spark SQL:
SELECT department_id, PERCENTILE(salary, 0.5) AS median_salary
FROM employees GROUP BY department_id;
```

**Q29. How do you optimise a query with multiple CTEs that are each referenced multiple times?**
In most databases, CTEs are re-evaluated each time they're referenced (unless materialised). Options:
1. Create a temp table from the expensive CTE, add an index, then reference the temp table
2. In PostgreSQL 12+, add `MATERIALIZED` keyword: `WITH cte AS MATERIALIZED (...)` 
3. In Spark SQL, cache the intermediate DataFrame before the expensive operation

**Q30. How do you detect data freshness issues in a SQL pipeline?**
```sql
-- Check max ingested timestamp vs. expected freshness
SELECT 
    table_name,
    MAX(ingested_at) AS last_ingested,
    CURRENT_TIMESTAMP - MAX(ingested_at) AS data_lag,
    CASE WHEN CURRENT_TIMESTAMP - MAX(ingested_at) > INTERVAL '2 hours' 
         THEN 'STALE' ELSE 'FRESH' END AS freshness_status
FROM (
    SELECT 'transactions' AS table_name, ingested_at FROM transactions
    UNION ALL
    SELECT 'customers', ingested_at FROM customers
) all_tables
GROUP BY table_name;
```

---

# ☁️ Cloud — AWS & Azure (30 Questions)

---

### Q1. Compare AWS and Azure for a real-time data pipeline. Which would you choose for dunnhumby?

**Answer:**
For dunnhumby's retail analytics use case, the choice depends on existing enterprise agreements and the existing team's expertise. Both are viable.

| Capability | AWS | Azure |
|-----------|-----|-------|
| Streaming ingestion | Kinesis Data Streams / MSK (Kafka) | Event Hubs (Kafka-compatible) |
| Stream processing | Kinesis Analytics / EMR (Flink) | Azure Stream Analytics / Databricks |
| Batch processing | EMR (Spark) | Azure Databricks |
| Orchestration | MWAA (managed Airflow) | Azure Data Factory / Managed Airflow |
| Object storage | S3 | ADLS Gen2 |
| Data warehouse | Redshift | Synapse Analytics |
| ML platform | SageMaker | Azure ML |
| Data governance | AWS Glue Data Catalog + Lake Formation | Microsoft Purview + Unity Catalog |

**For dunnhumby specifically:** If they're already Databricks-heavy, Azure is a natural fit (Unity Catalog, Azure Databricks, ADLS Gen2 is a mature stack). If they're multi-cloud or AWS-first, EMR + S3 + Redshift is battle-tested.

**Follow-up 1:** *"What's the biggest operational difference between Kinesis and Kafka (MSK/Event Hubs)?"*
Kinesis is fully serverless/managed (no broker management) but has a 7-day max retention and a proprietary API — lock-in risk. MSK/Event Hubs use the open Kafka API with portable code. For a new greenfield project, MSK or Event Hubs + Kafka producer/consumer code is the better long-term choice.

---

### Q2. What is Azure Data Lake Storage Gen2 (ADLS Gen2)? How is it different from Blob Storage?

**Answer:**
ADLS Gen2 is built on top of Azure Blob Storage but adds a hierarchical namespace — enabling true directory semantics with atomic renames, POSIX-compatible access control (ACLs at file/directory level), and dramatically better performance for analytics workloads.

**Key differences:**

| Feature | Blob Storage | ADLS Gen2 |
|---------|-------------|-----------|
| Namespace | Flat (container/blob) | Hierarchical (folder/file) |
| Rename operation | Copy + delete (slow, non-atomic) | Atomic O(1) rename |
| Access control | Container-level RBAC | File/directory-level ACLs (POSIX) |
| Analytics performance | Okay | Optimised (faster metadata operations) |
| Spark/Hadoop compatibility | Via `wasbs://` | Via `abfss://` (full Hadoop FS API) |

**Why it matters for data pipelines:** Atomic rename makes the "write to temp, rename to final" pattern safe and fast. Directory-level ACLs enable column/row-level governance without the complexity of a separate access control system.

**Follow-up 1:** *"How do you secure ADLS Gen2 in a multi-team data platform?"*
- Managed Identity for service authentication (no credentials in code)
- RBAC at storage account level for broad access
- ACLs at directory level for fine-grained team access
- Private endpoints to prevent public internet access
- Key Vault for any remaining secrets
- Diagnostic logs → Azure Monitor → alert on suspicious access patterns

---

### Q3. Explain the difference between AWS Glue, AWS EMR, and AWS Lambda for data processing.

**Answer:**

| Service | Best For | Limitations |
|---------|----------|-------------|
| **AWS Glue** | Serverless ETL, schema discovery (Glue Crawlers), Data Catalog, simple transformations | Limited tuning; slower cold start; not for complex custom Spark code |
| **AWS EMR** | Complex Spark/Flink jobs, full control over cluster config, long-running workloads | Cluster management overhead; cost if not using spot/auto-termination |
| **AWS Lambda** | Event-driven micro-transformations, file arrival triggers, lightweight API calls | 15-minute timeout; 10 GB memory limit; not for large data processing |
| **AWS Glue Streaming** | Continuous ETL from Kafka/Kinesis | Limited vs. native Spark Structured Streaming |

**Decision framework:**
- Simple ETL with schema crawling → Glue
- Complex PySpark with custom libs, ML, heavy joins → EMR
- Event-triggered lightweight processing → Lambda
- Real-time streaming → Kinesis + EMR (Flink/Spark Streaming) or MSK + EMR

**Follow-up 1:** *"What is AWS Glue Data Catalog and why is it important?"*
It's a central metadata repository for all data assets in your AWS account — stores table schemas, partitions, and locations. Integrated with Athena (query directly), EMR (Hive metastore), Redshift Spectrum (external tables). It's the AWS equivalent of the Hive Metastore, but fully managed.

---

### Q4. How does Amazon Redshift differ from a traditional OLTP database? How do you optimise it?

**Answer:**
Redshift is a columnar MPP (Massively Parallel Processing) data warehouse, NOT an OLTP database.

**Key design differences:**

| | OLTP (PostgreSQL/MySQL) | Redshift |
|---|---|---|
| Storage | Row-oriented | Columnar |
| Optimised for | Point reads/writes, high concurrency | Full-table scans, aggregations, low concurrency |
| Indexes | B-tree, hash | Sort keys, zone maps (no traditional indexes) |
| Transactions | ACID, row-level locking | Serialisable isolation, not for high-write workloads |
| Scaling | Vertical | Horizontal (node expansion) |

**Optimisation techniques:**
```sql
-- 1. Sort key: define column(s) by which data is physically sorted on disk
-- Best for: frequently filtered columns (date, region)
CREATE TABLE transactions (
    transaction_date DATE ENCODE az64,
    amount DECIMAL(10,2) ENCODE az64,
    customer_id VARCHAR(50) ENCODE lzo
) SORTKEY(transaction_date);

-- 2. Distribution key: controls how rows are distributed across nodes
-- DISTKEY on the join key prevents cross-node shuffles
CREATE TABLE transactions (...) DISTKEY(customer_id);
-- If table is small, use DISTSTYLE ALL (copy to every node)

-- 3. Column encoding: compression reduces I/O
-- AZ64: numeric/date (best compression)
-- LZO: strings (good compression, fast decompress)
-- ZSTD: general purpose, best ratio
-- RAW: no compression — only for sort keys

-- 4. Vacuum and Analyze after bulk loads
VACUUM DELETE ONLY transactions;   -- reclaim space from DELETEs
ANALYZE transactions;              -- update statistics for query planner

-- 5. Workload Management (WLM): separate queues for ETL vs. analyst queries
```

**Follow-up 1:** *"What is a Redshift Spectrum and when would you use it?"*
Spectrum allows Redshift to query data directly from S3 (in Parquet, ORC, CSV) without loading it into Redshift. Use it for: querying archival/historical data that's too large to load, federating queries across S3 + Redshift tables in a single SQL statement.

---

### Q5. What is AWS Lake Formation and how does it relate to IAM?

**Answer:**
Lake Formation provides fine-grained data access control (table/column/row level) on top of S3 + Glue Catalog — beyond what IAM can do natively.

**Without Lake Formation:** You control access to S3 buckets/prefixes via IAM policies (coarse-grained — you get the whole prefix or nothing).

**With Lake Formation:**
- **Column-level security:** Grant access to specific columns only (mask PII)
- **Row-level filtering:** Grant access to rows matching a condition (e.g., only rows where `region = 'US'`)
- **Tag-based access control:** Assign sensitivity tags (PII, Confidential) to columns; access policies reference tags

```python
# Grant column-level access via Lake Formation
lakeformation.grant_permissions(
    Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::account:role/analyst-role"},
    Resource={
        "TableWithColumns": {
            "DatabaseName": "retail_db",
            "Name": "customers",
            "ColumnNames": ["customer_id", "purchase_date", "amount"]
            # name, email, ssn NOT included — masked
        }
    },
    Permissions=["SELECT"]
)
```

**Follow-up 1:** *"How do you handle PII in an AWS data lake?"*
1. Classify PII columns using Glue/Macie
2. Mask at source: store hashed/tokenised version in Silver layer
3. Store original PII in separate high-security S3 bucket with KMS encryption + restricted Lake Formation access
4. Analyst-facing views use masked data only
5. Audit all access via CloudTrail + Lake Formation audit logs

---

### Q6. What is Azure Databricks Unity Catalog?

**Answer:**
Unity Catalog is Databricks' unified governance layer — a single place to manage access, lineage, and discovery for all data assets (tables, files, ML models, feature store) across all Databricks workspaces in an organisation.

**Without Unity Catalog:** Each Databricks workspace has its own Hive metastore, access controls, and no cross-workspace visibility.

**With Unity Catalog:**
```
Account-level governance:
├── Catalog (top level — e.g., "prod", "dev")
│   ├── Schema (database)
│   │   ├── Tables (Delta, external, views)
│   │   ├── Volumes (unstructured files)
│   │   └── Models (ML)
│   └── ...
└── ...

Access Control: GRANT SELECT ON TABLE prod.retail.transactions TO `analyst-group`
Column masking: GRANT SELECT WITH COLUMN MASK pii_mask ON TABLE prod.retail.customers
Row filter: GRANT SELECT WITH ROW FILTER region_filter ON TABLE prod.retail.orders

Lineage: automatic column-level lineage across all Unity-Catalog-registered tables
```

**Follow-up 1:** *"What's the difference between Hive Metastore and Unity Catalog?"*
Hive Metastore is workspace-scoped, lacks column-level security, has no row filters, and provides no lineage. Unity Catalog is account-scoped (multi-workspace), adds column masking, row filtering, tag-based governance, and automatic data lineage.

---

### Q7. Explain S3 storage classes and how you'd implement a data lifecycle policy.

**Answer:**
```
S3 Storage Classes (cost vs. retrieval latency):
├── S3 Standard: frequent access, millisecond retrieval, highest cost
├── S3 Standard-IA (Infrequent Access): 30-day minimum, 60% cheaper, same speed
├── S3 Glacier Instant: archival, millisecond retrieval, 68% cheaper than Standard
├── S3 Glacier Flexible: archival, 1-12 hour retrieval, very cheap
└── S3 Glacier Deep Archive: coldest, 12-48 hour retrieval, cheapest
```

**Lifecycle policy for a data pipeline:**
```json
{
  "Rules": [{
    "Status": "Enabled",
    "Transitions": [
      {"Days": 30,  "StorageClass": "STANDARD_IA"},
      {"Days": 90,  "StorageClass": "GLACIER_IR"},
      {"Days": 365, "StorageClass": "DEEP_ARCHIVE"}
    ],
    "Expiration": {"Days": 2555},
    "Filter": {"Prefix": "raw/transactions/"}
  }]
}
```

**For a banking pipeline:** Raw data kept 7 years (regulatory requirement) → transition to Glacier after 90 days → Deep Archive after 1 year → expire at 7 years.

**Follow-up 1:** *"What's S3 Intelligent-Tiering?"*
Automatically moves objects between frequent and infrequent access tiers based on access patterns, with no retrieval fee for the automated tiering. Best for data with unpredictable access patterns. Small monthly monitoring fee per object.

---

### Q8. What is AWS MSK (Managed Streaming for Apache Kafka) and how do you size it?

**Answer:**
MSK is a fully managed Apache Kafka service — AWS handles broker provisioning, patching, and ZooKeeper/KRaft management. You bring standard Kafka producer/consumer code.

**Sizing inputs:**
- **Message rate:** e.g., 500,000 messages/second
- **Message size:** e.g., 2 KB average
- **Throughput:** 500K × 2 KB = ~1 GB/s
- **Replication factor:** 3 (standard)
- **Total write throughput:** 3 GB/s

**Broker sizing:**
- Each MSK broker handles ~100-200 MB/s throughput
- For 3 GB/s write → minimum 15-20 brokers
- Start with `kafka.m5.4xlarge` (16 vCPU, 64 GB) for high-throughput

**Partition count:**
- Target throughput per partition: ~10 MB/s
- For 1 GB/s → 100 partitions minimum
- Rule of thumb: `max(Tp/Tc, Tp/Pp)` where Tp = target throughput, Tc = consumer throughput, Pp = producer throughput per partition

**Follow-up:** *"How do you ensure Kafka message ordering?"*
Messages within a single partition are ordered. To guarantee order for related messages (e.g., all events for one customer in order), use `customer_id` as the partition key. All events for the same customer_id go to the same partition → ordered delivery.

---

### Q9. What is Azure Data Factory? Compare it with Airflow.

**Answer:**

| Aspect | Azure Data Factory (ADF) | Apache Airflow (MWAA/Managed) |
|--------|--------------------------|-------------------------------|
| Interface | GUI-first, drag-and-drop pipelines | Code-first (Python DAGs) |
| Code management | JSON/ARM templates in Git | Python files in Git |
| Extensibility | Limited to built-in activities + Databricks/HDInsight | Unlimited — any Python code |
| Monitoring | Native Azure Portal integration | Airflow UI + custom metrics |
| Trigger types | Schedule, event (blob arrival, HTTP) | Schedule, sensor, external trigger |
| Debugging | GUI-based, limited logging | Full Python debugging |
| Cost model | Pay per activity run | Pay per infrastructure (MWAA: per environment) |
| Best for | Simple ETL, non-engineer users, Azure-native pipelines | Complex workflows, engineer-owned, cross-cloud |

**My preference:** For dunnhumby's complex, code-heavy pipelines with multiple engineers contributing, Airflow is superior. ADF is useful as a thin orchestration layer for triggering Databricks notebooks.

**Follow-up 1:** *"How do you deploy Airflow DAGs to production safely?"*
1. DAGs in Git with PR review
2. CI pipeline runs DAG import tests, task graph validation
3. Deploy to staging environment first
4. Use `catchup=False` for new DAGs to prevent backfill explosion
5. Blue-green deployment: new DAG version alongside old until validated

---

### Q10-30: AWS & Azure Quick-Fire

**Q10. What is the difference between EMR and EMR Serverless?**
EMR (managed clusters) requires provisioning, sizing, and managing cluster lifecycle. EMR Serverless automatically provisions compute based on workload with no cluster management. EMR Serverless is better for ad-hoc and variable workloads; EMR clusters are better for steady-state workloads where you want precise tuning and spot instance savings.

**Q11. How do you handle secrets in a production data pipeline on AWS/Azure?**
- **AWS:** AWS Secrets Manager (auto-rotation, fine-grained IAM) or Parameter Store (simple key-value, cheaper)
- **Azure:** Azure Key Vault with Managed Identity (no credentials in code ever)
- **Never:** environment variables with plaintext passwords, hardcoded credentials in code, credentials in Git

**Q12. What is AWS Glue Crawler and what are its limitations?**
Glue Crawlers automatically scan S3 paths, infer schema, and update the Glue Data Catalog. Limitations: (1) schema inference can be wrong for messy data, (2) it doesn't detect column type changes reliably, (3) it creates a new partition entry for every new S3 prefix it sees — can cause partition explosion if folder structure is not carefully designed, (4) crawler scheduling adds latency to schema discovery.

**Q13. Explain Kinesis vs. SQS vs. SNS.**
- **Kinesis Data Streams:** ordered stream with replay capability (7-day retention), multiple consumers, high throughput — for real-time analytics
- **SQS:** distributed queue, at-least-once delivery, no ordering (FIFO queue for ordered), consumers delete messages — for decoupling microservices
- **SNS:** pub/sub fan-out, pushes to multiple subscribers (SQS, Lambda, HTTP) simultaneously — for broadcasting events

**Q14. What is Azure Event Hubs and how is it Kafka-compatible?**
Event Hubs exposes a Kafka-compatible endpoint (port 9093) that accepts standard Kafka Producer/Consumer API calls. You can point existing Kafka client code at Event Hubs by changing only the bootstrap server URL and adding SASL/OAUTHBEARER authentication. No code rewrite needed. Internally, Event Hubs maps Kafka concepts: Topic ↔ Event Hub, Consumer Group ↔ Event Hub Consumer Group, Offset ↔ Event Hub Sequence Number.

**Q15. How do you implement CI/CD for a Databricks notebook pipeline?**
1. Store notebooks in Git (Databricks Repos)
2. On PR: run unit tests with `pytest` on PySpark logic (local Spark), lint with `flake8`/`black`
3. On merge to main: CI pipeline deploys to dev workspace, runs integration tests
4. Promote to prod: Databricks Asset Bundles (DAB) or Terraform for infrastructure as code
5. Databricks Jobs API to update job definition
6. Canary deployment: run new version on 5% of data first

**Q16. What is Redshift Serverless vs. Redshift provisioned?**
Provisioned: fixed node count, constant cost, best for predictable high-volume workloads. Serverless: no cluster management, scales automatically, pay per RPU-second, best for sporadic/variable query loads. Provisioned is cheaper at constant high utilisation; Serverless is cheaper for bursty workloads.

**Q17. How do you partition data in S3 for optimal Athena/Redshift Spectrum query performance?**
Partition by columns that are most commonly filtered: `s3://bucket/transactions/year=2024/month=01/day=15/`. Use Hive-style partitioning (Athena auto-discovers). Always partition by date first (most universal filter). Additional columns: region, business_unit if they're common WHERE clause predicates. Avoid high-cardinality partition keys (e.g., customer_id as partition creates millions of directories = partition explosion).

**Q18. What is AWS Lakeformation row-level security and how does it work?**
Row-level security in Lake Formation uses data filters — you create a filter expression (e.g., `region = 'US'`) and attach it to a GRANT. When the grantee queries the table through Athena or Glue, the filter is automatically applied server-side — they never see rows outside their allowed region. This happens transparently; the user writes `SELECT * FROM orders` but only gets rows matching the filter.

**Q19. Explain Azure Synapse Analytics vs Databricks.**
Synapse is an integrated workspace combining SQL data warehouse (dedicated/serverless SQL pools) + Spark pools + Pipelines (ADF) + Power BI. Databricks is purely a Spark + Delta Lake platform with superior ML capabilities. Synapse is better for teams wanting one Azure portal for all analytics; Databricks is better for teams doing heavy Spark development, ML/MLOps, or wanting the latest Delta Lake/Unity Catalog features.

**Q20. How do you handle cross-account or cross-subscription data access in AWS/Azure?**
- **AWS:** S3 bucket policies with cross-account principals + IAM role assumption (`sts:AssumeRole`). Lake Formation resource links for cross-account Glue Catalog sharing.
- **Azure:** Storage account RBAC with guest users from other tenants, or Azure AD B2B for cross-tenant access. Cross-subscription: managed identities work across subscriptions in the same tenant; cross-tenant requires explicit federation.

**Q21. What is spot/preemptible instance strategy for EMR/Databricks?**
Use spot for task nodes (stateless workers) — they can be terminated and Spark will retry on remaining nodes. Always use on-demand for master/driver node — losing the driver kills the whole job. In Databricks: use spot for workers in the cluster, on-demand for driver. Typical savings: 60-80% cost reduction for batch workloads.

**Q22. How does S3 consistency work? Has it changed?**
Before December 2020: S3 had eventual consistency for overwrite PUTs and DELETEs — you might read stale data after an overwrite. After December 2020: S3 provides **strong read-after-write consistency** for all operations. This means Spark jobs that overwrite S3 partitions no longer need workarounds like waiting or refreshing. Delta Lake's transaction log also handles this correctly.

**Q23. What is AWS Step Functions vs Airflow for orchestration?**
Step Functions: serverless, event-driven, JSON-defined state machines, native AWS integration, pay per state transition. Good for AWS-native microservice orchestration. Airflow: Python-defined DAGs, richer scheduling, better for complex data pipeline dependencies, larger community, better monitoring UI. For data pipelines with many interdependent tasks, Airflow is almost always the better choice.

**Q24. How would you set up monitoring and alerting for a production AWS data pipeline?**
1. **CloudWatch Metrics:** EMR cluster health, Glue job duration/failure, Lambda errors
2. **CloudWatch Alarms:** alert when job fails, when duration exceeds SLA threshold
3. **SNS → PagerDuty/Slack:** route alarms to the right team
4. **Custom metrics:** push business-level metrics (row counts, null rates) to CloudWatch from Spark
5. **X-Ray / OpenTelemetry:** distributed tracing for multi-step pipelines
6. **S3 access logs + Athena:** ad-hoc analysis of pipeline behaviour

**Q25. What is Azure Monitor vs AWS CloudWatch?**
Both are managed observability platforms — collect logs, metrics, and traces. Azure Monitor integrates with Application Insights (APM), Log Analytics (log querying in KQL), and Azure Dashboards. CloudWatch uses CloudWatch Insights (log querying in CWQL). Key difference: Azure Monitor's KQL (Kusto Query Language) is significantly more powerful for ad-hoc log analysis than CloudWatch Insights' simplified syntax.

**Q26. Explain the concept of a data mesh and how cloud platforms support it.**
Data mesh decentralises data ownership — each domain team owns, produces, and serves their own data products. Cloud platforms support it via: (1) separate accounts/subscriptions per domain with federated governance (Lake Formation / Unity Catalog), (2) self-service infrastructure (Terraform modules teams can deploy), (3) data catalogues (Glue/Purview) that aggregate metadata across domains, (4) cross-account access policies that enable discovery without centralised control.

**Q27. What is infrastructure as code (IaC) for data pipelines? How do you use it?**
All cloud resources defined in code (Terraform, AWS CDK, Azure Bicep, Pulumi) and version-controlled. For data pipelines:
```hcl
# Terraform example: create an EMR cluster
resource "aws_emr_cluster" "daily_pipeline" {
  name = "daily-transaction-processing"
  release_label = "emr-6.10.0"
  applications = ["Spark", "Hadoop"]
  # ... configs, IAM roles, security groups
}
```
Benefits: reproducible environments, drift detection, pull request review for infrastructure changes, easy disaster recovery.

**Q28. How do you handle cost optimisation for a cloud data pipeline?**
- Right-size clusters: monitor actual CPU/memory utilisation, reduce oversized nodes
- Spot/preemptible for batch workloads: 60-80% savings
- Auto-terminate clusters after job completion
- S3 lifecycle policies: move old data to Glacier
- Partition pruning: avoid full table scans
- Delta OPTIMIZE + VACUUM: reduce storage and scan size
- Reserved instances for steady-state workloads: 30-60% savings over on-demand
- Monitor with AWS Cost Explorer / Azure Cost Management; set budget alerts

**Q29. What is a VPC endpoint / Private Link and why does it matter for data pipelines?**
A VPC endpoint allows resources inside your VPC (e.g., EMR cluster) to access AWS services (S3, Glue) privately — traffic stays on the AWS network, never traverses the public internet. Private Link extends this to custom services. Benefits: (1) lower latency, (2) better security (no public internet exposure), (3) data doesn't leave AWS network — required for some compliance frameworks (PCI DSS, HIPAA, FCA for banking).

**Q30. You need to replicate data from an on-premises Oracle database to AWS S3 in near-real-time. What's your architecture?**
```
Oracle DB (on-prem)
    → Oracle LogMiner / Debezium (CDC agent — reads redo logs)
    → Kafka (on-prem or AWS MSK via Direct Connect)
    → AWS Direct Connect (private, low-latency link to AWS — not public internet)
    → MSK (managed Kafka in VPC)
    → Kafka Connect S3 Sink Connector (writes Avro/Parquet to S3)
    → S3 (raw landing)
    → Glue Crawler → Glue Catalog
    → AWS Glue / EMR (transformation)
    → Redshift (serving)
```
Key points: Direct Connect (not VPN) for bandwidth/latency; Debezium for zero-impact CDC; Kafka as buffer to handle Oracle outages; Avro with Schema Registry for schema evolution.

---

# 🔄 End-to-End Pipeline Architecture (30 Questions)

---

### Q1. Walk me through designing a complete data pipeline from scratch for a retail client.

**Answer (STAR-T format):**
For a client with 500 stores, 50M daily transactions, 2,000 analysts:

**Layer 1 — Ingestion:**
- POS transactions: Kafka → MSK (real-time), partitioned by store_id
- Daily files: SFTP → S3 landing → Lambda trigger → Glue validation
- Master data (products, customers): CDC via Debezium → Kafka → S3

**Layer 2 — Raw / Bronze:**
- Store exactly what arrived — no transformation
- Partition by `ingest_date` and `source_system`
- Parquet format, LZO compressed
- Schema registry (Glue Schema Registry) enforces contract at write time

**Layer 3 — Processed / Silver:**
- Spark on EMR/Databricks: clean nulls, standardise date formats, deduplicate by `transaction_id`
- Data quality checks (Great Expectations): row count, null rate, range checks
- Bad records → quarantine S3 path + alert
- Delta Lake format (ACID, schema enforcement)

**Layer 4 — Curated / Gold:**
- Star schema: `fact_transactions`, `dim_customer`, `dim_product`, `dim_store`, `dim_date`
- Pre-aggregated tables: daily/weekly/monthly metrics per store/product/customer
- dbt models with full lineage and test coverage

**Layer 5 — Serving:**
- Redshift / Databricks SQL for analyst queries
- Feature store for ML teams
- REST API (FastAPI) for application consumers

**Layer 6 — Orchestration:**
- Airflow DAGs: per-layer, with sensor dependencies
- SLA alerts, retry logic, DQ gate tasks that fail the DAG if quality checks don't pass

**Follow-up 1:** *"How do you ensure the pipeline is idempotent?"*
Each job is parameterised by `execution_date`. Writes use `partitionBy("execution_date").mode("overwrite")` with dynamic overwrite mode — re-running for the same date overwrites exactly the partitions for that date, no more. Silver MERGE uses `transaction_id` as unique key.

**Follow-up 2:** *"How long does each layer take to run?"*
Bronze ingestion: near-real-time (streaming) or <5 min (batch). Silver processing: 20-45 min for 50M records on a 20-node EMR cluster. Gold aggregation: 10-15 min. Total daily cycle: <90 min for all layers, with buffer before analyst SLA.

---

### Q2. What is CDC (Change Data Capture)? Compare log-based, trigger-based, and timestamp-based.

**Answer:**

**Log-based CDC (Debezium):**
- Reads database transaction logs (MySQL binlog, PostgreSQL WAL, Oracle redo log)
- Zero source database impact
- Captures INSERT, UPDATE, DELETE (including hard deletes — others can't)
- Ordered delivery within a table
- Most reliable for high-volume, production systems
- Limitation: requires DBA access to enable log reading, not available on all DBs

**Trigger-based CDC:**
- Database triggers on INSERT/UPDATE/DELETE write to a change table
- Works on any database that supports triggers
- Higher source database overhead (~10-20%)
- Can miss bulk operations (some `BULK INSERT` operations bypass triggers)
- Stored in DB until pipeline reads → can grow large

**Timestamp-based:**
- Query `WHERE updated_at > :last_run_timestamp`
- Simplest to implement
- Misses hard deletes entirely
- Clock drift between pipeline and DB can cause missed records
- "Gap" risk: if pipeline runs at T and a record is updated at T-epsilon, it might be missed

**Follow-up 1:** *"What's a Debezium Outbox Pattern?"*
Instead of reading the main DB transaction log directly (operational risk), the producing service writes to an "outbox" table within the same transaction as the business operation. Debezium reads the outbox table, not the main tables. Benefits: decouples service from CDC, explicit schema, easier to filter/transform before publishing.

---

### Q3. What is Apache Airflow? How do you design a production-grade DAG?

**Answer:**
Airflow is a workflow orchestration platform. You define pipelines as Python DAGs (Directed Acyclic Graphs) — each node is a task, each edge is a dependency.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email": ["data-alerts@company.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "sla": timedelta(hours=2),
    "on_failure_callback": pagerduty_alert,
}

with DAG(
    dag_id="daily_transaction_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # 6 AM daily
    catchup=False,
    max_active_runs=1,
    tags=["production", "transactions"],
    doc_md="""### Daily Transaction Pipeline\nProcesses daily POS transactions..."""
) as dag:

    wait_for_source = S3KeySensor(
        task_id="wait_for_source_file",
        bucket_name="landing-bucket",
        bucket_key="transactions/{{ ds }}/*.parquet",
        poke_interval=300,
        timeout=3600,
        mode="reschedule",  # releases worker slot while waiting
    )

    run_bronze = EmrAddStepsOperator(task_id="bronze_ingestion", ...)
    run_quality = PythonOperator(task_id="data_quality_check", ...)
    run_silver = EmrAddStepsOperator(task_id="silver_transformation", ...)
    run_gold = EmrAddStepsOperator(task_id="gold_aggregation", ...)

    wait_for_source >> run_bronze >> run_quality >> run_silver >> run_gold
```

**Follow-up 1:** *"Poke mode vs reschedule mode in sensors — when to use which?"*
Poke: worker slot is occupied the entire time sensor is waiting → wastes resources for long waits. Reschedule: sensor releases worker slot between checks and reschedules itself → use for waits >5 minutes. Always use `reschedule` for file arrival sensors in production.

**Follow-up 2:** *"What's XCom and when should you NOT use it?"*
XCom passes data between tasks via Airflow metadata DB. Only for small payloads (task IDs, status strings, counts). Never for DataFrames, large lists, or file contents — this corrupts the metadata DB and crashes Airflow. Pass large data via S3/blob storage and use XCom only for the path.

---

### Q4-30: Pipeline Architecture Quick-Fire

**Q4. What is the Medallion Architecture?**
Bronze (raw, as-arrived), Silver (cleaned, validated, conformed), Gold (business-ready aggregates). Each layer has different SLAs and quality guarantees. Bronze is append-only immutable; Silver is the first quality gate; Gold is what analysts consume. Also called the "multi-hop architecture."

**Q5. What's the difference between ETL and ELT?**
ETL: Extract → Transform (in pipeline) → Load into warehouse. Traditional approach when transformation engine is separate from storage.
ELT: Extract → Load (raw into data lake) → Transform inside the warehouse/lake engine. Modern approach: store everything first, transform with the powerful compute available in cloud warehouses (Redshift, BigQuery, Databricks SQL). ELT enables flexible re-transformation without re-ingestion.

**Q6. How do you implement pipeline observability?**
Four pillars: (1) **Metrics** — row counts in/out, processing time, records rejected; push to CloudWatch/Azure Monitor. (2) **Logs** — structured JSON logs with correlation IDs, task names, execution dates; centralised in ELK/CloudWatch Logs. (3) **Traces** — distributed tracing across pipeline steps (OpenTelemetry). (4) **Alerts** — SLA breach, quality gate failure, unexpected row count drop > 10%.

**Q7. What is a dead letter queue (DLQ) and how do you use it in data pipelines?**
A DLQ is a separate storage location for records that fail processing — schema validation failures, parse errors, business rule violations. Instead of failing the entire job, bad records are routed to the DLQ for manual inspection and reprocessing. In Kafka: configure `dead.letter.topic` in Kafka Connect. In streaming: `foreachBatch` with try/catch routes failures to a separate Delta table.

**Q8. How do you handle late-arriving data in a batch pipeline?**
Options: (1) Reprocessability — design pipelines to be re-run for any historical date without side effects (idempotent writes), then trigger backfill when late data arrives. (2) Late-data window — for each day's pipeline, wait 2 hours after nominal run time before finalising. (3) Accumulating snapshot fact table — update the fact table as late data arrives, preserving history. Whichever you choose, **document the SLA** for how late data can be and what happens outside that window.

**Q9. What is a Lambda architecture? When is Kappa better?**
Lambda: two parallel paths — batch layer (complete, accurate, slow) + speed layer (fast, approximate, streaming) + serving layer merges both. Kappa: single streaming path handles all data; historical reprocessing done by replaying Kafka. 

**Lambda better when:** batch and streaming have fundamentally different logic, or batch ML retraining is separate from real-time scoring.
**Kappa better when:** streaming logic covers all use cases, team doesn't want to maintain two codebases, Kafka retention is sufficient for historical replay.

**Q10. How do you implement exactly-once semantics end-to-end?**
True exactly-once requires: (1) **Kafka producer idempotence** (`enable.idempotence=true`) + transactions, (2) **Consumer offset commit after processing** (not before), (3) **Idempotent sink write** — use Delta MERGE with natural key, or write-to-temp-then-swap pattern. Any one of these failing means you have at-least-once, not exactly-once.

**Q11. What is a data contract?**
A formal agreement between a data producer and consumers specifying: schema (column names, types, nullability), SLA (freshness, availability), data quality expectations (completeness rate, valid value ranges), versioning policy, and owner contact. Enforced at write time (schema registry) and monitored at runtime (Great Expectations / dbt tests).

**Q12. How do you do a blue-green deployment for a data pipeline?**
Run both versions simultaneously on production data. Blue = current version, Green = new version. Route a subset of data (e.g., by `execution_date`) to Green. Compare outputs. Once validated, switch traffic to Green, keep Blue available for rollback for 24-48 hours. For table writes: write Green to a separate table schema, reconcile, then rename tables atomically.

**Q13. What is dynamic task generation in Airflow?**
```python
# Generate one task per table dynamically
TABLES = ["transactions", "customers", "products"]
for table in TABLES:
    EmrAddStepsOperator(
        task_id=f"process_{table}",
        steps=[{"Name": f"Process {table}", ...}]
    )
```
From Airflow 2.3+: `@task.dynamic` with `expand()` creates truly dynamic tasks where the list is determined at runtime (not DAG parse time).

**Q14. How do you handle pipeline dependencies between teams?**
Use `ExternalTaskSensor` in Airflow to wait for another team's DAG to succeed before starting. Alternatively, use an event-driven trigger: Team A writes a success marker file to S3 → `S3KeySensor` in Team B's DAG detects it. Define cross-team SLAs in a shared contract document.

**Q15. What is a data pipeline SLA and how do you monitor it?**
SLA = commitment to stakeholders that data will be available by a specific time. Define: freshness SLA (data for day D must be in Gold by 8 AM day D+1) and completeness SLA (99.5% of source records must arrive). Monitor with: Airflow SLA miss callbacks, CloudWatch alarms on partition creation timestamps, Great Expectations freshness expectations.

**Q16. What is event-driven architecture for data pipelines?**
Instead of schedule-based triggers, pipelines are triggered by events: file arrival (S3 Event Notification → SQS → Lambda → trigger EMR job), Kafka message (consumer detects threshold → trigger downstream), DB change (CDC event → trigger enrichment pipeline). Benefits: lower latency, no wasted runs when data doesn't arrive, natural decoupling. Complexity: harder to reason about execution order, need idempotency for duplicate events.

**Q17. How do you design a pipeline for regulatory reporting that requires full auditability?**
1. Immutable raw layer — raw data never modified after landing
2. Column-level lineage — every output cell traceable to source record
3. Audit table — log every transformation: `(source_record_hash, transformation_applied, timestamp, job_run_id)`
4. Idempotent runs with versioning — each run tagged with `reporting_period + version`
5. Reconciliation checks — automated comparison between pipeline output and expected totals
6. Full Git history of transformation code — any historical version reproducible

**Q18. What is schema-on-read vs schema-on-write?**
Schema-on-write: schema enforced when writing — reject data that doesn't match. Safer, more structured, better query performance. Used in Delta Lake with enforcement mode, schema registry. Schema-on-read: data stored as-is; schema applied when reading. More flexible, handles unknown structure, but errors surface late (at query time). Used in raw S3 with Glue Crawlers or Athena. 

**Best practice:** Schema-on-write for Silver+ layers; schema-on-read acceptable for Bronze (exactly as received).

**Q19. How do you test a data pipeline end-to-end?**
Three levels: (1) **Unit tests** — test transformation functions with mock data (pytest + local Spark). (2) **Integration tests** — run pipeline against a small subset of production data in a staging environment, assert output schema + row counts. (3) **Data reconciliation tests** — compare aggregated totals between source and destination for a given period (e.g., sum of transaction amounts must match). Automate all three in CI/CD.

**Q20. What is a backfill and how do you manage it safely?**
A backfill reruns historical pipeline runs (e.g., after fixing a bug in Silver transformation). Risks: (1) running all dates simultaneously overwhelms the cluster; (2) overwriting partially-processed data midway through. Safe approach: set `max_active_runs=1` + `max_active_tasks=1` for backfill DAG, process one date at a time in sequence. Use a separate DAG for backfills to avoid interfering with current runs.

**Q21. What is dbt and why is it valuable for data pipelines?**
dbt (data build tool) handles the T in ELT — it runs SQL transformations inside your data warehouse/lake. Value: (1) every model is a SQL file in Git (version control), (2) automatic dependency graph between models, (3) built-in testing (`not_null`, `unique`, custom SQL tests), (4) auto-generated documentation with lineage diagrams, (5) `ref()` function creates compile-time dependencies between models. Result: SQL transformations become software with tests, CI/CD, and documentation.

**Q22. How do you handle multi-tenancy in a data pipeline?**
Options: (1) **Schema isolation** — each tenant gets their own schema/database (strong isolation, high overhead). (2) **Row-level isolation** — single table with `tenant_id` column + row-level security (efficient, complex access control). (3) **Pipeline isolation** — separate pipeline deployments per tenant (maximum isolation, high operational overhead). For most retail analytics platforms: row-level isolation in Gold + RLS at query time is the right balance.

**Q23. What is the strangler fig pattern for pipeline migration?**
Gradually replace a legacy pipeline by routing traffic to both old and new systems in parallel, comparing outputs, then cutting over. Specifically for data: (1) run new pipeline alongside old, writing to a shadow table; (2) reconcile outputs daily for 2+ weeks; (3) switch downstream consumers to new table; (4) decommission old pipeline. This is exactly how we migrated 400 Hadoop pipelines in Project 2.

**Q24. How do you handle a pipeline with a dependency on an external API with rate limits?**
1. Add exponential backoff + jitter to all API calls
2. Batch API calls where possible (bulk endpoint vs. per-record endpoint)
3. Cache responses for repeated identical requests (Redis or DynamoDB)
4. Implement a token bucket rate limiter
5. Use a queue (SQS) to decouple pipeline from API — pipeline drops requests in queue; a separate worker consumes the queue at the API's rate limit

**Q25. What is the "small files problem" and how do you solve it?**
Many small files (< 128 MB) in S3/ADLS cause: slow listing (thousands of S3 API calls), poor Parquet read performance (overhead per file exceeds read time), Glue Crawler slowdown. 

Solutions: (1) `coalesce()` before writing. (2) Delta Lake `OPTIMIZE` command (compacts small files to 1 GB target). (3) Databricks `Auto Optimize` (auto-compaction on write). (4) Design ingestion to write fewer, larger files — batch micro-files before write. (5) Hive `CONCATENATE` on Hive tables.

**Q26. How do you pass large DataFrames between Airflow tasks?**
You don't. Airflow tasks should pass only metadata (S3 paths, job IDs, row counts) via XCom. Large data lives in S3/Delta. Task A writes to S3, stores path in XCom. Task B reads path from XCom, reads from S3. Attempting to pass DataFrames via XCom will crash Airflow's metadata database.

**Q27. What is CQRS and is it relevant to data engineering?**
Command Query Responsibility Segregation — separate write models (commands) from read models (queries). In data engineering context: your transactional write path (Kafka → Bronze) is separate from the read/serving path (Gold → BI). This is essentially what the medallion architecture implements — a natural CQRS pattern for data systems.

**Q28. How do you handle timezone-aware timestamp management in a global pipeline?**
Golden rule: store all timestamps in UTC in every layer. Apply timezone conversion only at the serving/display layer. In Spark: `F.to_utc_timestamp(F.col("local_ts"), "America/New_York")`. Use `TimestampType` in schema (UTC-based). When partitioning by date, be explicit: partition by UTC date, but expose a `local_date` column in the Gold layer for each region.

**Q29. What is data lineage and how do you implement it?**
Data lineage tracks the origin, transformations, and consumption of data — every table/column traceable back to its source. Implementation options: (1) Apache Atlas (open source, integrates with Spark/Hive), (2) OpenMetadata (modern, API-first), (3) dbt lineage (within dbt's transformation layer), (4) Delta Lake history (shows all writes/reads on Delta tables), (5) Unity Catalog (automatic column-level lineage in Databricks). For compliance: column-level lineage is required to answer "where did this reported figure come from?"

**Q30. How do you design a pipeline that needs to process 1 TB of data every 5 minutes?**
- **Streaming over batch:** 1 TB/5 min = 3.3 GB/s throughput — beyond any single-cluster batch approach
- **Kafka partitioning:** 3.3 GB/s → need ~330 partitions (10 MB/s per partition)
- **Spark Structured Streaming:** auto-scaling Databricks cluster, trigger `ProcessingTime("5 minutes")`
- **Write format:** Delta Lake with `maxFilesPerTrigger` tuning
- **Checkpoint:** S3/ADLS for exactly-once
- **Cluster sizing:** ~50 worker nodes (m5.4xlarge) to handle 3.3 GB/s with headroom
- **Monitor trigger processing time:** if micro-batch takes > 5 min, increase cluster or reduce scope

---

# 📐 Schema Management & Data Quality (30 Questions)

---

### Q1. What is schema evolution and what are the different compatibility types?

**Answer:**
Schema evolution is the process of changing a data schema over time without breaking existing producers or consumers.

**Compatibility types:**

```
BACKWARD compatibility:
- New schema CAN read data written by OLD schema
- Safe change: ADD optional field (new readers tolerate old data missing the field)
- Breaking change: REMOVE field (new reader expects it; old data doesn't have it)
- Use case: consumer upgrades before producer

FORWARD compatibility:
- OLD schema CAN read data written by NEW schema
- Safe change: REMOVE field (old reader ignores unknown fields if schema allows)
- Breaking change: ADD required field (old reader doesn't know how to handle it)
- Use case: producer upgrades before consumer

FULL compatibility:
- Both backward AND forward
- Safe changes ONLY: add optional fields with defaults
- Strictest — hardest to maintain but safest in practice

BREAKING (no compatibility):
- Rename field, change data type, change field from optional to required
- Always requires coordinated migration
```

**Follow-up 1:** *"In Avro, how does field aliasing help with schema evolution?"*
Avro aliases allow a field to be renamed without a breaking change: `{"name": "new_name", "aliases": ["old_name"]}`. When reading old data with old_name using a new schema, Avro uses the alias to map old_name → new_name transparently.

---

### Q2. What is a Schema Registry and how does it work with Kafka?

**Answer:**
A Schema Registry is a centralised service that stores and enforces versioned schemas for Kafka topics. The Confluent Schema Registry (or AWS Glue Schema Registry) is the standard.

**How it works:**
1. Producer registers schema on first write → gets schema ID
2. Producer serialises message as: `[magic_byte][schema_id_4_bytes][avro_payload]`
3. Consumer reads schema_id from message header
4. Consumer fetches schema from registry by ID (cached after first fetch)
5. Consumer deserialises using fetched schema

**Compatibility enforcement at write time:**
```python
# Producer config — enforce BACKWARD_TRANSITIVE compatibility
schema_registry_conf = {
    "url": "https://schema-registry:8081",
    "auto.register.schemas": False,  # Don't auto-register — require manual review
    "schema.compatibility": "BACKWARD"
}

# If a producer tries to register an incompatible schema:
# SchemaRegistryException: Schema being registered is incompatible with an earlier schema
```

**Follow-up 1:** *"What happens if you disable schema registry in a Kafka pipeline?"*
Schema drift becomes invisible. Producer can change a field from `INT` to `STRING` without warning. Consumers fail with deserialisation exceptions at runtime. Without a registry, you discover schema incompatibilities in production rather than at deployment time.

---

### Q3. How do you validate data quality in a production pipeline? Name the key dimensions.

**Answer:**
**Six dimensions of data quality:**

| Dimension | Definition | Example Check |
|-----------|------------|---------------|
| **Completeness** | All expected records/fields present | NULL rate < 1%, row count within 5% of expected |
| **Validity** | Data conforms to business rules | amount > 0, date in valid range, status in ('ACTIVE','INACTIVE') |
| **Consistency** | Agrees across systems | Sum of transaction amounts == reported total from source |
| **Timeliness** | Data is fresh enough | max(ingested_at) within last 2 hours |
| **Uniqueness** | No duplicates | COUNT(*) == COUNT(DISTINCT transaction_id) |
| **Accuracy** | Correct representation of reality | Customer age > 0 AND < 120 |

**Implementation with Great Expectations:**
```python
import great_expectations as ge

context = ge.get_context()
suite = context.create_expectation_suite("transactions.critical")

validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)

# Completeness
validator.expect_column_values_to_not_be_null("transaction_id")
validator.expect_column_values_to_not_be_null("amount", mostly=0.99)  # 99% non-null

# Validity
validator.expect_column_values_to_be_between("amount", min_value=0.01, max_value=1_000_000)
validator.expect_column_values_to_be_in_set("currency", ["USD", "GBP", "EUR"])

# Uniqueness
validator.expect_column_values_to_be_unique("transaction_id")

# Timeliness
validator.expect_column_max_to_be_between(
    "transaction_date", min_value=yesterday, max_value=today
)

# Row count (within 10% of yesterday's count)
validator.expect_table_row_count_to_be_between(
    min_value=yesterday_count * 0.9,
    max_value=yesterday_count * 1.1
)

results = validator.validate()
if not results["success"]:
    raise AirflowException(f"Data quality failed: {results}")
```

**Follow-up 1:** *"What do you do when a quality check fails in the middle of a pipeline?"*
Depends on severity: (1) Critical failure (NULL primary key, row count drop > 50%) → fail the task, alert PagerDuty, do not proceed. (2) Warning threshold (5% NULL on non-critical field) → log warning, continue, alert Slack. (3) Quarantine bad records → route to DLQ, process clean records, alert for manual review.

---

### Q4. How does Delta Lake handle schema enforcement and evolution?

**Answer:**
```python
# Schema ENFORCEMENT (default) — rejects mismatched writes
df_wrong = spark.createDataFrame([("A", "not_a_number")], ["id", "amount"])
df_wrong.write.format("delta").save("s3://table/transactions/")
# AnalysisException: Failed to merge incompatible data types

# Schema EVOLUTION — opt-in, adds new columns
df_new_col = df.withColumn("loyalty_points", F.lit(0))
df_new_col.write.format("delta") \
    .option("mergeSchema", "true") \  # adds loyalty_points column to schema
    .mode("append") \
    .save("s3://table/transactions/")

# Column mapping mode — allows renaming columns without rewriting data
spark.conf.set("spark.databricks.delta.columnMapping.mode", "name")
# After enabling: can rename/drop columns via ALTER TABLE without rewriting Parquet

# Check schema history
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://table/transactions/")
delta_table.history(10).select("version", "timestamp", "operation", "operationParameters").show()
```

**Follow-up 1:** *"What happens to old data when you add a new column via mergeSchema?"*
New column is added to the schema. Old data files don't physically have the column — Delta reads them and returns NULL for the new column. No data rewrite happens. This is why schema evolution is cheap in Delta — it's a metadata-only operation.

---

### Q5-30: Schema & Data Quality Quick-Fire

**Q5. What is a data contract and what should it include?**
A formal agreement between producer and consumer: (1) Schema (fields, types, nullability), (2) SLA (freshness window, availability %), (3) Quality expectations (completeness thresholds, valid value sets), (4) Versioning policy (how breaking changes are communicated), (5) Owner contact, (6) Example data. Enforced at write time (schema registry + pipeline validation), monitored at runtime (Great Expectations).

**Q6. How do you detect and handle null values in a PySpark pipeline?**
```python
# Detect
df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) 
           for c in df.columns]).show()

# Strategy per field:
# Required field (PK): drop row + DLQ
df_clean = df.filter(F.col("transaction_id").isNotNull())
df_nulls = df.filter(F.col("transaction_id").isNull())
df_nulls.write.format("delta").save("s3://quarantine/missing_pk/")

# Optional field: fill with default
df_filled = df.fillna({"loyalty_points": 0, "category": "UNKNOWN"})

# Derived field: compute from other columns
df_derived = df.withColumn("full_name", 
    F.when(F.col("full_name").isNull(), 
           F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
    .otherwise(F.col("full_name")))
```

**Q7. What is data drift and how do you monitor it?**
Data drift: distribution of values changes over time (not the schema — the actual data). Example: average transaction amount drops 40% compared to last month's average. Detection: (1) Statistical tests — KS test, PSI (Population Stability Index) comparing current vs. reference distribution. (2) Simple metric monitoring — track mean, stddev, min, max, null_rate per column per day, alert on > 2 sigma deviation. (3) Great Expectations profile comparison.

**Q8. How do you implement column-level data masking?**
```python
# In PySpark: mask PII at Silver layer
from pyspark.sql.functions import sha2, concat_ws

df_masked = df \
    .withColumn("email_hash", sha2(F.col("email"), 256)) \
    .withColumn("phone_token", sha2(F.concat(F.col("phone"), F.lit("secret_salt")), 256)) \
    .withColumn("card_number_masked", 
                F.concat(F.lit("****-****-****-"), F.col("card_number").substr(-4, 4))) \
    .drop("email", "phone", "card_number")

# In Delta Lake / Databricks: column mask function (Unity Catalog)
# CREATE OR REPLACE FUNCTION mask_pii(val STRING) RETURNS STRING
#   RETURN CASE WHEN IS_MEMBER('pii-access') THEN val ELSE '****' END;
# ALTER TABLE customers ALTER COLUMN email SET MASK mask_pii;
```

**Q9. What is the difference between a hard delete and a soft delete? How does each affect CDC?**
Hard delete: row physically removed from source DB. Only log-based CDC captures hard deletes (as DELETE events in the transaction log). Timestamp-based CDC misses them entirely — those rows simply stop appearing in updates. Handle hard deletes in the pipeline: CDC DELETE event → set `is_deleted = true` in Silver layer (never actually delete from the lake — preserve history).

Soft delete: row has a flag (`is_deleted = true`, `deleted_at = timestamp`). Any CDC method captures it since it's a regular UPDATE. Simpler to handle but means source DB retains rows forever.

**Q10. How do you reconcile data between two systems?**
```python
# Full reconciliation: compare aggregated totals
source_total = source_df.agg(F.sum("amount"), F.count("*")).collect()[0]
target_total = target_df.agg(F.sum("amount"), F.count("*")).collect()[0]

# Row-level reconciliation: find mismatches
source_keyed = source_df.select("transaction_id", F.hash(*source_df.columns).alias("row_hash"))
target_keyed = target_df.select("transaction_id", F.hash(*target_df.columns).alias("row_hash"))

mismatches = source_keyed.join(target_keyed, "transaction_id") \
    .filter(F.col("row_hash") != F.col("row_hash"))
```

**Q11. What are dbt tests and how do you implement custom ones?**
```yaml
# schema.yml — built-in tests
models:
  - name: fact_transactions
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
      - name: status
        tests:
          - accepted_values:
              values: ['COMPLETED', 'PENDING', 'FAILED']
```

```sql
-- custom test: macro in tests/generic/
{% test revenue_reconciles_with_source(model, column_name) %}
SELECT COUNT(*)
FROM {{ model }} t
JOIN {{ ref('source_daily_totals') }} s ON t.date = s.date
WHERE ABS(t.{{ column_name }} - s.expected_revenue) / s.expected_revenue > 0.01
-- Fails if model revenue deviates > 1% from source totals
{% endtest %}
```

**Q12. How do you manage schema versions in a data warehouse?**
1. Never drop columns — mark as deprecated in metadata
2. Additive-only changes (new columns, new tables) don't require versioning
3. Breaking changes require a new schema version: `v1_transactions` → `v2_transactions` with migration
4. dbt `--version` flag for model versioning
5. Schema Registry for Kafka topics: semantic versioning (MAJOR.MINOR.PATCH)
6. Document all changes in a CHANGELOG.md in the repo

**Q13. What is statistical process control (SPC) for data quality?**
Apply control charts (as in manufacturing quality control) to data metrics: track daily row count with UCL/LCL = mean ± 3σ. If metric falls outside control limits, trigger alert. More sophisticated than hard thresholds — adapts to seasonal patterns. Implemented with tools like Monte Carlo Data or custom Python using scipy.stats.

**Q14. What happens when a source system changes a column type from INT to STRING?**
Pipeline action plan: (1) Schema Registry rejects the incompatible schema if compatibility rules are set. (2) If not using a registry: Delta schema enforcement rejects the write. (3) Emergency fix: enable `mergeSchema=true`, accept STRING, cast back to INT in Silver transformation with NULL for non-parseable values. (4) Root cause: communicate with source system owner, agree on a migration plan, recast historical data.

**Q15. How do you handle duplicate records from an at-least-once Kafka consumer?**
```python
# Idempotent deduplication in streaming using foreachBatch + Delta MERGE
def dedup_and_upsert(batch_df, batch_id):
    deduped = batch_df.dropDuplicates(["event_id"])
    delta_table.alias("t").merge(
        deduped.alias("s"), "t.event_id = s.event_id"
    ).whenNotMatchedInsertAll().execute()
    # whenNotMatched only — don't update; first event wins

stream.writeStream.foreachBatch(dedup_and_upsert).start()
```

**Q16. What is data profiling and when do you do it?**
Automated analysis of a dataset's structure and content: count, distinct count, NULL rate, min/max/mean/stddev per column, value distribution histograms, outlier detection. Do it: (1) when onboarding a new data source, (2) weekly on production tables to detect drift, (3) before running a complex transformation to understand the data, (4) as part of CI/CD for model changes. Tools: Great Expectations `profiler`, AWS Glue DataBrew, dbt-osmosis.

**Q17. What is a "data quality gate" in a pipeline?**
A task in the orchestration DAG that runs quality checks and either passes or blocks the pipeline. If checks fail, downstream tasks don't execute. In Airflow: a `PythonOperator` that raises `AirflowException` on failure, breaking the DAG dependency chain. This prevents bad data from reaching the Gold layer and corrupting analyst reports.

**Q18. How do you handle encoding issues (UTF-8, Latin-1) in ingested files?**
```python
# Detect encoding
import chardet
with open("file.csv", "rb") as f:
    result = chardet.detect(f.read())
encoding = result["encoding"]  # e.g., 'ISO-8859-1'

# In Spark
df = spark.read.option("encoding", "ISO-8859-1").csv("s3://bucket/file.csv")

# Convert to UTF-8 at Bronze layer — all downstream processing assumes UTF-8
df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
df.write.option("encoding", "UTF-8").parquet("s3://silver/")
```

**Q19. How do you enforce data quality in dbt using source freshness?**
```yaml
# sources.yml
sources:
  - name: raw_transactions
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: ingested_at
    tables:
      - name: transactions
```
Run `dbt source freshness` in CI/CD — fails the build if source data is stale.

**Q20. What is a row-level data quality score?**
Score each row on multiple dimensions, store the score alongside the data:
```python
df_scored = df.withColumn("dq_score",
    (F.when(F.col("amount").isNull(), 0).otherwise(20) +
     F.when(F.col("customer_id").isNull(), 0).otherwise(20) +
     F.when(F.col("amount").between(0, 1_000_000), 20).otherwise(0) +
     F.when(F.col("transaction_date").isNotNull(), 20).otherwise(0) +
     F.when(F.col("currency").isin(["USD","GBP"]), 20).otherwise(0))
).withColumn("dq_pass", F.col("dq_score") >= 80)
```
Route rows with score < 80 to quarantine; > 80 to Silver.

**Q21-30: Schema & Quality Rapid-Fire**

**Q21. What's the difference between validation and verification in data quality?**
Validation: check data conforms to expected rules (is amount > 0?). Verification: confirm data is correct by cross-referencing another system (does our total match the bank's settlement total?). Validation is automated and runs every pipeline run. Verification is heavier — typically run daily or on a schedule.

**Q22. How do you handle array/nested JSON columns in schema management?**
Define schema explicitly (never rely on inferred schema for production):
```python
from pyspark.sql.types import *
schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType())
    ])))
])
df = spark.read.schema(schema).json("s3://raw/")
```
Explode arrays in Silver layer for relational analysis:
```python
df_exploded = df.withColumn("item", F.explode(F.col("items"))).select("transaction_id", "item.*")
```

**Q23. What is a checksum/hash for data integrity verification?**
Compute a hash of each record at source and compare at destination:
```python
df = df.withColumn("row_hash", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in df.columns]), 256))
# At destination: recompute and compare — any mismatch = corruption or transformation error
```

**Q24. How do you manage sensitive data (PII) in test environments?**
Never copy production PII to dev/test. Options: (1) Synthetic data generation (Faker, SDV — Statistical Data Vault), (2) Data masking: replace real PII with realistic but fake values, preserving distribution and referential integrity, (3) Tokenisation: replace with reversible tokens (token table kept only in prod). Run full DQ tests against synthetic data in staging.

**Q25. What are the implications of NULL handling in aggregations?**
SQL/Spark `SUM()` ignores NULLs. `COUNT(*)` counts NULLs; `COUNT(column)` doesn't. `AVG()` ignores NULLs (could skew average). `MIN()`/`MAX()` ignores NULLs. Always audit NULL rates before relying on aggregate results — a 20% NULL rate on `amount` means your `SUM(amount)` underreports by 20%.

**Q26. How do you test for referential integrity in a data warehouse?**
```sql
-- dbt test: every transaction must reference a valid customer
SELECT t.transaction_id
FROM fact_transactions t
LEFT JOIN dim_customer c ON t.customer_key = c.customer_key
WHERE c.customer_key IS NULL;
-- Test fails if any orphan transactions exist
```
Or in PySpark:
```python
orphans = transactions_df.join(customers_df, "customer_id", "left_anti")
assert orphans.count() == 0, f"Found {orphans.count()} transactions with invalid customer_id"
```

**Q27. How do you handle a schema change that breaks a downstream consumer?**
1. Never make breaking changes without a migration plan
2. Expand-and-contract pattern: add new_column, deploy consumers to use new_column, deprecate old_column, remove old_column after all consumers migrate
3. Version the API/table: serve both v1 and v2 simultaneously during migration
4. Schema Registry compatibility rules prevent accidental breaking changes at publish time

**Q28. What is "fail fast" vs "fail safe" in data quality?**
Fail fast: reject bad records immediately at ingestion (strict validation at Bronze) — stops bad data from propagating. Good for well-understood sources with stable schemas. Fail safe: accept all records, flag bad ones, route to quarantine — continue processing good records. Good for heterogeneous sources, external partners, or when business can't tolerate pipeline downtime due to DQ issues. In practice: fail fast for critical fields (PK, required business keys); fail safe for optional/enrichment fields.

**Q29. What is data observability and how does it differ from data quality?**
Data quality: is the data correct? (Static validation at a point in time)
Data observability: can you detect, triage, and resolve data issues quickly? (Ongoing monitoring, alerting, root cause analysis). Observability adds lineage (where did the data come from?), freshness monitoring, volume anomaly detection, and end-to-end health dashboards. Tools: Monte Carlo Data, Acceldata, Datafold, or custom dashboards on top of Great Expectations results.

**Q30. A business analyst tells you "the revenue number in the dashboard looks wrong." Walk me through your investigation.**
1. Check Airflow: did the pipeline run successfully? Any failures or retries?
2. Check data quality gate results for that date: did any checks fail?
3. Query raw Bronze data: what was the source total? Compare to Gold.
4. Trace through Silver: any records dropped at DQ gate?
5. Check Gold transformation: any filter that could be excluding valid records?
6. Check the BI report: is there a filter or date range issue in the dashboard itself?
7. Compare to prior day: when did the discrepancy start?
8. If confirmed data issue: notify analyst, reprocess affected partitions, postmortem.

---

# 💥 OOM & Data Skew (30 Questions)

---

### Q1. A Spark job fails with `java.lang.OutOfMemoryError: Java heap space`. Walk me through your full diagnostic and fix process.

**Answer:**

**Step 1: Determine if it's executor or driver OOM**
```bash
# Check the Spark UI → Stages → Failed Tasks → "stderr" log link
# Executor OOM: error in task log
# Driver OOM: error in application log / Airflow log
```

**Step 2: For EXECUTOR OOM:**
```python
# Diagnose:
# A) Check if it's a skewed partition — Spark UI → Stages → Tasks tab
#    Sort by Duration. If one task is 10x others → skew + OOM
# B) Check for unbounded collect_list / collect_set
# C) Check for large broadcast

# Fix A: More executor memory + overhead
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")  # Python UDF memory

# Fix B: Replace collect_list with window + array_agg with size limit
# Instead of collect_list (can be huge):
df.groupBy("customer_id").agg(F.collect_list("product_id"))
# Use sortable + truncated approach:
df.groupBy("customer_id").agg(F.slice(F.sort_array(F.collect_list("product_id")), 1, 100))

# Fix C: Tune broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")  # lower it
# Or force-disable: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Fix D: More, smaller partitions (less data per task)
spark.conf.set("spark.sql.shuffle.partitions", "4000")
df = df.repartition(2000)  # before heavy operations
```

**Step 3: For DRIVER OOM:**
```python
# Common causes:
# A) collect() on a large DataFrame
results = df.collect()  # ← collect() entire DF to driver = OOM
# Fix: write to storage instead
df.write.parquet("s3://output/")

# B) toPandas() on large DF
pdf = large_df.toPandas()  # OOM if DF > driver memory
# Fix: sample or aggregate first
pdf = large_df.sample(fraction=0.01).toPandas()

# C) broadcast() of large object
spark.sparkContext.broadcast(huge_dict)  # broadcast stored in driver first
# Fix: convert to DataFrame and join instead

# D) Too many DAG actions without checkpointing
# Fix: checkpoint to truncate long lineage
df.checkpoint()
```

**Follow-up 1:** *"What is memoryOverhead and when do you increase it?"*
memoryOverhead is the off-JVM memory per executor for Python workers, native libraries, OS processes, and JVM internal overhead. Default: `max(executor_memory * 0.1, 384MB)`. Increase to 2-4 GB when: using many Pandas UDFs, running complex Python logic in UDFs, or using off-heap memory.

**Follow-up 2:** *"How do you reproduce OOM locally for debugging?"*
You can't fully reproduce a 10 TB job locally. Instead: (1) Take a 1% sample of the skewed partition, (2) Reproduce the job on a small local Spark session, (3) Check if OOM occurs on the sample — if not, the issue is only at scale, debug in a dev cluster with the real data.

---

### Q2. What is data skew and how do you detect it before it causes issues?

**Answer:**
Data skew: highly uneven distribution of data across partitions. Some partitions have 10-100x more rows than others. In joins: the "skewed" key (e.g., NULL, 'UNKNOWN', a single super-popular customer) routes far more data to one reducer.

**Detection:**
```python
# Method 1: Check key distribution
skew_analysis = df.groupBy("join_key").count() \
    .withColumn("pct_of_total", F.col("count") / df.count() * 100) \
    .orderBy(F.col("count").desc())
skew_analysis.show(20)

# Method 2: Check partition sizes at runtime
df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
# Large variance = skew

# Method 3: Spark UI → Stages → Tasks
# Sort tasks by "Duration" — if P99 >> P50, skew exists
# Also look at "Shuffle Read Size" per task
```

**Key indicators:**
- Top 5 keys account for > 50% of rows
- Null/empty string values are the join key for many records
- A "CATCH-ALL" category has orders of magnitude more records
- Spark UI shows 95% of tasks finishing in 30 seconds but 1-2 tasks taking 3 hours

**Follow-up 1:** *"How do you handle NULL keys in a skewed join?"*
```python
# Option 1: exclude NULLs from join, handle separately
non_null = df.filter(F.col("key").isNotNull())
null_records = df.filter(F.col("key").isNull())

joined = non_null.join(lookup_df, "key")
null_handled = null_records.withColumn("lookup_col", F.lit("UNKNOWN"))
result = joined.union(null_handled)
```

---

### Q3-30: OOM & Skew Quick-Fire

**Q3. Explain the salting technique for data skew with complete code.**
```python
SALT_FACTOR = 50

# Salt the large skewed table
large_df_salted = large_df.withColumn(
    "salt", (F.rand() * SALT_FACTOR).cast("int")
).withColumn(
    "salted_key", F.concat_ws("_", F.col("customer_id").cast("string"), F.col("salt"))
)

# Explode the small lookup table to match all salt values
salt_range = spark.range(SALT_FACTOR).toDF("salt")
small_df_exploded = small_df.crossJoin(salt_range).withColumn(
    "salted_key", F.concat_ws("_", F.col("customer_id").cast("string"), F.col("salt"))
)

result = large_df_salted.join(small_df_exploded, "salted_key").drop("salt", "salted_key")
```
**Trade-off:** small table grows by SALT_FACTOR. Works well when small table is < 1 GB and SALT_FACTOR is 20-50.

**Q4. What is AQE (Adaptive Query Execution) and which of its features address skew?**
AQE (`spark.sql.adaptive.enabled=true`, default in Spark 3.2+) adapts execution plan at runtime based on actual statistics:
- **Adaptive coalescing:** merges small shuffle partitions into fewer, larger ones
- **Adaptive skew join:** detects skewed partitions, splits them, and processes with extra parallelism — no code change needed
- **Dynamic partition pruning:** skips partitions in fact table based on actual filter results

AQE skew join config: `spark.sql.adaptive.skewJoin.enabled=true`, `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256mb`.

**Q5. When does AQE NOT fix skew?**
(1) Streaming jobs — AQE only works in batch SQL, (2) RDD-based operations — AQE is SQL-layer only, (3) Skew caused by Python UDFs (opaque to Catalyst), (4) When all partitions are skewed (no "normal" partition as reference), (5) Very extreme skew where one partition is 1000x others — AQE's split may not be sufficient.

**Q6. What is "straggler" vs "skewed" partition? Different fix?**
Straggler: slow task due to bad hardware, network issue, or GC pressure — not a data volume issue. Fix: speculative execution (`spark.speculation=true`).
Skew: slow task due to too much data. Fix: salting, AQE, broadcast join. Misdiagnosing straggler as skew wastes time — check Spark UI's task details: is it GC time high? Is data size normal but duration high? → straggler.

**Q7. Explain Spark's sort-merge join and how skew affects it.**
Sort-merge join: (1) Both sides shuffle to align same keys on same partitions, (2) Sort each partition by key, (3) Merge sorted streams. Skew impact: if `customer_id = NULL` for 30% of rows, one partition after shuffle has 30% of all data → one task processes the entire NULL partition = 100x longer than others. Fix: handle NULLs separately before join.

**Q8. What is the Broadcast Hash Join and when does Spark automatically choose it?**
Broadcast hash join: small table is broadcast to all executors, each executor probes it locally for each row of the large table — no shuffle. Automatically chosen when: `spark.sql.autoBroadcastJoinThreshold` (default 10MB, tunable). AQE can switch to broadcast join mid-query if a filtered table turns out to be small enough. Zero shuffle = dramatic performance improvement for star schema joins.

**Q9. You have a 5 TB fact table and a 2 GB dimension table for a join. Design the optimal Spark strategy.**
2 GB is too large for broadcast (default 10 MB threshold, risky even with 200 MB threshold). Options:
1. Pre-filter dimension table before join — if you only need active customers (50% of 2 GB = 1 GB), try broadcast on the filtered subset
2. Partition-wise join: if fact table is partitioned by `region` and dimension by `region`, repartition both by the join key to collocate data
3. Bucketing: if both tables are pre-bucketed by `customer_id` with same bucket count, no shuffle needed at join time
4. Sort-merge join with AQE skew detection: the standard approach with AQE enabled

**Q10. What is bucketing in Spark and how does it help with repeated joins?**
```python
# Write bucketed table once
large_df.write \
    .bucketBy(200, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("bucketed_transactions")

# Subsequent joins on customer_id with same bucket count: NO SHUFFLE
bucketed_df.join(other_bucketed_df, "customer_id")
# Spark detects matching buckets → skip sort + shuffle stage entirely
```
Best for tables that are joined repeatedly on the same key. One-time write overhead; subsequent jobs are dramatically faster.

**Q11. What is GC (Garbage Collection) pressure and how does it relate to OOM?**
JVM GC collects unreachable objects. If GC can't free enough memory to satisfy an allocation, it throws OOM. Signs: Spark UI shows high GC time per task (> 5% = warning, > 20% = serious). Causes: many short-lived objects (row-by-row operations, UDFs), large object graphs (complex nested structures). Fixes: Tungsten off-heap memory (`spark.memory.offHeap.enabled=true`), avoid Python UDFs (use Pandas UDFs), reduce partition size, use more efficient data types (int32 instead of string for IDs).

**Q12. Explain memory fractions in Spark.**
```
JVM heap per executor (spark.executor.memory = 8g):
├── Reserved memory: 300 MB (internal Spark usage)
└── Usable: 7.7 GB
    ├── Spark memory (spark.memory.fraction = 0.6 default): 4.62 GB
    │   ├── Execution memory: shuffle, joins, sorts (dynamic)
    │   └── Storage memory: caching, broadcasts (dynamic)
    └── User memory (1 - 0.6 = 0.4): 3.08 GB
        └── User data structures, UDF objects, etc.
```
Unified memory model: execution and storage dynamically share the `spark.memory.fraction` pool. If execution needs more, it can evict cached data. Increase `spark.memory.fraction` (to 0.8) for computation-heavy jobs with little caching; increase `spark.memory.storageFraction` for cache-heavy jobs.

**Q13. What is the difference between `spark.executor.memory` and `spark.executor.memoryOverhead`?**
`executor.memory`: JVM heap available to each executor. `executor.memoryOverhead`: non-JVM memory (OS, Python worker processes for UDFs, Arrow buffers for Pandas UDFs). Total memory per executor = `executor.memory + executor.memoryOverhead`. YARN/Kubernetes will kill executors that exceed total memory. Common mistake: setting `executor.memory = 10g` on a 12 GB node without accounting for overhead → executor killed.

**Q14. How do you handle a skewed groupBy/aggregation (not a join)?**
```python
# Problem: groupBy on skewed key — one reducer gets all the data
df.groupBy("status").agg(F.sum("amount"))
# If 80% of rows have status='ACTIVE', one task gets 80% of data

# Fix: Two-phase aggregation (map-side partial aggregation + final)
# Spark does this automatically for commutative aggregations (sum, count)
# But for complex aggregations: pre-aggregate at a finer granularity first
df_partial = df.groupBy("status", (F.rand() * 100).cast("int").alias("shard")) \
               .agg(F.sum("amount").alias("partial_sum"), F.count("*").alias("partial_count"))

df_final = df_partial.groupBy("status") \
                     .agg(F.sum("partial_sum").alias("total"), F.sum("partial_count").alias("count"))
```

**Q15. What is spill in Spark and how do you reduce it?**
Spill: when a partition's data doesn't fit in memory during sort/shuffle, it's written to disk (spill to disk). Disk spill is 10-100x slower than memory operations. Detect: Spark UI → Stages → "Shuffle Spill (Memory)" and "Shuffle Spill (Disk)". Reduce: (1) Increase executor memory, (2) Increase parallelism (more partitions = smaller per partition), (3) Increase `spark.sql.shuffle.partitions`, (4) Use AQE coalesce to prevent tiny partitions post-shuffle.

**Q16. Explain what happens step-by-step when a Spark job runs out of memory due to a broadcast join on a too-large table.**
1. Spark driver evaluates `spark.sql.autoBroadcastJoinThreshold` (or explicit `broadcast()`)
2. Driver computes table statistics — if table appears small (e.g., accurate stats not available), it proceeds
3. Driver fetches the full "small" table: `df.collect()` on driver → **driver OOM** if table doesn't fit in driver memory
4. Or: driver broadcasts to all executors → each executor receives a copy → **executor OOM** if executor heap can't hold the broadcasted table
5. With 100 executors each receiving a 5 GB broadcast table: 100 × 5 GB = 500 GB of total broadcast memory consumed

Fix: explicitly set broadcast threshold lower, or avoid broadcast for large tables.

**Q17-30: OOM & Skew Speed Round**

**Q17. What's the fastest way to count distinct values for a 1 TB dataset?**
Approximate: `F.approx_count_distinct("customer_id", rsd=0.05)` uses HyperLogLog — 95% accuracy with 1% of the memory cost of exact count distinct. Exact distinct count requires a full shuffle — very expensive on 1 TB.

**Q18. What is off-heap memory in Spark?**
Memory allocated outside the JVM heap directly as native memory via `sun.misc.Unsafe`. Not subject to JVM GC. Configured with `spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size`. Useful when JVM GC becomes a bottleneck (large executor memory > 32 GB typically). Tungsten uses off-heap for binary processing of sort and hash operations.

**Q19. What is task serialisation overhead and how does it affect performance?**
Every task's closure (the lambda/function you pass to map/filter) must be serialised from driver → executor. Large closures (containing large Python objects, big lookup dicts) cause serialisation overhead. Fix: broadcast large objects instead of including in closure. Use `spark.sparkContext.broadcast(large_dict)` and reference inside the lambda.

**Q20. How do you diagnose and fix "Too many open files" error in Spark?**
Caused by writing too many small files simultaneously (many partitions × many write threads). Fix: (1) Coalesce before write to reduce partition count, (2) Increase OS `ulimit -n` on executors (typically from 1024 to 65536), (3) Use Delta Lake OPTIMIZE to compact after write, (4) Control output parallelism with `spark.sql.shuffle.partitions`.

**Q21. What is a sort-based shuffle vs hash-based shuffle?**
Hash shuffle: each map task creates one file per reduce task (N×M files — "shuffle explosion"). Sort shuffle: each map task writes a single sorted file with an index; reduce tasks read the relevant range. Spark uses sort shuffle by default (since 2.0). Hash shuffle only remains in legacy Tungsten paths. Sort shuffle produces O(M) files per stage instead of O(N×M).

**Q22. How do you handle OOM when caching a large dataset?**
```python
# Don't cache the entire dataset — cache only what you'll reuse
# Wrong: cache 10 TB table
df.cache()

# Right: filter first, then cache the small result
df_filtered = df.filter(F.col("region") == "US").filter(F.col("date") >= "2024-01-01")
df_filtered.cache()

# Or: use DISK_ONLY storage to avoid memory pressure
df.persist(StorageLevel.DISK_ONLY)

# Always unpersist when done
df_filtered.unpersist()
```

**Q23. What is the "fetch failed" error in Spark?**
`FetchFailed` means an executor tried to fetch shuffle data from another executor that died or became unreachable. The fetch is retried (up to `spark.maxRemoteBlockSizeFetchToMem` retries). Root causes: executor killed by YARN/K8s due to OOM, executor killed due to preemption (spot instance), network partition. Fix: (1) Increase executor memory to prevent OOM kills, (2) Increase `spark.task.maxFailures`, (3) Use on-demand instances for critical executors.

**Q24. How does Spark handle null handling differently from SQL in certain cases?**
In Spark DataFrame API: `df.filter(col("x") > 5)` silently drops NULL rows (NULLs evaluate to false). In SQL: same behaviour. But `df.filter(col("x").isNull() | (col("x") > 5))` includes NULLs. Spark window functions with NULLs in `orderBy`: NULLs sort to last by default (can be changed with `nullsFirst()`/`nullsLast()`).

**Q25. What is the maximum recommended size for a single Spark partition?**
200 MB–1 GB. Too small (< 10 MB): scheduling overhead dominates, driver OOM from too many tasks. Too large (> 2 GB): OOM risk during sort/join, poor parallelism. The "right" size depends on available executor memory and computation intensity. For I/O-heavy jobs: 512 MB–1 GB. For computation-heavy (many UDFs): 100–256 MB.

**Q26. What's the difference between `mapPartitions` and `foreachPartition`?**
`mapPartitions`: transforms partition → returns a new RDD. Use for transformations. `foreachPartition`: processes partition → returns nothing. Use for side effects (writing to external systems, sending to APIs). Both allow one-time initialisation per partition (DB connection, model load).

**Q27. When does `.explain()` show "Exchange" in the physical plan?**
"Exchange" indicates a shuffle — data redistributed across executors. A `HashPartitioning Exchange` appears before groupBy/join. A `RangePartitioning Exchange` appears before orderBy. Minimising Exchange nodes = minimising shuffles = faster job. Use bucketing or partitioning to eliminate Exchanges on repeated operations.

**Q28. What is data locality in Spark and why does it matter?**
Data locality: preference for running a task on the same node that holds the data. Levels: `PROCESS_LOCAL` (same JVM) → `NODE_LOCAL` (same node, different JVM) → `RACK_LOCAL` (same rack) → `ANY` (different rack/AZ). PROCESS_LOCAL is ~100x faster than ANY due to network vs. memory bandwidth difference. Spark waits for preferred locality; `spark.locality.wait` controls timeout before falling back to ANY.

**Q29. How do you fix "Exception: Task not serializable" in PySpark?**
The lambda/function being serialised contains a non-serialisable object (database connection, file handle, boto3 client). Fix: (1) Move object creation inside the function — don't capture it from outer scope. (2) Use `mapPartitions` — create the object once per partition inside the function. (3) For config/constants: broadcast them. Never capture mutable stateful objects in lambdas.

**Q30. You have a streaming job with growing memory usage over time that eventually crashes. What do you look for?**
State leak in Structured Streaming: (1) Aggregation with `groupBy` but no watermark — state grows indefinitely. Fix: add `withWatermark`. (2) `flatMapGroupsWithState` with state that never expires. Fix: implement `GroupStateTimeout.EventTimeTimeout`. (3) Checkpoint directory growing too large — check `checkpointLocation` size, ensure expired state is being cleaned. (4) Unbounded cache — `df.cache()` in streaming without `unpersist()`. (5) Memory leak in Python UDF (Pandas) — profile Python heap with `memory_profiler`.

---

# 🏛️ Architectural Problem-Solving Scenarios (30 Questions)

---

### Q1. Design a real-time fraud detection system for 100,000 transactions per second.

**Answer:**

```
Architecture:

Card Network API / ATM / POS
    → Kafka MSK (50 partitions, acks=all, replication=3)
    → Flink on EMR / Databricks (stateful stream processing)
        ├── Feature extraction: velocity (txns per minute), geography, merchant category
        ├── Real-time feature store lookup (Redis/DynamoDB: <2ms)
        ├── ML model inference (pre-loaded in Flink operator memory: <5ms)
        └── Rule engine (hard rules: block if amount > 50K for first-time merchant)
    → Decision: 
        ├── APPROVE → response topic → card network (< 200ms SLA)
        ├── REVIEW → case management queue → analyst dashboard
        └── BLOCK → block topic → card network + notification service
    → All events → S3 → Spark batch (daily model retraining)
```

**Key design decisions:**
- Kafka for ingestion: 100K TPS × 2 KB = 200 MB/s → 50 partitions × 4 MB/s each
- Flink over Spark Streaming: true record-by-record streaming, 50ms latency vs Spark's 500ms micro-batch
- Feature store (Redis): pre-computed features (30-day spend, last N transactions, device fingerprint) fetched in <2ms
- Keyed state in Flink: per-customer velocity state maintained in-memory per Flink operator; state backend = RocksDB for persistence
- Watermark: 30-second tolerance for late events (rare in payments, but international routing delays)

**Follow-up 1:** *"How do you ensure exactly-once fraud decisions?"*
Flink exactly-once via checkpointing + Kafka transactional producer for output. Idempotency key in output message = `(card_id, transaction_id)`. Downstream systems use idempotency key to deduplicate decisions.

**Follow-up 2:** *"How does the model get updated without downtime?"*
Blue-green model deployment in the feature store + inference service. New model deployed to shadow scoring alongside current model. Shadow results compared for 24h. If metrics are better, traffic switched via Flink job config update (no code change). Old model retained for rollback.

---

### Q2. Your data pipeline is missing its 8 AM SLA. The downstream BI dashboard is stale. Walk me through your incident response.

**Answer:**

**T+0: Detection**
- PagerDuty alert fires: Airflow SLA miss callback triggered
- Or: analyst reports stale data in dashboard

**T+5: Initial Triage**
```
1. Check Airflow UI: which task failed? What's the error?
2. Check if it's a delay or failure:
   - DAG running but slow → resource/data volume issue
   - DAG failed → error in task
3. Check upstream: did the source file/feed arrive?
   - If no: contact source team, trigger SLA breach process
   - If yes: pipeline issue
```

**T+10: Stakeholder Communication**
- Post in #data-incidents Slack: "Transaction pipeline delayed. Investigating. ETA: 9 AM."
- Don't go silent — update every 20 minutes even if no resolution yet

**T+15: Diagnosis by error type**
```
Out of memory: increase executor memory, resubmit job
Schema change: check source schema vs. expected — use backup schema or skip validation
Source data quality failure: check quarantine table, decide: proceed with clean records or wait for full set
Infrastructure: EMR cluster failed to launch → check AWS quota/spot availability
```

**T+30: Resolution**
- Fix + rerun the failed task (Airflow "Clear" from failure point — don't rerun entire DAG)
- Verify output row count matches expectation
- BI cache refresh (if applicable)

**T+60: Postmortem**
- RCA document: what happened, why detection took X minutes, what breaks earlier detection
- Action items: add monitoring that would have caught this 30 minutes earlier, fix the root cause

**Follow-up 1:** *"How do you reduce the blast radius when a single task fails?"*
Design DAGs with independent branches that can succeed even if one branch fails. Use `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` for tasks that should run if at least some upstream tasks succeeded. Add granular tasks (one EMR step per domain) rather than one monolithic step.

---

### Q3. Design a data platform to support 500 retail clients, 10 billion daily transactions, and 2,000 data analysts.

**Answer:**

**Storage Architecture:**
```
Raw Zone (S3/ADLS): client data as-received, partitioned by client_id + date
    → Bronze: standardised schema, Parquet, per-client prefix
    → Silver: cleaned, deduplicated, validated, unified schema, Delta Lake
    → Gold: client-agnostic aggregates + client-specific marts, Delta Lake
    → Serving: Databricks SQL / Redshift for analyst queries
```

**Multi-tenancy:**
- **Client data isolation:** separate S3 prefixes per client + Lake Formation row-level security
- **Shared infrastructure:** single Spark cluster processes all clients (cost efficiency)
- **Soft multi-tenancy:** row-level `client_id` column + Apache Ranger/Lake Formation policies

**Scalability for 10B daily transactions:**
- Partitioned Kafka: 10B / 86400s = ~115K TPS → 1,000 Kafka partitions
- Databricks auto-scaling clusters: scale out during ingestion (6 AM peak), scale in after
- Delta Lake Z-ORDER on (client_id, date) for common query patterns

**Analyst serving (2,000 concurrent users):**
- Databricks SQL endpoints with auto-scaling (serverless compute pools)
- Pre-aggregated Gold tables for common queries (sub-second response)
- Row-level security: analysts only see their assigned clients' data

**Governance:**
- Unity Catalog: centralised access control across all 500 clients
- Data contracts per client: schema, SLA, quality expectations
- Automated DQ gates: per-client anomaly detection

---

### Q4-30: Architecture Scenario Quick-Fire

**Q4. How would you migrate a legacy Hadoop/Hive/Oozie system to the cloud with zero downtime?**
Strangler fig pattern: (1) Run parallel — same pipeline in both Hadoop and cloud, compare outputs daily for 4 weeks. (2) Migrate consumer by consumer — each downstream system switches to cloud output independently. (3) Feature-flag cutover — switch the "source of truth" flag per pipeline, not all at once. (4) Keep Hadoop in read-only mode for 30 days post-cutover for rollback. (5) Decommission only after all consumers confirmed on new system.

**Q5. Design a self-serve data platform for non-technical business users.**
Components: (1) Data catalog (OpenMetadata/Purview) with business-friendly descriptions, (2) Certified data products in Gold layer with quality scores, (3) Databricks SQL / Redshift with pre-built dashboards, (4) Automated access provisioning (request via web portal → auto-grant RLS access), (5) Data literacy program — training business users on what data means and quality limitations.

**Q6. How would you design a pipeline that processes 50 TB of historical data in under 4 hours?**
- Estimate: 50 TB in 4h = 12.5 TB/h = ~3.5 GB/s throughput needed
- Spark on 100-node cluster: each node reads at ~500 MB/s = 50 GB/s total throughput → easily achievable
- Key: maximise parallelism (50 TB / 256 MB per partition = ~200,000 tasks), avoid shuffles, use Parquet column pruning
- Use spot instances for cost: 100 × r5.4xlarge spot instances ≈ $50/hour → ~$200 for 4h run

**Q7. What is the "dual-write" problem and how do you solve it?**
Writing to two systems atomically (DB + Kafka) without a distributed transaction. If you write to DB then Kafka fails → systems diverge. Solutions: (1) Transactional Outbox Pattern: write to DB + outbox table in same transaction; separate CDC reads outbox and writes to Kafka. (2) Change Data Capture from the DB: CDC on DB change log → Kafka. Both ensure the DB is the source of truth.

**Q8. Design a data warehouse for a retail loyalty programme.**
- Fact tables: `fact_transactions` (grain: one row per transaction), `fact_points` (grain: one row per point earn/burn event)
- Dimension tables: `dim_customer` (SCD2), `dim_product` (SCD1), `dim_store`, `dim_date`, `dim_offer`
- Aggregate tables: `agg_customer_monthly` (monthly spend/points per customer), `agg_store_daily`
- Key metrics: CLV (Customer Lifetime Value), churn probability, basket composition, campaign lift
- Update frequency: transactions streaming (near-real-time), dimensions daily batch

**Q9. How do you ensure a pipeline can handle 10x traffic spike during Black Friday?**
- **Pre-scaling:** provision extra EMR/Databricks capacity 24h before event
- **Auto-scaling:** set min=20, max=200 workers for Black Friday window
- **Kafka over-partitioned:** partition count set for 10x peak (over-partitioning is harmless)
- **Circuit breaker:** if pipeline falls behind by > 2h, pause non-critical downstream pipelines, prioritise core transactions
- **Load testing:** run chaos tests with 10x synthetic data 2 weeks before event
- **Runbook:** documented manual scale-up steps for on-call team

**Q10. How would you architect a feature store for ML models?**
Components: (1) **Offline store** (Delta Lake): historical feature values for model training, computed by batch Spark jobs. (2) **Online store** (Redis/DynamoDB): real-time feature values for inference, <5ms lookup. (3) **Feature computation layer** (Spark/Flink): computes features, writes to both stores. (4) **Feature registry** (Feast/Tecton metadata): schema, lineage, owner, statistics. Consistency challenge: offline and online stores can drift — use a single feature definition that computes both, validated daily.

**Q11. What is a data product and how is it different from a raw dataset?**
A data product is a curated, documented, tested, and governed data asset with an owner and SLA. It's described in the data catalog, has quality scores, semantic documentation ("what does this mean?"), and a clear contract. A raw dataset is just files or tables with no guarantee of quality, documentation, or ownership. dunnhumby's value proposition is essentially turning retailers' raw data into data products.

**Q12. How do you design a pipeline for exactly-once processing in a distributed system?**
True exactly-once requires: idempotent producers (Kafka `enable.idempotence=true`), transactional consumers (commit offset atomically with write), idempotent sinks (Delta MERGE with natural key). Even with all three, you achieve "effectively exactly-once" — the system behaves as if each message was processed once, even if internal retries happened. Accept at-least-once + idempotent sinks as the practical approach for most use cases.

**Q13. What is eventual consistency and when is it acceptable in a data pipeline?**
Eventual consistency: the system will become consistent after some delay, but intermediate states may be inconsistent. Acceptable for: analytics aggregations (slight lag in totals is acceptable), recommendation systems (stale recommendations by minutes are fine), dashboards (hourly refresh is acceptable). NOT acceptable for: financial reporting (must be exact at cut-off time), real-time fraud decisions (must be current), regulatory submissions.

**Q14. Design an alerting system for a data platform with 300+ pipelines.**
- **Metadata-driven:** each pipeline registers expected SLA, row count range, freshness window in a config table
- **Automated monitoring:** a "monitor" Airflow DAG runs every 30 minutes, checks all pipelines against their SLA
- **Severity levels:** P1 (regulatory/revenue impact) → PagerDuty; P2 (analyst-facing) → Slack alert; P3 (informational) → email daily digest
- **Noise reduction:** exponential backoff for repeated alerts, alert deduplication window (don't alert twice in 30 minutes for same issue)
- **Runbook links:** every alert includes a link to the runbook for that pipeline

**Q15. How do you handle a situation where a source system is unavailable for 6 hours?**
1. Detect via sensor timeout + alert
2. Assess downstream impact: which pipelines depend on this source?
3. Execute fallback: use previous day's data as substitute (clearly marked as "prior day")
4. Communicate to stakeholders: dashboard shows stale indicator with explanation
5. On recovery: trigger incremental catch-up job for the 6-hour gap
6. Design for resilience: all pipelines should have "no data arrived" handling (use previous partition, use fallback file, produce zero-row output)

**Q16. What is a serving layer and how do you design it for multiple consumer types?**
The serving layer is the last mile of the data platform — optimised for specific consumption patterns:
- **BI/Dashboards:** pre-aggregated wide tables, Databricks SQL / Redshift, fast GROUP BY queries
- **Data science/ML:** feature store, Python notebooks, access to Silver layer for custom feature engineering
- **Applications/APIs:** low-latency key-value lookups, Redis / DynamoDB / Cosmos DB (pre-computed per-entity summaries)
- **Exports:** batch CSV/Parquet exports to S3 for client data delivery (dunnhumby's core use case)

Design: single Gold layer as source of truth, multiple specialised serving layers materialised from Gold for each consumer type.

**Q17. What is schema-on-write vs. schema-on-read and when do you use each?**
Already covered in Schema section — advanced angle: **for a new data source**, start with schema-on-read in Bronze (accept anything), profile the data for 2 weeks, then define a strict schema and enforce it from Silver onward. This iterative approach beats requiring a perfect schema before you've seen the data.

**Q18. How do you design for multi-region data replication?**
- **Read replicas:** asynchronous replication to second region (AWS S3 Cross-Region Replication or Azure Geo-Redundant Storage) — eventual consistency, 0 RPO is not possible
- **Active-active:** write to both regions simultaneously (requires conflict resolution) — very complex, only for top-tier SLAs
- **Active-passive failover:** primary region handles all writes; secondary is hot standby; failover via DNS in 5-10 minutes
- **Regulatory constraints:** some data (EU PII under GDPR) cannot cross certain borders — use geo-fencing at the storage layer

**Q19. What is the CAP theorem and how does it apply to data pipelines?**
CAP theorem: a distributed system can only guarantee two of: **C**onsistency (all nodes see the same data), **A**vailability (every request gets a response), **P**artition tolerance (system works despite network partition). Data pipelines: choose CP (Delta Lake — consistent, partition-tolerant, may be unavailable during partition) over AP. Kafka is AP (available, partition-tolerant) — some replication lag acceptable. Never trade consistency for availability in financial pipelines.

**Q20. Design a data pipeline for a machine learning feature store.**
Covered in Q10 above. Additional: (1) Point-in-time correctness — training features must be computed as they would have been available at the time of the label (no data leakage). (2) Feature monitoring — detect feature distribution drift that could degrade model performance. (3) Backfill — must be able to compute historical features for model training. (4) Feature sharing — multiple models should share the same feature definitions to avoid discrepancy.

**Q21. How do you design a data pipeline that satisfies regulatory data residency requirements?**
- Data classification at source: tag each field with region constraints (EU-ONLY, US-ONLY, GLOBAL)
- Storage: separate S3 buckets/ADLS containers in each region; IAM/RBAC prevent cross-region access
- Processing: Spark clusters deployed in the same region as the data they process
- Audit: CloudTrail/Azure Monitor logs record all data access; regulators can verify no cross-border flow
- Tooling: use a data catalog (Purview/Glue) to track data residency classification across all assets

**Q22. What is a data hub vs. data lake vs. data warehouse vs. data lakehouse?**
- **Data lake:** raw storage for all data in original format (S3 + Parquet). Cheap, flexible, poor query performance
- **Data warehouse:** structured, optimised for analytical queries (Redshift, Synapse). Fast queries, expensive, rigid schema
- **Data lakehouse:** combines lake (cheap storage) + warehouse (ACID, SQL performance) via table formats (Delta, Iceberg) — best of both worlds
- **Data hub:** integration hub for data exchange between systems (messaging/API layer), not primarily a storage layer

**Q23. How do you handle a pipeline that must process both batch and streaming data with the same logic?**
Apache Beam: write once, run on batch (Dataflow) or streaming (Flink/Dataflow). Or: use Spark Structured Streaming's `availableNow` trigger for "batch-like" streaming runs. Or: accept code duplication (Lambda architecture) but abstract shared logic into a library that both batch and streaming jobs import. The cleanest solution architecturally is Delta Lake + Structured Streaming for both: same code path, streaming in production, batch trigger for backfill.

**Q24. What is the write-ahead log (WAL) pattern and how is it used in data systems?**
WAL: before any data modification is applied, the change is first written to an append-only log. If the system crashes, the log is replayed to recover. Used in: Kafka (commit log is essentially a WAL), Delta Lake transaction log (_delta_log), PostgreSQL WAL, Flink checkpoints. The WAL provides durability and enables recovery without expensive two-phase commit across distributed nodes.

**Q25. How do you architect a pipeline for GDPR right-to-erasure (right to be forgotten)?**
Challenge: data may be in many places (S3 raw files, Parquet, cached). Solutions:
1. **Crypto-shredding (best approach):** encrypt PII with per-customer key; to "delete" the customer, delete their encryption key — data becomes unreadable without rewriting files
2. **Pointer-based architecture:** raw files contain only customer_id token; actual PII in a separate "vault"; deleting from vault erases PII everywhere
3. **Hard delete + VACUUM:** for Delta Lake, delete records, then VACUUM to purge old file versions — slow, requires complete partition rewrite
4. Track deletion requests with execution proof for regulatory audit

**Q26. What monitoring would you set up on day 1 of a new data platform?**
Non-negotiables: (1) Pipeline completion times vs. SLA, (2) Row count anomaly detection (> 20% deviation from rolling 7-day average), (3) Data freshness (max ingested_at vs. current time), (4) Infrastructure health (cluster node failures, storage capacity), (5) Error rate per pipeline, (6) Kafka consumer lag (for streaming pipelines), (7) DQ gate pass rates. Alert via Slack (P2) and PagerDuty (P1). Dashboard in Grafana/CloudWatch showing all pipelines' health at a glance.

**Q27. How do you cost-optimise a Spark cluster on AWS?**
- Spot instances for task nodes (60-80% savings, Spark retries on node loss)
- On-demand for master node (driver death = job failure)
- Right-size: use Ganglia/Spark UI to check actual CPU/memory utilisation; over-provisioned clusters are common
- Instance type: compute-optimised (c5) for CPU-bound, memory-optimised (r5) for joins/aggregations
- Auto-termination: EMR cluster terminates after job completion (idle clusters are expensive)
- Reserved instances for steady-state workloads (30% savings over on-demand)
- S3 as storage (not HDFS) — no persistent cluster needed between runs

**Q28. You're asked to reduce the data platform's AWS cost by 40% in 3 months. What do you do?**
Step 1: Cost visibility — break down by service, team, pipeline using AWS Cost Explorer tags. 
Step 2: Quick wins (week 1-2): delete unused clusters, enable auto-termination, move logs to S3 Intelligent-Tiering, delete orphaned EBS volumes.
Step 3: Right-sizing (week 3-6): review cluster sizing vs. utilisation, switch to spot instances for batch workloads.
Step 4: Architectural (month 2-3): S3 lifecycle policies for old data, consolidate rarely-used pipelines, replace always-on Redshift cluster with Redshift Serverless for variable workloads.
Step 5: Reserved instances for baseline workloads — commit to 1-year reserved for known steady-state compute.

**Q29. Design an API layer on top of a data lake.**
```
Consumer App
    → API Gateway (rate limiting, authentication, HTTPS)
    → Lambda / FastAPI (request handling)
    → Cache layer (Redis / ElastiCache): sub-ms for hot queries
    → If cache miss: DynamoDB / RDS (pre-materialised lookup tables)
    → If complex query: Databricks SQL / Athena (ad-hoc)
    → Data: Delta Lake / Redshift (source of truth)
```
Key design decisions: cache pre-computed results for high-frequency queries, separate OLAP queries (async, high latency acceptable) from OLTP-style lookups (sync, <100ms required), enforce contract between API schema and underlying data schema.

**Q30. You join a team where the data pipeline is a "big ball of mud" — no documentation, unreliable, feared by everyone. What is your 90-day plan?**
- **Days 1-30:** Understand and document. Map all pipelines, owners, downstream consumers, SLAs. Don't change anything. Identify the 3 most critical pipelines (those with most downstream impact).
- **Days 31-60:** Stabilise critical pipelines. Add monitoring + alerting to top 3 pipelines. Add DQ checks where none exist. Fix most common failure modes. Write runbooks for every incident type seen in the last 3 months.
- **Days 61-90:** Propose and begin systematic improvement. Introduce standards (testing requirements, code review, staging environment). Present a roadmap to leadership: what the current state costs in engineer time and business disruption. Gain alignment on a 6-month modernisation plan.
- **Throughout:** Build trust. Quick wins matter. Fix the things people complain about loudest first.

---

## 📌 Final Tips

> **Before every answer:** Take 5 seconds. Say "Let me make sure I understand the question..." and restate it. This earns points and prevents wasted effort.

> **For architecture questions:** Always say "Can I clarify the scale requirements?" — interviewers love this.

> **For coding questions:** Think out loud the whole time. Narrate your approach before typing. A slightly wrong answer with great reasoning beats a correct answer with zero explanation.

> **For "have you used X?" questions:** If no → "I haven't used X directly, but I have used Y which solves the same problem. Here's how I'd approach X..." Never just say no.

> **Technology choice questions:** Always give 3 things: what you chose, why you chose it, and what you'd choose if constraints were different.

---

*Prepared for: dunnhumby Senior Big Data Engineer Interview | 20-Day Prep Plan*
