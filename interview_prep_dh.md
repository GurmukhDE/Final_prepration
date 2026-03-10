# 🎯 dunnhumby — Senior Big Data Engineer: Complete Interview Prep
> **240+ curated Q&A with grilling follow-ups · 5 portfolio projects · GitHub-ready**

---

## 📋 Table of Contents
1. [My Projects — Tell These Stories](#-my-projects)
2. [Area 1: Python & PySpark](#-area-1-python--pyspark-30-questions)
3. [Area 2: Advanced SQL Joins](#-area-2-advanced-sql-joins-30-questions)
4. [Area 3: Cloud Experience](#-area-3-cloud-experience-30-questions)
5. [Area 4: End-to-End Pipeline Architecture & Ingestion](#-area-4-end-to-end-pipeline-architecture--ingestion-30-questions)
6. [Area 5: Schema Management & Data Quality](#-area-5-schema-management--data-quality-30-questions)
7. [Area 6: OOM & Data Skew](#-area-6-oom--data-skew-30-questions)
8. [Area 7: Architectural Problem-Solving Scenarios](#-area-7-architectural-problem-solving-scenarios-30-questions)
9. [Area 8: Hiring Manager — Projects Deep Dive & Tech Choices](#-area-8-hiring-manager--projects-deep-dive--technology-choices-30-questions)

---

# 🏗️ My Projects

> Use these as your project stories. Adapt names/numbers to fit your actual experience. Always follow **STAR-T**: Situation → Task → Action → Result → Technology choice rationale.

---

### Project 1 — Real-Time Transaction Anomaly Detection Pipeline
**Company:** Bank of America | **Scale:** 2M+ transactions/day | **Team:** Lead DE on team of 4

**Situation:** The fraud analytics team had a 4-hour latency between transaction occurrence and anomaly flagging. This was causing significant financial losses and regulatory exposure under OCC guidelines.

**Task:** Design and deliver a near-real-time ingestion and anomaly scoring pipeline with sub-5-minute end-to-end latency.

**Action:**
- Designed a Kafka-based ingestion layer consuming CDC events from Oracle transactional systems using Debezium connectors
- Built PySpark Structured Streaming jobs on Databricks with 1-minute micro-batch triggers, applying feature engineering (velocity checks, merchant category rolling aggregates using window functions)
- Implemented a watermarking strategy (`withWatermark('event_time', '2 minutes')`) to handle late-arriving events from international branches
- Wrote dead letter queue (DLQ) logic routing malformed records to ADLS Bronze quarantine with schema violation metadata
- Achieved exactly-once semantics end-to-end using Kafka idempotent producers + Databricks Delta Lake ACID writes
- Built Airflow DAGs to orchestrate daily model retraining with ExternalTaskSensor dependencies

**Result:** End-to-end latency reduced from 4 hours to under 3 minutes. Flagged $2.3M in additional fraud in Q1 alone. Pipeline passed OCC data lineage audit with zero findings.

**Technology Choices:** Kafka over Kinesis (existing infra, stronger replay guarantees), Databricks over self-managed Spark (faster delivery, Unity Catalog for compliance), Delta Lake over Parquet (ACID + schema evolution), Airflow over custom schedulers (team familiarity, rich ecosystem).

---

### Project 2 — Enterprise Data Lake Migration: Hadoop → Azure Cloud
**Company:** Bank of America | **Scale:** 8 PB data, 400+ Spark jobs | **Team:** 2 senior DEs + 6 engineers

**Situation:** Legacy Hadoop/Hive/Oozie cluster was end-of-life. Licensing costs were $4M/year. Jobs were taking 6–8 hours and failing intermittently due to hardware issues.

**Task:** Migrate all 400+ batch pipelines to Azure (ADLS Gen2 + Databricks), with zero data loss and no disruption to 150+ downstream consumers.

**Action:**
- Led technical assessment: catalogued all Hive tables, Oozie workflows, HBase lookups, and MapReduce jobs using Apache Atlas for lineage mapping
- Designed a phased migration: (1) lift-and-shift Spark jobs to Databricks, (2) replace Oozie with Airflow, (3) convert Hive DDLs to Delta format, (4) migrate HBase to Azure Cosmos DB for low-latency lookups
- Built automated Hive-to-Delta conversion scripts in Python + Great Expectations validation suites to verify row counts, null rates, and aggregate checksums post-migration
- Implemented shadow mode: ran old and new pipelines in parallel for 3 weeks, comparing output with reconciliation DAGs
- Wrote runbooks and data contracts for all 150 downstream consumers

**Result:** Full migration completed in 6 months (target was 9). Infrastructure cost reduced by 58% ($2.3M/year saved). Average job runtime improved from 6.5 hours to 45 minutes due to Databricks AQE + Delta Z-ordering.

**Technology Choices:** Azure over GCP (existing enterprise agreement), Databricks over Azure Synapse Spark (better Delta Lake support, Unity Catalog maturity), Airflow over ADF (code-first pipelines, better testing, existing team expertise).

---

### Project 3 — Customer 360 Data Platform (Master Data Management)
**Company:** Bank of America | **Scale:** 80M customer records | **Team:** Cross-functional, 3 DE + 2 DS + 1 data steward

**Situation:** Customer data was siloed across 12 source systems (CRM, card, mortgage, investments). Analysts were joining these manually with inconsistent results. Regulatory reporting (CCAR, DFAST) required a single trusted view.

**Task:** Build a unified Customer 360 medallion architecture with a gold-layer master customer entity.

**Action:**
- Designed Bronze/Silver/Gold layers on ADLS Gen2 with Delta Lake
- Bronze: raw ingestion from 12 sources via ADF pipelines, schema-on-read, partitioned by source and ingest_date
- Silver: PySpark cleansing, standardisation (phone/address normalisation), deduplication using probabilistic entity resolution (Splink library), SCD Type 2 history tracking
- Gold: conformed customer dimension + fact tables for spending, credit, product holdings; dbt models for transformation with data contracts
- Implemented column-level encryption for PII (SSN, DOB) using Azure Key Vault-backed secrets in Databricks
- Built Great Expectations checkpoint suite with 180+ expectations, publishing HTML data docs to internal wiki

**Result:** Reduced time-to-insight for regulatory reporting from 3 days to 4 hours. Zero compliance findings in CCAR 2023 data review. 15 downstream teams adopted the platform within 6 months.

**Technology Choices:** dbt over pure PySpark for Gold layer transforms (version control, testing, documentation in one tool), Great Expectations over custom checks (extensible, data docs for stakeholders), Splink over rule-based deduplication (probabilistic matching handles messy data better at 80M scale).

---

### Project 4 — Self-Serve Analytics Platform with Data Governance
**Company:** Bank of America | **Scale:** 500+ analysts | **Team:** Platform team of 5 DEs

**Situation:** Data analysts were waiting 2–3 weeks for data access and had no visibility into data lineage or quality. Shadow IT was rampant — analysts were downloading data to local Excel files.

**Task:** Build a governed self-serve data platform that analysts can access safely, with full lineage and quality visibility.

**Action:**
- Deployed OpenMetadata as the enterprise data catalog, auto-ingesting metadata from Databricks, Azure Synapse, and Airflow
- Built a data product framework: each domain team owns their Delta tables as "data products" with defined SLAs, owners, and quality SLOs (data mesh principles)
- Implemented row-level security and column masking in Databricks Unity Catalog — analysts see masked PII by default, privileged access via approval workflow
- Created dbt project structure with schema.yml data contracts, auto-generating data docs published to internal wiki
- Built Airflow DAG that runs Great Expectations suite on every pipeline run and posts quality score to OpenMetadata

**Result:** Data access provisioning reduced from 14 days to same-day for 80% of requests. Shadow IT incidents dropped 70%. Platform NPS from analysts went from 23 to 71.

**Technology Choices:** OpenMetadata over Collibra (open-source, easier integration, lower cost), Unity Catalog over Apache Ranger (native Databricks integration, column-level lineage), data mesh pattern over central ownership (scales better with 12 domain teams).

---

### Project 5 — High-Availability Regulatory Reporting Pipeline (SLA-Critical)
**Company:** Bank of America | **Scale:** 500GB daily, feeds Fed reporting | **Team:** Solo design, 2 engineers to implement

**Situation:** The daily regulatory data submission pipeline had a 98.1% SLA but was missing SLA 3–4 times per month due to upstream data delays, causing regulatory risk.

**Task:** Redesign the pipeline to achieve 99.9% SLA compliance with graceful degradation.

**Action:**
- Audited all failure modes: upstream file delays, schema changes from sources, transient ADLS write failures, Airflow worker restarts
- Implemented an idempotency framework: every pipeline step writes to a staging partition with a job_id, final atomic rename only on success — re-running is always safe
- Built a smart sensor in Airflow: FileSensor in `reschedule` mode that checks file completeness (row count vs. manifest) before proceeding, with escalating alerts at 15/30/45 minutes
- Added a graceful degradation fallback: if upstream data is >1 hour late, pipeline automatically uses previous day's snapshot with a "STALE_DATA" flag, notifying downstream consumers
- Implemented circuit breaker for downstream API calls using tenacity library
- Built a Grafana dashboard reading Airflow metadata DB for real-time SLA tracking

**Result:** SLA compliance improved from 98.1% to 99.94% over 6 months. Zero missed regulatory submissions. Reduced on-call incidents by 80%.

**Technology Choices:** Airflow reschedule-mode sensors over poke mode (no wasted worker threads), tenacity over custom retry logic (handles jitter, exponential backoff, configurable), graceful degradation over hard-fail (regulatory reporting can't simply stop — a stale but flagged report is better than no report).

---

# 🐍 Area 1: Python & PySpark (30 Questions)

---

### Q1. What is the difference between a generator and a list in Python? Why does it matter for data engineering?

**Answer:**
A list stores all elements in memory at once. A generator yields elements one at a time, computing them lazily — only when `next()` is called. For data engineering, this is critical when processing files or database results that are too large to fit in memory.

```python
# BAD for large data — loads everything into RAM
rows = [process(row) for row in read_csv("huge_file.csv")]

# GOOD — processes one row at a time, constant memory
def row_generator(filepath):
    with open(filepath) as f:
        for line in f:
            yield process(line)

for row in row_generator("huge_file.csv"):
    write_to_db(row)
```

Generators are also composable — you can chain them into a pipeline:
```python
raw = read_file(path)
filtered = (r for r in raw if r['amount'] > 0)
enriched = (add_category(r) for r in filtered)
```
Each step is lazy — no data is materialised until you consume `enriched`.

**→ Follow-up: "Can a generator be iterated twice?"**
No. Once exhausted, a generator is empty. If you need to iterate multiple times, convert to a list (accepting memory cost) or recreate the generator.

**→ Follow-up: "What's the difference between `yield` and `yield from`?"**
`yield from` delegates to a sub-generator, flattening it. Useful for recursive generators (e.g., traversing a nested directory structure):
```python
def walk_partitions(path):
    for sub in list_dirs(path):
        yield from walk_partitions(sub)  # recursive delegation
    yield path
```

**→ Follow-up: "When would you NOT use a generator in a pipeline?"**
When you need random access (e.g., `rows[500]`), when you need the length upfront, or when the pipeline step needs to see all data to make decisions (e.g., computing global statistics before filtering).

---

### Q2. Explain Python's GIL. How does it affect data engineering workloads?

**Answer:**
The Global Interpreter Lock (GIL) is a mutex in CPython that allows only one thread to execute Python bytecode at a time. This means Python threads cannot run in true parallel on multi-core CPUs for CPU-bound tasks.

**Impact on data engineering:**
- **I/O-bound tasks** (reading files, API calls, DB queries): GIL is released during I/O waits — `threading` works fine here. Use `ThreadPoolExecutor` for parallel API calls or file reads.
- **CPU-bound tasks** (data transformation, parsing): GIL prevents parallelism — use `multiprocessing` or `ProcessPoolExecutor` instead, which spawns separate processes each with their own GIL.
- **Why PySpark is unaffected:** PySpark runs the actual computation in JVM (Java/Scala). Python is only the driver/glue layer. The GIL only affects the Python process — not the Spark executors.

```python
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# I/O-bound: use threads (GIL released during I/O)
with ThreadPoolExecutor(max_workers=20) as ex:
    results = list(ex.map(fetch_api, urls))

# CPU-bound: use processes (bypass GIL entirely)
with ProcessPoolExecutor(max_workers=8) as ex:
    results = list(ex.map(parse_heavy_json, raw_records))
```

**→ Follow-up: "What about async/await — does that bypass the GIL?"**
No — `asyncio` is single-threaded cooperative multitasking. It handles I/O-bound concurrency without threads at all, using an event loop. It's not affected by the GIL, but also doesn't bypass it. It's most useful for high-concurrency I/O (e.g., 1000 simultaneous API calls) where thread overhead would be too high.

**→ Follow-up: "PySpark UDFs run in Python — does the GIL matter?"**
Yes, for standard UDFs. Each executor serialises data through Python (via Py4J), and the Python process handling UDFs is subject to the GIL. This is one key reason why Pandas UDFs (vectorised, Arrow-based) are dramatically faster — they process a batch of rows in NumPy/pandas operations that release the GIL.

---

### Q3. What are Python decorators? Give a data engineering example.

**Answer:**
A decorator is a function that wraps another function to add behaviour before or after it runs, without modifying the original function. Implemented via the `@` syntax.

```python
import time
import logging
from functools import wraps

def retry(max_attempts=3, delay=2, backoff=2):
    """Retry decorator with exponential backoff for flaky pipeline steps."""
    def decorator(func):
        @wraps(func)  # preserves original function's __name__, __doc__
        def wrapper(*args, **kwargs):
            attempt = 0
            wait = delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt == max_attempts:
                        logging.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    logging.warning(f"Attempt {attempt} failed. Retrying in {wait}s...")
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        logging.info(f"{func.__name__} took {time.time() - start:.2f}s")
        return result
    return wrapper

@retry(max_attempts=5, delay=1)
@timer
def load_to_database(records):
    # flaky DB write
    db.bulk_insert(records)
```

**→ Follow-up: "Why does `@wraps(func)` matter?"**
Without it, the wrapper replaces the original function's metadata. `func.__name__` would return `"wrapper"` instead of `"load_to_database"`, breaking logging, debugging, and any introspection tools.

**→ Follow-up: "How would you stack multiple decorators and what is the execution order?"**
Decorators apply bottom-up (closest to the function first), but execute top-down. For `@retry @timer def f()`: `f` is first wrapped by `timer`, then that wrapped function is wrapped by `retry`. When called: `retry` runs first, then calls `timer`'s wrapper, which then calls the original `f`.

---

### Q4. Explain PySpark's execution model — what happens when you call `.show()` on a DataFrame?

**Answer:**
PySpark uses **lazy evaluation**. Transformations (`.filter()`, `.groupBy()`, `.join()`) don't execute immediately — they build a DAG (Directed Acyclic Graph) of logical operations. Only **actions** (`.show()`, `.count()`, `.write()`, `.collect()`) trigger actual execution.

When you call `.show()`:
1. **Logical Plan** is created from all the chained transformations
2. **Catalyst Optimizer** rewrites the logical plan — pushes down filters, eliminates unnecessary columns, reorders joins
3. **Physical Plan** is generated — chooses join strategies (broadcast, sort-merge, shuffle hash), determines scan methods
4. **Code generation** via Tungsten — generates bytecode for JVM execution
5. **DAG Scheduler** splits the physical plan into **stages** at shuffle boundaries
6. **Task Scheduler** assigns **tasks** (one per partition) to available **executors**
7. Results are collected to the driver and displayed

```python
df = spark.read.parquet("s3://data/transactions/")  # Nothing runs yet
df2 = df.filter(df.amount > 100)                     # Nothing runs yet
df3 = df2.groupBy("store_id").sum("amount")          # Nothing runs yet
df3.show()  # ← THIS triggers the entire DAG execution
```

**→ Follow-up: "What's the difference between a narrow and wide transformation?"**
Narrow: each output partition depends on only one input partition — no data movement. Examples: `filter`, `select`, `map`, `withColumn`. Wide: output partition depends on multiple input partitions — requires a **shuffle**. Examples: `groupBy`, `join`, `orderBy`, `distinct`. Shuffles are expensive (disk + network I/O) and create stage boundaries.

**→ Follow-up: "If I call `.cache()` and then `.show()` twice, does it execute twice?"**
The first `.show()` executes the DAG AND caches the result. The second `.show()` reads from cache — no recomputation. But `.cache()` is lazy too — the actual caching only happens on the first action after `.cache()` is called.

**→ Follow-up: "When should you avoid `.collect()`?"**
Always avoid it for large datasets — it brings ALL data to the driver, causing OOM. Use `.show(n)`, `.take(n)`, or `.write()` instead. Only use `.collect()` on DataFrames you know are small (e.g., after an aggregation that produces a few hundred rows).

---

### Q5. What is the Catalyst Optimizer? Give concrete examples of what it does.

**Answer:**
Catalyst is Spark SQL's query optimizer — a rule-based and cost-based optimizer that rewrites your logical query plan into a more efficient physical plan automatically.

**Key optimizations:**
1. **Predicate pushdown:** Moves `.filter()` as close to the data source as possible — for Parquet files, it avoids reading entire files if partition/column statistics rule out matching rows.
```python
# You write:
df.join(other, "id").filter(df.country == "US")
# Catalyst rewrites to filter BEFORE the join — dramatically reduces data shuffled
```

2. **Column pruning:** Only reads columns you actually use from Parquet files — never scans columns you don't need.

3. **Join reordering:** In multi-table joins, reorders to start with the smallest table first (minimises intermediate data size).

4. **Constant folding:** Evaluates constant expressions at compile time — `filter(lit(2) * lit(3) > 5)` becomes `filter(True)` and is eliminated.

5. **Null propagation:** Simplifies null-handling logic.

To see what Catalyst produces: `df.explain(True)` — shows Parsed Logical Plan → Analyzed → Optimized → Physical Plan.

**→ Follow-up: "Can you hint the optimizer when it makes a wrong decision?"**
Yes:
```python
from pyspark.sql.functions import broadcast
df.join(broadcast(small_df), "key")  # force broadcast join
df.hint("merge")   # force sort-merge join
df.hint("shuffle_hash")  # force shuffle hash join
df.repartition(200, "key")  # manually control partitioning before join
```

**→ Follow-up: "What is AQE (Adaptive Query Execution) and how does it differ from Catalyst?"**
Catalyst optimizes at **compile time** — before execution starts, based on statistics. AQE optimizes at **runtime** — it collects actual shuffle statistics mid-execution and changes the plan dynamically. AQE can: (1) coalesce shuffle partitions after seeing actual data sizes, (2) switch join strategies if a broadcast side is smaller than expected, (3) split skewed partitions. AQE is enabled by default since Spark 3.2.

---

### Q6. What is the difference between `repartition()` and `coalesce()`?

**Answer:**
Both change the number of partitions, but differently:

**`repartition(n)`:**
- Performs a **full shuffle** — redistributes all data across n new partitions
- Can increase OR decrease partition count
- Produces evenly distributed partitions (round-robin or hash-based)
- Expensive — use when you need more partitions OR when current partitions are badly skewed

**`coalesce(n)`:**
- **No shuffle** — combines existing partitions locally (narrow transformation)
- Can ONLY decrease partition count
- May produce uneven partitions (some will be larger)
- Cheap — use when reducing partitions after filtering has left many empty/small ones

```python
# After heavy filtering, many small partitions remain
df_filtered = df.filter(df.status == "ACTIVE")  # maybe only 5% of data
# DON'T: df_filtered.repartition(10) -- shuffles everything unnecessarily
# DO:
df_filtered.coalesce(10).write.parquet("output/")  # cheap, no shuffle

# Before a join, you want even distribution
df.repartition(200, "customer_id").join(other, "customer_id")  # balanced
```

**→ Follow-up: "What's the default number of shuffle partitions and why is it a problem?"**
Default is `spark.sql.shuffle.partitions = 200`. For a 10 GB dataset this might be fine (50 MB per partition), but for a 10 TB dataset you get 50 GB partitions causing OOM. For a 100 MB dataset you get 0.5 MB partitions causing task scheduling overhead. Rule of thumb: target 128–256 MB per partition. With AQE enabled, Spark can auto-coalesce small shuffle partitions — but you should still set a reasonable starting point.

---

### Q7. How do window functions work in PySpark? Write a rolling 7-day average example.

**Answer:**
Window functions perform calculations across a set of rows related to the current row — defined by a `Window` spec with partition and ordering.

```python
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, sum as spark_sum, rank, lag, lead

# Rolling 7-day average spend per customer
# IMPORTANT: rangeBetween uses the actual value difference, not row count
window_7d = (Window
    .partitionBy("customer_id")
    .orderBy(col("purchase_date").cast("long"))  # must cast date to numeric for rangeBetween
    .rangeBetween(-6 * 86400, 0))  # 6 days ago to current (in seconds)

df = df.withColumn("avg_7d_spend", avg("amount").over(window_7d))

# vs rowsBetween — counts rows not values
window_3rows = (Window
    .partitionBy("customer_id")
    .orderBy("purchase_date")
    .rowsBetween(-2, 0))  # current row + 2 preceding

# Ranking within group
window_rank = Window.partitionBy("category").orderBy(col("revenue").desc())
df = df.withColumn("rank_in_category", rank().over(window_rank))

# Lag/Lead — compare with previous/next row
window_ordered = Window.partitionBy("customer_id").orderBy("purchase_date")
df = df.withColumn("prev_purchase_amount", lag("amount", 1).over(window_ordered))
df = df.withColumn("days_since_last", 
    col("purchase_date").cast("long") - lag(col("purchase_date").cast("long"), 1).over(window_ordered))
```

**→ Follow-up: "What is the difference between `rank()`, `dense_rank()`, and `row_number()`?"**
- `row_number()`: Assigns unique sequential integers — no ties, always 1,2,3,4
- `rank()`: Ties get the same rank, next rank skips — 1,1,3,4 (gap after tie)
- `dense_rank()`: Ties get the same rank, no skipping — 1,1,2,3 (no gap)

**→ Follow-up: "Window functions cause shuffles — how do you control this?"**
Each unique combination of `partitionBy` keys ends up in the same partition. If you have many distinct customers, it triggers a shuffle. To control it: (1) ensure the data is already partitioned by the window partition key before the window op, (2) combine multiple window operations that use the same partitionBy/orderBy into a single pass rather than multiple `.withColumn` calls.

---

### Q8. What is a Pandas UDF and when should you use it over a standard UDF?

**Answer:**
A standard UDF in Spark processes **one row at a time** — Spark serialises each row to Python via Py4J, processes it, and serialises back. This has extreme overhead.

A **Pandas UDF** (vectorised UDF) processes a **batch of rows** as a pandas Series or DataFrame using Apache Arrow for zero-copy serialisation. Typically 10–100x faster than standard UDFs.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Standard UDF — avoid in production hot paths
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def categorise_amount_slow(amount):
    if amount > 1000: return 3.0
    elif amount > 100: return 2.0
    return 1.0

# Pandas UDF — vectorised, 10-100x faster
@pandas_udf(DoubleType())
def categorise_amount_fast(amounts: pd.Series) -> pd.Series:
    return pd.cut(amounts, bins=[0, 100, 1000, float('inf')], labels=[1.0, 2.0, 3.0])

# GROUPED_MAP — transform a whole group as a DataFrame
from pyspark.sql.functions import PandasUDFType
@pandas_udf("customer_id string, amount double, normalised double", PandasUDFType.GROUPED_MAP)
def normalise_within_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['normalised'] = (pdf['amount'] - pdf['amount'].mean()) / pdf['amount'].std()
    return pdf

df.groupby("customer_id").apply(normalise_within_group)
```

**Use standard UDF only when:** the logic cannot be expressed as vectorised operations (e.g., calling an external library that works row-by-row with no batch API).

**→ Follow-up: "When is even a Pandas UDF the wrong choice?"**
When a native Spark function exists. `F.when()`, `F.regexp_extract()`, `F.date_diff()`, window functions — all run in JVM with no Python serialisation at all. Always check if a native function exists first. UDFs — even Pandas UDFs — should be a last resort.

---

### Q9. Explain PySpark Structured Streaming — what are watermarks and output modes?

**Answer:**
Structured Streaming treats a live data stream as an unbounded table that grows continuously. You write the same DataFrame API as for batch — Spark handles the incremental execution.

**Watermarks** define how long Spark waits for late-arriving data. Events with timestamps older than `current_max_event_time - watermark_delay` are discarded from state.

```python
from pyspark.sql.functions import window, sum as spark_sum

# Read from Kafka
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "transactions")
    .load())

parsed = stream.select(
    F.from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Define watermark for late data tolerance
windowed = (parsed
    .withWatermark("event_time", "10 minutes")  # drop events >10min late
    .groupBy(
        window(col("event_time"), "5 minutes"),  # 5-min tumbling window
        col("store_id")
    )
    .agg(spark_sum("amount").alias("total_sales")))

# Write to Delta with checkpointing
query = (windowed.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/sales_stream")
    .outputMode("append")  # only new complete windows
    .trigger(processingTime="1 minute")
    .start("/delta/sales_stream"))
```

**Output modes:**
- `append`: Only new rows are written. For aggregations, only works with watermarks — rows only append when the window is finalised.
- `complete`: Entire result table is rewritten every trigger. Works without watermarks but can be expensive for large aggregations.
- `update`: Only rows that changed are written. More efficient than complete but not supported by all sinks.

**→ Follow-up: "What happens to an event that arrives after the watermark expires?"**
It is silently dropped. The state for its window has already been cleared. This is a deliberate trade-off — unbounded state would cause OOM. You must choose a watermark that balances completeness vs. memory.

**→ Follow-up: "What is the difference between micro-batch and continuous processing triggers?"**
`processingTime("1 minute")` = micro-batch, runs every 1 min. Latency = trigger interval + processing time. `continuous("1 second")` = truly record-by-record with 1-second checkpoint intervals, much lower latency (~ms) but limited operators. `AvailableNow` = process all available data in one shot (for scheduled streaming jobs).

---

### Q10. How does Delta Lake provide ACID transactions on top of a data lake?

**Answer:**
Delta Lake achieves ACID via a **transaction log** (`_delta_log/` directory containing JSON files). Every write operation appends a new entry to this log atomically. The transaction log is the source of truth — not the data files themselves.

**How each ACID property works:**
- **Atomicity:** A write either fully completes (new log entry committed) or is abandoned (temp files cleaned up). No partial writes are visible.
- **Consistency:** Schema enforcement at write time — if you try to write a DataFrame with wrong schema, it fails before any data is written.
- **Isolation:** Optimistic concurrency — multiple writers check for conflicts at commit time. If two writers modify the same partition, the second gets a `ConcurrentModificationException` and must retry.
- **Durability:** Transaction log + data files on durable object storage (ADLS/S3/GCS).

```python
from delta.tables import DeltaTable

# Upsert (MERGE) — key Delta operation
delta_table = DeltaTable.forPath(spark, "/delta/customers")
delta_table.alias("target").merge(
    new_data.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(set={
    "name": "source.name",
    "updated_at": "source.updated_at"
}).whenNotMatchedInsert(values={
    "customer_id": "source.customer_id",
    "name": "source.name",
    "updated_at": "source.updated_at"
}).execute()

# Time travel
df_yesterday = spark.read.format("delta").option("versionAsOf", 5).load("/delta/customers")
df_at_time = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("/delta/customers")
```

**→ Follow-up: "What is schema evolution in Delta Lake and how do you enable it?"**
By default Delta enforces schema — writes with new columns fail. `mergeSchema=True` allows adding new columns. `overwriteSchema=True` replaces schema entirely (destructive). In Spark config: `spark.databricks.delta.schema.autoMerge.enabled = true` for session-level auto-merge.

**→ Follow-up: "How does Delta handle concurrent writes and what happens when there's a conflict?"**
Delta uses optimistic concurrency: both writers read the current version, write their data files, then attempt to commit. The second committer checks if the first writer modified the same data — if yes, conflict exception; if they modified different partitions, both commits succeed. For streaming + batch concurrent writes, Delta handles this automatically.

---

### Q11. How do you handle schema inference vs. explicit schema definition in PySpark?

**Answer:**
**Schema inference** (`inferSchema=True`) scans a sample of the data to guess types — convenient but slow and unreliable for production:
- Requires a full scan pass just to determine types
- Type guessing can be wrong (e.g., "01" inferred as String not Int)
- Schema can vary run-to-run if sample changes

**Explicit schema** is always preferred in production:
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("customer_id", LongType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("transaction_time", TimestampType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),
])

df = spark.read.schema(schema).json("s3://raw/transactions/")

# Bad records handling — don't silently drop bad rows
df = spark.read.schema(schema).option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("s3://raw/transactions/")

# Separate good and bad records
good = df.filter(col("_corrupt_record").isNull())
bad = df.filter(col("_corrupt_record").isNotNull())
bad.write.mode("append").json("s3://quarantine/transactions/")
```

**→ Follow-up: "What are the three parse modes in Spark and when would you use each?"**
- `PERMISSIVE` (default): Puts malformed records into `_corrupt_record` column, null for other fields. Use when you want to keep processing and quarantine bad records.
- `DROPMALFORMED`: Silently drops bad records. Never use in production — you lose data without knowing.
- `FAILFAST`: Throws exception on first bad record. Use in strict ingestion where any bad record is unacceptable (e.g., financial reconciliation).

---

### Q12. Explain PySpark join strategies — when does Spark choose each?

**Answer:**
Spark has four join strategies:

**1. Broadcast Hash Join (BHJ):**
- When one side is smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
- Broadcasts the small table to ALL executors — no shuffle needed
- Fastest join type when applicable
- Force it: `df.join(broadcast(small_df), "key")`

**2. Sort-Merge Join (SMJ):**
- Default for large-large joins
- Both sides are sorted by join key, then merged linearly
- Requires a shuffle on both sides (expensive) but scales to any size
- Produces results in sorted order (useful downstream)

**3. Shuffle Hash Join:**
- One side is hashed into a hash table in memory
- Other side streams through and probes the table
- Works when one side fits in executor memory but is too big to broadcast
- Can cause OOM if the hash side is underestimated

**4. Cartesian/Broadcast Nested Loop Join:**
- For non-equi joins (no equality condition) or cross joins
- Very expensive — O(n*m) complexity

```python
# Check which join Spark chose:
df.join(other, "key").explain()

# Force strategies:
from pyspark.sql.functions import broadcast
df.join(broadcast(small_df), "key")        # broadcast
df.hint("merge").join(other, "key")        # sort-merge
df.hint("shuffle_hash").join(other, "key") # shuffle hash
```

**→ Follow-up: "When does a broadcast join hurt rather than help?"**
When the "small" table is actually larger than you think (stale statistics), the broadcast overwhelms executor memory. Also, broadcasting to 1000 executors takes time — for very wide clusters, the broadcast serialization/deserialization time can exceed the shuffle savings. Monitor with Spark UI's "Broadcast Exchange" stage.

---

### Q13. How do you optimise a slow PySpark job? Walk through your debugging process.

**Answer:**
My systematic approach using Spark UI:

**Step 1 — Identify the bottleneck:**
- Open Spark UI → Jobs tab → find the slow job
- Click into the job → find the slowest stage (longest duration bar)
- Click the stage → look at task distribution (are all tasks similar duration, or are some outliers?)

**Step 2 — Diagnose:**
- **All tasks slow uniformly** → data reading is slow (too few partitions, small files issue, full scan when predicate pushdown could help)
- **Few tasks are 10-100x slower** → data skew
- **GC time > 10% of task time** → memory pressure, too large rows per partition
- **Shuffle read/write is huge** → too many wide transformations, unnecessary joins/groupBys
- **Stage has 200 tasks but data is 10GB** → shuffle.partitions default too high, small task overhead

**Step 3 — Fix:**
```python
# Problem: reading whole table when only need recent data
# Fix: partition pruning — must filter on the partition column
df = spark.read.parquet("s3://data/transactions/") \
    .filter(col("year") == 2024).filter(col("month") == 1)  # pruned!

# Problem: 200 tiny shuffle partitions for a 100MB result
# Fix: AQE auto-coalesces, or manually
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Problem: Cartesian join by accident (missing join key)
# Always verify join type in .explain()

# Problem: reading same large DataFrame multiple times
df.cache()  # persist in memory after first action
df.count()  # materialize cache
# ... use df multiple times
df.unpersist()  # release when done
```

**→ Follow-up: "A job with 50 stages runs for 8 hours. How do you find which stages to focus on?"**
Open the Stages tab sorted by Duration descending. The top 3-5 stages by duration are your targets — in most pipelines, 80% of time is in 20% of stages. Look for shuffle-heavy stages (large "Shuffle Write" column) and stages with few tasks that could be parallelized more.

---

### Q14. What is checkpointing in Spark? When should you use it?

**Answer:**
Checkpointing saves the current state of an RDD/DataFrame to reliable storage (HDFS/ADLS), truncating the lineage graph. 

**Two types:**
1. **Reliable checkpointing** (`df.checkpoint()`): Materialises to configured checkpoint dir. Lineage is cut — useful for iterative algorithms (ML, graph processing) where lineage grows unboundedly.
2. **Local checkpointing** (`df.localCheckpoint()`): Saves to executor local disk. Faster but not fault-tolerant — if executor dies, data is lost.

**In Structured Streaming:** Checkpointing is mandatory — stores offset progress and state so the stream can resume exactly where it left off after failure.

```python
# Iterative algorithm — without checkpoint, lineage grows O(n) deep
sc.setCheckpointDir("hdfs://checkpoints/")
for i in range(100):
    rdd = rdd.map(update_step)
    if i % 10 == 0:
        rdd = rdd.checkpoint()  # truncate lineage every 10 iterations

# Streaming checkpoint
query = df.writeStream \
    .option("checkpointLocation", "/checkpoints/my_stream") \
    .start()
```

**→ Follow-up: "What's the performance cost of checkpointing?"**
A checkpoint triggers an action (full materialisation) + a write to distributed storage. For a 1TB dataset this could take several minutes. Use sparingly — only when lineage-related slowdowns or stack overflows occur, or for streaming (mandatory).

---

### Q15. How do you implement an idempotent PySpark pipeline?

**Answer:**
An idempotent pipeline produces the same result regardless of how many times it runs for the same input parameters (same date, same batch ID).

```python
def run_pipeline(processing_date: str):
    # Step 1: Read source with date filter
    raw = spark.read.parquet("s3://raw/transactions/") \
        .filter(col("processing_date") == processing_date)
    
    # Step 2: Transform
    result = transform(raw)
    
    # Step 3: Write — idempotent via partition overwrite
    # Option A: Dynamic partition overwrite (safest for partitioned tables)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    result.write.mode("overwrite") \
        .partitionBy("processing_date") \
        .parquet("s3://curated/transactions/")
    # Only overwrites the specific processing_date partition, not entire table
    
    # Option B: Delta Lake MERGE (for non-partitioned tables)
    DeltaTable.forPath(spark, "/delta/transactions") \
        .alias("t").merge(
            result.alias("s"),
            f"t.transaction_id = s.transaction_id AND t.processing_date = '{processing_date}'"
        ).whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
```

Key principle: **delete-then-insert** or **upsert** rather than append-only — because re-running would create duplicates.

**→ Follow-up: "What's the danger of `mode('overwrite')` without dynamic partition overwrite?"**
It drops and recreates the ENTIRE table, not just the relevant partition. If you re-run for `2024-01-15`, you'd delete all other dates' data. Always use dynamic partition overwrite mode or Delta merge for partition-specific idempotency.

---

### Q16–Q30: Quick-Fire with Detailed Answers

---

### Q16. What is the difference between `persist()` and `cache()` in Spark?

`cache()` = `persist(StorageLevel.MEMORY_AND_DISK)` — stores in memory first, spills to disk if needed. `persist()` lets you choose: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `OFF_HEAP`. Use `OFF_HEAP` to reduce JVM GC pressure for large cached DataFrames. Use `DISK_ONLY` for infrequently re-used DataFrames where memory is scarce.

**→ Follow-up:** "When does caching hurt?" When a DataFrame is used only once — caching adds write overhead with no read benefit. Also when cached data no longer fits in memory and causes excessive spilling, which can be slower than recomputing.

---

### Q17. How do you handle NULL values correctly in PySpark joins?

NULLs in join keys are treated as non-matching — `NULL != NULL` in SQL semantics. This can silently drop rows.

```python
# Check for nulls in join key before joining
print(df.filter(col("customer_id").isNull()).count())

# Options:
# 1. Fill nulls before join (if business logic allows)
df = df.fillna({"customer_id": "UNKNOWN"})
# 2. Use eqNullSafe (NULL == NULL returns True)
df.join(other, df["key"].eqNullSafe(other["key"]))
# 3. Explicitly handle nulls post-join
result = df.join(other, "customer_id", "left") \
    .withColumn("customer_name", F.coalesce(col("name"), F.lit("UNKNOWN")))
```

**→ Follow-up:** "If you have a large number of NULL join keys and they're causing skew, how do you handle that?" Filter them out before the join (if NULLs can be excluded), or route NULL-keyed records to a separate path and union the results.

---

### Q18. What is broadcast variable and accumulator in Spark?

**Broadcast variable:** Efficiently distributes a read-only value to all executors once (not per task). Used for lookup tables.
```python
lookup_dict = {"US": "United States", "IN": "India"}
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)

def map_country(code):
    return broadcast_lookup.value.get(code, "Unknown")
map_country_udf = udf(map_country, StringType())
```

**Accumulator:** A distributed counter/sum that tasks can add to — driver reads the final value. Used for metrics/debugging.
```python
bad_records = spark.sparkContext.accumulator(0)
def process(row):
    if row.amount < 0:
        bad_records.add(1)
    return row
df.foreach(process)
print(f"Bad records: {bad_records.value}")
```

**→ Follow-up:** "What's the pitfall of accumulators?" In case of task retries (Spark retries failed tasks), an accumulator can be incremented multiple times for the same logical task — causing overcounting. Use them for approximate metrics only, not for exact business logic.

---

### Q19. Explain Delta Lake's Z-ordering. When does it help and when doesn't it?

Z-ordering co-locates related data within files by rewriting data so that rows with similar values of the Z-order columns are in the same files. Enables data skipping — files whose column stats don't match the query filter are skipped entirely.

```python
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "/delta/transactions") \
    .optimize().zOrderBy("customer_id", "transaction_date")
```

**Helps when:** queries frequently filter on the Z-order columns (e.g., "WHERE customer_id = X AND transaction_date BETWEEN ..."). Can reduce data scanned by 90%+.

**Doesn't help when:** you're doing full table scans, the cardinality is too low (e.g., Z-ordering on a boolean column), or the query filters on columns not in the Z-order spec.

**→ Follow-up:** "What's the difference between Z-ordering and partitioning?" Partitioning is a physical directory split — guarantees full partition elimination. Z-ordering is a file-level optimisation within partitions — provides file-level skipping via column statistics. Use partitioning for coarse-grained filtering (by date, region) and Z-ordering for fine-grained high-cardinality filters (by customer_id, product_id).

---

### Q20. How do you write unit tests for PySpark code?

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()

def test_transform_filters_negative_amounts(spark):
    schema = StructType([
        StructField("id", StringType()), 
        StructField("amount", DoubleType())
    ])
    input_df = spark.createDataFrame([("A", 100.0), ("B", -50.0), ("C", 0.0)], schema)
    
    result = filter_positive_amounts(input_df)  # function under test
    
    assert result.count() == 1
    assert result.first()["id"] == "A"

def test_schema_after_transform(spark):
    # ...
    assert set(result.columns) == {"id", "amount", "category"}
```

**→ Follow-up:** "How do you test a pipeline that reads from S3?" Mock the read step — use `spark.createDataFrame()` with test data instead of reading from actual storage. Test the transformation logic in isolation. Integration tests (reading from actual S3) run in CI with a dedicated test bucket.

---

### Q21. What is Spark's memory model? How is executor memory divided?

Executor memory = `spark.executor.memory` + `spark.executor.memoryOverhead` (off-heap).

Within `spark.executor.memory`:
- **Reserved Memory (300MB):** Always reserved for Spark internals.
- **User Memory (1 - spark.memory.fraction):** For data structures in user code.
- **Spark Memory (spark.memory.fraction = 0.6):** Shared between:
  - Storage Memory (cache): `spark.memory.storageFraction = 0.5` of Spark memory
  - Execution Memory (shuffle, sort, join): remainder

If execution needs more memory, it can evict storage memory (cached data spills to disk). This unified memory model prevents OOM in most cases by dynamically balancing between cache and execution.

**→ Follow-up:** "What is `spark.executor.memoryOverhead` and when do you increase it?" It's off-heap memory for JVM overhead, native memory, Python worker processes (for PySpark UDFs), etc. Increase it when you see tasks dying with "Container exceeded physical memory" errors in YARN, or when using many Python UDFs.

---

### Q22. How do you implement a slowly changing dimension (SCD Type 2) in PySpark?

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# New/changed records arriving
updates = spark.read.parquet("s3://staging/customer_updates/")

target = DeltaTable.forPath(spark, "/delta/dim_customer")

# Step 1: Expire changed records
target.alias("t").merge(
    updates.alias("u"),
    "t.customer_id = u.customer_id AND t.is_current = true AND t.name <> u.name"
).whenMatchedUpdate(set={
    "is_current": "false",
    "expiry_date": current_timestamp()
}).execute()

# Step 2: Insert new versions (both brand new + changed)
new_versions = updates.withColumn("effective_date", current_timestamp()) \
    .withColumn("expiry_date", lit("9999-12-31").cast("timestamp")) \
    .withColumn("is_current", lit(True))

new_versions.write.format("delta").mode("append").save("/delta/dim_customer")
```

**→ Follow-up:** "How do you query the active record as of a specific date?" 
```sql
SELECT * FROM dim_customer 
WHERE customer_id = 12345 
AND effective_date <= '2024-06-01' 
AND expiry_date > '2024-06-01'
```

---

### Q23. What is data lineage and how do you implement it in a Spark pipeline?

Data lineage tracks where data comes from, how it was transformed, and where it went — enabling impact analysis (if source X changes, what breaks?) and root-cause analysis (why does this report show wrong numbers?).

**Implementation approaches:**
1. **OpenLineage (open standard):** Spark listener that automatically emits lineage events to a backend (Marquez, OpenMetadata, Atlan). Zero code change — just add the listener.
```python
spark = SparkSession.builder \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.url", "http://marquez:5000") \
    .getOrCreate()
```
2. **Databricks Unity Catalog:** Captures column-level lineage automatically for all notebook/job runs — no setup needed.
3. **Manual:** Log job inputs/outputs to a metadata table with job_id, source tables, target tables, transform description, run timestamp.

**→ Follow-up:** "What is column-level lineage and why is it harder to implement?" It tracks that `output.total_revenue` was computed from `transactions.amount` — not just that the output table came from the transactions table. Hard because it requires parsing SQL/PySpark transformation logic. Tools like Databricks Unity Catalog and OpenLineage with Spark listener handle this automatically by analysing the physical query plan.

---

### Q24. How do you handle small files in a data lake?

Small files (< 1 MB) cause severe performance issues — the file open/close overhead dominates actual read time, and the name node (HDFS) or object store list operations become bottlenecks.

```python
# Problem detection: many partitions with few rows
df.rdd.getNumPartitions()  # if >> your data size in MB, you have small files

# Solution 1: Coalesce before write
df.coalesce(optimal_file_count).write.parquet("output/")

# Solution 2: Repartition by partition key to avoid cross-partition skew  
df.repartition(20, "date").write.partitionBy("date").parquet("output/")

# Solution 3: Delta Lake OPTIMIZE (compacts small files)
DeltaTable.forPath(spark, "/delta/transactions").optimize()

# Solution 4: Databricks Auto Optimize
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

Target file size: 128 MB – 1 GB for Parquet/Delta.

**→ Follow-up:** "What causes small files in the first place?" Streaming jobs writing every few minutes create small files. Over-partitioned DataFrames write one file per partition. High-cardinality `partitionBy` columns create too many directories. Frequent MERGE operations on Delta create small "insert" files.

---

### Q25. What are PySpark's broadcast join pitfalls beyond memory?

1. **Stale table statistics:** If Spark uses `ANALYZE TABLE` statistics that are outdated, it might decide to broadcast a "small" table that has since grown to 50GB — executor OOM.
2. **Driver serialisation bottleneck:** Broadcasting a 500MB table means the driver serialises it, sends it to all executors (say 200 executors × 500MB = 100GB of network traffic). On wide clusters, broadcast can take longer than a shuffle.
3. **Nested broadcast:** Broadcasting inside a loop (e.g., different lookup table per iteration) recreates the broadcast each time — very expensive.
4. **Join condition mismatch:** Broadcast hash join only works for equi-joins. Non-equi joins fall back to broadcast nested loop join — O(n×m).

---

### Q26. Explain PySpark's `explode()` function and its performance implications.

`explode()` converts an array/map column into multiple rows — one row per array element. Essential for nested JSON processing.

```python
from pyspark.sql.functions import explode, col

# Input: one row with array
# {"order_id": "A", "items": ["book", "pen", "paper"]}
df.select("order_id", explode("items").alias("item"))
# Output: 3 rows — (A, book), (A, pen), (A, paper)

# explode_outer — keeps rows with null/empty arrays (explode drops them)
df.select("order_id", F.explode_outer("items").alias("item"))

# posexplode — also returns index
df.select("order_id", F.posexplode("items").alias("pos", "item"))
```

**Performance:** `explode` can massively increase row count — a table with avg 100-item arrays goes 100x larger after explode. This is a hidden multiplier that can cause OOM or extremely slow downstream operations. Always `filter` or `select` the minimum needed before exploding.

**→ Follow-up:** "How would you handle deeply nested JSON in PySpark?" Use `schema_of_json()` to infer schema from a sample, then `from_json()` to parse. For deeply nested structs: `select("col.nested.deeply.value")` using dot notation or `col("col").getField("nested").getField("value")`.

---

### Q27. How do you tune Spark for reading many small Parquet files efficiently?

```python
# 1. Merge small files at read time
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB max per partition
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB "opening cost" estimate

# 2. Reduce parallelism for very small reads (avoid 10000 tasks for 1GB)
spark.conf.set("spark.default.parallelism", "200")

# 3. Use wildcard patterns to let Spark merge files
spark.read.parquet("s3://data/2024/01/*/")  # Spark merges into larger partitions internally

# 4. Coalesce during ETL to prevent accumulation
df.coalesce(target_files).write.mode("overwrite").parquet("output/")

# 5. Delta OPTIMIZE for Delta tables
DeltaTable.forPath(spark, path).optimize().where("date >= '2024-01-01'")
```

---

### Q28. What is the difference between Hive metastore and Unity Catalog?

**Hive Metastore:** Legacy metadata catalog. Single namespace (one catalog). No column-level security. No column-level lineage. No data governance beyond basic Ranger/Sentry policies. Shared across all workspaces unsafely.

**Unity Catalog (Databricks):**
- Three-level namespace: `catalog.schema.table`
- Row-level security and column masking out of the box
- Column-level data lineage automatically captured
- Centralized audit logging of all data access
- Single governance layer across multiple Databricks workspaces
- Data sharing across organizations (Delta Sharing)

```sql
-- Unity Catalog three-level namespace
SELECT * FROM prod_catalog.finance_schema.transactions;

-- Column masking
ALTER TABLE prod_catalog.finance_schema.customers 
  ALTER COLUMN ssn SET MASK mask_ssn USING COLUMNS (user_role);
```

---

### Q29. How would you implement a PySpark pipeline with dynamic partition pruning?

Dynamic Partition Pruning (DPP) allows Spark to filter partitions of a large fact table based on values discovered at runtime from a small dimension table — without you explicitly coding it.

```python
# DPP happens automatically when:
# 1. Large partitioned fact table joins with small dimension table
# 2. Filter is applied to the dimension table  
# 3. spark.sql.optimizer.dynamicPartitionPruning.enabled = True (default)

fact = spark.read.parquet("s3://data/fact_transactions/")  # partitioned by store_id
dim_stores = spark.read.parquet("s3://data/dim_stores/")

# This query triggers DPP:
result = fact.join(dim_stores, "store_id") \
    .filter(dim_stores.region == "NORTHEAST")
# Spark first evaluates the dim_stores filter, gets matching store_ids,
# then only reads fact_transactions partitions for those store_ids
```

**→ Follow-up:** "What are the conditions for DPP to activate?" The join key must be the fact table's partition key. The filtering must be on the dimension side. The dimension side must be small enough to build a runtime filter. AQE must be enabled.

---

### Q30. How do you implement monitoring and observability for a PySpark pipeline?

```python
# 1. Custom Spark Listener — captures job/stage/task metrics
from pyspark.scheduler import SparkListener

class PipelineMetricsListener(SparkListener):
    def onJobEnd(self, jobEnd):
        metrics = {
            "job_id": jobEnd.jobId,
            "result": str(jobEnd.jobResult),
            "duration_ms": jobEnd.time
        }
        send_to_datadog(metrics)

spark._jvm.PythonUtils.invokeMethod(...)  # register listener

# 2. Write pipeline metadata to a control table
def log_pipeline_run(job_name, status, rows_in, rows_out, duration):
    spark.createDataFrame([{
        "job_name": job_name, "run_time": datetime.now(),
        "status": status, "rows_read": rows_in,
        "rows_written": rows_out, "duration_seconds": duration
    }]).write.format("delta").mode("append").save("/delta/pipeline_log")

# 3. Data quality metrics to monitoring system
dq_results = df.select(
    F.count("*").alias("total_rows"),
    F.sum(col("amount").isNull().cast("int")).alias("null_amounts"),
    F.min("transaction_date").alias("min_date"),
    F.max("transaction_date").alias("max_date")
).first()

if dq_results.null_amounts / dq_results.total_rows > 0.05:
    send_alert(f"Null rate {dq_results.null_amounts/dq_results.total_rows:.1%} exceeds threshold")
```

---

---

# 🗄️ Area 2: Advanced SQL Joins (30 Questions)

---

### Q1. What is the difference between INNER JOIN, LEFT JOIN, RIGHT JOIN, and FULL OUTER JOIN?

**Answer:**
- **INNER JOIN:** Returns only rows where the join condition matches in BOTH tables. Non-matching rows from either side are excluded.
- **LEFT JOIN (LEFT OUTER):** Returns ALL rows from the left table. For non-matching rows, right-side columns are NULL.
- **RIGHT JOIN:** Mirror of LEFT JOIN — all rows from right table.
- **FULL OUTER JOIN:** Returns ALL rows from both sides. Non-matching rows have NULLs on the opposite side.

```sql
-- INNER: only matched customers
SELECT c.name, o.order_id 
FROM customers c INNER JOIN orders o ON c.id = o.customer_id;

-- LEFT: all customers, NULL order for those with no orders
SELECT c.name, o.order_id 
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;

-- FULL OUTER: all customers AND all orders, NULLs where no match
SELECT c.name, o.order_id 
FROM customers c FULL OUTER JOIN orders o ON c.id = o.customer_id;
```

**→ Follow-up: "A LEFT JOIN is returning more rows than the left table — why?"**
One-to-many relationship: each left row matches multiple right rows — the result explodes. Check the right table for duplicate join keys. Fix: deduplicate the right side first, or use a subquery/CTE with deduplication, or use EXISTS instead of JOIN if you only need existence check.

**→ Follow-up: "How do you find rows in Table A that do NOT exist in Table B?"**
Three equivalent approaches — anti-join patterns:
```sql
-- Option 1: LEFT JOIN + IS NULL (most portable)
SELECT a.* FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL;

-- Option 2: NOT EXISTS (often most readable, optimizer-friendly)
SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id);

-- Option 3: NOT IN (DANGER: fails silently if b has ANY NULLs)
SELECT * FROM a WHERE a.id NOT IN (SELECT id FROM b WHERE id IS NOT NULL);
-- Always add WHERE id IS NOT NULL if using NOT IN
```

---

### Q2. What is a SELF JOIN? Write a query to find employees who earn more than their manager.

**Answer:** A self join joins a table to itself, treating it as two different tables. Used to compare rows within the same table.

```sql
-- Find employees who earn more than their direct manager
SELECT 
    e.name AS employee_name,
    e.salary AS employee_salary,
    m.name AS manager_name,
    m.salary AS manager_salary
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.salary > m.salary;

-- Also useful: find all pairs of customers in the same city
SELECT a.name, b.name, a.city
FROM customers a
JOIN customers b ON a.city = b.city AND a.customer_id < b.customer_id;
-- The < prevents (A,B) and (B,A) duplicates and self-pairing
```

**→ Follow-up: "How would you find all employees in a management chain above a given employee?"** Use a recursive CTE:
```sql
WITH RECURSIVE management_chain AS (
    -- Base case: start with the employee
    SELECT employee_id, name, manager_id, 0 AS depth
    FROM employees WHERE employee_id = 1001
    
    UNION ALL
    
    -- Recursive: find the manager of each level
    SELECT e.employee_id, e.name, e.manager_id, mc.depth + 1
    FROM employees e
    INNER JOIN management_chain mc ON e.employee_id = mc.manager_id
)
SELECT * FROM management_chain ORDER BY depth;
```

---

### Q3. What is a CROSS JOIN? When is it legitimately useful?

**Answer:** A CROSS JOIN produces a cartesian product — every row in Table A combined with every row in Table B. N rows × M rows = N×M result rows. Generally avoided for large tables, but legitimately useful for:

1. **Generating a date spine × dimension cross:**
```sql
-- Generate all store × date combinations for a reporting scaffold
WITH date_spine AS (
    SELECT generate_series(
        '2024-01-01'::date, '2024-12-31'::date, '1 day'::interval
    )::date AS report_date
)
SELECT s.store_id, d.report_date
FROM stores s CROSS JOIN date_spine d;
-- Then LEFT JOIN actuals to this scaffold to show zero-revenue days
```

2. **Parameter combination testing**
3. **Pairing every customer with every product for scoring**

**→ Follow-up: "How do you accidentally get a CROSS JOIN in production?"** By forgetting the join condition: `FROM a, b` (implicit syntax) without a WHERE clause. Or joining on a non-unique constant: `ON 1=1`. Always use explicit `JOIN ... ON` syntax and validate row counts.

---

### Q4. Explain NON-EQUI JOINs. Give a retail/banking use case.

**Answer:** A non-equi join uses a condition other than `=` — such as `BETWEEN`, `<`, `>`, `!=`.

```sql
-- Assign a pricing tier to each transaction based on amount ranges
SELECT 
    t.transaction_id,
    t.amount,
    pt.tier_name,
    pt.discount_rate
FROM transactions t
JOIN pricing_tiers pt 
    ON t.amount BETWEEN pt.min_amount AND pt.max_amount;

-- Find all customers who made a purchase within 30 days of signing up
SELECT c.customer_id, c.signup_date, o.order_date
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
    AND o.order_date BETWEEN c.signup_date AND c.signup_date + INTERVAL '30 days';

-- Banking: apply the interest rate that was valid at the time of the transaction
SELECT l.loan_id, l.transaction_date, r.interest_rate
FROM loan_transactions l
JOIN rate_history r 
    ON l.loan_id = r.loan_id
    AND l.transaction_date BETWEEN r.effective_date AND r.end_date;
```

**→ Follow-up: "Why are non-equi joins expensive and how do you mitigate this?"** They can't use hash joins — fall back to nested loop or sort-merge with inequality predicates. Mitigate by: (1) applying equality conditions first to reduce dataset size, (2) using partitioning on the equality part of the condition, (3) in Spark: hints for the most selective join first.

---

### Q5. What is a LATERAL JOIN? How does it differ from a regular subquery?

**Answer:** A LATERAL join allows a subquery to reference columns from preceding tables in the FROM clause — it runs once per row of the outer query. Regular subqueries are evaluated independently of the outer row.

```sql
-- Find the 3 most recent orders for each customer (top-N per group)
SELECT c.customer_id, c.name, recent.order_id, recent.order_date
FROM customers c
CROSS JOIN LATERAL (
    SELECT order_id, order_date
    FROM orders o
    WHERE o.customer_id = c.customer_id  -- references outer table c
    ORDER BY order_date DESC
    LIMIT 3
) AS recent;

-- Without LATERAL, this would require window functions + CTE
-- LATERAL is often more readable for per-row subqueries
```

**→ Follow-up: "In what databases is LATERAL available?"** PostgreSQL (LATERAL), SQL Server (CROSS APPLY / OUTER APPLY), Oracle (LATERAL), BigQuery (UNNEST is effectively a lateral join). MySQL 8.0+ supports it. Not in older MySQL or SQLite.

**→ Follow-up: "CROSS APPLY vs OUTER APPLY in SQL Server?"** CROSS APPLY = INNER-like: excludes outer rows where the lateral subquery returns no rows. OUTER APPLY = LEFT-like: keeps outer rows even when lateral subquery returns nothing (NULLs).

---

### Q6. What is a window function? Write a query for running total and moving average.

```sql
-- Running total of sales per store, ordered by date
SELECT 
    store_id,
    sale_date,
    daily_sales,
    SUM(daily_sales) OVER (
        PARTITION BY store_id 
        ORDER BY sale_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    
    -- 7-day moving average (current + 6 preceding days)
    AVG(daily_sales) OVER (
        PARTITION BY store_id
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d,
    
    -- Percent of store's total
    daily_sales / SUM(daily_sales) OVER (PARTITION BY store_id) AS pct_of_total,
    
    -- Rank within store by sales descending
    RANK() OVER (PARTITION BY store_id ORDER BY daily_sales DESC) AS sales_rank
FROM store_daily_sales;
```

**→ Follow-up: "ROWS BETWEEN vs RANGE BETWEEN — what's the difference?"**
- `ROWS BETWEEN`: counts physical rows — `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` = the 7 rows including current.
- `RANGE BETWEEN`: treats rows with the same ORDER BY value as a group — `RANGE BETWEEN 6 PRECEDING AND CURRENT ROW` on a date column includes all rows within 6 units of the current value. Critical for date arithmetic where multiple rows can share a date.

---

### Q7. How do you find customers who purchased in 3 consecutive months?

```sql
-- Method 1: Self-join approach
WITH monthly_purchases AS (
    SELECT DISTINCT customer_id, 
           DATE_TRUNC('month', purchase_date) AS purchase_month
    FROM transactions
)
SELECT DISTINCT a.customer_id
FROM monthly_purchases a
JOIN monthly_purchases b 
    ON a.customer_id = b.customer_id 
    AND b.purchase_month = a.purchase_month + INTERVAL '1 month'
JOIN monthly_purchases c 
    ON a.customer_id = c.customer_id 
    AND c.purchase_month = a.purchase_month + INTERVAL '2 months';

-- Method 2: Window function (gaps and islands) - more elegant
WITH monthly_purchases AS (
    SELECT DISTINCT customer_id,
           DATE_TRUNC('month', purchase_date) AS purchase_month,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('month', purchase_date)) AS rn
    FROM transactions
),
numbered AS (
    SELECT *,
           DATE_TRUNC('month', purchase_month) - (rn * INTERVAL '1 month') AS group_key
    FROM monthly_purchases
)
SELECT customer_id
FROM numbered
GROUP BY customer_id, group_key
HAVING COUNT(*) >= 3;
```

**→ Follow-up: "Explain the 'gaps and islands' technique in the second query."** By subtracting the row number (multiplied by one month interval) from the purchase month, consecutive months produce the same `group_key`. Non-consecutive months produce different keys. Grouping by `customer_id, group_key` and counting gives streak length. Any streak ≥ 3 means 3 consecutive months.

---

### Q8. Write a SQL query for session analysis — find session duration per user.

```sql
-- A session ends when a user is inactive for > 30 minutes
WITH ordered_events AS (
    SELECT 
        user_id,
        event_time,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
    FROM user_events
),
session_boundaries AS (
    SELECT *,
           -- Mark session start: no previous event OR gap > 30 min
           CASE WHEN prev_event_time IS NULL 
                     OR event_time - prev_event_time > INTERVAL '30 minutes'
                THEN 1 ELSE 0 END AS is_session_start
    FROM ordered_events
),
sessions AS (
    SELECT *,
           SUM(is_session_start) OVER (
               PARTITION BY user_id ORDER BY event_time
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS session_id
    FROM session_boundaries
)
SELECT 
    user_id,
    session_id,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end,
    EXTRACT(EPOCH FROM MAX(event_time) - MIN(event_time)) / 60 AS duration_minutes,
    COUNT(*) AS event_count
FROM sessions
GROUP BY user_id, session_id
ORDER BY user_id, session_start;
```

---

### Q9. What is GROUPING SETS, ROLLUP, and CUBE?

```sql
-- ROLLUP: hierarchical subtotals (country → region → city → grand total)
SELECT country, region, city, SUM(sales)
FROM store_sales
GROUP BY ROLLUP(country, region, city);
-- Produces: (country, region, city), (country, region), (country), ()

-- CUBE: all combinations of subtotals
SELECT product, region, SUM(sales)
FROM sales
GROUP BY CUBE(product, region);
-- Produces: (product, region), (product), (region), ()

-- GROUPING SETS: explicit list of groupings you want
SELECT product, region, channel, SUM(sales)
FROM sales
GROUP BY GROUPING SETS (
    (product, region),
    (product, channel),
    (region),
    ()  -- grand total
);

-- GROUPING() function: tells you which columns are subtotals
SELECT 
    CASE WHEN GROUPING(country) = 1 THEN 'ALL' ELSE country END AS country,
    CASE WHEN GROUPING(region) = 1 THEN 'ALL' ELSE region END AS region,
    SUM(sales)
FROM sales
GROUP BY ROLLUP(country, region);
```

**→ Follow-up: "When would you use GROUPING SETS over multiple UNION ALL queries?"** GROUPING SETS scans the table once; UNION ALL scans it N times. For large tables, GROUPING SETS is dramatically more efficient. The optimizer can also leverage shared aggregation state.

---

### Q10. Write a customer cohort retention query.

```sql
-- Cohort: users who first purchased in month X. Retention: % who also purchased in month X+N
WITH first_purchase AS (
    SELECT customer_id, DATE_TRUNC('month', MIN(purchase_date)) AS cohort_month
    FROM orders
    GROUP BY customer_id
),
cohort_activity AS (
    SELECT 
        f.customer_id,
        f.cohort_month,
        DATE_TRUNC('month', o.purchase_date) AS activity_month,
        DATEDIFF(month, f.cohort_month, DATE_TRUNC('month', o.purchase_date)) AS months_since_first
    FROM first_purchase f
    JOIN orders o ON f.customer_id = o.customer_id
)
SELECT 
    cohort_month,
    months_since_first,
    COUNT(DISTINCT customer_id) AS retained_customers,
    COUNT(DISTINCT customer_id) * 100.0 / 
        FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month ORDER BY months_since_first
        ) AS retention_rate
FROM cohort_activity
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;
```

---

### Q11. What is a correlated subquery? When is it a performance problem?

```sql
-- Correlated: inner query references outer row — runs once per outer row
SELECT customer_id, name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count,
    (SELECT MAX(amount) FROM orders o WHERE o.customer_id = c.customer_id) AS max_order
FROM customers c;

-- PROBLEM: if customers has 10M rows, inner query runs 10M times
-- SOLUTION: rewrite as JOIN with aggregation
SELECT c.customer_id, c.name, agg.order_count, agg.max_order
FROM customers c
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS order_count, MAX(amount) AS max_order
    FROM orders GROUP BY customer_id
) agg ON c.customer_id = agg.customer_id;
-- Aggregation runs once — O(n) not O(n²)
```

**→ Follow-up: "When IS a correlated subquery acceptable?"** When used with EXISTS/NOT EXISTS for existence checks (optimizer converts to semi-join, not correlated execution). Also acceptable when the result set is very small (few thousand rows).

---

### Q12. How do you deduplicate records in SQL while keeping the most recent?

```sql
-- Method 1: ROW_NUMBER (most versatile)
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id          -- dedup key
               ORDER BY updated_at DESC       -- keep most recent
           ) AS rn
    FROM transactions
)
SELECT * FROM ranked WHERE rn = 1;

-- Method 2: DISTINCT ON (PostgreSQL only — very efficient)
SELECT DISTINCT ON (event_id) *
FROM transactions
ORDER BY event_id, updated_at DESC;

-- Method 3: Self-join anti-pattern (avoid — slow)
SELECT * FROM transactions t1
WHERE NOT EXISTS (
    SELECT 1 FROM transactions t2
    WHERE t2.event_id = t1.event_id AND t2.updated_at > t1.updated_at
);

-- For Delta Lake in SQL:
MERGE INTO target USING source ON target.event_id = source.event_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

### Q13. Find the top N products per category — write the most efficient query.

```sql
-- Method 1: ROW_NUMBER window function (most universal)
WITH ranked_products AS (
    SELECT 
        product_id, product_name, category, revenue,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank
    FROM product_revenue
)
SELECT product_id, product_name, category, revenue
FROM ranked_products WHERE rank <= 3;

-- Method 2: LATERAL / CROSS APPLY (PostgreSQL/SQL Server)
SELECT c.category, top3.*
FROM (SELECT DISTINCT category FROM products) c
CROSS JOIN LATERAL (
    SELECT product_name, revenue
    FROM products p
    WHERE p.category = c.category
    ORDER BY revenue DESC LIMIT 3
) top3;
```

**→ Follow-up: "RANK vs ROW_NUMBER for top-N — which is correct and when?"** If you want exactly N rows, use ROW_NUMBER (breaks ties arbitrarily). If ties should all be included, use RANK (might return more than N rows). For "the 3 highest-revenue products" where ties should both be included, use RANK or DENSE_RANK.

---

### Q14. Explain the execution order of a SQL query.

**Standard logical execution order:**
1. `FROM` + `JOIN` — determine source tables, apply joins
2. `WHERE` — filter rows (before aggregation)
3. `GROUP BY` — form groups
4. `HAVING` — filter groups (after aggregation)
5. `SELECT` — evaluate expressions, aliases
6. `DISTINCT` — deduplicate
7. `ORDER BY` — sort (can reference SELECT aliases here)
8. `LIMIT/TOP` — limit rows

**Why it matters:**
```sql
-- WRONG: can't use alias in WHERE (alias defined in SELECT which runs later)
SELECT amount * 1.1 AS adjusted_amount FROM orders WHERE adjusted_amount > 100;

-- RIGHT: repeat expression or use subquery/CTE
SELECT amount * 1.1 AS adjusted_amount FROM orders WHERE amount * 1.1 > 100;

-- WRONG: can't use aggregate in WHERE
SELECT customer_id FROM orders WHERE COUNT(*) > 5 GROUP BY customer_id;

-- RIGHT: aggregates go in HAVING
SELECT customer_id FROM orders GROUP BY customer_id HAVING COUNT(*) > 5;
```

---

### Q15. What is the difference between WHERE and HAVING?

`WHERE` filters **individual rows** before grouping — operates on raw data, can't use aggregate functions.
`HAVING` filters **groups** after `GROUP BY` — can use aggregate functions.

```sql
-- WHERE: filter before aggregation
SELECT store_id, SUM(sales) AS total_sales
FROM transactions
WHERE transaction_date >= '2024-01-01'  -- row-level filter (indexed, fast)
GROUP BY store_id
HAVING SUM(sales) > 100000;             -- group-level filter (after aggregation)

-- Common mistake: using HAVING for non-aggregate filters (works but wasteful)
-- BAD: aggregates all stores first, then filters
SELECT store_id, SUM(sales) FROM transactions
GROUP BY store_id HAVING store_id LIKE 'NYC%';

-- GOOD: filter rows BEFORE aggregation
SELECT store_id, SUM(sales) FROM transactions
WHERE store_id LIKE 'NYC%' GROUP BY store_id;
```

---

### Q16. Write a query to identify gaps in a date sequence (missing dates).

```sql
-- Find all missing dates in a transactions table for the last 6 months
WITH date_series AS (
    -- Generate all expected dates
    SELECT generate_series(
        CURRENT_DATE - INTERVAL '6 months',
        CURRENT_DATE,
        '1 day'::interval
    )::date AS expected_date
),
existing_dates AS (
    SELECT DISTINCT DATE(transaction_date) AS tx_date FROM transactions
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '6 months'
)
SELECT d.expected_date AS missing_date
FROM date_series d
LEFT JOIN existing_dates e ON d.expected_date = e.tx_date
WHERE e.tx_date IS NULL
ORDER BY missing_date;
```

---

### Q17. How do you pivot data in SQL without a PIVOT keyword?

```sql
-- Conditional aggregation pivot
SELECT 
    store_id,
    SUM(CASE WHEN product_category = 'Electronics' THEN sales ELSE 0 END) AS electronics_sales,
    SUM(CASE WHEN product_category = 'Clothing' THEN sales ELSE 0 END) AS clothing_sales,
    SUM(CASE WHEN product_category = 'Food' THEN sales ELSE 0 END) AS food_sales
FROM store_sales
GROUP BY store_id;

-- Using FILTER (PostgreSQL/modern SQL):
SELECT 
    store_id,
    SUM(sales) FILTER (WHERE product_category = 'Electronics') AS electronics_sales,
    SUM(sales) FILTER (WHERE product_category = 'Clothing') AS clothing_sales
FROM store_sales GROUP BY store_id;
```

---

### Q18. How do you calculate year-over-year growth in SQL?

```sql
WITH monthly_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month,
        SUM(amount) AS revenue
    FROM sales
    GROUP BY 1, 2
)
SELECT 
    curr.year, curr.month, curr.revenue AS current_revenue,
    prev.revenue AS prior_year_revenue,
    ROUND((curr.revenue - prev.revenue) * 100.0 / NULLIF(prev.revenue, 0), 2) AS yoy_growth_pct
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.month = prev.month AND curr.year = prev.year + 1
ORDER BY curr.year, curr.month;
```

---

### Q19. What are CTEs vs subqueries vs temp tables — when to use each?

**CTE (WITH clause):**
- Readable, reusable within the same query, can be recursive
- In most databases, NOT materialised — optimizer can inline them
- In PostgreSQL, CTEs are materialised by default (acts as optimization fence) — use `WITH ... AS MATERIALIZED` / `NOT MATERIALIZED` to control

**Subquery:**
- Inline, not reusable — fine for simple one-off filters
- No name, harder to read for complex logic

**Temp Table:**
- Physically materialised to disk/memory — always an execution boundary
- Useful when you need to reference results multiple times in complex multi-step logic
- Has statistics — optimizer can make better decisions
- Survives across multiple statements in a session

**Rule of thumb:**
- Simple, readable logic → CTE
- Need to reference same results 3+ times across different queries → Temp table
- Performance-critical pipeline step → Temp table (force materialization, allow statistics)

---

### Q20. What is SCD Type 2 — write the full SQL implementation?

```sql
-- Source: incoming data with potential changes
-- Target: dim_customer with SCD Type 2

-- Step 1: Identify changed records
WITH changes AS (
    SELECT s.customer_id, s.name, s.email, s.city
    FROM staging_customers s
    JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = TRUE
    WHERE s.name <> d.name OR s.email <> d.email OR s.city <> d.city
)
-- Step 2: Expire old records
UPDATE dim_customer
SET is_current = FALSE, expiry_date = CURRENT_DATE - INTERVAL '1 day'
WHERE customer_id IN (SELECT customer_id FROM changes) AND is_current = TRUE;

-- Step 3: Insert new versions for changed + brand new records
INSERT INTO dim_customer (customer_id, name, email, city, effective_date, expiry_date, is_current)
SELECT s.customer_id, s.name, s.email, s.city,
       CURRENT_DATE, '9999-12-31'::date, TRUE
FROM staging_customers s
LEFT JOIN dim_customer d ON s.customer_id = d.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL  -- brand new
   OR (s.name <> d.name OR s.email <> d.email OR s.city <> d.city);  -- changed
```

---

### Q21. Explain EXPLAIN ANALYZE — how do you read a query execution plan?

```sql
EXPLAIN ANALYZE SELECT c.name, COUNT(o.order_id)
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.name;
```

**Key things to look for:**
- **Seq Scan vs Index Scan:** Seq Scan on large tables = missing index
- **Hash Join vs Nested Loop:** Nested Loop on large tables = potential performance issue
- **actual rows vs estimated rows:** Large discrepancy = stale statistics → run `ANALYZE table_name`
- **Buffers hit vs read:** High disk reads = data not in cache
- **Sort:** Expensive for large datasets without index support
- **Total cost:** Outer-most node's cost is the query cost estimate

**→ Follow-up: "When do you add an index and when do indexes hurt?"** Add: frequently filtered columns in WHERE, JOIN keys, ORDER BY columns in range queries. Hurt: on tables with heavy INSERT/UPDATE/DELETE (index maintenance overhead), low-cardinality columns (e.g., boolean — optimizer skips the index anyway), too many indexes slow down writes.

---

### Q22. How do you handle NULL in aggregations, joins, and comparisons?

```sql
-- NULL in aggregations: ignored by SUM/COUNT/AVG/MAX/MIN
SELECT AVG(amount) FROM orders;  -- NULLs excluded from average
SELECT COUNT(*) FROM orders;      -- counts all rows including NULL amounts
SELECT COUNT(amount) FROM orders; -- counts only non-NULL amounts

-- NULL in comparisons: always evaluates to UNKNOWN, not TRUE/FALSE
SELECT * FROM orders WHERE amount = NULL;    -- WRONG: returns 0 rows
SELECT * FROM orders WHERE amount IS NULL;   -- CORRECT
SELECT * FROM orders WHERE amount IS NOT NULL;

-- NULL in CASE WHEN: 
CASE WHEN amount > 0 THEN 'positive' 
     WHEN amount < 0 THEN 'negative'
     WHEN amount = 0 THEN 'zero'
     ELSE 'null or unknown'  -- catches NULL (doesn't match any WHEN)
END

-- COALESCE vs NULLIF:
COALESCE(amount, 0)           -- replace NULL with 0
NULLIF(amount, 0)             -- return NULL if amount = 0 (prevents division by zero)
amount / NULLIF(other_col, 0) -- safe division
```

---

### Q23. Write a SQL query to find products frequently bought together (market basket).

```sql
-- Find pairs of products purchased in the same order, ranked by co-occurrence
WITH order_products AS (
    SELECT order_id, product_id
    FROM order_items
),
product_pairs AS (
    SELECT 
        a.product_id AS product_a,
        b.product_id AS product_b,
        COUNT(DISTINCT a.order_id) AS co_purchase_count
    FROM order_products a
    JOIN order_products b 
        ON a.order_id = b.order_id 
        AND a.product_id < b.product_id  -- avoid duplicates and self-pairs
    GROUP BY a.product_id, b.product_id
)
SELECT 
    p1.product_name AS product_a_name,
    p2.product_name AS product_b_name,
    pp.co_purchase_count,
    ROUND(pp.co_purchase_count * 100.0 / 
          (SELECT COUNT(DISTINCT order_id) FROM order_items), 2) AS support_pct
FROM product_pairs pp
JOIN products p1 ON pp.product_a = p1.product_id
JOIN products p2 ON pp.product_b = p2.product_id
WHERE pp.co_purchase_count >= 10
ORDER BY co_purchase_count DESC
LIMIT 20;
```

---

### Q24. How does indexing work? What is a composite index and how should columns be ordered?

An index is a B-tree (or other) data structure that allows O(log n) lookups instead of O(n) full scans.

**Composite index column ordering — the leftmost prefix rule:**
```sql
CREATE INDEX idx_store_date_product ON sales(store_id, sale_date, product_id);

-- Uses index: store_id is leftmost
SELECT * FROM sales WHERE store_id = 'NYC001';
SELECT * FROM sales WHERE store_id = 'NYC001' AND sale_date = '2024-01-01';
SELECT * FROM sales WHERE store_id = 'NYC001' AND sale_date = '2024-01-01' AND product_id = 'P100';

-- Does NOT use index: skips leftmost column
SELECT * FROM sales WHERE sale_date = '2024-01-01';  -- no store_id filter
SELECT * FROM sales WHERE product_id = 'P100';       -- skips store_id AND sale_date
```

**Rule:** Put the column with the highest cardinality AND most commonly queried first. Put range conditions last (they stop prefix matching).

---

### Q25. What is a covering index?

A covering index includes all columns needed by a query — the engine never touches the main table (heap), only the index. This is called an index-only scan.

```sql
-- Query: frequently run
SELECT store_id, SUM(amount) FROM transactions 
WHERE store_id = 'NYC001' AND transaction_date >= '2024-01-01'
GROUP BY store_id;

-- Covering index: includes all referenced columns
CREATE INDEX idx_covering ON transactions(store_id, transaction_date) INCLUDE (amount);
-- Now the query uses only the index — no heap access, very fast
```

---

### Q26. How do you detect and fix a N+1 query problem?

N+1: fetching 1 parent record, then issuing N individual queries for each child — extremely common in ORMs.

```sql
-- N+1 anti-pattern (conceptually):
SELECT * FROM customers;
-- Then for each customer:
SELECT * FROM orders WHERE customer_id = ?;  -- runs N times

-- Fix: single JOIN
SELECT c.*, o.order_id, o.amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

-- In data engineering: N+1 shows up as loops with individual DB queries
-- WRONG:
for customer_id in customer_ids:
    result = db.query(f"SELECT * FROM orders WHERE customer_id = {customer_id}")

-- RIGHT: single query with IN clause or JOIN
result = db.query(f"SELECT * FROM orders WHERE customer_id IN ({','.join(customer_ids)})")
```

---

### Q27. What is the difference between TRUNCATE, DELETE, and DROP?

- **DELETE:** Removes rows matching WHERE clause. Logged row-by-row. Can be rolled back. Doesn't reset auto-increment. Slow for large tables. Triggers fire.
- **TRUNCATE:** Removes all rows. Minimal logging (logs page deallocations). Resets auto-increment. Very fast. Cannot be filtered by WHERE. In most databases, can be rolled back within a transaction.
- **DROP:** Removes the entire table structure, data, indexes, and constraints. Irreversible (without backup). Removes the object entirely.

**In data pipelines:** For loading patterns: `TRUNCATE` + `INSERT` (atomically via transaction) is faster than `DELETE` + `INSERT` for full table refreshes. For partition replacement: use partition-level operations rather than DELETE.

---

### Q28. How would you find the second highest salary without using LIMIT/TOP?

```sql
-- Method 1: Subquery
SELECT MAX(salary) FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Method 2: DENSE_RANK
SELECT salary FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked WHERE rnk = 2;

-- Method 3: Parameterised for Nth highest
SELECT salary FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked WHERE rnk = :n;  -- works for any N
```

---

### Q29. Explain partitioning in BigQuery/Snowflake/Databricks and how it affects query performance.

**Partitioning** physically divides table data into separate storage segments based on a column's value. Queries with filters on the partition column only scan relevant partitions — called **partition pruning**.

```sql
-- BigQuery: date partitioned table
CREATE TABLE `project.dataset.transactions`
PARTITION BY DATE(transaction_date)
CLUSTER BY store_id, product_category  -- also cluster for further pruning
AS SELECT * FROM staging;

-- Query with partition pruning (cheap — only reads Jan 2024 partition)
SELECT SUM(amount) FROM transactions
WHERE transaction_date BETWEEN '2024-01-01' AND '2024-01-31';
-- vs. without filter: scans entire table (very expensive in BigQuery)
```

**Clustering (BigQuery) / Z-ordering (Databricks):** Within each partition, sorts and groups data by cluster columns. Allows block-level skipping within a partition.

**→ Follow-up: "What is partition explosion and how do you prevent it?"** Too many partitions (e.g., partitioning by user_id with 100M users → 100M partitions) causes metadata overhead, too-small files, and query planning slowdown. Prevent by using coarser-grained partition keys (date, not datetime), adding a secondary bucketing/clustering strategy, or using range partitioning with fewer buckets.

---

### Q30. Write a query to calculate a churn rate — customers active last period but not this period.

```sql
WITH current_period AS (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_date BETWEEN '2024-10-01' AND '2024-12-31'
),
previous_period AS (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE order_date BETWEEN '2024-07-01' AND '2024-09-30'
),
churn_analysis AS (
    SELECT 
        COUNT(DISTINCT p.customer_id) AS active_last_period,
        COUNT(DISTINCT c.customer_id) AS retained_this_period,
        COUNT(DISTINCT p.customer_id) - COUNT(DISTINCT c.customer_id) AS churned
    FROM previous_period p
    LEFT JOIN current_period c ON p.customer_id = c.customer_id
)
SELECT 
    active_last_period,
    retained_this_period,
    churned,
    ROUND(churned * 100.0 / NULLIF(active_last_period, 0), 2) AS churn_rate_pct
FROM churn_analysis;
```

---

---

# ☁️ Area 3: Cloud Experience (30 Questions)

---

### Q1. Compare Azure and GCP data stacks. Which would you recommend for a retail analytics platform?

**Answer:**

| Layer | Azure | GCP |
|---|---|---|
| Streaming Ingestion | Event Hubs (Kafka-compatible) | Pub/Sub |
| Batch Processing | Databricks / Azure Synapse Spark | Dataproc (managed Spark) |
| Streaming Processing | Azure Stream Analytics / Databricks | Dataflow (Apache Beam) |
| Data Lake Storage | ADLS Gen2 | GCS |
| DWH | Azure Synapse Analytics | BigQuery |
| Orchestration | Azure Data Factory / Cloud Composer | Cloud Composer (managed Airflow) |
| ML Platform | Azure ML | Vertex AI |
| Governance | Microsoft Purview / Databricks Unity Catalog | Dataplex |

**For dunnhumby (retail analytics at massive scale):** I'd recommend Azure with Databricks. Rationale: (1) dunnhumby likely has existing Microsoft enterprise agreements, (2) Databricks is native to Azure and provides Delta Lake + Unity Catalog for data governance, (3) Azure Event Hubs' Kafka API compatibility means existing Kafka producers don't need to change, (4) Azure Synapse covers ad-hoc SQL analytics for business teams. GCP BigQuery would be a strong alternative — its serverless model and near-zero management overhead is compelling for analytics-heavy workloads.

**→ Follow-up: "What's the single biggest advantage BigQuery has over Synapse?"** Truly serverless — no clusters to provision or size. You pay per query (per TB scanned). No capacity planning. Scales to any query instantly. Synapse dedicated SQL pools require upfront capacity commitment.

---

### Q2. What is Azure Data Lake Storage Gen2 and how does it differ from Blob Storage?

**Answer:** ADLS Gen2 is built on top of Azure Blob Storage but adds:
1. **Hierarchical Namespace (HNS):** True directory structure (not simulated with `/` in names). Enables atomic directory rename operations — critical for safe ETL patterns (write to temp → rename to final atomically).
2. **POSIX-compatible ACLs:** Fine-grained file/folder level permissions (not just container-level RBAC).
3. **Performance optimisation:** HNS enables more efficient metadata operations — listing large directories is orders of magnitude faster.
4. **Multi-protocol access:** Access via HDFS API (Spark), Blob API, and ADLS Gen2 API.

```python
# Atomic write pattern using ADLS HNS rename
# Write to staging
df.write.parquet("abfss://container@storage.dfs.core.windows.net/staging/date=2024-01-01/")
# Atomic rename to production (only possible with HNS)
fs.rename("staging/date=2024-01-01", "production/date=2024-01-01")
```

**→ Follow-up: "What is the difference between `wasbs://` and `abfss://`?"** `wasbs` = old Azure Blob Storage protocol (WASB - Windows Azure Storage Blob). `abfss` = Azure Data Lake Storage Gen2 protocol with TLS. Always use `abfss` for ADLS Gen2 — better performance, proper HNS support, secure.

---

### Q3. What is Azure Data Factory vs Apache Airflow vs Databricks Workflows? When do you use each?

**ADF (Azure Data Factory):**
- GUI-based, low-code pipeline orchestration
- Best for: non-engineers, simple ETL copy activities, trigger-based pipelines, Data Flow (visual Spark)
- Weaknesses: poor testability, limited version control, hard to debug complex logic, no unit tests

**Apache Airflow (Cloud Composer on GCP, MWAA on AWS, Astronomer on Azure):**
- Code-first Python DAGs — full testability, Git-friendly, rich ecosystem of operators
- Best for: complex multi-system orchestration, pipelines that need code review, teams with DE skills
- Weaknesses: overhead of managing Airflow itself, slower to onboard non-Python users

**Databricks Workflows:**
- Native to Databricks — zero setup, orchestrates notebooks/scripts/Delta Live Tables
- Best for: Spark-heavy pipelines, data science/ML workflows within Databricks
- Weaknesses: Databricks-only, limited cross-system orchestration compared to Airflow

**My choice for production:** Airflow for complex enterprise pipelines — code-first, testable, version-controlled. Databricks Workflows for Spark jobs that are entirely within Databricks. ADF only if the team has no Python capability.

**→ Follow-up: "What are the different Airflow executor types and when does each matter?"**
- `LocalExecutor`: tasks run in subprocesses on the scheduler node. Fine for small teams, single machine.
- `CeleryExecutor`: tasks distributed to multiple worker nodes via message queue (Redis/RabbitMQ). Scale out with more workers.
- `KubernetesExecutor`: each task gets its own ephemeral pod — ultimate isolation, no resource contention, scales to zero. Best for cloud-native deployments. Overhead: pod startup time per task.

---

### Q4. How do you secure a data pipeline on Azure? Walk through all layers.

```
Network Layer:
├── VNet injection for Databricks clusters
├── Private endpoints for ADLS, Key Vault, Event Hubs
├── No public IP on data services
└── NSG rules + Azure Firewall

Identity & Access:
├── Managed Identity for pipeline-to-service auth (no passwords)
├── Service Principals for CI/CD pipelines
├── RBAC on ADLS (Storage Blob Data Contributor/Reader)
└── Unity Catalog for column-level and row-level security

Secrets Management:
├── Azure Key Vault — no secrets in code or notebooks
├── Databricks Secret Scopes backed by Key Vault
└── Regular secret rotation

Data Protection:
├── Encryption at rest: Azure Storage Service Encryption (SSE) with CMK
├── Encryption in transit: TLS 1.2+ enforced
├── Column-level masking for PII in Unity Catalog
└── Delta Lake audit logs

Compliance:
├── Microsoft Purview for data classification (PII detection)
├── Azure Monitor + Diagnostic Logs for audit trail
└── GDPR: right to erasure via Delta vacuum + targeted deletes
```

**→ Follow-up: "What is Managed Identity and why is it better than a Service Principal with a secret?"** Managed Identity is automatically managed by Azure — no credential to create, store, rotate, or accidentally expose. The identity is tied to the Azure resource (e.g., Databricks cluster). Service Principal requires a client secret that must be stored somewhere (Key Vault at minimum) and rotated periodically — more operational burden and an extra secret to secure.

---

### Q5. What is Delta Live Tables (DLT) in Databricks? How does it compare to traditional Spark pipelines?

**Delta Live Tables:** A declarative framework for building reliable data pipelines. You declare what the table should contain — DLT handles the how (execution order, retries, checkpointing, schema evolution).

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw transactions from Kafka")
def raw_transactions():
    return (spark.readStream
        .format("kafka")
        .option("subscribe", "transactions")
        .load())

@dlt.table(comment="Cleaned and validated transactions")
@dlt.expect_or_drop("valid_amount", "amount > 0")        # data quality constraint
@dlt.expect_or_quarantine("valid_date", "transaction_date IS NOT NULL")
def cleaned_transactions():
    return (dlt.read_stream("raw_transactions")
        .select(col("value").cast("string").alias("raw"))
        .select(from_json("raw", schema).alias("data"))
        .select("data.*"))

@dlt.table(comment="Daily store aggregates — Gold layer")
def daily_store_agg():
    return (dlt.read("cleaned_transactions")
        .groupBy("store_id", "date")
        .agg(sum("amount").alias("total_sales")))
```

**DLT advantages over traditional Spark:** Auto-manages dependencies (no need to manually sequence jobs), built-in data quality with `@dlt.expect`, automatic lineage, simpler streaming checkpointing, pipeline-level monitoring.

**DLT weaknesses:** Less flexibility for complex custom logic, harder to unit test, Databricks-locked.

---

### Q6. How does BigQuery pricing work? How do you optimise costs?

**Two pricing models:**
1. **On-demand:** $5 per TB scanned. Pay only for what you query — good for variable workloads.
2. **Capacity (slots):** Fixed monthly commitment (100 slots min). Better for consistent heavy usage.

**Cost optimisation:**
```sql
-- 1. Always partition + filter on partition column
SELECT * FROM transactions WHERE DATE(transaction_date) = '2024-01-01';
-- vs. full scan: WHERE EXTRACT(YEAR FROM transaction_date) = 2024 (can't prune)

-- 2. Never SELECT * — only select needed columns (columnar storage)
SELECT transaction_id, amount FROM transactions; -- reads 2 columns only

-- 3. Use clustered tables to reduce scanned blocks within partitions
CREATE TABLE transactions PARTITION BY DATE(created_at) CLUSTER BY store_id, product_id;

-- 4. Materialise expensive subqueries to avoid recomputation
-- Reusing a CTE N times = N scans in BigQuery
-- Use temp tables for expensive intermediates

-- 5. Use BI Engine for repeated dashboard queries (in-memory cache layer)
```

**→ Follow-up: "What is BigQuery slot contention and how do you handle it?"** Slots = units of computation. On-demand queries compete for a shared slot pool. If many large queries run simultaneously, they queue. Solution: (1) Use reservations for predictable workloads, (2) Break very large queries into smaller ones, (3) Schedule heavy queries off-peak, (4) Use BI Engine for dashboards (avoids slots).

---

### Q7. How do you implement a CDC pipeline using Debezium + Kafka + Spark?

```
Architecture:
Source DB (Oracle/Postgres) → Debezium Connector → Kafka Topic → Spark Structured Streaming → Delta Lake

Flow:
1. Debezium reads DB transaction log (redo log / WAL)
2. Emits change events to Kafka: {before: {...}, after: {...}, op: "c/u/d"}
3. Spark reads Kafka stream
4. Parses change events, applies to Delta table via MERGE
```

```python
from pyspark.sql.functions import from_json, col
from delta.tables import DeltaTable

# Kafka source
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.inventory.customers") \
    .load()

# Parse Debezium envelope
cdc_schema = StructType([
    StructField("before", customer_schema),
    StructField("after", customer_schema),
    StructField("op", StringType()),  # c=create, u=update, d=delete
])
parsed = raw.select(from_json(col("value").cast("string"), cdc_schema).alias("cdc")) \
    .select("cdc.*")

# Apply to Delta via foreachBatch
def upsert_to_delta(batch_df, batch_id):
    target = DeltaTable.forPath(spark, "/delta/customers")
    
    # Separate inserts/updates from deletes
    upserts = batch_df.filter(col("op").isin(["c", "u"])).select("after.*")
    deletes = batch_df.filter(col("op") == "d").select("before.customer_id")
    
    # Upsert
    target.alias("t").merge(upserts.alias("s"), "t.customer_id = s.customer_id") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    # Hard delete (or soft delete with is_deleted flag)
    target.alias("t").merge(deletes.alias("s"), "t.customer_id = s.customer_id") \
        .whenMatchedDelete().execute()

parsed.writeStream.foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoints/cdc_customers").start()
```

---

### Q8. What is a Data Lakehouse architecture? How does it differ from a Data Lake + Data Warehouse?

**Traditional two-tier:**
```
Raw Data → Data Lake (ADLS/S3) → ETL → Data Warehouse (Synapse/Redshift) → BI
- Data duplicated in two places
- Expensive DWH storage
- ETL latency
- Data lake: cheap but no ACID, no SQL
- DWH: fast SQL but expensive, rigid schema
```

**Lakehouse (Delta Lake / Apache Iceberg):**
```
Raw Data → Data Lake with table format → Direct SQL + ML
- Single copy of data on cheap object storage
- ACID transactions via transaction log
- SQL performance via vectorized execution + Z-ordering + data skipping
- Schema enforcement + evolution
- Time travel
- Direct ML model training on same data (no copy)
```

**→ Follow-up: "Delta Lake vs Apache Iceberg vs Apache Hudi — key differences?"**
- **Delta Lake:** Databricks-native, best Unity Catalog integration, transaction log in JSON/Parquet, strongest Spark optimisation
- **Apache Iceberg:** Vendor-neutral, excellent multi-engine support (Spark, Flink, Presto, Trino, Hive), best for multi-cloud/multi-engine environments
- **Apache Hudi:** Optimized for CDC/streaming upserts, strong Hive/HBase ecosystem integration, Row-based and columnar storage modes

---

### Q9. How do you handle data in multiple cloud regions with compliance requirements?

**Architecture considerations:**
```
Region-Local Processing:
├── Each region has its own landing zone (ADLS EU West, ADLS US East)
├── PII data processed and stored only in its originating region
├── Aggregate/anonymised data can cross regions
└── Encryption with region-specific CMKs in Azure Key Vault

Cross-Region Patterns:
├── Federated query: query regional stores without moving data
├── Data residency labels: tag each dataset with residency requirement
├── GDPR: EU data never leaves EU without explicit consent + SCCs
└── Replication only for non-sensitive aggregates (with legal review)

Implementation:
├── Azure Policy: deny storage account creation outside approved regions
├── Resource locks on cross-region replication settings
├── Audit logs: all cross-region data movement logged + reviewed
└── Data classification: auto-tag PII using Microsoft Purview + Azure ML
```

---

### Q10. Explain Databricks cluster types — when do you use each?

- **All-purpose cluster:** Interactive, multi-user, long-running. Use for: notebooks, development, exploration. Expensive to leave running 24/7 — always set auto-termination.
- **Job cluster:** Ephemeral — created for a job, terminated on completion. Use for: production pipelines. Cost-efficient. New cluster per run ensures clean state.
- **SQL Warehouse:** Optimised for SQL analytics, BI tools (Tableau, Power BI). Auto-scales, serverless option. Use for: business analysts running SQL, BI dashboards.
- **Delta Live Tables cluster:** Managed by DLT framework. Don't configure manually.

**Spot/Preemptible instances:** Use for job clusters on non-time-critical batch jobs — 60-90% cheaper. Risk: spot interruptions. Databricks handles graceful checkpointing for interruptions.

**→ Follow-up: "What is cluster auto-scaling and what are its pitfalls?"** Databricks auto-scaling adds/removes workers based on task queue depth. Pitfall: scale-up has a ~2-3 minute lag (new node provisioning + Spark registration). For jobs with bursty parallelism, this lag means the burst is over before new nodes arrive. Solution: pre-provision a larger cluster for known burst patterns, or use instance pools (pre-warmed nodes).

---

### Q11. What is Apache Kafka's exactly-once semantics and how does it work?

**Three delivery guarantees:**
- **At-most-once:** Fire and forget. Messages can be lost. Fastest.
- **At-least-once:** Retry on failure. Messages might be duplicated. Common default.
- **Exactly-once:** Every message delivered and processed exactly once. Most complex.

**How Kafka achieves exactly-once:**
```python
# Producer side — idempotent producer (deduplicates retries by sequence number)
producer_config = {
    "enable.idempotence": True,    # assigns sequence numbers per partition
    "acks": "all",                  # all ISR must ack
    "max.in.flight.requests.per.connection": 5,
    "retries": 2147483647,
}

# Transactional producer (atomically write to multiple partitions)
producer.init_transactions()
producer.begin_transaction()
producer.produce("topic-a", key="k1", value="v1")
producer.produce("topic-b", key="k2", value="v2")
producer.commit_transaction()  # atomic — both or neither

# Consumer side — read committed only
consumer_config = {
    "isolation.level": "read_committed",  # only see committed transactional messages
}
```

**End-to-end exactly-once (Kafka + Spark + Delta):**
Spark Structured Streaming + Delta Lake provides end-to-end exactly-once: Spark tracks offsets in checkpoint, Delta ACID ensures no duplicate writes even if the job restarts mid-batch.

---

### Q12. What is Cloud Composer (managed Airflow on GCP)? What are its operational challenges?

Cloud Composer is GKE-hosted, fully managed Apache Airflow. Google manages Airflow scheduler, workers, webserver, and the underlying Kubernetes cluster.

**Key configurations:**
```python
# Connecting Cloud Composer to GCP services
# GCS connection — auto-configured via Workload Identity
# BigQuery operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

bq_task = BigQueryInsertJobOperator(
    task_id="run_bq_transform",
    configuration={
        "query": {"query": "SELECT ...", "useLegacySql": False}
    },
    gcp_conn_id="google_cloud_default",  # uses Workload Identity
)
```

**Operational challenges:**
1. **Environment upgrades:** Composer upgrades take 30-60 minutes — plan around maintenance windows
2. **Worker scaling lag:** Like all Kubernetes-based scaling — 2-3 min for new pods
3. **Cost:** Always-on cluster costs ~$300-1000/month even with no DAGs running
4. **Debugging:** Logs across multiple GKE pods — use Cloud Logging for unified view
5. **DAG serialization:** Complex Python imports slow DAG parsing — keep DAG files lightweight

---

### Q13. How do you implement data sharing across teams/organizations using cloud-native tools?

**Options:**

1. **Delta Sharing (open protocol, works across clouds):**
```python
# Provider shares a Delta table
spark.sql("""
    CREATE SHARE retail_data_share;
    ADD TABLE delta.`/delta/transactions` TO SHARE retail_data_share;
    CREATE RECIPIENT partner_org USING ID 'partner-org-id';
    GRANT SELECT ON SHARE retail_data_share TO RECIPIENT partner_org;
""")
# Consumer reads without copying data
```

2. **BigQuery Analytics Hub:** Create Data Exchanges — publish/subscribe to BigQuery datasets across projects/orgs.

3. **Azure Purview Data Sharing:** Cross-tenant sharing with governance controls.

4. **Snowflake Secure Data Sharing:** Live access to data without copying — consumers query directly.

**Best practice:** Data sharing should be governed — log all access, apply column masking to shared PII columns, set expiry on shares, require recipient to agree to data use terms.

---

### Q14. What is Event-Driven Architecture and how do you implement it for data pipelines?

Instead of scheduled batch jobs, pipelines trigger in response to events:

```
File arrives in ADLS → Event Grid notification → Triggers ADF pipeline
                                                → Triggers Azure Function for validation
                                                → Triggers Databricks job via REST API

Message on Kafka topic → Spark Structured Streaming reads continuously
                      → Delta Live Tables auto-triggers on new data
```

```python
# Azure Event Grid trigger in ADF — pipeline starts when file lands
{
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/data/landing/transactions/",
    "events": ["Microsoft.Storage.BlobCreated"],
    "scope": "/subscriptions/.../storageAccounts/myaccount"
  }
}

# Airflow FileSensor (polling — less ideal than event-driven)
sensor = FileSensor(
    task_id="wait_for_file",
    filepath="/data/landing/transactions/{{ ds }}/",
    mode="reschedule",  # don't hold a worker slot while waiting
    poke_interval=300,  # check every 5 minutes
    timeout=7200,       # fail after 2 hours
)
```

**→ Follow-up: "FileSensor poke mode vs reschedule mode — why does it matter at scale?"** Poke mode: the sensor task holds its worker slot the entire time it's waiting. If you have 100 sensors all waiting, they occupy 100 worker slots. Reschedule mode: the task releases its slot between checks — only holds it during the actual check (seconds). For large pipelines with many sensors, reschedule mode is critical to avoid worker exhaustion.

---

### Q15–Q30: Remaining Cloud Questions (Condensed with Full Answers)

---

### Q15. What is GCP Dataflow and when would you choose it over Spark?

Dataflow is a fully managed Apache Beam service. Beam's unified API handles both batch and streaming with the same code — just change the runner. Key advantage: truly serverless auto-scaling (no cluster management). Choose Dataflow over Spark when: you need true serverless (no cluster sizing), your team knows Beam, you're all-in on GCP, or you need the Beam ecosystem (connectors). Choose Spark when: team has Spark expertise, you need Delta Lake, you're on Databricks/Azure, or you need MLlib.

**→ Follow-up:** "What is the beam programming model?" You define a Pipeline with PCollections (data) and PTransforms (operations like ParDo, GroupByKey, Combine). The same pipeline code runs on Dataflow (GCP), Spark, Flink, or locally — just swap the runner.

---

### Q16. How do you implement a data quality SLA dashboard on cloud?

```python
# Pipeline logs quality metrics to a Delta table after each run
def log_quality_metrics(table_name, run_date, metrics):
    spark.createDataFrame([{
        "table_name": table_name, "run_date": run_date,
        "total_rows": metrics["total_rows"],
        "null_pct": metrics["null_pct"],
        "duplicate_pct": metrics["duplicate_pct"],
        "freshness_hours": metrics["freshness_hours"],
        "passed": metrics["passed"]
    }]).write.format("delta").mode("append").save("/delta/quality_metrics")

# Databricks SQL dashboard queries this table
# Grafana/Power BI connects to Databricks SQL endpoint for visualization
# Alerting: Databricks SQL Alerts trigger PagerDuty/Slack on threshold breach
```

---

### Q17. What is Snowflake's micro-partition architecture?

Snowflake stores data in **micro-partitions**: immutable, compressed columnar files of 50–500 MB. Each micro-partition stores metadata (min/max per column). Query pruning skips micro-partitions that can't contain matching rows based on these statistics — called **micro-partition pruning**. Clustering keys improve pruning by sorting data so similar values are in the same micro-partitions. Unlike traditional partitioning, you don't need to explicitly partition at creation time — clustering can be added/changed later.

---

### Q18. How does Kubernetes help in data engineering (K8s for data)?

1. **KubernetesExecutor in Airflow:** Each task = one pod — isolation, resource limits, custom images per task.
2. **Spark on Kubernetes:** `spark.master = k8s://...` — Spark submits executor pods dynamically. No persistent Spark cluster — truly ephemeral.
3. **Debezium connectors in Kafka Connect:** Run as K8s deployments with resource limits.
4. **Isolation:** Test pipelines in dedicated namespaces — no interference with production.

**→ Follow-up:** "What is a resource quota in Kubernetes and why does it matter for data pipelines?" Resource quotas limit CPU/memory per namespace. Without them, a rogue Spark job can consume all cluster resources, starving other pipelines. Always set `resources.limits` on Spark executor pods.

---

### Q19. What is dbt and how does it fit into the modern data stack?

dbt (data build tool) handles the **T in ELT** — SQL-based transformations with software engineering best practices: version control, testing, documentation, modular models.

```sql
-- models/mart/customer_ltv.sql
{{ config(materialized='table', partition_by={'field': 'first_purchase_date', 'data_type': 'date'}) }}

WITH customer_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}  -- reference another dbt model
),
ltv AS (
    SELECT customer_id, SUM(amount) AS lifetime_value,
           MIN(order_date) AS first_purchase_date
    FROM customer_orders GROUP BY customer_id
)
SELECT * FROM ltv
```

```yaml
# schema.yml - data contracts + tests
models:
  - name: customer_ltv
    columns:
      - name: customer_id
        tests: [not_null, unique]
      - name: lifetime_value
        tests: [not_null, {dbt_utils.expression_is_true: {expression: ">= 0"}}]
```

**→ Follow-up:** "dbt vs Spark for transformations — when do you use which?" dbt is SQL-only — great for business logic transformations on data already in a DWH/lakehouse. Spark handles complex transformations that need Python (ML feature engineering, custom parsing, very large dataset processing). In practice: use Spark for ingestion + heavy processing, dbt for business-logic transformations close to the serving layer.

---

### Q20. How do you implement a multi-hop (medallion) architecture on Databricks?

```python
# Bronze: raw, immutable, exactly as received
@dlt.table(name="bronze_transactions", 
           table_properties={"quality": "bronze"})
def bronze():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("abfss://landing@storage.dfs.core.windows.net/transactions/")

# Silver: cleansed, validated, typed
@dlt.table(name="silver_transactions")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 1000000")
@dlt.expect_or_drop("valid_date", "transaction_date IS NOT NULL")
def silver():
    return (dlt.read_stream("bronze_transactions")
        .select(
            col("transaction_id").cast("string"),
            col("amount").cast("double"),
            to_timestamp(col("transaction_date")).alias("transaction_date"),
            col("store_id"),
            current_timestamp().alias("processed_at")
        ).dropDuplicates(["transaction_id"]))

# Gold: business aggregates, ready for analytics
@dlt.table(name="gold_daily_store_sales")
def gold():
    return (dlt.read("silver_transactions")
        .groupBy(to_date("transaction_date").alias("date"), "store_id")
        .agg(sum("amount").alias("total_sales"), count("*").alias("transaction_count")))
```

---

### Q21. How do you handle multi-cloud or hybrid cloud data architectures?

**Key patterns:**
1. **Apache Iceberg as table format:** Vendor-neutral, works across AWS (Athena), GCP (BigQuery), Azure (Synapse). Store data in one cloud, query from any engine.
2. **Delta Sharing:** Share live Delta tables across clouds without copying.
3. **Separate by workload:** Operational data in Azure (enterprise agreement), ML training in GCP (Vertex AI + TPUs), archival in AWS (Glacier).
4. **Data replication:** Apache Kafka MirrorMaker 2 for cross-cloud topic replication. Azure Data Factory for cross-cloud data movement.

---

### Q22. What is Azure Synapse Analytics? When would you use it vs Databricks?

Azure Synapse combines a dedicated SQL pool (MPP data warehouse), serverless SQL pool (query data lake with SQL), and Apache Spark pool in one workspace.

**Use Synapse when:** Your team is SQL-heavy (analysts, not engineers), you want one tool that covers SQL analytics + Spark + pipeline orchestration, you're doing classic EDW workloads.

**Use Databricks when:** You need Delta Lake + Unity Catalog, ML/MLflow integration, streaming pipelines, or superior Spark performance and ecosystem.

**Both together:** Synapse for SQL analytics and BI, Databricks for data engineering and ML — connected via Azure Data Lake storage as the shared layer.

---

### Q23. How do you optimise Spark jobs running on Kubernetes vs YARN?

**YARN (Hadoop cluster):**
- Dynamic resource allocation well-tested on YARN
- Container reuse between tasks — faster startup
- Simpler networking — all in one cluster

**Kubernetes:**
- Each executor = a pod: overhead per-task for pod creation
- Solution: `spark.kubernetes.executor.podTemplateFile` for pre-configured images, instance pools for pre-warmed nodes
- Better isolation — namespace-level quotas
- Auto-scaling via KEDA (Kubernetes Event Driven Autoscaling)
- Node affinity for GPU nodes (ML workloads)

```yaml
# Pod template for Spark executors
spec:
  containers:
    - name: spark-executor
      resources:
        requests: {cpu: "2", memory: "8Gi"}
        limits: {cpu: "4", memory: "16Gi"}
      volumeMounts:
        - name: tmp, mountPath: /tmp  # local SSD for shuffle
```

---

### Q24. What is GDPR's right to erasure and how do you implement it in a data lake?

The right to be forgotten: when a user requests deletion of their personal data, you must delete it from ALL systems including backups and data lakes.

```python
# Delta Lake: targeted delete + vacuum
from delta.tables import DeltaTable

# Step 1: Hard delete from current data
DeltaTable.forPath(spark, "/delta/customers") \
    .delete(f"customer_id = '{customer_id}'")

# Step 2: Vacuum to remove physical files (removes old versions containing PII)
spark.sql(f"VACUUM delta.`/delta/customers` RETAIN 0 HOURS")
# WARNING: disables time travel for this table — need to accept this trade-off

# Better approach: pseudonymisation
# Store PII in a separate, erasable mapping table
# Main data lake stores only pseudonymous IDs
# On erasure: delete from mapping table — PII is inaccessible without it
```

---

### Q25. How do you implement CI/CD for data pipelines?

```
Git Push → GitHub Actions / Azure DevOps
  ├── Lint: flake8, black (Python), sqlfluff (SQL)
  ├── Unit Tests: pytest with local SparkSession
  ├── Integration Tests: Databricks test cluster with test data
  ├── dbt tests: dbt test on test schema
  ├── Security scan: bandit, checkov (IaC)
  └── Deploy:
      ├── Dev: auto-deploy on PR merge to dev branch
      ├── Staging: auto-deploy on merge to main, run full regression
      └── Prod: manual approval gate, blue-green deployment
```

```yaml
# .github/workflows/pipeline-ci.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: pip install -r requirements.txt
      - run: pytest tests/unit/ -v --cov=src
      - run: flake8 src/
      - name: Run dbt tests
        run: dbt test --target ci --profiles-dir ./profiles
  deploy-staging:
    needs: test
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy to Databricks staging
        run: databricks bundle deploy --target staging
```

---

### Q26. What is Apache Iceberg and how does it compare to Delta Lake?

| Feature | Delta Lake | Apache Iceberg |
|---|---|---|
| Transaction log | JSON files in `_delta_log/` | Metadata JSON + manifest files |
| Multi-engine | Spark-native, others via connector | Spark, Flink, Presto, Trino, Hive, BigQuery natively |
| Schema evolution | Add/rename columns, drop | Full DDL (add, rename, drop, reorder, type widening) |
| Partition evolution | Limited | True partition evolution — change scheme without rewrite |
| Time travel | Version + timestamp | Snapshot IDs |
| Vendor | Databricks-driven | Apache Foundation (vendor neutral) |

**Choose Iceberg when:** Multi-cloud, multi-engine environment (query same data from Spark AND Athena AND BigQuery). Choose Delta when: Databricks-centric stack, need Unity Catalog, best-in-class Spark performance.

---

### Q27. How do you monitor cloud data pipelines in production?

```python
# Layered monitoring:

# 1. Infrastructure: Azure Monitor / GCP Cloud Monitoring
# CPU, memory, disk, network on all services

# 2. Pipeline metrics: custom metrics to Azure Monitor via SDK
from azure.monitor.opentelemetry import configure_azure_monitor
configure_azure_monitor(connection_string="...")

# 3. Data freshness: check latest partition timestamp
latest_partition = spark.sql("SELECT MAX(processing_date) FROM delta.`/delta/transactions`").first()[0]
freshness_hours = (datetime.now() - latest_partition).seconds / 3600
if freshness_hours > 2:
    send_pagerduty_alert(f"Transactions data is {freshness_hours:.1f}h stale")

# 4. Row count anomaly detection: flag unusual deviations
today_rows = get_row_count(today)
avg_rows = get_7day_avg_rows()
if abs(today_rows - avg_rows) / avg_rows > 0.2:  # >20% deviation
    send_alert("Row count anomaly detected")

# 5. Airflow: task duration SLA monitoring
task = PythonOperator(..., sla=timedelta(hours=2), 
                      on_sla_miss=send_slack_alert)
```

---

### Q28. What is GCS vs ADLS Gen2 — key differences for Spark workloads?

Both are object stores for data lake use cases. Key differences for Spark:

**ADLS Gen2:** Hierarchical namespace enables atomic directory rename (critical for Hadoop/Spark write-then-rename pattern), true POSIX ACLs, slightly better performance for Spark HNS operations. Protocol: `abfss://`.

**GCS:** No hierarchical namespace by default (objects with `/` in name simulate directories). Directory rename is NOT atomic — implemented as copy + delete. GCS connector uses a different approach. Protocol: `gs://`. GCS often slightly cheaper storage cost.

**Practical impact:** For Delta Lake or pipelines using directory rename for atomicity, ADLS Gen2 is superior. For BigQuery-centric GCP pipelines, GCS is natural.

---

### Q29. How do you handle schema drift in a cloud landing zone?

Schema drift = incoming data has new, missing, or type-changed columns compared to expected.

```python
# Strategy 1: Schema-on-read with drift detection
def ingest_with_drift_detection(incoming_df, expected_schema):
    # Find new columns
    new_cols = set(incoming_df.columns) - set(expected_schema.fieldNames())
    missing_cols = set(expected_schema.fieldNames()) - set(incoming_df.columns)
    
    if new_cols:
        alert(f"New columns detected: {new_cols}. Review before adding to schema.")
        incoming_df = incoming_df.drop(*new_cols)  # drop until reviewed
    
    if missing_cols:
        for col_name in missing_cols:
            incoming_df = incoming_df.withColumn(col_name, F.lit(None))  # fill with null
    
    return incoming_df.select(expected_schema.fieldNames())

# Strategy 2: Delta Lake schema evolution (controlled)
# Only allow additive changes automatically; breaking changes require manual approval
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# New columns added automatically; type changes fail (require manual migration)
```

---

### Q30. How do you design for disaster recovery in a cloud data platform?

```
RPO (Recovery Point Objective): max data loss acceptable
RTO (Recovery Time Objective): max downtime acceptable

DR Strategies by tier:

Tier 1 (RPO=0, RTO<1hr): Critical regulatory pipelines
├── Active-active: run in two regions simultaneously
├── Synchronous replication of Delta logs
└── Azure Traffic Manager / GCP Global Load Balancer for failover

Tier 2 (RPO<1hr, RTO<4hr): Core analytics pipelines  
├── Active-passive: primary region active, secondary warm
├── Asynchronous ADLS geo-redundant storage (GRS) replication
└── Databricks jobs replicated to secondary workspace
    
Tier 3 (RPO<24hr, RTO<24hr): Non-critical data products
├── Daily backup to secondary region
├── Delta Lake time travel for point-in-time recovery
└── Terraform for infrastructure-as-code fast rebuild

Testing: quarterly DR drills — actually failover to secondary and verify
```

---

---

# 🔄 Area 4: End-to-End Pipeline Architecture & Ingestion (30 Questions)

---

### Q1. Walk me through the layers of a production data pipeline end-to-end.

**Answer:**
```
Source Systems
    ↓
[1. INGESTION LAYER]
    - File-based: SFTP/S3 drops, picked up by Airflow FileSensor
    - API-based: REST polling with pagination, rate limiting
    - Streaming: Kafka/Event Hubs topic consumers
    - CDC: Debezium reading database transaction logs
    ↓
[2. LANDING / BRONZE LAYER]
    - Exact copy of source data — no transformation
    - Immutable: never overwritten
    - Partitioned by: source_system, ingest_date
    - Format: raw JSON/CSV/Avro as received, or Parquet for columnar
    - Bad records written to quarantine path with error metadata
    ↓
[3. STAGING / SILVER LAYER]
    - Schema enforcement and type casting
    - Deduplication (event_id + updated_at logic)
    - Null handling and standardisation (phone formats, addresses)
    - Joining with reference data (store names, product categories)
    - Data quality checks — Great Expectations / dbt tests
    - Partitioned by: business_date, region
    ↓
[4. CURATED / GOLD LAYER]
    - Business-ready aggregates
    - Star/snowflake schema for BI tools
    - Pre-computed metrics (YTD totals, rolling averages)
    - Optimised for query patterns (Z-ordered, clustered)
    - dbt transformations with full test coverage
    ↓
[5. SERVING LAYER]
    - BI: Databricks SQL / Snowflake / BigQuery → Tableau / Power BI
    - API: FastAPI serving Gold tables via REST
    - ML: Feature store (Databricks Feature Store) for model training + serving
    - Reverse ETL: Hightouch/Census pushing segments back to Salesforce/Ads
```

**→ Follow-up: "Why keep Bronze immutable?"** You need a reprocessing capability — if there's a bug in Silver or Gold logic, you need to be able to replay from Bronze without going back to the source. Sources often don't retain historical data. Bronze is your insurance policy.

---

### Q2. What are the different ingestion patterns and when do you use each?

**1. Full Load:**
- Copies entire source table every run
- Simple but expensive for large tables
- Use when: table is small, source doesn't support incremental, need full refresh for correctness

**2. Incremental/Delta Load:**
- Only loads new/changed records since last run
- Use when: source has a reliable `updated_at` timestamp
- Risk: misses hard deletes, clock drift between systems

**3. CDC (Change Data Capture):**
- Reads database transaction log — captures inserts, updates, AND deletes
- Use when: you need a complete faithful replica, source is a transactional DB
- Tools: Debezium (log-based), Attunity, Fivetran

**4. Event Streaming:**
- Continuous ingestion from Kafka/Event Hubs
- Sub-minute latency
- Use when: real-time or near-real-time is required

**5. API Polling:**
- Periodic calls to REST API
- Handle pagination, rate limits, backoff
- Use when: source only exposes HTTP API

```python
# Incremental load pattern with watermark
def incremental_load(last_run_ts: datetime):
    query = f"""
        SELECT * FROM source_table 
        WHERE updated_at > '{last_run_ts.isoformat()}'
        AND updated_at <= NOW()
    """
    df = read_from_source(query)
    current_max_ts = df.agg(F.max("updated_at")).first()[0]
    
    df.write.format("delta").mode("append") \
        .partitionBy("business_date") \
        .save("/delta/silver/table")
    
    # Save watermark for next run
    save_watermark("silver.table", current_max_ts)
```

**→ Follow-up: "What are the dangers of timestamp-based incremental loads?"**
1. Misses hard deletes — source row deleted but not captured
2. Clock drift — source DB and pipeline server clocks differ
3. Updated_at not always reliable — some systems don't update it on all changes
4. Late updates — a record updated after pipeline cutoff is missed until next run

---

### Q3. How do you design an idempotent ingestion pipeline?

Idempotency = running the same pipeline multiple times for the same input produces the same result — no duplicates, no data loss.

```python
def run_ingestion(processing_date: str, source: str):
    """Fully idempotent — safe to re-run any number of times"""
    
    # Step 1: Check if already successfully completed
    if is_already_complete(processing_date, source):
        log.info(f"Already complete for {processing_date}. Skipping.")
        return
    
    # Step 2: Read source data
    df = read_source(processing_date, source)
    
    # Step 3: Write to staging partition (isolated from production)
    staging_path = f"/staging/{source}/{processing_date}/"
    df.write.mode("overwrite").parquet(staging_path)
    
    # Step 4: Validate staging output
    validate(staging_path, expected_min_rows=1000)
    
    # Step 5: Atomic swap to production (using dynamic partition overwrite)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.read.parquet(staging_path) \
        .write.mode("overwrite") \
        .partitionBy("processing_date") \
        .parquet("/delta/bronze/transactions/")
    
    # Step 6: Mark as complete
    mark_complete(processing_date, source)
    
    # Step 7: Cleanup staging
    cleanup_staging(staging_path)
```

**→ Follow-up: "What happens if the pipeline fails between step 5 and step 6?"** On re-run, step 1 returns False (not marked complete), so it re-runs from scratch. Step 5 overwrites the same partition again (idempotent due to `mode("overwrite")`). This is exactly the desired behaviour — the pipeline is safe to retry at any point.

---

### Q4. How do you design an Airflow DAG for a complex multi-dependency pipeline?

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "on_failure_callback": send_pagerduty_alert,
    "sla": timedelta(hours=4),  # alert if task hasn't completed in 4h
    "on_sla_miss": send_sla_alert,
}

with DAG(
    dag_id="retail_daily_pipeline",
    schedule_interval="0 6 * * *",  # 6 AM daily
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,  # prevent concurrent runs for same date
    tags=["retail", "daily"],
) as dag:
    
    # Wait for upstream pipeline from yesterday
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_cdc_pipeline",
        external_dag_id="cdc_ingestion",
        external_task_id="validate_output",
        execution_delta=timedelta(hours=0),
        mode="reschedule",  # critical — don't hold worker slot
        timeout=7200,
    )
    
    # Fan out — parallel tasks
    ingest_stores = PythonOperator(task_id="ingest_stores", python_callable=ingest_stores_fn)
    ingest_products = PythonOperator(task_id="ingest_products", python_callable=ingest_products_fn)
    ingest_transactions = PythonOperator(task_id="ingest_transactions", python_callable=ingest_transactions_fn)
    
    # Fan in — depends on all three
    build_silver = PythonOperator(task_id="build_silver", python_callable=build_silver_fn)
    
    build_gold_sales = PythonOperator(task_id="build_gold_sales", python_callable=build_gold_sales_fn)
    build_gold_customer = PythonOperator(task_id="build_gold_customer", python_callable=build_gold_customer_fn)
    
    validate_output = PythonOperator(task_id="validate_output", python_callable=validate_fn)
    notify_downstream = PythonOperator(task_id="notify_downstream", python_callable=notify_fn)
    
    # Dependency graph
    wait_for_upstream >> [ingest_stores, ingest_products, ingest_transactions]
    [ingest_stores, ingest_products, ingest_transactions] >> build_silver
    build_silver >> [build_gold_sales, build_gold_customer]
    [build_gold_sales, build_gold_customer] >> validate_output >> notify_downstream
```

**→ Follow-up: "What is `catchup` and when do you set it to True vs False?"** `catchup=True` (Airflow default): When a DAG is first enabled (or re-enabled after a pause), Airflow will run all missed DAG runs from `start_date` to now. Useful for backfilling historical data. `catchup=False`: Only runs the most recent scheduled interval. Use for production pipelines where missed runs shouldn't auto-backfill (backfill manually if needed to avoid flooding the system).

---

### Q5. How do you implement pipeline monitoring and alerting?

**Three-tier approach:**

**Tier 1 — Task-level (Airflow):**
```python
# Every DAG has failure alerts
default_args = {
    "on_failure_callback": lambda context: send_slack_alert(
        f"Task {context['task_instance'].task_id} failed in DAG "
        f"{context['task_instance'].dag_id} on {context['ds']}"
    ),
    "sla": timedelta(hours=2),
}
```

**Tier 2 — Data-level (Quality checks in pipeline):**
```python
def validate_output(table_path, date):
    df = spark.read.format("delta").load(table_path)
    metrics = df.filter(col("processing_date") == date).agg(
        count("*").alias("row_count"),
        countDistinct("transaction_id").alias("unique_txns"),
        sum(col("amount").isNull().cast("int")).alias("null_amounts")
    ).first()
    
    assert metrics.row_count >= EXPECTED_MIN_ROWS, f"Too few rows: {metrics.row_count}"
    assert metrics.null_amounts / metrics.row_count < 0.01, "Null rate too high"
```

**Tier 3 — Business-level (Anomaly detection):**
```python
# Compare today vs 7-day average — alert on >20% deviation
today = get_daily_metric(date)
avg_7d = get_7day_avg(date)
if abs(today - avg_7d) / avg_7d > 0.20:
    send_pagerduty(f"Revenue anomaly: {today} vs avg {avg_7d}")
```

---

### Q6. What is exactly-once delivery semantics in a pipeline? How do you achieve it?

**Exactly-once** = every event/record is processed and delivered exactly one time — no duplicates, no loss.

**Why it's hard:** Networks fail. Processes crash. You can't know if your last write succeeded before the crash.

**Practical approaches:**

1. **Idempotent writes + at-least-once delivery:** Write same message N times but result is same as writing once (UPSERT/MERGE on unique key). This is how 99% of production pipelines achieve "effectively exactly-once."

2. **Two-phase commit:** Coordinate transaction across producer + broker + consumer. Expensive but truly atomic. Kafka's transactional API does this.

3. **Kafka + Spark Structured Streaming + Delta:** Spark checkpoints Kafka offsets; Delta ACID ensures each batch writes atomically. If job crashes mid-batch: restart reads same offsets, writes same data — Delta MERGE handles idempotency.

```python
# Exactly-once with Kafka + Delta:
def upsert_batch(batch_df, batch_id):
    DeltaTable.forPath(spark, "/delta/events").alias("t") \
        .merge(batch_df.dropDuplicates(["event_id"]).alias("s"),
               "t.event_id = s.event_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

stream.writeStream \
    .option("checkpointLocation", "/checkpoints/events") \
    .foreachBatch(upsert_batch) \
    .start()
```

**→ Follow-up: "Is true exactly-once ever possible in a distributed system?"** Theoretically yes (with 2PC + idempotent operations), practically you trade off: performance decreases as consistency guarantees increase. Most production systems accept "effectively exactly-once" via idempotent writes as a pragmatic compromise.

---

### Q7. How does Debezium work for CDC? What are the failure modes?

Debezium is an open-source CDC platform that reads database **transaction logs** (Oracle redo log, Postgres WAL, MySQL binlog, SQL Server CDC).

```
Architecture:
Source DB ─[transaction log]─→ Debezium Connector ─→ Kafka Topic ─→ Consumers

Change event structure (Debezium envelope):
{
  "before": { "id": 1, "name": "Alice", "email": "old@email.com" },
  "after":  { "id": 1, "name": "Alice", "email": "new@email.com" },
  "source": { "db": "inventory", "table": "customers", "ts_ms": 1704067200000 },
  "op": "u",  // c=create, u=update, d=delete, r=snapshot read
  "ts_ms": 1704067200000
}
```

**Failure modes:**
1. **Log position lost:** If the Kafka connector offset is lost, Debezium must re-snapshot (expensive). Mitigation: reliable Kafka offset storage, regular Zookeeper backups.
2. **DB log truncated before Debezium reads it:** DBAs may purge logs on a schedule. Coordinate log retention with Debezium lag. Mitigation: monitor Debezium lag, alert when it approaches log retention window.
3. **Schema change on source:** DDL changes (ALTER TABLE) can break Debezium. Mitigation: use schema registry, configure `schema.history.internal.kafka.topic`.
4. **Kafka consumer group rebalance:** During rebalance, processing pauses. Mitigation: cooperative rebalancing (incremental), tune session timeout.

---

### Q8. How do you build a pipeline that handles late-arriving data?

**Definition:** Late data = data that arrives after the processing window has already closed (e.g., a transaction from yesterday arriving today).

**Strategies:**
```python
# Strategy 1: Watermarking (Spark Structured Streaming)
# Accept late data up to 2 hours, then drop
(df.withWatermark("event_time", "2 hours")
   .groupBy(window("event_time", "1 hour"), "store_id")
   .agg(sum("amount")))

# Strategy 2: Lambda — reconciliation batch job
# Streaming captures ~99% real-time; daily batch re-processes with full late data
# Downstream uses streaming results with daily correction

# Strategy 3: Event time reprocessing
# Partition by event_date (when the event happened), not ingest_date
# Late data lands in correct historical partition automatically
# Downstream refreshes affected partition only
df.write.partitionBy("event_date")  # NOT "ingest_date"
   .mode("overwrite") \
   .save("/delta/transactions/")
# Dynamic partition overwrite handles this cleanly

# Strategy 4: Correction records
# Source system emits correction events — pipeline applies corrections
# to already-processed records via MERGE
```

**→ Follow-up: "For a retail daily sales report, how do you handle a store's transactions arriving 3 hours late due to connectivity issues?"** Option A: Soft SLA — wait up to 6 hours before closing the day's report. Airflow sensor waits for completeness signal (row count vs. manifest). Option B: Publish preliminary report, then re-publish when late data arrives (flag as "revised"). Option C: Accept the miss — close the day on time, late data goes into next day's reconciliation run with a backdated correction.

---

### Q9. How do you implement a file-based ingestion pipeline with data validation?

```python
import great_expectations as ge
from airflow.sensors.filesystem import FileSensor

# Step 1: Sensor — wait for file and verify completeness
def check_file_completeness(file_path, manifest_path):
    """Verify file row count matches manifest before processing"""
    manifest = read_manifest(manifest_path)
    actual_rows = count_rows(file_path)
    if actual_rows != manifest["expected_rows"]:
        raise ValueError(f"Row count mismatch: expected {manifest['expected_rows']}, got {actual_rows}")

# Step 2: Schema validation before loading
def validate_schema(df, expected_schema):
    """Fail fast if schema doesn't match"""
    for field in expected_schema:
        if field.name not in df.columns:
            raise SchemaError(f"Missing column: {field.name}")
    actual_types = {f.name: f.dataType for f in df.schema}
    for field in expected_schema:
        if not isinstance(actual_types.get(field.name), type(field.dataType)):
            raise SchemaError(f"Type mismatch: {field.name}")

# Step 3: Business rule validation with Great Expectations
def run_quality_checks(df):
    ge_df = ge.from_pandas(df.toPandas())  # for small DataFrames
    results = ge_df.expect_column_values_to_not_be_null("transaction_id")
    results &= ge_df.expect_column_values_to_be_between("amount", min_value=0, max_value=1e7)
    results &= ge_df.expect_column_values_to_be_in_set("currency", ["USD", "EUR", "GBP"])
    if not results["success"]:
        raise DataQualityError(results.to_json_dict())

# Step 4: Quarantine bad records, load good records
def load_with_quarantine(df, target_path, quarantine_path):
    good = df.filter(col("_corrupt_record").isNull())
    bad = df.filter(col("_corrupt_record").isNotNull())
    bad.withColumn("error_time", current_timestamp()) \
       .write.mode("append").json(quarantine_path)
    good.drop("_corrupt_record") \
        .write.format("delta").mode("append") \
        .partitionBy("processing_date").save(target_path)
    log.info(f"Loaded {good.count()} good, quarantined {bad.count()} bad records")
```

---

### Q10. What is a DAG in Airflow and what makes a good production DAG?

**DAG (Directed Acyclic Graph):** A directed graph of tasks with no cycles — defines both the tasks to run and their dependency order. "Directed" = dependencies have direction (A → B). "Acyclic" = no circular dependencies.

**Production DAG best practices:**
```python
# 1. Use task groups for organisation
from airflow.utils.task_group import TaskGroup

with TaskGroup("ingestion") as ingestion:
    t1 = PythonOperator(task_id="ingest_transactions", ...)
    t2 = PythonOperator(task_id="ingest_stores", ...)

with TaskGroup("transformation") as transformation:
    t3 = PythonOperator(task_id="build_silver", ...)

ingestion >> transformation

# 2. Keep DAG files lightweight — no heavy imports at module level
# (Airflow parses ALL DAG files every 30s by default)
# BAD: from pyspark.sql import SparkSession at top of DAG file
# GOOD: import inside the python_callable function

# 3. Use templated variables for date parameterisation
def my_task(ds, **kwargs):  # ds = execution date string
    process_date(ds)

# 4. Set meaningful task_ids — they appear in logs and UI
# BAD: task_1, task_2
# GOOD: ingest_transactions_bronze, validate_silver_schema

# 5. Max active tasks to prevent overloading target systems
dag = DAG(..., max_active_tasks=4)  # at most 4 tasks run simultaneously
```

---

### Q11. What is the difference between Airflow's `execution_date` and the actual run time?

`execution_date` is the **start of the scheduled interval** — not when the DAG actually runs. For a daily DAG scheduled at midnight: the run for `execution_date = 2024-01-01` actually triggers at `2024-01-02 00:00` (after the interval completes). This is confusing but intentional — it lets you process "data for 2024-01-01" with confidence the day is complete.

```python
# In operators: use execution_date macros for the data date
def process(ds, ds_nodash, **context):
    # ds = "2024-01-01" (execution_date as string)
    # ds_nodash = "20240101"
    path = f"s3://data/transactions/date={ds}/"
    
    # data_interval_start = execution_date
    # data_interval_end = next execution_date
    # run after data_interval_end has passed
```

**→ Follow-up: "What is `catchup` risk in production?"** If you pause a daily DAG for 30 days and re-enable it with `catchup=True`, Airflow queues 30 DAG runs simultaneously. This can overwhelm your infrastructure. Always set `catchup=False` for production and backfill manually with `airflow dags backfill` command with controlled concurrency.

---

### Q12–Q30: Remaining Pipeline Architecture Questions

---

### Q12. How do you design for pipeline fault tolerance — what happens when a step fails mid-run?

Design principles: (1) Every step must be **restartable** without side effects. (2) Use **idempotent writes** — partition overwrite or MERGE. (3) **Checkpoint progress** — record last successful step in a control table. (4) Fail at the **finest granularity** — don't fail the entire pipeline when one partition fails; quarantine it. (5) Use **dead letter queues** for unparseable records — don't block the main flow. (6) **Circuit breaker** for downstream API calls — after N consecutive failures, stop calling to prevent cascading.

**→ Follow-up:** "A batch of 1000 files fails on file #600. What's your approach?" Log the failed file + error to a control table. Process remaining 400 files. Run a reconciliation job that retries only failed files. Never reprocess already-successful files — idempotency + control table prevents this.

---

### Q13. What is the difference between a Lambda and Kappa architecture?

**Lambda:** Two parallel paths — batch layer (accurate but slow) + speed layer (fast but approximate). Serving layer merges both views. Problem: maintain two codebases for same business logic.

**Kappa:** Single streaming path handles everything. For reprocessing: replay from Kafka (requires sufficient retention). Simpler — one codebase. Problem: requires Kafka to retain full history or have a re-ingestion mechanism.

**My recommendation for dunnhumby:** Kappa with Delta Lake. Spark Structured Streaming + Delta handles both streaming and batch reprocessing in one codebase. Historical reprocessing replays from source or Delta Bronze (immutable). Eliminates dual-codebase maintenance.

---

### Q14. What are common causes of pipeline failures and how do you prevent them?

1. **Schema change on source:** Prevention — schema registry, drift detection at landing, fail-fast with alert to source team before writing.
2. **Source data volume spike:** Prevention — AQE, auto-scaling clusters, monitor input row counts vs baseline.
3. **Upstream pipeline delay:** Prevention — ExternalTaskSensor with timeout + fallback logic.
4. **Network timeout on external API:** Prevention — retry with backoff, circuit breaker, timeout settings.
5. **Disk space exhaustion:** Prevention — monitor shuffle disk usage, VACUUM Delta tables regularly, set TTL on staging paths.
6. **OOM on executor:** Prevention — tune memory settings, test with representative data volumes, use AQE.
7. **Airflow scheduler lag:** Prevention — limit DAG file complexity, use task groups, consider KubernetesExecutor for scale.

---

### Q15. How do you implement a data pipeline that serves both batch and real-time consumers?

**Unified Lakehouse approach:**
```
Streaming: Kafka → Spark SS → Delta Lake (streaming writes)
Batch: Source files → Spark batch → Delta Lake (batch writes)
Same Delta table serves both:
  → Real-time consumers: read Delta with low latency (seconds with trigger.AvailableNow)
  → Batch consumers: scheduled reads, read consistent snapshot via Delta time travel
  → BI tools: Databricks SQL with caching for sub-second dashboard queries
```

Delta's ACID ensures batch and streaming writers don't conflict. Consumers always read a consistent snapshot regardless of in-progress writes.

---

### Q16. What is a control table pattern in pipeline orchestration?

```sql
-- Pipeline control table: track state of each pipeline run
CREATE TABLE pipeline_control (
    pipeline_name    VARCHAR(100),
    processing_date  DATE,
    status           VARCHAR(20),  -- RUNNING, SUCCESS, FAILED, SKIPPED
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    rows_read        BIGINT,
    rows_written     BIGINT,
    error_message    TEXT,
    retry_count      INT DEFAULT 0
);

-- At start of run:
INSERT INTO pipeline_control VALUES ('transactions_silver', '2024-01-01', 'RUNNING', NOW(), NULL, ...)

-- At end of successful run:
UPDATE pipeline_control SET status='SUCCESS', end_time=NOW(), rows_written=1234567
WHERE pipeline_name='transactions_silver' AND processing_date='2024-01-01';

-- Idempotency check:
SELECT status FROM pipeline_control 
WHERE pipeline_name='transactions_silver' AND processing_date='2024-01-01';
-- If 'SUCCESS' → skip. If 'FAILED' or NULL → run.
```

---

### Q17. How do you handle API rate limiting in an ingestion pipeline?

```python
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class RateLimitedAPIClient:
    def __init__(self, requests_per_minute: int):
        self.min_interval = 60.0 / requests_per_minute
        self.last_call = 0
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(RateLimitError)
    )
    def get(self, url: str, params: dict):
        # Enforce minimum interval
        elapsed = time.time() - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        
        response = requests.get(url, params=params)
        self.last_call = time.time()
        
        if response.status_code == 429:  # Too Many Requests
            retry_after = int(response.headers.get("Retry-After", 60))
            time.sleep(retry_after)
            raise RateLimitError(f"Rate limited, retry after {retry_after}s")
        
        response.raise_for_status()
        return response.json()

# For large backfills: use ThreadPoolExecutor with semaphore
from concurrent.futures import ThreadPoolExecutor
import threading

semaphore = threading.Semaphore(10)  # max 10 concurrent requests

def fetch_with_limit(page):
    with semaphore:
        return client.get(f"/api/data?page={page}")

with ThreadPoolExecutor(max_workers=20) as ex:
    results = list(ex.map(fetch_with_limit, range(1, total_pages + 1)))
```

---

### Q18. What is Apache NiFi and when would you use it vs Airflow?

**Apache NiFi:** Visual, drag-and-drop dataflow tool. Real-time data routing, transformation, and movement. Built-in processors for 300+ sources/targets. Excellent for: IoT ingestion, edge-to-cloud data movement, real-time routing with guaranteed delivery, non-technical teams building simple flows.

**Use NiFi when:** Real-time data routing with guaranteed delivery needed, team is less technical, diverse source systems needing adapters, fine-grained data provenance required.

**Use Airflow when:** Complex workflow orchestration with business logic, Python-first team, need code review and testing, complex dependencies between tasks.

In practice: NiFi for the streaming data movement layer, Airflow for orchestrating batch pipeline logic. They complement each other.

---

### Q19. How do you backfill historical data in a production pipeline safely?

```python
# Controlled backfill — don't flood the cluster
# Airflow: use backfill command with concurrency limit
# airflow dags backfill -s 2024-01-01 -e 2024-03-31 --reset-dagruns -j 2 my_dag
# -j 2 = max 2 parallel dag runs at once

# In Python: process dates sequentially or in small batches
from datetime import date, timedelta

def backfill(start_date: date, end_date: date, batch_size: int = 7):
    current = start_date
    while current <= end_date:
        batch_end = min(current + timedelta(days=batch_size - 1), end_date)
        log.info(f"Processing {current} to {batch_end}")
        
        # Check if already done
        if is_complete(current, batch_end):
            log.info("Already complete, skipping")
        else:
            run_pipeline(current, batch_end)
        
        current = batch_end + timedelta(days=1)
        time.sleep(30)  # cool-down between batches
```

**→ Follow-up: "How do you communicate a backfill to downstream teams?"** (1) Announce the date range in advance. (2) Downstream pipelines should handle re-delivery gracefully (idempotent reads). (3) Flag backfilled data with `is_backfill=True` if needed. (4) Coordinate if downstream has caches or materialized views that need refreshing.

---

### Q20. What is a data product and how is it different from a data pipeline?

**Data pipeline:** A process — it runs, transforms data, and finishes. Focused on moving/transforming data.

**Data product:** A dataset treated as a product with a defined SLA, owner, quality metrics, schema contract, documentation, and versioning. Consumers rely on it with confidence. Key attributes:
- **Owner:** Domain team accountable for quality and freshness
- **SLA:** "Data available by 8 AM, ≥ 99.5% daily completeness"
- **Schema contract:** Breaking changes require versioning and migration period
- **Discoverability:** Listed in data catalog with business description
- **Quality metrics:** Published and monitored

**→ Follow-up:** "How does data mesh relate to data products?" Data mesh is an organisational pattern where each business domain owns their data products. Instead of a centralised data team owning everything, the retail domain owns retail data products, the loyalty domain owns loyalty data products. The central platform team provides the self-serve infrastructure (Delta Lake, Unity Catalog, Airflow).

---

### Q21. How do you handle duplicate records in an ingestion pipeline?

**Sources of duplicates:**
- At-least-once Kafka delivery (consumer reads same message twice after restart)
- Source system sends the same file twice
- Network retry causes double insert
- ETL re-run without cleanup

**Deduplication strategies:**
```python
# Strategy 1: Dedup in Spark before write (stateless, simple)
df.dropDuplicates(["event_id"])  # exact duplicate
df.dropDuplicates(["event_id", "event_time"])  # slightly different timestamps

# Strategy 2: Keep latest per key (prefer for CDC)
from pyspark.sql.window import Window
w = Window.partitionBy("event_id").orderBy(col("updated_at").desc())
df.withColumn("rn", F.row_number().over(w)).filter(col("rn") == 1).drop("rn")

# Strategy 3: Delta MERGE for idempotent write (handles cross-batch duplicates)
DeltaTable.forPath(spark, "/delta/events").alias("t") \
    .merge(df.alias("s"), "t.event_id = s.event_id") \
    .whenMatchedUpdate(condition="s.updated_at > t.updated_at", 
                       set={"updated_at": "s.updated_at", "payload": "s.payload"}) \
    .whenNotMatchedInsertAll() \
    .execute()

# Strategy 4: Bloom filter for large-scale fast dedup
# Check if event_id seen before without loading entire history
from pybloom_live import BloomFilter
seen = BloomFilter(capacity=1_000_000_000, error_rate=0.001)
```

---

### Q22. Explain the concept of a "data contract" and why it's important.

A data contract is a formal agreement between a data producer and its consumers about:
- **Schema:** Column names, types, nullability
- **Semantics:** What each column means (business definition)
- **SLA:** Freshness, completeness guarantees
- **Versioning:** How breaking changes are handled (notice period, migration support)

```yaml
# data-contract.yaml (open standard: datacontract.com)
dataContractSpecification: 0.9.3
id: transactions-daily-v2
info:
  title: Daily Transactions Gold Table
  version: 2.0.0
  owner: retail-data-team@company.com
  sla:
    availability: 99.5%
    freshness: 8:00 AM UTC daily

models:
  transactions_gold:
    fields:
      transaction_id:
        type: string
        required: true
        unique: true
      amount:
        type: double
        required: true
        minimum: 0

quality:
  - name: completeness
    type: sql
    query: "SELECT COUNT(*) FROM transactions_gold WHERE processing_date = CURRENT_DATE"
    mustBe: ">= 1000000"
```

**→ Follow-up:** "Who owns the data contract — producer or consumer?" The producer owns and publishes it, but it's negotiated with consumers. Producers must version any breaking changes and provide migration support. This is the core shift in data mesh — domain teams are accountable for their data product quality.

---

### Q23. What is a medallion architecture and what are its benefits over a single-layer data lake?

**Single-layer data lake problems:** Raw data mixed with curated data. No quality guarantees. Can't reprocess if transformation bug found. Hard to understand which data is production-ready.

**Medallion (Bronze/Silver/Gold):**
- **Bronze:** Trust nothing — exact source copy, audit trail. Never overwritten.
- **Silver:** Trust the structure — typed, deduped, validated. Reload from Bronze if Silver logic changes.
- **Gold:** Trust the semantics — business logic applied, ready for analytics. Reload from Silver if Gold logic changes.

**Benefits:** Each layer has a clear contract. Bugs can be fixed by reprocessing from the previous layer. Lineage is clear. Different teams access appropriate layers (analysts → Gold, data scientists → Silver, data engineers → Bronze).

---

### Q24. How do you handle timezone issues in a global data pipeline?

```python
# Always store timestamps in UTC — convert at serving layer
from pyspark.sql.functions import to_utc_timestamp, from_utc_timestamp, col

# Ingestion: convert source timezone to UTC
df = df.withColumn("event_time_utc", 
    to_utc_timestamp(col("event_time_local"), col("store_timezone")))

# Storage: always UTC
df.write.parquet("s3://data/events/")  # event_time_utc column

# Serving: convert to local timezone for reporting
df.withColumn("event_time_local", 
    from_utc_timestamp(col("event_time_utc"), "America/New_York"))

# Airflow scheduling: use pendulum for timezone-aware scheduling
import pendulum
with DAG(schedule="0 6 * * *", 
         start_date=pendulum.datetime(2024, 1, 1, tz="UTC")) as dag:
    ...

# Common pitfall: Daylight Saving Time transitions
# Spring forward: 1 hour is skipped — some "daily" pipelines run twice or miss an hour
# Fall back: 1 hour is repeated — records with same local time but different UTC
# Solution: always use UTC internally, DST is only a display concern
```

---

### Q25. How do you implement SLA tracking for a data pipeline?

```python
# SLA = data available by X time with Y completeness

# Method 1: Airflow SLA miss
from airflow.models import SlaMiss
task = PythonOperator(
    task_id="build_gold",
    sla=timedelta(hours=2),  # must complete within 2h of dag start
    on_sla_miss=notify_sla_team,
)

# Method 2: Separate SLA checker job
def check_sla(table, expected_date, sla_time, min_rows):
    """Runs after SLA deadline to verify compliance"""
    now = datetime.utcnow()
    if now < sla_time:
        return  # not yet at deadline
    
    latest = spark.sql(f"SELECT MAX(processing_date) FROM {table}").first()[0]
    row_count = spark.sql(f"SELECT COUNT(*) FROM {table} WHERE processing_date='{expected_date}'").first()[0]
    
    sla_met = (latest >= expected_date and row_count >= min_rows)
    
    log_sla_result(table, expected_date, sla_met, row_count, now)
    if not sla_met:
        send_pagerduty(f"SLA BREACH: {table} for {expected_date}")

# Method 3: Observability platform
# Write SLA metrics to Grafana/DataDog and set threshold alerts
```

---

### Q26. How do you manage pipeline configuration across dev/staging/prod environments?

```python
# config/environments.py
CONFIGS = {
    "dev": {
        "source_path": "abfss://dev@storage.dfs.core.windows.net/landing/",
        "target_path": "abfss://dev@storage.dfs.core.windows.net/bronze/",
        "spark_configs": {"spark.sql.shuffle.partitions": "10"},
        "enable_sampling": True,
        "sample_fraction": 0.01,
    },
    "prod": {
        "source_path": "abfss://prod@storage.dfs.core.windows.net/landing/",
        "target_path": "abfss://prod@storage.dfs.core.windows.net/bronze/",
        "spark_configs": {"spark.sql.shuffle.partitions": "400"},
        "enable_sampling": False,
    }
}

# Secrets: never in config files — always Key Vault / Databricks Secrets
db_password = dbutils.secrets.get(scope="kv-scope", key="db-password")

# Databricks: use environment-specific job configs in bundle
# databricks.yml
bundle:
  name: retail_pipeline
targets:
  dev:
    workspace: {host: "https://adb-dev.azuredatabricks.net"}
    variables: {env: "dev", max_workers: 4}
  prod:
    workspace: {host: "https://adb-prod.azuredatabricks.net"}
    variables: {env: "prod", max_workers: 20}
```

---

### Q27. What is schema-on-read vs schema-on-write?

**Schema-on-write (traditional DWH):** Schema defined BEFORE data is loaded. Data validated against schema on ingestion — rejects non-conforming data. Fast reads, rigid. Used in: Snowflake, Redshift, traditional databases.

**Schema-on-read (data lake):** Data stored as-is (JSON, CSV, Parquet). Schema applied when querying. Flexible but risky — schema errors discovered late, no quality guarantee at ingestion.

**Modern approach (lakehouse):** Schema-on-write with evolution. Delta Lake enforces schema at write time but allows controlled evolution. Best of both worlds: quality at ingestion + flexibility to evolve.

---

### Q28. How do you design a pipeline that processes 1 billion rows daily within a 2-hour window?

```
Constraints: 1B rows / 2 hours = ~139K rows/second throughput

Design:
1. Parallelism: 1B rows / (128 MB/partition × avg row size) → determine partition count
   If avg row = 1KB: 1B × 1KB = 1TB. At 128MB/partition = ~8000 partitions
   → Use ~200-400 executors × 20 cores = 4000-8000 parallel tasks

2. Pipeline stages:
   - Ingestion: parallel reads from 200+ Kafka partitions (match Spark parallelism)  
   - Transformation: avoid wide transformations (shuffles) where possible
   - Writing: partition by date/store, Z-order for common query patterns
   - No Python UDFs in hot path — native Spark functions only

3. Resource sizing (Databricks):
   - 50 workers × 8 cores × 16GB = 400 cores, 800GB RAM
   - Or auto-scaling 10-100 workers

4. Optimisations:
   - AQE enabled (auto-coalesce shuffle partitions)
   - Broadcast small dimension tables
   - Predicate pushdown at source
   - Columnar Parquet, Snappy compression
   - Skip sorting unless required downstream
```

---

### Q29. Explain the concept of pipeline lineage — why is it important and how is it captured?

**Lineage** = tracking the origin and transformation history of every piece of data: which source → which transformation → which output.

**Why critical:**
- **Impact analysis:** "If I change table X, what downstream reports break?"
- **Root cause analysis:** "Why does this report show wrong numbers?" → trace back through lineage
- **Regulatory compliance:** "Prove that this financial aggregate was calculated from approved sources"
- **Data trust:** Consumers know where data came from

**Implementation:**
```python
# Method 1: OpenLineage (automatic, zero-code)
# Add Spark listener — captures lineage from SQL query plans automatically
SparkConf().set("spark.extraListeners", 
    "io.openlineage.spark.agent.OpenLineageSparkListener")

# Method 2: Databricks Unity Catalog
# Automatic column-level lineage for all SQL operations in Databricks
# Visualised in Catalog Explorer UI

# Method 3: dbt
# dbt builds lineage DAG from ref() relationships automatically
# dbt docs generate → interactive lineage graph

# Method 4: Manual control table
def log_lineage(job_name, inputs, outputs, run_id):
    spark.createDataFrame([{
        "job_name": job_name, "run_id": run_id,
        "input_tables": json.dumps(inputs),
        "output_tables": json.dumps(outputs),
        "run_time": datetime.now()
    }]).write.format("delta").mode("append").save("/delta/lineage_log")
```

---

### Q30. How do you design a multi-tenant data pipeline where different clients have different schemas?

```python
# Pattern: configuration-driven, schema-per-tenant

tenant_configs = {
    "client_a": {
        "schema": client_a_schema,
        "transformations": client_a_transforms,
        "output_path": "/delta/client_a/transactions/"
    },
    "client_b": {
        "schema": client_b_schema,  # different column names
        "transformations": client_b_transforms,
        "output_path": "/delta/client_b/transactions/"
    }
}

def process_tenant(tenant_id: str, raw_path: str):
    config = tenant_configs[tenant_id]
    
    # Read with tenant-specific schema
    df = spark.read.schema(config["schema"]).json(raw_path)
    
    # Apply tenant-specific transformations
    df = config["transformations"](df)
    
    # Canonicalise to common schema for cross-tenant analytics
    df_canonical = canonicalise(df, CANONICAL_SCHEMA)
    
    # Write to tenant-isolated path (data isolation)
    df.write.format("delta").mode("append").save(config["output_path"])
    
    # Write to unified cross-tenant store with tenant_id column
    df_canonical.withColumn("tenant_id", lit(tenant_id)) \
        .write.format("delta").mode("append") \
        .partitionBy("tenant_id").save("/delta/all_tenants/transactions/")

# Row-level security in Unity Catalog: each client only sees their partition
```

---

---

# 📐 Area 5: Schema Management & Data Quality (30 Questions)

---

### Q1. What is schema evolution and what are the different compatibility types?

**Answer:** Schema evolution = changing a schema over time while minimising disruption to producers and consumers. Apache Avro / Schema Registry defines formal compatibility levels:

- **Backward compatible:** New schema can read data written with old schema. Safe for consumers to upgrade first. Example: adding a new optional field (old data has no value → uses default).
- **Forward compatible:** Old schema can read data written with new schema. Safe for producers to upgrade first. Example: adding a new field — old consumers ignore unknown fields.
- **Full compatible:** Both backward AND forward. Safest, most restrictive.
- **None:** No compatibility checking — breaking changes allowed.

```
Rule of thumb for each change type:
✅ Backward AND Forward: Add nullable field with default
✅ Backward only: Delete a field (new readers handle missing field, old readers still exist)
✅ Forward only: Add required field (old readers can't read it)
❌ Breaking: Change field type (int → string), rename field, change field semantics
```

**→ Follow-up: "How does Confluent Schema Registry enforce compatibility?"**
Schema Registry maintains a version history per topic. When a producer tries to register a new schema version, Schema Registry validates it against the configured compatibility level. If incompatible, registration is rejected — the producer cannot publish until the schema is fixed. Consumers look up schema by schema ID embedded in each message.

---

### Q2. How do you handle schema evolution in Delta Lake?

```python
# Default: Delta REJECTS writes with new/different schema
df_new.write.format("delta").mode("append").save("/delta/transactions")
# → AnalysisException if schema differs

# Option 1: mergeSchema — adds new columns, doesn't break existing
df_new.write.format("delta").mode("append") \
    .option("mergeSchema", "true").save("/delta/transactions")
# New columns added to table schema; existing rows have NULL for new columns

# Option 2: overwriteSchema — replaces schema entirely (DESTRUCTIVE)
df_new.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("/delta/transactions")

# Option 3: Column Mapping Mode (rename/drop without rewrite)
spark.sql("""
    ALTER TABLE delta.`/delta/transactions` 
    SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")
spark.sql("ALTER TABLE delta.`/delta/transactions` RENAME COLUMN old_name TO new_name")
spark.sql("ALTER TABLE delta.`/delta/transactions` DROP COLUMN deprecated_col")

# Session-level auto-merge (dangerous — use with governance)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

**→ Follow-up: "What happens to existing data when you add a new column to a Delta table?"**
No data rewrite occurs — Delta's column mapping means existing files don't change. New rows have the value; existing rows return NULL for the new column. This is why new columns MUST have a nullable type or provide a default — otherwise existing rows violate the schema.

**→ Follow-up: "How do you handle a source system renaming a column in an existing feed?"**
Don't propagate the rename directly — it's a breaking change for consumers. (1) Accept both old and new name in Bronze, (2) in Silver, use `COALESCE(new_name, old_name)` to handle both, (3) only standardise to new name in Gold with a deprecation notice to consumers. This provides a migration period.

---

### Q3. What is Great Expectations and how do you integrate it into a pipeline?

Great Expectations (GE) is a data validation framework — define **expectations** (assertions about your data), run them as **checkpoints** in your pipeline.

```python
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Setup
context = gx.get_context()

# Define expectations on a DataFrame
validator = context.get_validator(
    batch_request=RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_connector",
        data_asset_name="transactions",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "2024-01-01"}
    ),
    expectation_suite_name="transactions_suite"
)

# Column-level expectations
validator.expect_column_values_to_not_be_null("transaction_id")
validator.expect_column_values_to_be_unique("transaction_id")
validator.expect_column_values_to_be_between("amount", min_value=0, max_value=1_000_000)
validator.expect_column_values_to_be_in_set("currency", ["USD", "EUR", "GBP", "JPY"])
validator.expect_column_value_lengths_to_be_between("store_id", min_value=3, max_value=10)

# Table-level expectations
validator.expect_table_row_count_to_be_between(min_value=100_000, max_value=10_000_000)
validator.expect_table_columns_to_match_ordered_list(
    ["transaction_id", "amount", "currency", "store_id", "transaction_date"]
)

# Save suite
validator.save_expectation_suite(discard_failed_expectations=False)

# Run as checkpoint (in pipeline)
checkpoint = context.add_or_update_checkpoint(
    name="transactions_checkpoint",
    validations=[{"batch_request": batch_request, "expectation_suite_name": "transactions_suite"}],
    action_list=[
        {"name": "store_validation_result", "action": {"class_name": "StoreValidationResultAction"}},
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
        # FAIL pipeline on quality failure:
        {"name": "raise_error_if_any_check_fails", "action": {"class_name": "SlackNotificationAction", ...}}
    ]
)

result = context.run_checkpoint(checkpoint_name="transactions_checkpoint")
if not result.success:
    raise DataQualityError(f"Checkpoint failed: {result.describe()}")
```

**→ Follow-up: "What's the difference between `expect_or_drop` and `expect_or_fail` in Delta Live Tables?"**
`expect_or_drop` (DLT's `@dlt.expect_or_drop`): Records violating the expectation are removed from the output — pipeline continues with clean records. `expect_or_fail`: Any violation fails the entire pipeline run — hard stop. Use `expect_or_drop` for records that can be quarantined; `expect_or_fail` for critical integrity checks where ANY bad record is unacceptable.

---

### Q4. What are the dimensions of data quality? Give a pipeline example for each.

**6 core dimensions:**

1. **Completeness:** All expected data is present — no missing records, no unexpected nulls.
```sql
-- Check: null rate per column
SELECT COUNT(*) - COUNT(amount) AS null_amounts FROM transactions;
-- Check: expected row count
SELECT COUNT(*) FROM transactions WHERE processing_date = CURRENT_DATE;
-- Alert if count < 1,000,000 (baseline from historical analysis)
```

2. **Validity:** Data conforms to format/type/range rules.
```sql
SELECT COUNT(*) FROM transactions WHERE amount < 0 OR amount > 1000000;
SELECT COUNT(*) FROM transactions WHERE NOT transaction_id REGEXP '^TXN-[0-9]{10}$';
```

3. **Consistency:** Same data has same values across systems.
```sql
-- Cross-system reconciliation
SELECT ABS(a.total - b.total) / b.total AS variance_pct
FROM source_system_agg a JOIN data_lake_agg b ON a.date = b.date
WHERE ABS(a.total - b.total) / b.total > 0.001;  -- flag >0.1% variance
```

4. **Timeliness / Freshness:** Data is available within the SLA window.
```python
latest = spark.sql("SELECT MAX(event_time) FROM transactions").first()[0]
freshness = (datetime.now() - latest).seconds / 3600
assert freshness < 2, f"Data is {freshness:.1f}h stale — SLA breach"
```

5. **Uniqueness:** No duplicate records for a given business key.
```sql
SELECT transaction_id, COUNT(*) FROM transactions 
GROUP BY transaction_id HAVING COUNT(*) > 1;
```

6. **Accuracy:** Data correctly represents the real-world entity it describes. (Hardest to automate — often requires cross-referencing source of truth.)

**→ Follow-up: "How do you prioritise which quality checks to implement first?"** Business impact × detection difficulty. Start with: (1) uniqueness on primary keys — duplicates corrupt everything downstream, (2) completeness on critical fields — nulls in join keys are silent data loss, (3) timeliness — SLA breaches affect business decisions. Accuracy and consistency checks come later as they require more context.

---

### Q5. What is a data contract and how do you enforce it technically?

A data contract is a formal, versioned agreement about schema, semantics, quality SLAs, and ownership.

```yaml
# Using Open Data Contract Standard
dataContractSpecification: 0.9.3
id: urn:datacontract:retail:transactions-gold:v2
info:
  title: Transactions Gold
  version: 2.1.0
  owner: retail-team
  contact: {name: Data Team, email: data@company.com}
  sla:
    freshness: 08:00 UTC daily
    completeness: ">= 99.5% of expected rows"

models:
  transactions:
    fields:
      transaction_id: {type: string, required: true, unique: true}
      amount:        {type: number, required: true, minimum: 0}
      store_id:      {type: string, required: true, pattern: "^[A-Z]{3}[0-9]{4}$"}
      created_at:    {type: timestamp, required: true}

quality:
  - type: sql
    query: "SELECT COUNT(*) FROM transactions WHERE created_at::date = CURRENT_DATE"
    mustBe: ">= 500000"

# Technical enforcement:
# 1. dbt: schema.yml with data_tests
# 2. Delta Lake: schema enforcement on write
# 3. Great Expectations checkpoint on pipeline output
# 4. Schema Registry for Kafka topics
# 5. dbt-contract: fail CI if schema.yml doesn't match database
```

---

### Q6. How do you implement data validation at each layer of a medallion architecture?

```python
# Bronze: minimal validation — just "is it parseable?"
# ❌ Don't reject data here — preserve everything for audit
bronze_checks = [
    "file_is_not_empty",
    "json_is_valid",  
    "mandatory_envelope_fields_present",  # source_id, ingest_time
]
# Bad records → _corrupt_record column, written to quarantine path

# Silver: structural validation
silver_checks = [
    expect_column_values_to_not_be_null("transaction_id"),
    expect_column_values_to_not_be_null("amount"),
    expect_column_values_to_be_between("amount", 0, 1e7),
    expect_table_row_count_to_be_between(expected * 0.9, expected * 1.1),
    # Reconciliation: Silver count should = Bronze count - known rejects
]

# Gold: business rule validation
gold_checks = [
    # Revenue should be within 5% of prior period (anomaly detection)
    "revenue_variance_vs_prior_week < 0.05",
    # Every store in dim_stores should appear in transactions (completeness)
    "all_active_stores_have_transactions",
    # No future-dated transactions
    "max(transaction_date) <= current_date",
    # Cross-system reconciliation vs source system totals
    "abs(gold_total - source_total) / source_total < 0.001",
]
```

---

### Q7. What is schema drift and how do you detect it automatically?

Schema drift = the incoming data schema changes without advance notice. Common in:
- API responses adding new fields
- Source systems adding columns
- File formats changing (new CSV headers)
- Type changes (amount was int, now string)

```python
def detect_schema_drift(incoming_df, expected_schema: StructType) -> dict:
    incoming_fields = {f.name: f.dataType for f in incoming_df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    new_cols = set(incoming_fields.keys()) - set(expected_fields.keys())
    removed_cols = set(expected_fields.keys()) - set(incoming_fields.keys())
    type_changes = {
        col: (expected_fields[col], incoming_fields[col])
        for col in expected_fields.keys() & incoming_fields.keys()
        if type(expected_fields[col]) != type(incoming_fields[col])
    }
    
    drift = {"new": list(new_cols), "removed": list(removed_cols), "type_changes": type_changes}
    
    if any(drift.values()):
        send_alert(f"Schema drift detected: {json.dumps(drift)}")
        log_schema_drift_event(drift)
    
    return drift

def handle_drift(incoming_df, drift: dict, policy: str = "additive_only"):
    if policy == "reject_any_drift":
        if any(drift.values()):
            raise SchemaDriftError(f"Schema drift detected: {drift}")
    elif policy == "additive_only":
        if drift["removed"] or drift["type_changes"]:
            raise SchemaDriftError("Non-additive schema change rejected")
        # New columns allowed — add to schema
    elif policy == "forward_compatible":
        # Drop new/unknown columns, fill removed with NULL, reject type changes
        pass
```

**→ Follow-up: "A source system changed an amount field from INT to DECIMAL(18,2). What's your response?"** This is a type change — potentially breaking. (1) Alert the source team and your team immediately. (2) Check if existing INT values are still valid as DECIMAL — likely yes. (3) In Silver, cast to DECIMAL explicitly: `col("amount").cast(DecimalType(18,2))`. (4) Verify no precision loss for existing values. (5) Update expected schema in schema registry. (6) Downstream impact analysis — anything hardcoded to INT type?

---

### Q8. How do you implement data reconciliation between source and target?

```python
def reconcile(source_df, target_df, key_cols, metric_cols, date_filter):
    """Compare source and target row counts and aggregate sums"""
    
    source_agg = source_df.filter(col("processing_date") == date_filter) \
        .agg(*[count("*").alias("row_count")] + 
             [sum(c).alias(f"sum_{c}") for c in metric_cols])
    
    target_agg = target_df.filter(col("processing_date") == date_filter) \
        .agg(*[count("*").alias("row_count")] +
             [sum(c).alias(f"sum_{c}") for c in metric_cols])
    
    s = source_agg.first()
    t = target_agg.first()
    
    results = {}
    # Row count check
    row_variance = abs(s.row_count - t.row_count) / max(s.row_count, 1)
    results["row_count"] = {
        "source": s.row_count, "target": t.row_count,
        "variance_pct": row_variance, "passed": row_variance < 0.001
    }
    
    # Sum checks per metric
    for c in metric_cols:
        variance = abs(getattr(s, f"sum_{c}") - getattr(t, f"sum_{c}")) / max(abs(getattr(s, f"sum_{c}")), 1)
        results[c] = {"variance_pct": variance, "passed": variance < 0.0001}
    
    # Log and alert
    for check, result in results.items():
        if not result["passed"]:
            send_alert(f"Reconciliation FAILED: {check} variance {result['variance_pct']:.2%}")
    
    log_reconciliation(date_filter, results)
    return all(r["passed"] for r in results.values())
```

---

### Q9. What is a data quality score and how do you track it over time?

```python
def compute_quality_score(df, date: str) -> float:
    """Compute composite DQ score 0-100 for a DataFrame"""
    metrics = {}
    total = df.count()
    
    # Completeness (30% weight)
    null_counts = df.select([
        (count(when(col(c).isNull(), c)) / total).alias(c) 
        for c in ["transaction_id", "amount", "store_id"]
    ]).first()
    avg_null_rate = sum(null_counts) / len(null_counts)
    metrics["completeness"] = (1 - avg_null_rate) * 30
    
    # Validity (25% weight)
    invalid_amount = df.filter((col("amount") < 0) | (col("amount") > 1e7)).count()
    metrics["validity"] = (1 - invalid_amount / total) * 25
    
    # Uniqueness (25% weight)
    duplicates = total - df.dropDuplicates(["transaction_id"]).count()
    metrics["uniqueness"] = (1 - duplicates / total) * 25
    
    # Timeliness (20% weight)
    max_lag = df.select(F.max(F.datediff(current_date(), col("transaction_date")))).first()[0]
    metrics["timeliness"] = max(0, (1 - max_lag / 7)) * 20  # degrade over 7 days
    
    score = sum(metrics.values())
    
    # Persist score history
    spark.createDataFrame([{"date": date, "score": score, **metrics}]) \
        .write.format("delta").mode("append").save("/delta/dq_scores")
    
    return score
```

---

### Q10–Q30: Remaining Schema & Data Quality Questions

---

### Q10. How do you handle PII data in a data pipeline? What is pseudonymisation vs anonymisation?

**Anonymisation:** Irreversibly removes all identifying information — cannot be re-linked to the individual. GDPR compliant — no longer "personal data". Methods: k-anonymity, l-diversity, differential privacy.

**Pseudonymisation:** Replaces identifiers with artificial IDs — CAN be re-linked with the mapping table. Still considered personal data under GDPR but reduced risk.

```python
# Pseudonymisation in practice
from pyspark.sql.functions import sha2, concat_ws, lit

# Hash PII with a secret salt (store salt in Key Vault)
salt = dbutils.secrets.get("kv-scope", "pii-salt")
df = df.withColumn("customer_pseudo_id", 
    sha2(concat_ws("|", col("ssn"), lit(salt)), 256)) \
    .drop("ssn", "full_name", "date_of_birth")  # drop originals

# Store PII → pseudo_id mapping in separate, access-controlled table
pii_mapping.write.format("delta") \
    .option("delta.columnMapping.mode", "name") \
    .save("/delta/pii_mapping/")  # restricted access — no analysts

# Main table uses only pseudo_id — analytics can proceed without PII
```

---

### Q11. How do you detect anomalies in incoming data?

```python
# Statistical anomaly detection on ingestion metrics
def detect_anomalies(current_metrics: dict, historical_df):
    """z-score based anomaly detection on row counts and sums"""
    stats = historical_df.select(
        mean("row_count").alias("mean_rows"),
        stddev("row_count").alias("std_rows"),
        mean("total_amount").alias("mean_amount"),
        stddev("total_amount").alias("std_amount")
    ).first()
    
    z_score_rows = abs(current_metrics["row_count"] - stats.mean_rows) / max(stats.std_rows, 1)
    z_score_amount = abs(current_metrics["total_amount"] - stats.mean_amount) / max(stats.std_amount, 1)
    
    anomalies = []
    if z_score_rows > 3:
        anomalies.append(f"Row count z-score={z_score_rows:.1f} (expected ~{stats.mean_rows:.0f}, got {current_metrics['row_count']})")
    if z_score_amount > 3:
        anomalies.append(f"Total amount z-score={z_score_amount:.1f}")
    
    return anomalies

# More sophisticated: use Prophet (Facebook's time series) for trend-aware anomaly detection
# Or: Databricks Anomaly Detection notebook with moving z-scores
```

---

### Q12. What are dbt tests and how do you structure them?

```yaml
# schema.yml — declarative tests
models:
  - name: transactions_silver
    description: "Cleaned transactions"
    columns:
      - name: transaction_id
        description: "Unique transaction identifier"
        tests:
          - not_null
          - unique
      - name: amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: store_id
        tests:
          - not_null
          - relationships:      # referential integrity
              to: ref('dim_stores')
              field: store_id
      - name: currency
        tests:
          - accepted_values:
              values: ['USD', 'EUR', 'GBP', 'JPY']
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('transactions_bronze')
      - dbt_utils.recency:
          datepart: day
          field: transaction_date
          interval: 1  # must have data from within the last 1 day
```

```sql
-- tests/assert_no_future_transactions.sql (singular test)
SELECT transaction_id
FROM {{ ref('transactions_silver') }}
WHERE transaction_date > CURRENT_DATE
```

---

### Q13. How do you manage schema versioning across microservices that produce data?

**Schema Registry approach:**
```python
# Each producer registers schema before publishing
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

schema_registry = SchemaRegistryClient({"url": "http://schema-registry:8081"})

avro_schema = """
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.company.retail",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "new_field", "type": ["null", "string"], "default": null}
  ]
}
"""
# Schema ID embedded in Kafka message header — consumer looks up schema by ID
# Backward compatible: new_field has null default — old consumers work fine
```

---

### Q14–Q20. Quick Fire Questions

**Q14. What is a slowly changing dimension Type 3?** SCD Type 3 adds a separate column for the previous value: `current_city`, `previous_city`, `changed_date`. Tracks only ONE level of history — the current and immediately prior value. Useful when you only ever need "current vs. one version ago" comparison. Simpler than Type 2 but limited history.

**Q15. What is data profiling?** Analysing a dataset to understand its characteristics before transformation: distinct values per column, null rates, min/max/avg, distribution histograms, frequent values, string patterns. Tools: Great Expectations profiler, pandas-profiling, Databricks Data Explorer.

**Q16. How do you test a data pipeline in isolation?** Use containerised test databases (Docker PostgreSQL), mock S3 with MinIO, create small representative test DataFrames in Spark with `createDataFrame`, mock external API calls with `unittest.mock`. Test transformation logic independently of I/O.

**Q17. What is idempotency and why is it harder than it sounds?** Trivially idempotent writes overwrite the same location. Hard cases: (1) append-only logs — re-run creates duplicates unless deduplicated. (2) Sending emails or API calls — can't un-send. (3) Windowed aggregations — re-running a window can produce different results if new data arrived for that window. Solution: store completion state, use MERGE not INSERT, make external calls conditional on state check.

**Q18. What is data lineage vs data provenance?** Lineage = where data came from and how it moved (forward-looking: source → transformation → target). Provenance = the complete history of a data item including all changes (backward-looking: for this specific record, trace every modification). Provenance is more granular and harder to capture.

**Q19. How does Delta Lake handle GDPR right to erasure?** Hard delete with `DELETE FROM delta.table WHERE customer_id = 'X'`, then `VACUUM` to physically remove old files. But time travel is disabled for that data. Better: pseudonymisation — delete the PII mapping; analytics data becomes non-personal. Or: encrypted columns with customer-managed keys — delete the key, data is cryptographically erased ("crypto shredding").

**Q20. What is data masking and when do you use it?** Replacing sensitive values with fictitious but realistic-looking values. Static masking: applied at rest (stored masked in non-prod). Dynamic masking: applied at query time based on user role (original stored, masked on read). Unity Catalog dynamic masking: `ALTER TABLE ... ALTER COLUMN ssn SET MASK mask_ssn_function`.

---

### Q21. How do you validate data after a migration from Hive to Delta Lake?

```python
# Comprehensive migration validation
def validate_migration(hive_table: str, delta_path: str, partition_cols: list):
    hive_df = spark.table(hive_table)
    delta_df = spark.read.format("delta").load(delta_path)
    
    # 1. Row count match
    assert hive_df.count() == delta_df.count(), "Row count mismatch"
    
    # 2. Schema match (column names and types)
    hive_schema = {f.name: str(f.dataType) for f in hive_df.schema.fields}
    delta_schema = {f.name: str(f.dataType) for f in delta_df.schema.fields}
    assert hive_schema == delta_schema, f"Schema mismatch: {hive_schema} vs {delta_schema}"
    
    # 3. Aggregate checksums per partition
    for partition_value in get_recent_partitions(hive_table, 30):
        hive_sum = hive_df.filter(col("date") == partition_value).agg(sum("amount")).first()[0]
        delta_sum = delta_df.filter(col("date") == partition_value).agg(sum("amount")).first()[0]
        variance = abs(hive_sum - delta_sum) / max(abs(hive_sum), 1)
        assert variance < 0.0001, f"Sum mismatch for {partition_value}: {variance:.4%}"
    
    # 4. Sample row comparison (spot check)
    sample_keys = hive_df.select("transaction_id").sample(0.001).collect()
    for row in sample_keys[:100]:
        hive_row = hive_df.filter(col("transaction_id") == row.transaction_id).first()
        delta_row = delta_df.filter(col("transaction_id") == row.transaction_id).first()
        assert hive_row == delta_row, f"Row mismatch for {row.transaction_id}"
    
    print("✅ Migration validation PASSED")
```

---

### Q22. How do you handle schema evolution with Avro in a Kafka pipeline?

```python
# Avro with Schema Registry — producer registers schema, consumer deserialises
# Key rules:
# ADD field with default → backward compatible
# REMOVE field → forward compatible (existing consumers still use it)
# RENAME field → BREAKING — use aliases instead

# Avro schema with alias (safe rename)
new_schema = {
    "type": "record", "name": "Transaction",
    "fields": [
        {
            "name": "transaction_amount",  # new name
            "aliases": ["amount"],          # old name as alias — backward compatible
            "type": "double"
        }
    ]
}

# Consumer using Spark: evolution handled by specifying both old and new field
df = (spark.readStream.format("kafka")
    .load()
    .select(from_avro(col("value"), schema_registry_conf).alias("data"))
    .select(
        coalesce(col("data.transaction_amount"), col("data.amount")).alias("amount")
    ))
```

---

### Q23. What is a data catalog and how do you keep it up to date automatically?

A data catalog is a metadata repository — enables discovery, understanding, and governance of data assets.

**Auto-update approaches:**
1. **Push model:** Pipelines emit metadata events when they run (OpenLineage → Marquez/OpenMetadata)
2. **Pull model:** Catalog scans sources on schedule (OpenMetadata connectors, Databricks UC auto-sync)
3. **CI/CD integration:** dbt docs deployed to catalog on every dbt run

**What to auto-populate:** Schema, column descriptions, lineage, row counts, partition info, last updated time, quality scores, owner (from Git blame on dbt model).

**What requires human input:** Business descriptions, column semantics, data classification (PII), SLA commitments, ownership.

---

### Q24. How do you ensure data quality in a real-time streaming pipeline?

```python
# Quality checks in foreachBatch
def process_and_validate_batch(batch_df, batch_id):
    # 1. Schema validation
    for col_name in REQUIRED_COLUMNS:
        null_count = batch_df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            quarantine_df = batch_df.filter(col(col_name).isNull())
            quarantine_df.write.mode("append").json("/quarantine/streaming/")
            batch_df = batch_df.filter(col(col_name).isNotNull())
    
    # 2. Range validation
    invalid = batch_df.filter((col("amount") < 0) | (col("amount") > 1e6))
    if invalid.count() > 0:
        invalid.write.mode("append").json("/quarantine/invalid_amount/")
        batch_df = batch_df.filter((col("amount") >= 0) & (col("amount") <= 1e6))
    
    # 3. Emit metrics
    emit_metric("batch_rows", batch_df.count())
    emit_metric("quarantined_rows", invalid.count())
    
    # 4. Write clean data
    batch_df.write.format("delta").mode("append").save("/delta/silver/transactions")

stream.writeStream.foreachBatch(process_and_validate_batch) \
    .option("checkpointLocation", "/checkpoints/streaming").start()
```

---

### Q25–Q30. Final Schema/DQ Questions

**Q25. What is data observability and how does it differ from data quality?** Data quality = checking specific rules at a point in time (this column has nulls). Data observability = continuous monitoring of data health across dimensions — freshness, volume, schema, distribution, lineage. Tools: Monte Carlo, Acceldata, Bigeye. Think of it as infrastructure monitoring but for data — you set baselines, get alerted on anomalies automatically without pre-defining every rule.

**Q26. How do you handle nulls in aggregate calculations and why does it matter?** `SUM(amount)` ignores NULLs — which may be correct or may silently undercount. `COUNT(*)` counts rows with NULL. `COUNT(amount)` counts non-NULL only. Always be explicit: decide whether NULL means zero (use `COALESCE(amount, 0)`) or truly unknown/excluded. Document the decision in data contract.

**Q27. What is a data assertion vs a data expectation?** Data assertion: binary — passes or fails, hard stop. Used for invariants that MUST hold. Data expectation: probabilistic — "99.5% of transactions should have non-null amounts." Allows for some tolerance. Great Expectations: mostly expectations. Delta Live Tables `@dlt.expect`: quarantines violations (soft) or fails (hard).

**Q28. How do you implement data freshness monitoring?** Track `MAX(event_time)` or `MAX(processing_date)` per table. Compare to current time. Alert thresholds: warning at 1.5× expected interval, critical at 3× expected interval. For streaming: monitor consumer lag in Kafka (consumer_group_lag metric in Prometheus). For batch: Airflow SLA miss notifications.

**Q29. What is Dataplex (GCP) and how does it compare to Microsoft Purview?** Both are data governance platforms. Dataplex: GCP-native, automatic metadata discovery from GCS/BigQuery/Dataproc, data quality jobs, unified security across GCP. Purview: Azure-native, scans Azure and non-Azure sources, data classification with ML sensitivity labels, lineage from ADF/Synapse. Both provide catalog, lineage, and access governance — choose based on cloud platform.

**Q30. How do you version a dbt model when you need to make a breaking change?** Create a new model version: `models/transactions/transactions_v2.sql`. Old consumers keep using `ref('transactions')` → maps to v1. New consumers use `ref('transactions', v=2)`. Run both in parallel during migration period (dbt model versioning feature, dbt 1.5+). After all consumers migrated: deprecate v1 with a `deprecated` flag in schema.yml.

---
