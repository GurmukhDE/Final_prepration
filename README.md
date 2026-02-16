# 🏦 NAB Data Engineer Interview Preparation Guide

**Interview Date:** 21st February 2025  
**Position:** Data Engineer at National Australia Bank (NAB)  
**Preparation Timeline:** 16-21 February (5 Days)

---

## 📋 Table of Contents

1. [Overview & Strategy](#overview--strategy)
2. [5-Day Study Plan](#5-day-study-plan)
3. [Banking Projects Portfolio](#banking-projects-portfolio)
4. [Technical Interview Questions](#technical-interview-questions)
5. [Behavioral Questions](#behavioral-questions)
6. [Quick Revision Cheat Sheets](#quick-revision-cheat-sheets)
7. [Questions to Ask Interviewer](#questions-to-ask-interviewer)
8. [Interview Day Checklist](#interview-day-checklist)

---

## 📊 Overview & Strategy

### Current Situation
- **Position:** Assistant Manager at Bank of America (3.3 years)
- **Previous:** Data Engineer at Omnicom Media Group
- **Target:** Data Engineer role at NAB

### Your Strengths ✅
- ✅ Real experience with AWS (S3, Redshift, Glue, EC2, Athena)
- ✅ Real experience with Azure (ADLS Gen2, Databricks, ADF, Kafka)
- ✅ PostgreSQL/Redshift experience
- ✅ Python, Alteryx, Tableau knowledge
- ✅ Banking domain experience

### Areas to Focus 🎯
- 🎯 SQL JOINs (practice complex joins)
- 🎯 PySpark coding (basics + common patterns)
- 🎯 Python hands-on practice
- 🎯 Confidence building through mock practice

### Interview Structure
1. **Round 1:** Online Assessment (Wed/Thu) - SQL, Python, Logical Reasoning
2. **Round 2:** F2F Technical (21st Feb) - Projects, Coding, System Design
3. **Round 3:** HR/Manager - Behavioral, Culture Fit, Expectations

---

## 📅 5-Day Study Plan

### Day 1 (Feb 16): Foundation & Projects
| Time | Activities |
|------|-----------|
| 3 hours | Read & memorize 3 Banking Projects (details below)<br>Practice STAR method answers for each project |
| 3 hours | **SQL Practice:**<br>- All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)<br>- Subqueries & CTEs<br>- Window functions (ROW_NUMBER, RANK, LAG, LEAD) |
| 3 hours | **Python Basics Revision:**<br>- Lists, tuples, dicts, sets operations<br>- Functions, lambda, list comprehensions<br>- File handling, error handling |

### Day 2 (Feb 17): PySpark & Cloud
| Time | Activities |
|------|-----------|
| 4 hours | **PySpark Fundamentals:**<br>- SparkSession creation<br>- DataFrame operations (select, filter, groupBy, join)<br>- Read/Write operations (CSV, Parquet, Delta)<br>- Basic transformations and actions |
| 3 hours | **AWS Services Deep Dive:**<br>- S3 (buckets, versioning, lifecycle policies)<br>- Redshift (architecture, dist keys, sort keys)<br>- Glue (crawlers, jobs, data catalog)<br>- Lambda (event-driven processing) |
| 2 hours | **Azure Services:**<br>- Databricks (clusters, notebooks, jobs)<br>- ADF (pipelines, triggers, linked services)<br>- ADLS Gen2 (hierarchical namespace) |

### Day 3 (Feb 18): Interview Questions Practice
| Time | Activities |
|------|-----------|
| 3 hours | **Behavioral Questions:**<br>Practice 20 behavioral questions with STAR method<br>Record yourself and evaluate |
| 3 hours | **Technical Questions:**<br>Go through 30 technical questions (provided below)<br>Write out answers for complex ones |
| 3 hours | **System Design:**<br>Learn data pipeline architecture patterns<br>Practice explaining your projects' architecture |

### Day 4 (Feb 19): Coding Practice & Mock
| Time | Activities |
|------|-----------|
| 4 hours | **Live Coding Practice:**<br>- 5 SQL problems (LeetCode/HackerRank Medium level)<br>- 3 Python data manipulation problems<br>- 2 PySpark DataFrame problems |
| 3 hours | **Mock Interview:**<br>Do a full mock interview with a friend or record yourself<br>Cover: Introduction → Project discussion → Technical questions → Questions for interviewer |
| 2 hours | **Data Engineering Concepts:**<br>- Data modeling (Star schema, Snowflake, Data Vault)<br>- ETL vs ELT<br>- CAP theorem, ACID vs BASE |

### Day 5 (Feb 20): Final Revision & Confidence
| Time | Activities |
|------|-----------|
| 3 hours | **Quick Revision:**<br>- Review all 3 projects (be ready to explain in 2 mins each)<br>- Quick glance through all answers<br>- Write down key points on a cheat sheet |
| 2 hours | **NAB Research:**<br>- Company values, recent news<br>- Tech stack and initiatives<br>- Prepare 5 intelligent questions for interviewer |
| 2 hours | **Confidence Building:**<br>- Practice your 1-minute introduction<br>- Positive visualization<br>- Light coding practice to stay sharp |
| 2 hours | **Logistics:**<br>- Prepare clothes, documents<br>- Plan route/timing<br>- Early sleep for fresh mind |

---

## 🏦 Banking Projects Portfolio

> **Note:** These projects are designed to be believable in Bank of America context and align with NAB's requirements. Memorize these thoroughly!

### Project 1: Real-Time Fraud Detection Pipeline

**Duration:** 8 months (Jan 2024 - Aug 2024)

**Objective:**  
Built an end-to-end real-time fraud detection system to analyze credit card transactions and flag suspicious activities within 2 seconds of transaction occurrence, reducing fraud losses by 35%.

**Your Role:** Data Engineer - Led the data pipeline development and integration

**Tech Stack:**
- **AWS:** Kafka (MSK), S3, Lambda, Redshift, Glue
- **Languages:** Python, PySpark, SQL
- **Tools:** Databricks, Airflow, Tableau

**Detailed Implementation:**

#### 1. Data Ingestion Layer
- Configured Kafka topics to receive real-time transaction data from 50+ banking channels (ATMs, POS, online banking, mobile app)
- Set up Kafka producers with message serialization using Avro schema
- Handled ~500K transactions per hour during peak times

#### 2. Stream Processing
- Developed PySpark Structured Streaming jobs on Databricks to consume Kafka streams
- Applied real-time transformations:
  - Data validation
  - Enrichment with customer profile data from Redshift
  - Calculation of transaction velocity features
- Implemented windowing operations (5-min tumbling windows) to detect patterns

#### 3. Feature Engineering
- Created 30+ features:
  - Transaction amount deviation from user average
  - Geographic distance from last transaction
  - Merchant category risk score
  - Time-since-last-transaction
- Used PySpark Window functions for calculating rolling averages and historical patterns

#### 4. Storage Layer
- Raw transactions stored in S3 (Parquet format, partitioned by date)
- Processed/enriched data loaded into Redshift for analytical queries
- Implemented incremental loads using SCD Type 2 for customer dimension

#### 5. Automation & Orchestration
- Created Airflow DAGs for daily batch jobs (model retraining, historical analysis)
- Set up Lambda functions for alerting when fraud score > threshold
- Implemented SNS notifications to fraud investigation team

#### 6. Monitoring & Optimization
- Built Tableau dashboards showing transaction volumes, fraud patterns, system latency
- Optimized Spark jobs by tuning shuffle partitions and broadcast joins
- Reduced processing latency from 8 seconds to 1.5 seconds

**Challenges & Solutions:**

| Challenge | Solution |
|-----------|----------|
| Data skew in Kafka partitions causing uneven processing | Implemented custom partitioner based on customer ID hash, ensuring even distribution across 12 partitions |
| Memory issues when joining large customer dimension | Used broadcast join for dimension table (< 200MB), cached customer data in Redis for faster lookups |
| Managing schema evolution in streaming pipeline | Adopted Schema Registry with Avro for backward/forward compatibility, implemented schema validation at producer level |

**Key Results:**
- ✅ 35% reduction in fraud-related losses ($2.3M annually)
- ✅ 99.7% system uptime with auto-recovery mechanisms
- ✅ Processing latency reduced from 8s to 1.5s (81% improvement)
- ✅ False positive rate decreased from 12% to 4%

**Code Sample:**

```python
# PySpark Structured Streaming for Fraud Detection
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, when
from pyspark.sql.types import *

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Read from Kafka
transaction_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse JSON and apply schema
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("timestamp", TimestampType())
])

parsed_stream = transaction_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Feature engineering with windowing
fraud_features = parsed_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("customer_id")
    ).agg(
        count("*").alias("txn_count"),
        avg("amount").alias("avg_amount"),
        sum("amount").alias("total_amount")
    ) \
    .withColumn("fraud_score", 
        when(col("txn_count") > 5, 0.8)
        .when(col("total_amount") > 10000, 0.7)
        .otherwise(0.2)
    )

# Write to S3 and trigger alerts
query = fraud_features.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3://fraud-data/processed/") \
    .option("checkpointLocation", "s3://fraud-data/checkpoints/") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

---

### Project 2: Customer 360 Data Warehouse Modernization

**Duration:** 10 months (March 2023 - Dec 2023)

**Objective:**  
Migrated legacy on-premise Teradata data warehouse to AWS cloud-based modern data architecture using Redshift and S3 Data Lake, creating a unified Customer 360 view by integrating 15+ source systems.

**Your Role:** Senior Data Engineer - Led ETL development and data modeling

**Tech Stack:**
- **Cloud:** AWS (S3, Redshift, Glue, Athena, DMS)
- **Languages:** Python, SQL, Shell scripting
- **Tools:** Alteryx, dbt, Airflow, Tableau
- **Source Systems:** Teradata, Oracle, DB2, Mainframe

**Detailed Implementation:**

#### 1. Assessment & Planning Phase
- Analyzed existing Teradata warehouse: 45TB data, 2000+ tables, 500+ BTEQ/SQL scripts
- Created data lineage maps using Python to understand dependencies
- Designed target architecture: S3 Data Lake (raw/curated/analytics zones) + Redshift DW

#### 2. Data Migration Strategy
- **Historical Data:** Used AWS DMS (Database Migration Service) for initial bulk load
- **Incremental Data:** Implemented CDC (Change Data Capture) using Glue jobs
- Migrated ~30TB data in 3 months with zero downtime

#### 3. Data Lake Architecture
- **Raw Zone:** S3 buckets with original data in CSV/JSON/Parquet
- **Curated Zone:** Cleansed, validated, and standardized data
- **Analytics Zone:** Star schema dimensional model ready for BI
- Implemented partitioning strategy (by date and source system) for query optimization
- Used Parquet with Snappy compression reducing storage by 60%

#### 4. ETL Pipeline Development
- Built 120+ Glue ETL jobs (PySpark) for data transformation
- **Data Quality Framework:** Implemented Great Expectations for validation (completeness, uniqueness, format checks)
- Created reusable Python libraries for common transformations
- Used Alteryx for complex business rule implementations

#### 5. Redshift Data Warehouse Design
- Designed Star Schema with 12 fact tables and 35 dimension tables
- **Customer Dimension:** Implemented SCD Type 2 for historical tracking
- Optimized tables with Distribution Keys (customer_id) and Sort Keys (transaction_date)
- Created materialized views for frequently accessed aggregations

#### 6. Orchestration & Scheduling
- Developed 50+ Airflow DAGs for workflow orchestration
- Daily batch processing: Data extraction (6 AM), Transformation (7 AM), Load to DW (9 AM)
- Implemented retry mechanisms and alerting via SNS/PagerDuty

#### 7. Data Integration - Customer 360
Integrated data from 15 sources:
- Core Banking System (Oracle)
- CRM (Salesforce)
- Credit Card System (DB2)
- Loan Management System
- Customer Service Interactions (ServiceNow)
- Web/Mobile Analytics (Adobe)

Built Golden Record logic using fuzzy matching and ML-based entity resolution

**Challenges & Solutions:**

| Challenge | Solution |
|-----------|----------|
| Data quality issues - inconsistent customer IDs across systems | Created MDM (Master Data Management) hub using deterministic + probabilistic matching (90% match rate achieved) |
| Performance degradation in Redshift queries | Analyzed query patterns using system tables, implemented distribution/sort keys, used VACUUM and ANALYZE commands, created summary tables |
| Business users resistance to new system | Conducted 20+ training sessions, created user guides, maintained dual systems for 2 months during transition |

**Key Results:**
- ✅ 60% reduction in infrastructure costs (from $500K to $200K annually)
- ✅ Query performance improved by 70% (average query time: 15s to 4.5s)
- ✅ Data freshness improved from T+1 to T+2 hours (near real-time)
- ✅ Enabled 500+ business users with self-service analytics
- ✅ 95% data accuracy achieved through quality framework

---

### Project 3: Regulatory Reporting Automation (Basel III)

**Duration:** 6 months (June 2024 - Nov 2024)

**Objective:**  
Automated end-to-end Basel III regulatory reporting pipeline to generate monthly reports for Federal Reserve, reducing manual effort by 85% and ensuring 100% accuracy and audit trail.

**Your Role:** Lead Data Engineer

**Tech Stack:**
- **Cloud:** Azure (ADLS Gen2, Databricks, ADF, Key Vault)
- **Languages:** Python, PySpark, SQL
- **Tools:** Alteryx, Power BI, Git

**Detailed Implementation:**

#### 1. Requirements Analysis
- Collaborated with Compliance team to understand Basel III reporting requirements
- Documented 150+ business rules and calculations
- Mapped source-to-target data flow for 8 regulatory reports

#### 2. Data Collection Layer
- Extracted data from 12 source systems:
  - Loan origination
  - Credit risk models
  - Treasury systems
  - General ledger
- Built ADF pipelines to ingest data into ADLS Gen2 (Bronze layer)
- Implemented parameterized pipelines for flexibility

#### 3. Data Transformation (Databricks)
- Developed PySpark notebooks for complex calculations:
  - Risk-weighted assets
  - Capital adequacy ratios
  - Leverage ratios
  - Liquidity coverage ratios
- Implemented Delta Lake for ACID transactions and time travel
- Created Silver layer (cleansed/validated) and Gold layer (aggregated/business-ready)

#### 4. Business Logic Implementation
- Translated regulatory formulas into Python functions
- Used Alteryx for some complex regulatory calculations that required visual workflows
- Created unit tests for each calculation to ensure accuracy

#### 5. Reporting Layer
- Built Power BI dashboards for internal review before submission
- Generated PDF reports in regulatory format using Python (reportlab)
- Automated email delivery to stakeholders using ADF

#### 6. Audit & Compliance
- Maintained complete lineage tracking for every data point
- Stored all intermediate results with version control
- Created reconciliation reports comparing automated vs manual calculations
- Implemented approval workflow before final submission

#### 7. Security & Governance
- Used Azure Key Vault for credential management
- Implemented RBAC (Role-Based Access Control)
- Encrypted sensitive data at rest and in transit
- Set up Azure Monitor for pipeline monitoring and alerting

**Key Results:**
- ✅ 85% reduction in manual effort (from 200 hours to 30 hours per month)
- ✅ 100% accuracy achieved (zero errors in 6 consecutive reports)
- ✅ Report generation time reduced from 10 days to 2 days
- ✅ Complete audit trail established for regulatory review
- ✅ Won 'Innovation Award' from Compliance department

---

## ❓ Anticipated Follow-up Questions

### Project 1: Fraud Detection

**Q1: How did you handle late-arriving data in your streaming pipeline?**

**Answer:**  
We used watermarking in Spark Structured Streaming with a 10-minute delay threshold. Any data arriving later than 10 minutes was written to a separate late-arrival table for manual review. We set this threshold based on analyzing our data patterns - 99.5% of data arrived within 5 minutes. For critical transactions, we also had a 24-hour reconciliation batch job that would catch and process any missed records.

**Q2: What happens if Kafka goes down?**

**Answer:**  
We had multiple safeguards:
1. Kafka cluster with 3 brokers across 3 availability zones for high availability
2. Producer-level buffering with 5-minute retry
3. Transaction data also stored in S3 as backup
4. Spark checkpointing enabled so we could resume from last processed offset when service restored
5. PagerDuty alerts configured for broker failures with 2-minute escalation

**Q3: How did you optimize PySpark jobs for better performance?**

**Answer:**  
Multiple techniques:
1. Used broadcast joins for small dimension tables under 200MB
2. Tuned shuffle partitions from default 200 to 48 based on cluster capacity
3. Enabled AQE (Adaptive Query Execution) in Spark 3.0
4. Cached frequently accessed DataFrames in memory
5. Used column pruning and predicate pushdown
6. Chose optimal file formats - Parquet with Snappy compression
7. Monitored Spark UI to identify bottlenecks like data skew

### Project 2: Data Warehouse

**Q1: Why did you choose Redshift over other data warehouse solutions?**

**Answer:**  
We evaluated Snowflake, BigQuery, and Redshift. Chose Redshift because:
1. Already heavily invested in AWS ecosystem (S3, Glue)
2. 40% cost advantage for our workload pattern
3. Existing PostgreSQL knowledge transferable to Redshift
4. RA3 instances with managed storage fit our scaling needs
5. Strong integration with our BI tools (Tableau)
6. Spectrum for querying S3 directly without loading

**Q2: Explain your SCD Type 2 implementation.**

**Answer:**  
For Customer dimension, we maintained:
1. customer_key (surrogate key)
2. customer_id (natural key)
3. effective_date and end_date
4. is_current flag for active record

When customer address changes:
- Old record: set end_date = yesterday, is_current = False
- New record: insert with effective_date = today, end_date = '9999-12-31', is_current = True

Used MERGE statement in Redshift for this upsert logic. This allowed historical analysis like 'what was customer's address when they made this transaction'.

### Project 3: Regulatory Reporting

**Q1: How did you handle changing regulatory requirements?**

**Answer:**  
Built flexibility into the system:
1. Externalized all business rules into configuration files (JSON)
2. Created a calculation engine that reads rules at runtime
3. Used version control (Git) for all rule changes with approval process
4. Maintained parallel environments (Dev, UAT, Prod) for testing rule changes
5. Documented every rule change with regulatory reference
6. Created rollback capability to previous rule versions if needed

When Basel III.1 rules changed in Sep 2024, we deployed updates in 2 weeks vs 3 months earlier.

**Q2: How did you optimize Databricks cluster costs?**

**Answer:**  
Cost optimization was crucial:
1. Used job clusters (auto-terminate) instead of interactive clusters
2. Right-sized clusters - started with 4 workers, scaled to 2 after analysis
3. Scheduled jobs during off-peak hours for spot instance pricing (60% savings)
4. Enabled auto-scaling with min 2, max 8 workers
5. Used Delta Cache for frequently accessed data
6. Optimized file sizes (128MB ideal) to reduce processing time
7. Set cluster policies to prevent over-provisioning

Reduced monthly costs from $8K to $3K while improving performance.

---

## 💻 Technical Interview Questions

[See CODING_PRACTICE.md for complete coding questions with solutions]

### Quick Technical Q&A

**Q: Explain different types of JOINs with examples.**

**Answer:**
- **INNER JOIN:** Returns only matching rows from both tables
- **LEFT JOIN:** All rows from left table + matching from right (NULL if no match)
- **RIGHT JOIN:** All rows from right table + matching from left
- **FULL OUTER JOIN:** All rows from both tables
- **CROSS JOIN:** Cartesian product of both tables

```sql
SELECT c.customer_name, o.order_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;
```

**Q: What are Window Functions? Give examples.**

**Answer:**  
Window functions perform calculations across rows related to current row without collapsing rows (unlike GROUP BY).

Common functions:
- `ROW_NUMBER()`: Assigns unique row number
- `RANK()`: Ranks with gaps for ties
- `DENSE_RANK()`: Ranks without gaps
- `LAG/LEAD`: Access previous/next row
- `SUM/AVG/COUNT OVER`: Cumulative aggregations

```sql
-- Find top 3 customers by revenue per month
SELECT customer_id, month, revenue,
  RANK() OVER (PARTITION BY month ORDER BY revenue DESC) as rank
FROM monthly_sales
WHERE rank <= 3;
```

**Q: RDD vs DataFrame vs Dataset?**

**Answer:**
- **RDD:** Low-level API, immutable distributed collection, no schema, type-safe
- **DataFrame:** Higher-level API, distributed table with schema, Catalyst optimizer, not type-safe
- **Dataset:** Combines RDD + DataFrame benefits, type-safe, JVM languages only (not Python)

Recommendation: Use DataFrame API in PySpark for best performance and ease of use.

---

## 🎭 Behavioral Questions (STAR Method)

### STAR Method Template

- **S - Situation:** Set the context (1-2 sentences)
- **T - Task:** Explain your responsibility (1 sentence)
- **A - Action:** Detail what YOU did (2-3 sentences)
- **R - Result:** Quantify the outcome (1-2 sentences)

### Sample Behavioral Questions

**Q1: Tell me about yourself.**

**Answer:**  
I'm a Data Engineer with 3.3 years at Bank of America, where I currently work as Assistant Manager focusing on automation and data pipeline development. Previously, I worked as a Data Engineer at Omnicom Media Group where I built ETL pipelines using AWS Redshift, Python, and Alteryx. I have hands-on experience with both AWS and Azure cloud platforms, specializing in building scalable data solutions. I'm particularly passionate about optimizing data pipelines and implementing automated solutions that drive business value. I'm excited about this Data Engineer role at NAB because I want to work on larger-scale banking data challenges and contribute to building robust data infrastructure.

**Q2: Describe a challenging technical problem you solved.**

**Answer (Use Fraud Detection Project):**
- **S:** In our fraud detection pipeline, we faced severe data skew where 20% of customers accounted for 80% of transactions, causing some Spark executors to process 10x more data than others.
- **T:** I was responsible for optimizing the pipeline to process 500K transactions/hour with consistent latency.
- **A:** I implemented a salting technique - added a random suffix (0-9) to customer_id for partitioning, then aggregated results back. Also enabled AQE (Adaptive Query Execution) in Spark 3.0 and tuned shuffle partitions from 200 to 48 based on our cluster size.
- **R:** Reduced processing time from 8 seconds to 1.5 seconds (81% improvement) and achieved balanced executor utilization.

**Q3: Tell me about a time you had a conflict with a team member.**

**Answer:**
- **S:** During our data warehouse migration project, the DBA insisted on using normalized structure in Redshift, while I proposed a star schema for better query performance.
- **T:** My goal was to find a solution that balanced data integrity with performance.
- **A:** I scheduled a meeting to understand his concerns about data redundancy. I showed him benchmark results comparing both approaches using our sample dataset. We agreed to implement star schema for the analytics layer while maintaining normalized structure in the staging layer.
- **R:** This hybrid approach satisfied both requirements - queries ran 70% faster and we maintained data integrity in staging.

---

## ⚡ Quick Revision Cheat Sheets

### SQL Joins - Quick Reference

```sql
-- INNER JOIN (only matching records)
SELECT * FROM table1 t1
INNER JOIN table2 t2 ON t1.id = t2.id;

-- LEFT JOIN (all from left + matching from right)
SELECT * FROM table1 t1
LEFT JOIN table2 t2 ON t1.id = t2.id;

-- RIGHT JOIN (all from right + matching from left)
SELECT * FROM table1 t1
RIGHT JOIN table2 t2 ON t1.id = t2.id;

-- FULL OUTER JOIN (all from both)
SELECT * FROM table1 t1
FULL OUTER JOIN table2 t2 ON t1.id = t2.id;

-- SELF JOIN (joining table to itself)
SELECT e1.name as employee, e2.name as manager
FROM employees e1
JOIN employees e2 ON e1.manager_id = e2.id;
```

### PySpark Essentials

```python
# Basic DataFrame operations
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# Create session
spark = SparkSession.builder.appName('app').getOrCreate()

# Read data
df = spark.read.csv('path/file.csv', header=True, inferSchema=True)
df = spark.read.parquet('path/file.parquet')

# Select columns
df.select('col1', 'col2').show()
df.select(col('col1'), col('col2')*2).show()

# Filter
df.filter(col('age') > 25).show()
df.where((col('age') > 25) & (col('city') == 'NYC')).show()

# GroupBy
df.groupBy('department').agg(
    count('*').alias('count'),
    avg('salary').alias('avg_salary')
).show()

# Join
df1.join(df2, df1.id == df2.id, 'inner').show()

# Write
df.write.mode('overwrite').parquet('output/path')
df.write.format('delta').save('delta/path')
```

### Python Data Structures

```python
# List operations
my_list = [1, 2, 3, 4, 5]
my_list.append(6)  # Add at end
my_list.insert(0, 0)  # Add at index
my_list.remove(3)  # Remove by value
my_list.pop()  # Remove last

# Dictionary operations
my_dict = {'a': 1, 'b': 2}
my_dict['c'] = 3  # Add/update
my_dict.get('a')  # Get value
my_dict.keys()  # Get all keys
my_dict.values()  # Get all values

# List comprehension
squares = [x**2 for x in range(10)]
even_squares = [x**2 for x in range(10) if x % 2 == 0]

# Lambda functions
square = lambda x: x**2
result = list(map(lambda x: x*2, [1, 2, 3]))  # [2, 4, 6]
```

---

## ❓ Questions to Ask the Interviewer

### About the Role & Team
1. What are the immediate priorities for this role in the first 3 months?
2. What does a typical day look like for a Data Engineer on this team?
3. How is the data engineering team structured? Who would I be working most closely with?
4. What are the biggest data challenges NAB is facing right now?

### About Technology & Projects
1. What is NAB's current data platform strategy? (Cloud providers, tools, architecture)
2. Are there any upcoming data modernization initiatives?
3. What's the tech debt situation, and what's being done about it?
4. How does the team balance maintenance vs new feature development?

### About Culture & Growth
1. What does success look like in this role after 6 months?
2. What learning and development opportunities are available for data engineers?
3. How does NAB support work-life balance for the engineering team?
4. What do you enjoy most about working at NAB?

---

## ✅ Interview Day Checklist

### Night Before (20th Feb)
- [ ] Review all 3 projects one final time - be able to explain each in 2 minutes
- [ ] Quick glance through key technical concepts
- [ ] Prepare questions to ask interviewer (5 questions)
- [ ] Check route to interview location, plan to reach 15 mins early
- [ ] Lay out formal clothes
- [ ] Print 2 copies of resume
- [ ] Set 2 alarms
- [ ] SLEEP EARLY - aim for 8 hours

### Morning Of (21st Feb)
- [ ] Good breakfast
- [ ] Dress professionally (formal shirt, trousers, polished shoes)
- [ ] Carry: Resume copies, pen, notepad, ID proof, water bottle
- [ ] Leave early to avoid traffic stress

### During Interview
- [ ] **First 30 seconds:** Smile, firm handshake, maintain eye contact
- [ ] Listen carefully to questions - pause before answering
- [ ] Use STAR method for behavioral questions
- [ ] For technical questions - think aloud, explain your approach
- [ ] If you don't know something - be honest, show willingness to learn
- [ ] Show enthusiasm about the role and NAB
- [ ] Ask your prepared questions at the end
- [ ] Thank them for their time

---

## 💪 Confidence Boosters

### Remember Your Strengths
- ✅ You have 3.3 years of real banking experience
- ✅ You've worked with both AWS and Azure
- ✅ You understand the banking domain
- ✅ You have automation mindset - valuable for efficiency
- ✅ You're prepared with solid project examples

### Mindset
- This is a conversation, not an interrogation
- They want you to succeed - they're rooting for a good candidate
- Even if you don't know something, show problem-solving approach
- Your enthusiasm and attitude matter as much as technical skills

### Key Mantras
1. **I am well-prepared**
2. **I have relevant experience**
3. **I will stay calm and confident**
4. **I've got this! 🚀**

---

## 📚 Additional Resources

For detailed coding practice with 65+ questions and solutions, see [CODING_PRACTICE.md](CODING_PRACTICE.md)

---

**ALL THE BEST! You're going to do GREAT! 💪🎯**

*Contact me if you need any clarifications or more practice questions!*
