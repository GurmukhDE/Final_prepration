
# 📕 ULTRA ENTERPRISE DATA ENGINEERING VOLUME
### NAB & GlobalLogic – Senior / Lead / Principal / Architect Level
### Fully Expanded Technical + Regulatory + Performance + War-Room Guide

=====================================================================
PART 1 — 3-HOUR BRUTAL PANEL SIMULATION (FULL TRANSCRIPT STYLE)
=====================================================================

ROUND 1 — SQL & WAREHOUSE PERFORMANCE (45 MINUTES)

Panel: "You have a 15TB Redshift fact table. CFO says monthly reporting must move from 25 minutes to under 3 minutes. Walk us through your approach."

Candidate Model Answer (Expanded):

Step 1 — Baseline Metrics
- Capture execution time
- Capture CPU %, disk spill %, queue wait time
- Extract SVL_QUERY_SUMMARY metrics
- Identify skew across slices

Step 2 — Execution Plan Analysis
- Identify DS_BCAST_INNER (bad distkey)
- Detect nested loop joins
- Check predicate pushdown
- Validate partition elimination

Step 3 — Distribution Strategy
- If fact joins dimension on customer_id → DISTKEY(customer_id)
- Small dimension tables → DISTSTYLE ALL
- High skew column? → EVEN + pre-aggregation

Step 4 — Sort Key Optimization
- Compound vs Interleaved sort
- If filter heavily on date → SORTKEY(transaction_date)

Step 5 — Pre-Aggregation Layer
- Monthly aggregate table
- Materialized view refresh schedule

Step 6 — Concurrency Scaling
- Enable for BI queue
- Separate ETL and BI workloads via WLM

Follow-up Grill:

Panel: "What if skew remains after changing distkey?"
Answer:
- Identify top skewed key (e.g., one customer = 30% data)
- Salt the key
- Isolate heavy key into separate workload
- Pre-aggregate heavy customer data

Panel: "How do you defend the cost increase?"
Answer:
- Show credit/hour savings post-optimization
- Present cost per report before/after
- Present compute utilization improvement
- Link to SLA compliance & business benefit

=====================================================================
PART 2 — FULL REGULATORY REPORTING CASE STUDY (BANKING DEPTH)
=====================================================================

Scenario:
NAB must submit Basel III capital adequacy report quarterly.

Requirements:
- Historical reconstruction
- Late trade adjustments
- Audit trail for every change
- Immutable raw storage
- Reconciliation checkpoints

Architecture:

Source Systems:
- Core Banking
- Loan System
- Card System
- Risk Engine

Ingestion Layer:
- CDC via AWS DMS
- Bronze immutable Delta storage

Transformation Layer:
- Data Vault 2.0 (Hubs, Links, Satellites)
- SCD Type 2 for reporting mart

Gold Layer:
- Risk-weighted asset aggregation
- Exposure at default calculation
- Capital ratio computation

Critical Controls:

1. Reconciliation Checkpoint
   - Source row count vs target
   - Hash total validation

2. Late Trade Handling
   - Backdated correction triggers reprocessing
   - Rebuild affected partition only

3. Timezone Handling
   - Store all timestamps UTC
   - Convert to reporting timezone in Gold layer

4. Audit Reconstruction
   - Delta time travel
   - Satellite load_date tracking
   - Full lineage documentation

Failure Simulation:

Quarter-end mismatch discovered.

War Room Response:
1. Freeze report
2. Run reconciliation diff
3. Identify transformation error
4. Rebuild impacted partition
5. Revalidate totals
6. Document RCA
7. Submit amended report

=====================================================================
PART 3 — SPARK TUNING MATHEMATICAL HANDBOOK
=====================================================================

Executor Memory Formula:

Executor Memory = Total Memory - Memory Overhead
Usable Memory ≈ 0.6 × Executor Memory (after Spark internal allocation)

Partition Sizing:

Ideal Partition Size = 128MB – 256MB

If dataset = 2TB:
2TB = 2048GB
2048GB / 0.128GB ≈ 16,000 partitions

Shuffle Partitions:
spark.sql.shuffle.partitions = data_size / target_partition_size

GC Pressure Reduction:
- Use Kryo serialization
- Increase spark.memory.fraction
- Avoid wide transformations when possible

Broadcast Join Threshold:
Default ~10MB
Increase if dimension < executor memory / number_of_cores

Skew Detection:
- Compare partition sizes
- Use Spark UI stage metrics
- Enable spark.sql.adaptive.skewJoin.enabled

Streaming Exactly-Once:

- Checkpoint directory
- Idempotent merge
- Deduplication key
- Watermarking

=====================================================================
PART 4 — REDSHIFT VS SNOWFLAKE WAR-ROOM GUIDE
=====================================================================

Architecture Philosophy:

Redshift:
- Node-based cluster
- Distribution & sort keys critical
- Manual tuning required

Snowflake:
- Separation of compute/storage
- Auto micro-partitioning
- Automatic scaling

Performance Comparison:

Redshift Best For:
- Predictable workloads
- Controlled cost
- Fine-grained tuning

Snowflake Best For:
- Variable workloads
- Multi-team concurrency
- Minimal tuning overhead

Cost Modeling:

Redshift:
Cost = Node type × Number of nodes × Hours

Snowflake:
Cost = Credits/hour × Warehouse size × Runtime

Optimization Levers:

Redshift:
- Vacuum
- Analyze
- Distkey tuning
- Sortkey tuning

Snowflake:
- Clustering key
- Auto suspend
- Result cache
- Search optimization service

Failure Scenarios:

Redshift Node Failure:
- Multi-AZ replication
- Snapshot restore

Snowflake Region Failure:
- Cross-region replication
- Failover group

=====================================================================
PART 5 — EXECUTIVE ARCHITECTURE DEFENSE
=====================================================================

How to Present to Board:

1. Start with Business Outcome
2. Present Risk Mitigation
3. Show Cost Impact
4. Demonstrate Scalability Plan
5. Show Compliance Coverage

Trade-Off Example:

Decision: Delta Lake vs Snowflake

Delta Pros:
- Open format
- Lower storage cost
- Streaming support

Snowflake Pros:
- Less operational overhead
- Auto optimization
- Strong governance UI

Defense Strategy:
- Match architecture to business volatility
- Provide 3-year cost forecast
- Present regulatory compliance mapping

=====================================================================
END OF ULTRA ENTERPRISE VOLUME
=====================================================================

This document includes:

✔ Brutal panel simulation (expanded)
✔ Regulatory reporting deep dive
✔ Spark mathematical tuning handbook
✔ Redshift vs Snowflake war-room comparison
✔ Executive-level architecture defense
✔ Failure handling scenarios
✔ Trade-off modeling

You are now prepared at Principal / Architect level.
