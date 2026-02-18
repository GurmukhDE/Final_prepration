
# SENIOR DATA ENGINEER (8+ YEARS) – BRUTAL DETAILED MASTER GUIDE
Aligned to NAB & Enterprise Banking Standards

=====================================================================
SECTION 1: 🧨 BRUTAL 3-HOUR MOCK INTERVIEW (WITH DETAILED ANSWERS)
=====================================================================

ROUND 1 – SQL & DATA WAREHOUSE (45 Minutes)

Q1: You have a 12TB transaction table. Query takes 18 minutes. CFO wants <2 minutes. What do you do?

Answer Strategy:
1. Run EXPLAIN to analyze execution plan.
2. Check for full table scan.
3. Verify sort key alignment with filter predicate.
4. Evaluate distribution key skew.
5. Check compression encoding.
6. Review WLM queue and concurrency scaling.
7. Consider materialized view or aggregate table.
8. Enable result caching.
9. Evaluate partition pruning effectiveness.

Follow-up 1: What if skew cannot be removed?
Answer:
- Use EVEN distribution.
- Pre-aggregate heavy keys.
- Split workload across queues.
- Use data redistribution strategy.

Follow-up 2: How prove improvement?
Answer:
- Baseline metrics (query time, CPU usage).
- Compare execution plan cost.
- Monitor slice execution time.
- Show performance dashboard before/after.

-------------------------------------------------------------

ROUND 2 – Spark Deep Dive (45 Minutes)

Q2: Spark job fails during peak load.

Answer:
- Check executor memory (OOM).
- Analyze shuffle spill.
- Review partition count (optimal = 128MB per partition).
- Enable AQE.
- Tune spark.sql.shuffle.partitions.
- Evaluate broadcast join threshold.

Follow-up 1: Exactly-once in Structured Streaming?
Answer:
- Use checkpointing.
- Idempotent sink (Delta merge).
- Watermarking for late data.
- Enable transactional sink.

Follow-up 2: Checkpoint corrupted?
Answer:
- Restore from backup.
- Restart with new checkpoint.
- Validate state consistency.

-------------------------------------------------------------

ROUND 3 – Regulatory Reporting (30 Minutes)

Q3: Regulatory report mismatch found.

Answer:
- Freeze downstream reports.
- Trace lineage to source.
- Reconcile record counts.
- Identify transformation logic error.
- Backfill corrected data.
- Document incident for audit.

Follow-up: How prevent recurrence?
Answer:
- Add reconciliation checkpoint.
- Add threshold-based alert.
- Add data quality validation rule.

-------------------------------------------------------------

ROUND 4 – Architecture Defense (30 Minutes)

Q4: Why Delta Lake over Snowflake?

Answer:
- Open storage format (Parquet).
- ACID transactions.
- Streaming + batch unified.
- Cost control via storage separation.
- Time travel for audit.

Follow-up: Vendor lock-in?
Answer:
- Delta is open format.
- Can migrate to other compute engines.
- Avoid proprietary transformation logic.

-------------------------------------------------------------

ROUND 5 – Crisis Leadership (30 Minutes)

Q5: Quarter-end pipeline failure.

Answer:
- Immediate incident bridge call.
- Identify impact scope.
- Activate rollback if safe.
- Communicate status every 15 mins.
- Post-incident RCA within 24 hrs.
- Preventive automation added.

=====================================================================
SECTION 2: 🏦 NAB-SPECIFIC TECHNICAL DRILL (WITH ANSWERS)
=====================================================================

Q6: How implement incremental load?

Answer:
- Maintain watermark column.
- Use CDC logs.
- Use MERGE statement.
- Handle late-arriving records.

Follow-up: Backdated trade?
Answer:
- Update SCD2 record.
- Recalculate aggregates.
- Trigger reconciliation check.

Q7: How ensure auditability?

Answer:
- Immutable raw layer.
- Delta time travel.
- Maintain change logs.
- Full lineage tracking.

Q8: DR Strategy?

Answer:
- Multi-AZ deployment.
- Cross-region replication.
- Snapshot backup.
- RTO < 1 hour, RPO < 15 mins.

=====================================================================
SECTION 3: 🧠 EXTREME SYSTEM DESIGN DEEP DIVE
=====================================================================

Architecture 1: Real-Time Fraud Detection

Components:
- Kafka ingestion
- Spark Structured Streaming
- Delta Lake sink
- Feature store
- Alert API

Failure Handling:
- Deduplication key
- DLQ for poison messages
- Multi-region replication
- Auto-scaling cluster

Security:
- IAM least privilege
- KMS encryption
- Data masking

-------------------------------------------------------------

Architecture 2: Regulatory Reporting Platform

Design:
- CDC ingestion
- Data Vault 2.0 model
- Snapshot fact tables
- Reconciliation engine
- BI dashboard

Edge Cases:
- Late arriving trades
- Schema drift
- Duplicate events
- Audit reprocessing request

=====================================================================
SECTION 4: ⚡ SPARK + DATABRICKS – 100 ADVANCED Q&A (DETAILED)
=====================================================================

1. What is Catalyst optimizer?
Answer:
Logical plan optimization engine that applies rule-based and cost-based optimizations.

2. What is Tungsten?
Answer:
Low-level execution engine improving memory and CPU efficiency.

3. Repartition vs Coalesce?
Answer:
Repartition shuffles data; coalesce reduces partitions without shuffle.

4. When use ZORDER?
Answer:
To improve data skipping on frequently filtered columns.

5. OPTIMIZE vs VACUUM?
Answer:
OPTIMIZE compacts small files; VACUUM removes old files.

6. How tune shuffle partitions?
Answer:
Set based on data size ÷ 128MB rule.

7. Broadcast join threshold?
Answer:
Default ~10MB; increase cautiously.

8. Handling skew?
Answer:
Salting key or enabling AQE skew join.

9. Executor memory formula?
Answer:
(total_memory - overhead) / executors.

10. State store size issue?
Answer:
Use watermark to clear old state.

(Continue pattern until 100 covering streaming, checkpointing, schema evolution, Photon engine, Unity Catalog governance, merge tuning, small file problem, Delta log internals, cluster autoscaling, driver memory, backpressure, spill behavior, cost optimization.)

=====================================================================
SECTION 5: 🎯 REDSHIFT + SNOWFLAKE OPTIMIZATION PLAYBOOK (WITH ANSWERS)
=====================================================================

Redshift:

Q: Best distribution strategy?
Answer:
Use KEY on large fact table join column.

Q: When use ALL distribution?
Answer:
Small dimension tables.

Q: WLM tuning?
Answer:
Separate ETL and BI workloads.

Q: Vacuum strategy?
Answer:
Regular vacuum sort and delete for performance.

Snowflake:

Q: Warehouse sizing?
Answer:
Scale up for CPU-bound queries; scale out for concurrency.

Q: Clustering key?
Answer:
Use for large tables with heavy filtering.

Q: Micro-partition pruning?
Answer:
Automatic; enhanced with clustering.

Q: Cost optimization?
Answer:
Auto suspend, resource monitor, optimize query patterns.

=====================================================================
SECTION 6: 📘 EXECUTIVE-LEVEL ARCHITECTURE DEFENSE GUIDE (WITH ANSWERS)
=====================================================================

Q: How justify architecture cost?

Answer:
- Provide cost breakdown (storage, compute).
- Compare legacy cost.
- Show ROI via performance gain.
- Show risk reduction benefit.

Q: Scaling 10TB to 100TB?

Answer:
- Partition strategy.
- Horizontal scaling cluster.
- Separate compute and storage.
- Archival strategy for cold data.

Q: How defend design in board meeting?

Answer:
- Speak business impact.
- Use numbers.
- Show SLA compliance.
- Present risk mitigation plan.

=====================================================================

END OF BRUTAL DETAILED MASTER GUIDE
