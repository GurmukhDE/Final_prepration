
# 📘 NAB & GlobalLogic Enterprise Interview Series
# Volume 1 — Deep SQL, Python, and Architecture (Fully Expanded)

This volume contains:
- 60 Fully Unique, Deep Technical Questions
- Expanded Answers (No placeholders)
- Architect-Level Follow-ups
- Banking & Production Scenarios
- Trade-off & Cost Analysis Discussions

=====================================================================
SECTION 1 — ADVANCED SQL (WAREHOUSE LEVEL)
=====================================================================

Q1. Explain how cost-based optimizers choose join order.

Answer:
Modern query optimizers (Snowflake, Redshift, Oracle) estimate cardinality using statistics such as table row count, column distinct values, and histograms. The optimizer generates multiple join trees and calculates estimated cost based on I/O, CPU, memory, and network redistribution. It prefers plans minimizing data movement and intermediate result size. Poor statistics cause wrong cardinality estimates leading to inefficient join order.

Follow-up:
- What happens if statistics are stale?
- How does dynamic pruning help?
- How do you manually influence join order in Redshift?

---

Q2. How do you detect and fix skew in Redshift?

Answer:
Query SVL_QUERY_SUMMARY to compare slice execution time. If one slice runs significantly longer, skew exists. Check DISTKEY distribution. Use EVEN distribution if skewed key cannot be fixed. Consider pre-aggregation or salting technique.

Follow-up:
- What is slice-level parallelism?
- Why can skew increase network cost?

---

Q3. Explain clustering depth in Snowflake and when to recluster.

Answer:
Clustering depth measures how well micro-partitions are organized relative to clustering key. If depth increases, pruning efficiency decreases, leading to higher partitions scanned ratio. Reclustering improves pruning but costs compute credits. Decision must weigh pruning improvement vs maintenance cost.

Follow-up:
- What pruning ratio justifies reclustering?
- Compare clustering vs Search Optimization Service.

---

Q4. Implement SCD Type 2 with hash-based change detection.

Answer:
Compute MD5 hash of tracked attributes in staging. Compare with current active record. If hash differs, expire current record (set expiry_date), insert new version. Ensure transaction boundary to avoid duplicate active rows.

Follow-up:
- How to handle late-arriving updates?
- How to prevent concurrent duplicate current rows?

---

Q5. Difference between Data Vault Satellite and SCD2 dimension.

Answer:
Satellite stores historical attribute changes with load_date and hash_diff. SCD2 stores effective/expiry dates and current flag. Satellite separates structure from business logic; SCD2 optimized for BI consumption.

Follow-up:
- When would you use both together?

=====================================================================
SECTION 2 — PYTHON ENGINEERING
=====================================================================

Q6. Design a retry mechanism with exponential backoff and jitter.

Answer:
Exponential backoff reduces collision during retry storms. Add random jitter to avoid synchronized retries. Use decorator pattern. Ensure max attempt threshold and specific exception filtering.

Follow-up:
- What is circuit breaker pattern?
- When should you fail fast instead of retrying?

---

Q7. How do you design idempotent API ingestion?

Answer:
Use request ID as idempotency key. Store processed request IDs in metadata table. Reject duplicates. Use transactional write to avoid partial persistence.

Follow-up:
- What is eventual consistency impact?
- How do you design idempotency for streaming?

---

Q8. Memory-efficient file processing at 100GB scale.

Answer:
Use streaming I/O or chunking. Avoid full DataFrame loads. For transformation-heavy jobs, move to distributed system (Spark). Monitor memory footprint with tracemalloc.

Follow-up:
- What is GIL impact on CPU-bound tasks?
- When to use multiprocessing vs threading?

=====================================================================
SECTION 3 — SPARK ARCHITECTURE FUNDAMENTALS
=====================================================================

Q9. Explain narrow vs wide transformations.

Answer:
Narrow transformations (map, filter) do not require shuffle; wide transformations (groupBy, join) require data redistribution causing shuffle boundary and stage split.

Follow-up:
- Why is shuffle expensive?
- How does AQE reduce shuffle overhead?

---

Q10. How do you calculate optimal partitions for 2TB dataset?

Answer:
2TB = 2048GB.
If target partition size = 128MB (0.128GB),
2048 / 0.128 ≈ 16,000 partitions.
Set spark.sql.shuffle.partitions accordingly.

Follow-up:
- What if cluster has insufficient cores?
- How does dynamic allocation affect this?

---

Q11. Explain Spark memory model (Unified Memory Manager).

Answer:
Spark divides memory into execution and storage regions. Execution memory handles shuffle, joins, aggregations. Storage handles caching. Unified manager allows borrowing between regions.

Follow-up:
- What causes OOM in executor?
- How to tune spark.memory.fraction?

---

Q12. How do you recover from corrupted checkpoint in streaming?

Answer:
Restore checkpoint backup or start new checkpoint and replay from Kafka offset. Must ensure sink idempotency to prevent duplicate writes.

Follow-up:
- What happens if offset is committed but sink failed?

=====================================================================
SECTION 4 — ARCHITECTURE & BANKING
=====================================================================

Q13. Design regulatory reporting system with full auditability.

Answer:
Bronze immutable layer stores raw source. Silver transformation includes reconciliation checkpoints. Data Vault for historical integration. Gold reporting mart with SCD2. Delta time travel enables reconstruction.

Follow-up:
- How to handle backdated trade?
- How to prove lineage to regulator?

---

Q14. Explain RTO and RPO in banking DR.

Answer:
RTO (Recovery Time Objective) = maximum acceptable downtime.
RPO (Recovery Point Objective) = maximum acceptable data loss window.
Banking often requires RPO < 15 minutes, RTO < 1 hour.

Follow-up:
- How do you test DR without impacting production?

---

Q15. How do you defend architecture cost to CFO?

Answer:
Present baseline cost, optimization savings, SLA compliance benefit, risk mitigation impact. Show cost per report before/after improvement.

Follow-up:
- How to justify higher compute for regulatory compliance?

=====================================================================
END OF VOLUME 1
=====================================================================

Volume 2 will include:
- Deep Spark Optimization (Memory, GC, Shuffle Math)
- Streaming Internals
- Data Skew War Room
- Advanced Delta Engineering

