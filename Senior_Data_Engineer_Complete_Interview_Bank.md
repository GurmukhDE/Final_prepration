
# Senior Data Engineer (8+ Years Experience)
## Complete Enterprise Interview Question Bank
Aligned to NAB & GlobalLogic JD

==================================================================
SECTION 1: ADVANCED SQL (ENTERPRISE LEVEL)
==================================================================

Q1: How do you optimize a slow-running query in Redshift/Snowflake?
Answer:
- Analyze EXPLAIN plan
- Optimize distribution key and sort key (Redshift)
- Use clustering keys (Snowflake)
- Avoid SELECT *
- Partition pruning
- Predicate pushdown
- Materialized views
- Vacuum & Analyze
- Detect skew

Follow-up:
What causes skew?
- Uneven key distribution
- Large fact join imbalance

Edge Case:
What if stats are outdated?
- Wrong execution plan
- Full table scan
- High query latency

------------------------------------------------------------------

Q2: Explain execution plan reading.
Answer:
- Identify scan type
- Check join type (Hash, Merge, Nested Loop)
- Look for repartition/shuffle
- Identify high cost operations

Scenario:
You have 5TB financial table. Query takes 25 min.
What to check?
- Sort key alignment
- Compression encoding
- Concurrency scaling
- WLM queue saturation

------------------------------------------------------------------

Q3: How to design incremental load SQL?
Answer:
- Use watermark column
- CDC based extraction
- Merge logic
- SCD Type 2 handling

Edge Case:
Late arriving data.
Solution:
- Backfill
- Surrogate key mapping
- Timestamp correction

==================================================================
SECTION 2: PYTHON DATA ENGINEERING
==================================================================

Q4: Design production ETL framework.
Answer:
- Modular architecture
- Logging (structured)
- Retry mechanism
- Idempotent design
- Config-driven pipelines
- Secrets management
- Unit testing
- CI/CD validation

Follow-up:
How ensure idempotency?
- Hash comparison
- Merge instead of insert
- Watermark tracking

Edge Case:
Partial load failure mid execution.
- Write to staging
- Transactional commit
- Rollback logic
- Checkpointing

------------------------------------------------------------------

Q5: Process 100GB file efficiently.
Answer:
- Chunk processing
- Generators
- Multiprocessing
- Move to Spark if distributed required

==================================================================
SECTION 3: SPARK & BIG DATA
==================================================================

Q6: Explain Spark execution flow.
- Logical plan
- Catalyst optimization
- Physical plan
- DAG execution

Q7: Narrow vs Wide transformation?
- Narrow: No shuffle
- Wide: Shuffle involved

Edge Case:
Data skew in join.
Solution:
- Salting
- Broadcast join
- Repartition
- AQE

Scenario:
Spark job failing due to OOM.
Steps:
- Increase executor memory
- Reduce partition size
- Cache selectively
- Optimize joins

==================================================================
SECTION 4: DELTA LAKE & DATABRICKS
==================================================================

Q8: Why Delta over Parquet?
- ACID
- Schema evolution
- Time travel
- Merge support

Edge Case:
Concurrent writes.
- Optimistic concurrency control

Scenario:
Need rollback yesterday load.
- Use Time Travel

==================================================================
SECTION 5: AWS ARCHITECTURE & DESIGN
==================================================================

Q9: Design enterprise ingestion platform.

Requirements:
- Oracle DB
- CSV Files
- Kafka streaming
- Load to Redshift

Architecture:
- S3 Landing
- Glue/EMR Spark
- Lambda trigger
- SNS/SQS alert
- Redshift
- CloudWatch

Edge Cases:
- S3 duplicate events
- Lambda retry storm
- Redshift connection limit
- IAM misconfiguration
- Schema drift
- Cross-region failover

------------------------------------------------------------------

Q10: How ensure high availability?
- Multi-AZ
- Auto-scaling
- Retry logic
- Dead letter queue

==================================================================
SECTION 6: DATA MODELING
==================================================================

Q11: Star vs Data Vault 2.0

Star:
- BI optimized
- Denormalized
- Faster aggregation

Data Vault:
- Regulatory compliance
- Historical tracking
- Scalable

Edge Case:
Late arriving dimension.
- Surrogate key
- Unknown member
- Backfill process

==================================================================
SECTION 7: SYSTEM DESIGN (VERY HARD)
==================================================================

Q12: Design real-time fraud detection system.

Requirements:
- <2 sec latency
- Exactly once
- High throughput

Design:
- Kafka
- Spark Streaming
- Delta Lake
- Feature store
- Alerting service

Edge Cases:
- Duplicate messages
- Consumer lag
- Schema drift
- Silent corruption
- Region outage
- Timezone mismatch
- Double counting

------------------------------------------------------------------

Q13: How design idempotent ETL?
- Unique business key
- Merge statement
- Transaction logs
- Checkpointing

==================================================================
SECTION 8: CI/CD, DEVOPS & GOVERNANCE
==================================================================

Q14: CI/CD for data engineering.
- Git strategy
- PR review
- Jenkins
- Docker
- Terraform
- Automated data tests

Edge Case:
Prevent bad deployment.
- Linting
- Pre-commit
- Staging validation

------------------------------------------------------------------

Q15: DR Strategy.
- Multi-AZ
- Cross-region replication
- RTO/RPO
- Snapshots

Test DR:
- Simulate outage
- Validate data consistency

==================================================================
SECTION 9: LEADERSHIP & PROGRAM MANAGEMENT
==================================================================

Q16: Manage multiple workstreams.
- Sprint planning
- Risk register
- Budget tracking
- Stakeholder alignment

Scenario:
Business wants urgent feature.
- Impact analysis
- Phased delivery
- Executive communication

------------------------------------------------------------------

Q17: Conflict resolution between teams.
- Data-driven decision
- Escalation matrix
- Clear documentation

==================================================================
SECTION 10: BANKING DOMAIN QUESTIONS
==================================================================

- Basel compliance impact
- P&L reconciliation design
- Audit trail implementation
- Data lineage tracking
- GDPR compliance

------------------------------------------------------------------

Q18: Production failure at 2AM.
Steps:
1. Log review
2. Identify stage
3. Data validation
4. Rollback if needed
5. Stakeholder notification
6. RCA
7. Prevent recurrence

==================================================================
SECTION 11: EXECUTIVE LEVEL QUESTIONS
==================================================================

- Cloud cost optimization
- Redshift cost reduction
- Scaling 10TB to 100TB
- Platform maturity measurement
- Governance & audit readiness

==================================================================

END OF COMPLETE INTERVIEW QUESTION BANK
