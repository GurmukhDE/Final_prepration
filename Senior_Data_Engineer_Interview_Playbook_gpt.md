# Senior Data Engineer (8+ Years) -- Complete Interview Playbook

Aligned to NAB & GlobalLogic JD

------------------------------------------------------------------------

## 1. Advanced SQL

### Q: How do you optimize a slow-running SQL query in Redshift/Snowflake?

**Answer:** - Analyze execution plan (EXPLAIN) - Optimize
distribution/sort keys (Redshift) - Use clustering (Snowflake) -
Partition pruning - Predicate pushdown - Avoid SELECT \* - Check skew -
Vacuum & Analyze (Redshift)

### Edge Case:

What if distribution key is skewed? - Leads to uneven node processing -
Change dist key - Use EVEN distribution - Consider composite key

------------------------------------------------------------------------

## 2. Python Data Engineering

### Q: How do you design a production-grade Python ETL?

**Answer:** - Modular architecture - Structured logging - Retry logic -
Idempotency - Config-driven - Secrets management - Unit tests - CI/CD
integration

### Edge Case:

Partial load failure? - Write to staging first - Use transactional
merge - Watermark logic - Rollback mechanism

------------------------------------------------------------------------

## 3. Spark / PySpark

### Q: How Spark optimizes queries?

-   Catalyst optimizer
-   Tungsten engine
-   Lazy evaluation
-   DAG optimization

### Data Skew Fix:

-   Salting
-   Broadcast join
-   Repartition
-   AQE

------------------------------------------------------------------------

## 4. Delta Lake

Why Delta over Parquet? - ACID transactions - Schema evolution - Time
travel - MERGE support

Edge Case: Concurrent writes? - Optimistic concurrency control

------------------------------------------------------------------------

## 5. AWS Architecture Design

Design ingestion from: - Oracle DB - CSV files - Kafka - Load to
Redshift

Architecture: - S3 landing - Glue/EMR Spark - Lambda triggers - SNS
alerts - Redshift - CloudWatch monitoring

Edge Cases: - Duplicate events - Retry storms - IAM misconfiguration -
Connection limits

------------------------------------------------------------------------

## 6. Data Modeling

### Star Schema vs Data Vault 2.0

Star: - BI focused - Denormalized - Fast queries

Data Vault: - Audit focused - Scalable - Historical tracking

Late Arriving Dimension Handling: - Surrogate key - Unknown member -
Backfill process

------------------------------------------------------------------------

## 7. System Design

### Real-Time Fraud Detection

Requirements: - \<2 sec latency - Exactly-once - High throughput

Consider: - Kafka - Spark Streaming - Delta Lake - Feature Store -
Alerting system

Edge Cases: - Duplicate messages - Schema drift - Regional failure -
Silent data corruption

------------------------------------------------------------------------

## 8. CI/CD & DevOps

Pipeline: - Git branching - PR reviews - Jenkins - Docker - Terraform -
Automated tests

Prevent Bad SQL Deployment: - SQL linting - Pre-commit hook - Validation
environment

------------------------------------------------------------------------

## 9. DR & Production Readiness

DR Strategy: - Multi-AZ - Cross-region replication - Snapshot backups -
RTO/RPO defined

Test DR: - Simulate outage - Data consistency validation - DNS switch

------------------------------------------------------------------------

## 10. Leadership & Program Management

Managing Multiple Workstreams: - Sprint planning - Risk register -
Budget tracking - Dependency mapping

Stakeholder Conflict: - Impact analysis - Phased rollout - Executive
alignment

------------------------------------------------------------------------

## 11. Banking-Specific Questions

-   Basel regulations impact on data
-   P&L reconciliation logic
-   Audit trails
-   GDPR compliance
-   Data lineage tracking

------------------------------------------------------------------------

## 12. Troubleshooting Scenario

Pipeline failed at 2 AM: 1. Check logs 2. Identify root cause 3.
Validate data integrity 4. Rollback if needed 5. Notify stakeholders 6.
RCA documentation 7. Prevent recurrence

------------------------------------------------------------------------

## 13. Hard Edge Case Questions

-   Idempotent ETL design
-   Exactly-once delivery
-   Handling schema drift
-   Avoiding double counting
-   Timezone issues in financial transactions
-   Detecting silent data corruption

------------------------------------------------------------------------

## 14. Executive-Level Questions

-   Cloud cost optimization
-   Redshift cost control
-   Scaling from 10TB to 100TB
-   Measuring platform maturity

------------------------------------------------------------------------

# End of Playbook
