# Marketing Big Data Pipeline

An end-to-end big data project on a single-node Hadoop cluster: marketing events are generated as a star schema, stored on **HDFS**, checked and processed with **Spark**, exposed through **Hive** tables, and analysed with **HiveQL**.

<p align="center"><img src="https://raw.githubusercontent.com/AhlammAhmedd/hdfs-spark-hive-project/main/marketing_big_data_pipeline.png" alt="Marketing big data pipeline" width="100%"></p>

## Pipeline

| Step | What it does | Output on HDFS |
|------|--------------|----------------|
| 1. Generate and upload | Creates the star schema (PySpark), then uploads it | `/MarketingData` (staging) |
| 2. Quality checks | Nulls, duplicates, orphan keys, business rules | `/MarketingProcessed/dq_report` |
| 3. Processing | Cleans the fact table, builds 3 summary tables | `/MarketingProcessed` |
| 4. Hive tables | 8 external tables in the `marketing` database | (metadata only) |
| 5. Analysis | 10 HiveQL queries, results saved as CSV | `/MarketingResults` |

**Stack:** Hadoop (HDFS, YARN) · Spark 4.1.3 · Hive 4.2.1 · Java 21 · Parquet

## Data model

Star schema with synthetic data.

| Table | Rows | Notes |
|-------|------|-------|
| `fact_events` | 10,000,000 | Parquet, partitioned by `year_month` (2024-01 to 2025-12) |
| `dim_customers` | 500,000 | country, segment (New, Regular, VIP) |
| `dim_products` | 10,000 | category, brand, list price |
| `dim_campaigns` | 500 | channel, objective, budget |
| `dim_date` | 730 | calendar attributes, `is_weekend` |

## Files

| File | Purpose |
|------|---------|
| `generate_data.py` | Generates the star schema locally |
| `marketing_quality_checks.py` | Quality checks and report |
| `marketing_processing.py` | Cleaning and summary tables |
| `marketing_hive_tables.sql` | Hive database and external tables |
| `marketing_hive_analysis.sql` | 10 analytical queries, results saved to HDFS |
| `run_pipeline.sh` | Runs the steps in order |
| `Marketing_Project_Documentation.pdf` | Full documentation with screenshots |

## How to run

HDFS and YARN must be running.

```bash
spark-submit generate_data.py
hdfs dfs -mkdir -p /MarketingData
hdfs dfs -put ~/MarketingData/* /MarketingData/

spark-submit marketing_quality_checks.py
spark-submit marketing_processing.py

beeline -u jdbc:hive2:// -n hadoop     # then run marketing_hive_tables.sql and marketing_hive_analysis.sql

hdfs dfs -ls /MarketingResults
hdfs dfs -cat /MarketingResults/q2_roas_per_channel/*
```

## Sample result

Return on ad spend per channel (`q2_roas_per_channel`):

| Channel | Revenue | Ad cost | ROAS |
|---------|---------|---------|------|
| email | 24,309,215.49 | 315,870.95 | 76.96 |
| tiktok | 24,870,075.88 | 323,455.24 | 76.89 |
| instagram | 25,359,670.41 | 331,281.03 | 76.55 |
| youtube | 28,754,049.33 | 376,937.20 | 76.28 |
| facebook | 24,766,723.56 | 324,783.95 | 76.26 |
| google_ads | 24,971,041.54 | 327,785.81 | 76.18 |
