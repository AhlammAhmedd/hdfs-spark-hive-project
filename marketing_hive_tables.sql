-- Run with: beeline -u jdbc:hive2:// -n hadoop --silent=true -f marketing_hive_tables.sql

CREATE DATABASE IF NOT EXISTS marketing;
USE marketing;


-- processed layer (/MarketingProcessed)

CREATE EXTERNAL TABLE IF NOT EXISTS fact_events_clean (
  event_id BIGINT, event_date DATE, customer_id INT, product_id INT, campaign_id INT,
  event_type STRING, device STRING, price DOUBLE, quantity INT, revenue DOUBLE, ad_cost DOUBLE
)
PARTITIONED BY (year_month STRING)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingProcessed/fact_events_clean';

MSCK REPAIR TABLE fact_events_clean;

CREATE EXTERNAL TABLE IF NOT EXISTS campaign_daily_summary (
  event_date DATE, campaign_id BIGINT, channel STRING, objective STRING,
  impressions BIGINT, clicks BIGINT, purchases BIGINT,
  ad_cost DOUBLE, revenue DOUBLE, ctr_pct DOUBLE, roas DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingProcessed/campaign_daily_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS customer_summary (
  customer_id BIGINT, total_events BIGINT, orders BIGINT, total_spent DOUBLE,
  first_event DATE, last_event DATE, country STRING, segment STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingProcessed/customer_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS product_summary (
  product_id BIGINT, views BIGINT, add_to_cart BIGINT, purchases BIGINT,
  revenue DOUBLE, category STRING, brand STRING, list_price DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingProcessed/product_summary';


-- dimensions (/MarketingData)

CREATE EXTERNAL TABLE IF NOT EXISTS dim_customers (
  customer_id BIGINT, country STRING, gender STRING, segment STRING, signup_date DATE
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingData/dim_customers';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
  product_id BIGINT, category STRING, brand STRING, list_price DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingData/dim_products';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_campaigns (
  campaign_id BIGINT, channel STRING, objective STRING, budget DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingData/dim_campaigns';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_key INT, full_date DATE, `year` INT, `quarter` INT, `month` INT,
  month_name STRING, day_of_week INT, is_weekend BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs:///MarketingData/dim_date';


-- checks (metadata and LIMIT only, no jobs)

SHOW TABLES;

SHOW PARTITIONS fact_events_clean;

SELECT * FROM fact_events_clean LIMIT 5;

SELECT * FROM campaign_daily_summary LIMIT 5;

DESCRIBE FORMATTED fact_events_clean;
