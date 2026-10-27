-- Runs the analytical queries and saves each result as a CSV file on HDFS
-- under /MarketingResults/<query_name>/
-- Run with: beeline -u jdbc:hive2:// -n hadoop --silent=true -f marketing_hive_export.sql

USE marketing;


-- Q1: marketing funnel
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q1_funnel'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       SUM(purchases) AS purchases,
       ROUND(100 * SUM(clicks) / SUM(impressions), 2) AS ctr_pct,
       ROUND(100 * SUM(purchases) / SUM(clicks), 2) AS click_to_purchase_pct
FROM campaign_daily_summary;


-- Q2: ROAS per channel
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q2_roas_per_channel'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT channel,
       CAST(ROUND(SUM(revenue), 2) AS DECIMAL(18,2)) AS revenue,
       CAST(ROUND(SUM(ad_cost), 2) AS DECIMAL(18,2)) AS ad_cost,
       ROUND(SUM(revenue) / SUM(ad_cost), 2) AS roas
FROM campaign_daily_summary
GROUP BY channel
ORDER BY roas DESC;


-- Q3: CTR per channel
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q3_ctr_per_channel'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT channel,
       SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       ROUND(100 * SUM(clicks) / SUM(impressions), 2) AS ctr_pct
FROM campaign_daily_summary
GROUP BY channel
ORDER BY ctr_pct DESC;


-- Q4: top 10 campaigns by revenue
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q4_top_campaigns'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT campaign_id, channel, objective,
       CAST(ROUND(SUM(revenue), 2) AS DECIMAL(18,2)) AS revenue,
       CAST(ROUND(SUM(ad_cost), 2) AS DECIMAL(18,2)) AS ad_cost,
       ROUND(SUM(revenue) / SUM(ad_cost), 2) AS roas
FROM campaign_daily_summary
GROUP BY campaign_id, channel, objective
ORDER BY revenue DESC
LIMIT 10;


-- Q5: revenue per product category
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q5_revenue_per_category'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT category,
       SUM(purchases) AS orders,
       CAST(ROUND(SUM(revenue), 2) AS DECIMAL(18,2)) AS revenue
FROM product_summary
GROUP BY category
ORDER BY revenue DESC;


-- Q6: revenue per customer country
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q6_revenue_per_country'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT country,
       SUM(orders) AS orders,
       CAST(ROUND(SUM(total_spent), 2) AS DECIMAL(18,2)) AS revenue
FROM customer_summary
GROUP BY country
ORDER BY revenue DESC;


-- Q7: customer segments
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q7_customer_segments'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT segment,
       COUNT(*) AS customers,
       CAST(ROUND(SUM(total_spent), 2) AS DECIMAL(18,2)) AS revenue,
       ROUND(AVG(total_spent), 2) AS avg_spent_per_customer
FROM customer_summary
GROUP BY segment
ORDER BY revenue DESC;


-- Q8: top 10 customers by spend
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q8_top_customers'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT customer_id, country, segment, orders, total_spent
FROM customer_summary
ORDER BY total_spent DESC
LIMIT 10;


-- Q9: weekend vs weekday
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q9_weekend_vs_weekday'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT d.is_weekend,
       SUM(c.purchases) AS orders,
       CAST(ROUND(SUM(c.revenue), 2) AS DECIMAL(18,2)) AS revenue
FROM campaign_daily_summary c
JOIN dim_date d ON c.event_date = d.full_date
GROUP BY d.is_weekend;


-- Q10: monthly revenue
INSERT OVERWRITE DIRECTORY 'hdfs:///MarketingResults/q10_monthly_revenue'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT d.`year`, d.`month`,
       CAST(ROUND(SUM(c.revenue), 2) AS DECIMAL(18,2)) AS revenue
FROM campaign_daily_summary c
JOIN dim_date d ON c.event_date = d.full_date
GROUP BY d.`year`, d.`month`
ORDER BY d.`year`, d.`month`;
