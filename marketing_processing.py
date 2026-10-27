from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("marketing_processing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

staging = "hdfs:///MarketingData"
processed = "hdfs:///MarketingProcessed"

fact = spark.read.parquet(f"{staging}/fact_events")
customers = spark.read.parquet(f"{staging}/dim_customers")
products = spark.read.parquet(f"{staging}/dim_products")
campaigns = spark.read.parquet(f"{staging}/dim_campaigns")


# Step 1: clean the fact table
rows_before = fact.count()

clean = (
    fact.dropDuplicates(["event_id"])
    .filter(F.col("price") > 0)
    .filter(F.col("customer_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("quantity", F.when(F.col("event_type") == "purchase", F.col("quantity")).otherwise(0))
)

clean.repartition("year_month").write.mode("overwrite") \
    .partitionBy("year_month").parquet(f"{processed}/fact_events_clean")

clean = spark.read.parquet(f"{processed}/fact_events_clean")
rows_after = clean.count()
print("rows before:", rows_before)
print("rows after: ", rows_after)
clean.show(5)


# Step 2: campaign daily summary
campaign_daily = (
    clean.join(F.broadcast(campaigns.select("campaign_id", "channel", "objective")), "campaign_id")
    .groupBy("event_date", "campaign_id", "channel", "objective")
    .agg(
        F.count(F.when(F.col("event_type") == "impression", 1)).alias("impressions"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("clicks"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
        F.round(F.sum("ad_cost"), 2).alias("ad_cost"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )
    .withColumn("ctr_pct", F.when(F.col("impressions") > 0,
                                  F.round(100 * F.col("clicks") / F.col("impressions"), 2)))
    .withColumn("roas", F.when(F.col("ad_cost") > 0,
                               F.round(F.col("revenue") / F.col("ad_cost"), 2)))
)

campaign_daily.coalesce(1).write.mode("overwrite").parquet(f"{processed}/campaign_daily_summary")
campaign_daily.show(5)


# Step 3: customer summary
customer_summary = (
    clean.groupBy("customer_id")
    .agg(
        F.count("*").alias("total_events"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("orders"),
        F.round(F.sum("revenue"), 2).alias("total_spent"),
        F.min("event_date").alias("first_event"),
        F.max("event_date").alias("last_event"),
    )
    .join(F.broadcast(customers.select("customer_id", "country", "segment")), "customer_id")
)

customer_summary.coalesce(1).write.mode("overwrite").parquet(f"{processed}/customer_summary")
customer_summary.show(5)


# Step 4: product summary
product_summary = (
    clean.groupBy("product_id")
    .agg(
        F.count(F.when(F.col("event_type") == "view", 1)).alias("views"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchases"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )
    .join(F.broadcast(products.select("product_id", "category", "brand", "list_price")), "product_id")
)

product_summary.coalesce(1).write.mode("overwrite").parquet(f"{processed}/product_summary")
product_summary.show(5)

spark.stop()
