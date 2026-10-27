from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("marketing_quality_checks").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

staging = "hdfs:///MarketingData"
processed = "hdfs:///MarketingProcessed"

# data loading
tables = {
    "fact_events": spark.read.parquet(f"{staging}/fact_events"),
    "dim_customers": spark.read.parquet(f"{staging}/dim_customers"),
    "dim_products": spark.read.parquet(f"{staging}/dim_products"),
    "dim_campaigns": spark.read.parquet(f"{staging}/dim_campaigns"),
    "dim_date": spark.read.parquet(f"{staging}/dim_date"),
}
keys = {
    "fact_events": "event_id",
    "dim_customers": "customer_id",
    "dim_products": "product_id",
    "dim_campaigns": "campaign_id",
    "dim_date": "date_key",
}
fact = tables["fact_events"]
results = []

# data inspection
print("========DATASET SIZE========")
counts = {}
for name, df in tables.items():
    counts[name] = df.count()
    print(name, "| rows:", counts[name], "| columns:", len(df.columns))

print("========SCHEMA========")
for name, df in tables.items():
    print("--", name)
    df.printSchema()

print("========SAMPLE DATA (fact_events, first 5 rows)========")
fact.show(5)

# data quality checks
print("========NULL VALUES PER COLUMN========")
for name, df in tables.items():
    print("--", name)
    df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show(vertical=True)

print("========DUPLICATE KEYS========")
for name, df in tables.items():
    dups = counts[name] - df.select(keys[name]).distinct().count()
    print(name, f"({keys[name]}) duplicates:", dups)
    results.append((f"duplicate {keys[name]} in {name}", dups))

print("========STATISTICS SUMMARY (fact_events)========")
fact.select("price", "quantity", "revenue", "ad_cost").describe().show()

print("========REFERENTIAL INTEGRITY========")
for col, dim in [("customer_id", "dim_customers"), ("product_id", "dim_products"), ("campaign_id", "dim_campaigns")]:
    orphans = (fact.select(col).distinct()
               .join(tables[dim].select(col), col, "left_anti")
               .count())
    print(f"{col} in fact_events but not in {dim}:", orphans)
    results.append((f"orphan {col} (fact vs {dim})", orphans))

print("========BUSINESS RULES (fact_events)========")
start = F.lit("2024-01-01").cast("date")
end = F.lit("2025-12-31").cast("date")
not_purchase = F.col("event_type") != "purchase"
is_purchase = F.col("event_type") == "purchase"

rules = fact.select(
    F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("price not positive"),
    F.sum(F.when(F.col("ad_cost") < 0, 1).otherwise(0)).alias("negative ad_cost"),
    F.sum(F.when(not_purchase & (F.col("revenue") > 0), 1).otherwise(0)).alias("revenue on non-purchase"),
    F.sum(F.when(is_purchase & (F.col("revenue") <= 0), 1).otherwise(0)).alias("purchase without revenue"),
    F.sum(F.when(not_purchase & (F.col("quantity") > 0), 1).otherwise(0)).alias("quantity on non-purchase"),
    F.sum(F.when((F.col("event_date") < start) | (F.col("event_date") > end), 1).otherwise(0)).alias("date out of range"),
).collect()[0].asDict()

for rule, failed in rules.items():
    failed = int(failed or 0)
    print(rule, ":", failed)
    results.append((rule, failed))

print("========DATA QUALITY REPORT========")
report = (spark.createDataFrame(results, ["check_name", "failed_rows"])
          .withColumn("status", F.when(F.col("failed_rows") == 0, "PASS").otherwise("FAIL")))
report.show(30, truncate=False)

print("========SAVING REPORT========")
report.coalesce(1).write.mode("overwrite").parquet(f"{processed}/dq_report")
print("saved to", f"{processed}/dq_report")

spark.stop()
