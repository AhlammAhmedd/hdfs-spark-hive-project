from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("marketing_data").getOrCreate()

rows = 10000000
out = "file:///home/hadoop/MarketingData"


def pick(values):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.rand() * len(values)).cast("int") + 1)


customers = (
    spark.range(1, 500001).withColumnRenamed("id", "customer_id")
    .withColumn("country", pick(["EG", "SA", "AE", "KW", "QA", "JO", "MA"]))
    .withColumn("gender", pick(["F", "M"]))
    .withColumn("segment", pick(["New", "Regular", "VIP"]))
    .withColumn("signup_date", F.expr("date_add('2022-01-01', cast(rand() * 1000 as int))"))
)
    
products = (
    spark.range(1, 10001).withColumnRenamed("id", "product_id")
    .withColumn("category", pick(["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books"]))
    .withColumn("brand", F.concat(F.lit("Brand_"), (F.rand() * 100).cast("int")))
    .withColumn("list_price", F.round(F.rand() * 500 + 5, 2))
)

campaigns = (
    spark.range(1, 501).withColumnRenamed("id", "campaign_id")
    .withColumn("channel", pick(["facebook", "google_ads", "instagram", "email", "tiktok", "youtube"]))
    .withColumn("objective", pick(["awareness", "traffic", "conversion", "retargeting"]))
    .withColumn("budget", F.round(F.rand() * 50000 + 1000, 2))
)

dates = (
    spark.range(0, 730)
    .withColumn("full_date", F.expr("date_add('2024-01-01', cast(id as int))"))
    .select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.dayofweek("full_date").isin(6, 7).alias("is_weekend"),
    )
)

fact = (
    spark.range(0, rows, 1, 8).withColumnRenamed("id", "event_id")
    .withColumn("event_date", F.expr("date_add('2024-01-01', cast(rand() * 730 as int))"))
    .withColumn("customer_id", (F.rand() * 500000).cast("int") + 1)
    .withColumn("product_id", (F.rand() * 10000).cast("int") + 1)
    .withColumn("campaign_id", (F.rand() * 500).cast("int") + 1)
    .withColumn("r", F.rand())
    .withColumn("event_type",
                F.when(F.col("r") < 0.55, "impression")
                 .when(F.col("r") < 0.75, "click")
                 .when(F.col("r") < 0.90, "view")
                 .when(F.col("r") < 0.97, "add_to_cart")
                 .otherwise("purchase"))
    .withColumn("device", pick(["mobile", "desktop", "tablet"]))
    .withColumn("price", F.round(F.rand() * 500 + 5, 2))
    .withColumn("quantity", (F.rand() * 3).cast("int") + 1)
    .withColumn("revenue", F.when(F.col("event_type") == "purchase", F.round(F.col("price") * F.col("quantity"), 2)).otherwise(0.0))
    .withColumn("ad_cost", F.when(F.col("event_type") == "click", F.round(F.rand() * 2, 2)).otherwise(0.0))
    .withColumn("year_month", F.date_format("event_date", "yyyy-MM"))
    .drop("r")
)

customers.coalesce(1).write.mode("overwrite").parquet(out + "/dim_customers")
products.coalesce(1).write.mode("overwrite").parquet(out + "/dim_products")
campaigns.coalesce(1).write.mode("overwrite").parquet(out + "/dim_campaigns")
dates.coalesce(1).write.mode("overwrite").parquet(out + "/dim_date")
fact.write.mode("overwrite").partitionBy("year_month").parquet(out + "/fact_events")

spark.stop()
