# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Marketing Campaign Dataset
# MAGIC
# MAGIC This notebook generates a synthetic dataset for demonstrating an end-to-end
# MAGIC marketing analytics workflow in Databricks.
# MAGIC
# MAGIC The dataset contains no real customer, company, product, or campaign data.
# MAGIC All values are generated solely for demonstration purposes.

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

countries = ["Germany", "France", "Italy", "Spain", "UK"]
channels = ["Google Ads", "LinkedIn", "Email", "Webinar"]
products = [
    "Endoscopy",
    "Surgical Instruments",
    "Imaging",
    "Training"
]

rows = []

start_date = date(2026, 1, 1)

for i in range(1000):
    impressions = random.randint(1_000, 100_000)
    ctr = random.uniform(0.005, 0.08)
    clicks = int(impressions * ctr)

    conversion_rate = random.uniform(0.01, 0.15)
    conversions = int(clicks * conversion_rate)

    spend = round(random.uniform(500, 15_000), 2)
    revenue = round(conversions * random.uniform(300, 3000), 2)

    rows.append(
        (
            f"C{i+1:04}",
            start_date + timedelta(days=random.randint(0, 180)),
            random.choice(countries),
            random.choice(channels),
            random.choice(products),
            impressions,
            clicks,
            conversions,
            spend,
            revenue
        )
    )

columns = [
    "campaign_id",
    "date",
    "country",
    "channel",
    "product_category",
    "impressions",
    "clicks",
    "conversions",
    "spend",
    "revenue"
]

campaign_df = spark.createDataFrame(rows, columns)

display(campaign_df)

# COMMAND ----------

from pyspark.sql import functions as F

campaign_kpi_df = (
    campaign_df
    .withColumn(
        "ctr",
        F.col("clicks") / F.col("impressions")
    )
    .withColumn(
        "conversion_rate",
        F.col("conversions") / F.col("clicks")
    )
    .withColumn(
        "roas",
        F.col("revenue") / F.col("spend")
    )
    .withColumn(
        "cost_per_conversion",
        F.when(
            F.col("conversions") > 0,
            F.col("spend") / F.col("conversions")
        )
    )
)

display(campaign_kpi_df)

# COMMAND ----------

channel_performance = (
    campaign_kpi_df
    .groupBy("channel")
    .agg(
        F.sum("spend").alias("total_spend"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("clicks").alias("total_clicks"),
        F.sum("conversions").alias("total_conversions")
    )
    .withColumn(
        "roas",
        F.col("total_revenue") / F.col("total_spend")
    )
    .orderBy(F.desc("roas"))
)

display(channel_performance)

# COMMAND ----------

campaign_kpi_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("campaign_analytics")

# COMMAND ----------

df = spark.table("campaign_analytics")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     channel,
# MAGIC     SUM(spend) AS total_spend,
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC     SUM(revenue) / SUM(spend) AS roas
# MAGIC FROM campaign_analytics
# MAGIC GROUP BY channel
# MAGIC ORDER BY roas DESC;