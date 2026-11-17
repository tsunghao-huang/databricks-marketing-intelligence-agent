# Databricks notebook source
# MAGIC %md
# MAGIC ## Campaign Performance Classification
# MAGIC
# MAGIC The thresholds used to classify campaigns as Healthy, Review, or Critical
# MAGIC are illustrative business rules created for this proof of concept.
# MAGIC
# MAGIC They are not intended to represent industry benchmarks or production rules.
# MAGIC In a production implementation, the thresholds would be agreed with business
# MAGIC stakeholders and calibrated using historical campaign performance.

# COMMAND ----------

from pyspark.sql import functions as F

campaign_df = spark.table("campaign_analytics")

display(campaign_df)

# COMMAND ----------

# Count records where basic business rules are violated.
quality_summary = campaign_df.select(
    F.count("*").alias("total_rows"),

    # impressions cannot be negative;
    F.sum(
        F.when(F.col("impressions") < 0, 1).otherwise(0)
    ).alias("negative_impressions"),

    # clicks shouldn't exceed impressions;
    F.sum(
        F.when(F.col("clicks") > F.col("impressions"), 1).otherwise(0)
    ).alias("clicks_above_impressions"),
    
    # conversions shouldn't exceed clicks;
    F.sum(
        F.when(F.col("conversions") > F.col("clicks"), 1).otherwise(0)
    ).alias("conversions_above_clicks"),

    # campaign spend should be positive.
    F.sum(
        F.when(F.col("spend") <= 0, 1).otherwise(0)
    ).alias("invalid_spend")
)

display(quality_summary)


# COMMAND ----------

campaign_performance = (
    campaign_df
    .select(
        "campaign_id",
        "date",
        "country",
        "channel",
        "product_category",
        "impressions",
        "clicks",
        "conversions",
        "spend",
        "revenue",
        "ctr",
        "conversion_rate",
        "roas",
        "cost_per_conversion"
    )
)

display(campaign_performance)

# COMMAND ----------

campaign_alerts = (
    campaign_performance
    .withColumn(
        "performance_status",
        F.when(
            (F.col("roas") < 1.0) &
            (F.col("spend") > 5000),
            "Critical"
        )
        .when(
            (F.col("roas") < 2.0) |
            (F.col("conversion_rate") < 0.03),
            "Review"
        )
        .otherwise("Healthy")
    )
)

display(campaign_alerts)

# COMMAND ----------

campaign_alerts = (
    campaign_alerts
    .withColumn(
        "attention_reason",
        F.when(
            (F.col("roas") < 1.0) &
            (F.col("spend") > 5000),
            F.lit("High spend with revenue below campaign spend")
        )
        .when(
            F.col("conversion_rate") < 0.03,
            F.lit("Low conversion rate")
        )
        .when(
            F.col("roas") < 2.0,
            F.lit("Low return on advertising spend")
        )
        .otherwise(
            F.lit("Performance within defined thresholds")
        )
    )
)

display(campaign_alerts)

# COMMAND ----------

campaign_alerts.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("campaign_alerts")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     campaign_id,
# MAGIC     country,
# MAGIC     channel,
# MAGIC     product_category,
# MAGIC     spend,
# MAGIC     revenue,
# MAGIC     roas,
# MAGIC     conversion_rate,
# MAGIC     performance_status,
# MAGIC     attention_reason
# MAGIC FROM campaign_alerts
# MAGIC WHERE performance_status != 'Healthy'
# MAGIC ORDER BY roas ASC;

# COMMAND ----------

