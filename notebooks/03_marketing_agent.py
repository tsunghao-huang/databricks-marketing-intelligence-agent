# Databricks notebook source
from pyspark.sql import functions as F

# helper functions for the AI Agent
def get_campaigns_requiring_attention(limit=5):
    
    df = spark.table("campaign_alerts")
    
    result = (
        df
        .filter(F.col("performance_status") != "Healthy")
        .orderBy(F.col("roas").asc())
        .limit(limit)
        .select(
            "campaign_id",
            "country",
            "channel",
            "product_category",
            "spend",
            "revenue",
            "roas",
            "conversion_rate",
            "performance_status",
            "attention_reason"
        )
    )
    
    return result


def get_channel_performance():
    
    df = spark.table("campaign_alerts")
    
    result = (
        df
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
        .orderBy(F.col("roas").desc())
    )
    
    return result


def get_country_performance(country):
    
    df = spark.table("campaign_alerts")
    
    result = (
        df
        .filter(
            F.lower(F.col("country")) == country.lower()
        )
        .groupBy("channel")
        .agg(
            F.sum("spend").alias("total_spend"),
            F.sum("revenue").alias("total_revenue"),
            F.sum("conversions").alias("total_conversions")
        )
        .withColumn(
            "roas",
            F.col("total_revenue") / F.col("total_spend")
        )
        .orderBy(F.col("roas").desc())
    )
    
    return result
    

# COMMAND ----------

attention_df = get_campaigns_requiring_attention(5)

display(attention_df)
display(get_channel_performance())
display(get_country_performance("Germany"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION workspace.default.get_campaigns_requiring_attention(
# MAGIC     max_campaigns INT
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC     campaign_id STRING,
# MAGIC     country STRING,
# MAGIC     channel STRING,
# MAGIC     product_category STRING,
# MAGIC     spend DOUBLE,
# MAGIC     revenue DOUBLE,
# MAGIC     roas DOUBLE,
# MAGIC     conversion_rate DOUBLE,
# MAGIC     performance_status STRING,
# MAGIC     attention_reason STRING
# MAGIC )
# MAGIC COMMENT 'Returns marketing campaigns requiring attention, ranked from lowest ROAS upward. Use this tool when a user asks which campaigns are underperforming, problematic, critical, or should be investigated.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         campaign_id,
# MAGIC         country,
# MAGIC         channel,
# MAGIC         product_category,
# MAGIC         spend,
# MAGIC         revenue,
# MAGIC         roas,
# MAGIC         conversion_rate,
# MAGIC         performance_status,
# MAGIC         attention_reason
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             campaign_id,
# MAGIC             country,
# MAGIC             channel,
# MAGIC             product_category,
# MAGIC             spend,
# MAGIC             revenue,
# MAGIC             roas,
# MAGIC             conversion_rate,
# MAGIC             performance_status,
# MAGIC             attention_reason,
# MAGIC             ROW_NUMBER() OVER (ORDER BY roas ASC) AS campaign_rank
# MAGIC         FROM workspace.default.campaign_alerts
# MAGIC         WHERE performance_status != 'Healthy'
# MAGIC     )
# MAGIC     WHERE campaign_rank <= max_campaigns;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Test the tool without AI
# MAGIC SELECT *
# MAGIC FROM workspace.default.get_campaigns_requiring_attention(5);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create Tool #2, channel performance
# MAGIC CREATE OR REPLACE FUNCTION workspace.default.get_channel_performance()
# MAGIC RETURNS TABLE (
# MAGIC     channel STRING,
# MAGIC     total_spend DOUBLE,
# MAGIC     total_revenue DOUBLE,
# MAGIC     total_clicks BIGINT,
# MAGIC     total_conversions BIGINT,
# MAGIC     roas DOUBLE
# MAGIC )
# MAGIC COMMENT 'Returns aggregated marketing performance by channel ranked by ROAS. Use this tool when a user asks to compare marketing channels or identify the best or worst performing channel.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         channel,
# MAGIC         SUM(spend) AS total_spend,
# MAGIC         SUM(revenue) AS total_revenue,
# MAGIC         SUM(clicks) AS total_clicks,
# MAGIC         SUM(conversions) AS total_conversions,
# MAGIC         SUM(revenue) / SUM(spend) AS roas
# MAGIC     FROM workspace.default.campaign_alerts
# MAGIC     GROUP BY channel
# MAGIC     ORDER BY roas DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.default.get_channel_performance();

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create Tool #3, country analysis
# MAGIC CREATE OR REPLACE FUNCTION workspace.default.get_country_performance(
# MAGIC     requested_country STRING
# MAGIC )
# MAGIC RETURNS TABLE (
# MAGIC     channel STRING,
# MAGIC     total_spend DOUBLE,
# MAGIC     total_revenue DOUBLE,
# MAGIC     total_conversions BIGINT,
# MAGIC     roas DOUBLE
# MAGIC )
# MAGIC COMMENT 'Analyzes marketing channel performance for a specific country. Use this tool when a user asks about campaign or channel performance within a particular country.'
# MAGIC RETURN
# MAGIC     SELECT
# MAGIC         channel,
# MAGIC         SUM(spend) AS total_spend,
# MAGIC         SUM(revenue) AS total_revenue,
# MAGIC         SUM(conversions) AS total_conversions,
# MAGIC         SUM(revenue) / SUM(spend) AS roas
# MAGIC     FROM workspace.default.campaign_alerts
# MAGIC     WHERE LOWER(country) = LOWER(requested_country)
# MAGIC     GROUP BY channel
# MAGIC     ORDER BY roas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.default.get_country_performance('Germany');