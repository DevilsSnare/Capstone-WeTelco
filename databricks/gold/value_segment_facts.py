# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

def rev_tier(customer_information,billing):
    joined_data = customer_information.join(billing, on="customer_id")
    revenue_by_tier = joined_data \
    .groupby("value_segment") \
    .agg(sum("bill_amount").alias("total_revenue"))
    return revenue_by_tier

    

# COMMAND ----------

def no_of_customers(customer_information):
    customers_per_tier = customer_information \
    .groupby("value_segment") \
    .agg(count("customer_id").alias("num_customers"))
    return customers_per_tier

# COMMAND ----------

def rating_by_value_segments(customer_information,customer_rating):
    result = customer_information.join(customer_rating, on="customer_id")
    ratings=result.groupby("value_segment").agg(avg("rating").alias("rating_by_value_segments"))
    #average_rating_by_tier = joined_df.groupBy("tier").agg(avg("rating").alias("avg_rating"))
    return ratings

# COMMAND ----------

def inactive_customers(customer_information,billing):
    current_date = to_date(date_add(date_sub(last_day(to_date("payment_date")), 90), 1))
    last_quarter_end = last_day(date_sub(current_date, 1))

# Filter billing records for the last quarter
    filtered_billing = billing.filter((col("payment_date").isNull()) | (col("payment_date") > last_quarter_end))

# Join billing and customer_information tables on customer_id
    joined_df = filtered_billing.join(customer_information, "customer_id")

# Group by tier and count the number of customers in each tier
    customers_without_payments = joined_df.groupBy("value_segment").agg(countDistinct("customer_id").alias("inactive_customers"))
    return customers_without_payments


# COMMAND ----------

def csat(customer_information,customer_rating):
    joined_df = customer_information.join(customer_rating, "customer_id")

# Calculate the CSAT score per tier
    csat_score = joined_df.groupBy("value_segment").agg(avg(col("rating")).alias("csat_score"))
    return csat_score

# COMMAND ----------

@dlt.create_table(
  comment="Aggregated facts by value segment",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def value_segment_facts():
    customer_information=dlt.read("customer_information_clean")
    #customer_information = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_information_clean")
    billing = dlt.read("billing_clean")
    #billing = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/billing_clean")
    customer_rating=dlt.read("customer_rating_clean")
    #customer_rating=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_rating_clean")
    device_information=dlt.read("device_information_clean")
    #device_information=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/device_information_clean")
    plans=dlt.read("plans_clean")
    #plans=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/plans_clean")

    revenue_by_tier=rev_tier(customer_information,billing)
    customer_per_tier=no_of_customers(customer_information)
    rating_by_value_segment=rating_by_value_segments(customer_information,customer_rating)
    inactive_customers_by_tier=inactive_customers(customer_information,billing)
    csat_score_by_tier=csat(customer_information,customer_rating)
    sheet2_fact=revenue_by_tier.join(customer_per_tier,"value_segment","inner")\
    .join(rating_by_value_segment,"value_segment","inner")\
    .join(inactive_customers_by_tier,"value_segment","inner")\
    .join(csat_score_by_tier,"value_segment","inner")
    return sheet2_fact

