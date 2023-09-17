# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q1: Find customers who have multiple devices and their total bill amount.

# COMMAND ----------

@dlt.create_table(
  comment="The ad-hoc queries, ingested from delta",
  table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customers_with_multiple_devices():
    billing_clean_df = dlt.read('billing_clean')
    
    # Apply the logic to calculate the summary for customers with multiple devices
    summary_df = billing_clean_df.groupBy("customer_id").agg(
        countDistinct("billing_id").alias("device_count"),
        sum("bill_amount").alias("total_bill_amount")
    ).filter("device_count > 1")

    return summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q2: generate a summary of customer billing information, including the total bill amount, the number of bills issued, and the average bill amount for each customer.

# COMMAND ----------

@dlt.create_table(
  comment="The ad-hoc queries, ingested from delta",
  table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_billing_summary():
    billing_clean_df = dlt.read('billing_clean')

    # Apply the logic to calculate the billing summary
    summary_df = billing_clean_df.groupBy("customer_id").agg(
        count("billing_id").alias("number_of_bills"),
        sum("bill_amount").alias("total_bill_amount"),
        avg("bill_amount").alias("average_bill_amount")
    )

    return summary_df

