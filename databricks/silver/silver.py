# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

from pyspark.sql.functions import col, when, mean
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Define a UDF to calculate the mean of bill_amount for each customer_id
def calculate_mean_udf(bill_amount_col, customer_id_col):
    window_spec = Window().partitionBy(customer_id_col)
    return mean(bill_amount_col).over(window_spec)

@dlt.create_table(
  comment="The cleaned billing, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def billing_clean():
    billing_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/billing_raw")
    billing_df = billing_df.select([col(column).alias(column.lower()) for column in billing_df.columns])
    
    # Replace '?' with null and cast bill_amount to a numeric type (e.g., double)
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount") == '?', None).otherwise(col("bill_amount").cast("double")))
    
    # Calculate the mean of bill_amount for each customer_id using the UDF
    billing_df = billing_df.withColumn("mean_bill_amount", calculate_mean_udf("bill_amount", "customer_id"))
    
    # Fill null values in bill_amount with the calculated mean_bill_amount
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount").isNull(), col("mean_bill_amount")).otherwise(col("bill_amount")))
    
    # Drop the mean_bill_amount column as it's no longer needed
    billing_df = billing_df.drop("mean_bill_amount")
    
    return billing_df


# COMMAND ----------


