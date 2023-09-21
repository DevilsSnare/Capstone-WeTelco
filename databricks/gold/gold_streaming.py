# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import dlt

# COMMAND ----------

def suspected_reason(df):
    result = df.withWatermark("eventprocessedutctime", "10 minutes").groupBy('receiver_number', 'suspected_reason').agg(count('receiver_number').alias('count_of_suspected_reasons'))
    return result

# COMMAND ----------

def user_reported(df):
    result_01 = df.withWatermark("eventprocessedutctime", "10 minutes").groupBy('receiver_number').agg(sum(when(col('user_reported'), 1).otherwise(0)).alias('times_user_reported'))
    # result_02 = df.withWatermark("eventprocessedutctime", "10 minutes").groupBy('receiver_number').agg(sum(when(col('user_reported'), 0).otherwise(1)).alias('times_user_not_reported'))
    # result= result_01.join(result_02, 'receiver_number', 'inner')
    return result_01

# COMMAND ----------

def call_duration(df):
    result = df.withColumn("call_duration", (col("end_time").cast(DoubleType()) - col("start_time").cast(DoubleType())).alias("TimeRange"))
    result = result.withWatermark("eventprocessedutctime", "10 minutes").groupBy("receiver_number").agg(sum("call_duration").alias("totall_call_duration_seconds"))
    return result

# COMMAND ----------

@dlt.create_table(
  comment="The streaming fraud aggregated facts",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraud_facts():
    fraud_facts_df = dlt.read_stream('fraud_clean')
    # customers = dlt.read('customer_information_clean')
    # fraud_facts_df = spark.read.format("delta").load("dbfs:/pipelines/5ab99e73-b59e-4633-8ada-1040eb106e78/tables/fraud_clean")
    # customers = spark.read.format("delta").load("dbfs:/pipelines/5ab99e73-b59e-4633-8ada-1040eb106e78/tables/customer_information_clean")
    x = suspected_reason(fraud_facts_df)
    # y = user_reported(fraud_facts_df)
    # z = call_duration(fraud_facts_df)
    # fraud_facts_df = x.join(y, "receiver_number", 'inner').join(z, "receiver_number", "inner")
    # fraud_facts_df = fraud_facts_df.join(customers, col('receiver_number')==col('customer_phone'), 'inner').drop('receiver_number')
    # fraud_facts_df = fraud_facts_df.select('customer_phone', 'full_name', 'customer_email', 'connection_type', 'value_segment', 'suspected_reason', 'count_of_suspected_reasons', 'totall_call_duration_seconds','times_user_reported', 'times_user_not_reported')
    return x

# COMMAND ----------


