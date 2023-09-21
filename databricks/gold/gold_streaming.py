# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import dlt

# COMMAND ----------

def metric_func(df):
    """
    Calculate metrics related to suspected fraud reasons based on a DataFrame.

    This function takes a DataFrame containing fraud-related data and calculates
    various metrics, including the count of suspected reasons, the number of times
    users reported, and the total call duration in seconds.

    Args:
        df (DataFrame): DataFrame containing fraud-related data with columns like
                        'receiver_number', 'suspected_reason', 'start_time', 'end_time',
                        'user_reported', and 'eventprocessedutctime'.

    Returns:
        DataFrame: A DataFrame with columns 'receiver_number', 'suspected_reason',
                    'count_of_suspected_reasons', 'times_user_reported', and
                    'totall_call_duration_seconds'.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        metrics_df = metric_func(df)
    """
    result = df.withColumn("call_duration", (col("end_time").cast(DoubleType()) - col("start_time").cast(DoubleType())).alias("TimeRange"))
    result = result.withWatermark("eventprocessedutctime", "10 minutes").groupBy('receiver_number', 'suspected_reason').agg(count('receiver_number').alias('count_of_suspected_reasons'),sum(when(col('user_reported'), 1).otherwise(0)).alias('times_user_reported'), sum("call_duration").alias("totall_call_duration_seconds"))
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
    """
    Calculate aggregated facts related to fraud using streaming data and customer information.

    This function reads streaming data from 'fraud_clean' and customer information from 'customer_information_clean'.
    It then calculates various fraud-related facts, such as suspected reasons, call duration, and user reporting,
    and combines them with customer information.

    Returns:
        DataFrame: A DataFrame with columns containing aggregated fraud facts and customer information.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        fraud_facts_df = fraud_facts()
    """
    fraud_facts_df = dlt.read_stream('fraud_clean')
    customers = dlt.read('customer_information_clean')
    met = metrics(fraud_facts_df)
    fraud_facts_df = met.join(customers, col('receiver_number')==col('customer_phone'), 'inner').drop('receiver_number')
    fraud_facts_df = fraud_facts_df.select('customer_phone', 'full_name', 'customer_email', 'connection_type', 'value_segment', 'suspected_reason', 'count_of_suspected_reasons', 'totall_call_duration_seconds','times_user_reported', 'times_user_not_reported')
    return fraud_facts_df

# COMMAND ----------


