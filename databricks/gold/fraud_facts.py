# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

selected_data = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/fraud_clean")

display(selected_data)

# COMMAND ----------

display(selected_data.dtypes)

# COMMAND ----------

def fraud_call(selected_data):

# Group by suspected_reason and count occurrences

    fraudulent_call_counts = selected_data.groupBy("suspected_reason").count()

    return fraudulent_call_counts

# COMMAND ----------

def call_duration(selected_data):
    call_duration_by_customer = selected_data.groupBy("receiver_number").agg(
    sum("call_duration").alias("total_call_duration")
    )
    return call_duration_by_customer



# COMMAND ----------

calls_df = selected_data.withColumn("call_duration", col("end_time") - col("start_time"))

# Group the DataFrame by the receiver number and calculate the sum of call durations in seconds
call_duration_by_receiver_df = calls_df.groupBy("receiver_number").agg(
    sum("call_duration").alias("total_call_duration_seconds")
)

# Show the resulting DataFrame
display(call_duration_by_receiver_df)

# COMMAND ----------

@dlt.create_table(
  comment="The stream aggregated facts",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def stream_facts():
    selected_data = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/fraud_clean")
    fraud_calls=fraud_call(selected_data)

# COMMAND ----------


