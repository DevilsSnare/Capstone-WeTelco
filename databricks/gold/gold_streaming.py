# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

def fraud_call(selected_data):
    fraudulent_call_counts = selected_data.groupBy("suspected_reason").count()
    return fraudulent_call_counts

fraud_call(df).show()

# COMMAND ----------

def user_reported(df):
    result = df.groupBy('receiver_number').agg(sum(when(col('user_reported'), 1).otherwise(0)).alias('times user reported'))
    result2 = df.groupBy('receiver_number').agg(sum(when(col('user_reported'), 0).otherwise(1)).alias('times user not reported'))
    result= result.join(result2, 'receiver_number', 'inner')
    return result

# COMMAND ----------



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
    fraud_facts_df = spark.read.format("delta").load("dbfs:/pipelines/5ab99e73-b59e-4633-8ada-1040eb106e78/tables/fraud_clean")


    return fraud_facts_df
