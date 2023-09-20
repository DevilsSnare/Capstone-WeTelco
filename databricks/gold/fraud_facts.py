# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------



# COMMAND ----------

def fraud_clean(): 
    fraud_df = dlt.read('fraud_clean')
    fraud_df = fraud_df.select([col(column).alias(column.lower()) for column in fraud_df.columns])
    
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from wetelco_catlog.wetelco_schema.fraud_clean

# COMMAND ----------

fraud= spark.read.format('delta').load('/pipelines/f636fc3e-2c68-449f-a7c2-c15c55813162/fraud_clean')
display(fraud)

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use wetelco

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fraud_clean

# COMMAND ----------

selected_data = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/fraud_clean")

display(selected_data)

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


