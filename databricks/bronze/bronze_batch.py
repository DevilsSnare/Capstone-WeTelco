# Databricks notebook source
# MAGIC %md
# MAGIC ##### creating delta live tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The raw billing batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def billing_raw():
    billing = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Billing')
    return billing

# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_information batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_information_raw():
    customer_information = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Customer_information')
    return customer_information

# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_rating batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_raw():
    customer_rating = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Customer_rating')
    return customer_rating

# COMMAND ----------

@dlt.create_table(
  comment="The raw device_information, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def device_information_raw():
    device_information = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Device_Information')
    return device_information

# COMMAND ----------

@dlt.create_table(
  comment="The raw plans, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    plans = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Plans')
    return plans

# COMMAND ----------


