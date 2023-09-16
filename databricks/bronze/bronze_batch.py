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
    billing = billing.withColumnRenamed('Customer_Id', 'customer_id')
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
    customer_information = customer_information.withColumnRenamed('Customer_Id', 'customer_id')
    customer_information = customer_information.withColumnRenamed('Full_Name', 'full_name')
    customer_information = customer_information.withColumnRenamed('Customer_Email', 'customer_email')
    customer_information = customer_information.withColumnRenamed('Customer_Phone', 'customer_phone')
    customer_information = customer_information.withColumnRenamed('Connection_type', 'connection_type')
    customer_information = customer_information.withColumnRenamed("system status", "system_status")
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
    device_information = device_information.withColumnRenamed('Customer_id', 'customer_id')
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
    plans = plans.withColumnRenamed("Voice Service", "voice_service") \
                 .withColumnRenamed("Mobile Data", "mobile_data") \
                 .withColumnRenamed("Spam Detection", "spam_detection") \
                 .withColumnRenamed("Fraud Prevention","fraud_prevention") \
                 .withColumnRenamed('OTT', 'ott') \
                 .withColumnRenamed('Emergency', 'emergency')
    return plans

# COMMAND ----------


