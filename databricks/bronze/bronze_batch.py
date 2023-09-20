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


    """
    Reads raw billing data from a Delta Lake table and returns it as a DataFrame.

    This function loads raw billing data from a Delta Lake table located at
    '/mnt/wetelcodump/bronze/Billing' using Apache Spark's DataFrame API.
    
    Parameters:
    None

    Returns:
    pyspark.sql.DataFrame:
        A DataFrame containing raw billing batch data loaded from the Delta Lake table.

    Example Usage:
    To access the raw billing data, you can call this function as follows:

    >>> raw_billing_data = billing_raw()
    >>> raw_billing_data.show()
    
    Note:
    - The function assumes that the Apache Spark session `spark` is already
      initialized and available in the calling environment.
    """
 


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
    customer_information = customer_information.withColumnRenamed("system status", "system_status")
    return customer_information


    """
    Reads raw customer information data from a Delta Lake table and returns it as a DataFrame.

    This function loads raw customer information data from a Delta Lake table located at
    '/mnt/wetelcodump/bronze/Customer_information' using Apache Spark's DataFrame API.
    It also renames the 'system status' column to 'system_status' for consistency.

    Parameters:
    None

    Returns:
    pyspark.sql.DataFrame:
        A DataFrame containing raw customer information loaded from the Delta Lake table.

    Example Usage:
    To access the raw customer information data, you can call this function as follows:

    >>> customer_info_data = customer_information_raw()
    >>> customer_info_data.show()
    
    Note:
    - The function assumes that the Apache Spark session `spark` is already
      initialized and available in the calling environment.
    """


# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_rating batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_raw():
    """
    Load customer ratings data from a Delta table.

    Returns:
        DataFrame: A DataFrame containing customer ratings.
    """
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
    """
    Load device information data from a Delta table.

    Returns:
        DataFrame: A DataFrame containing device information.
    """
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
    """
    Load plans data from a Delta table and perform column renaming.

    Returns:
        DataFrame: A DataFrame containing plan information with renamed columns.
    """
    plans = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Plans')
    plans = plans.withColumnRenamed("Voice Service", "voice_service").withColumnRenamed("Mobile Data", "mobile_data").withColumnRenamed("Spam Detection", "spam_detection").withColumnRenamed("Fraud Prevention","fraud_prevention")
    return plans

# COMMAND ----------


