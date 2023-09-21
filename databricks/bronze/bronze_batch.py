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
    customer_information = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Customer_information')
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
    """
    Reads raw customer rating data from a Delta Lake table.

    This function reads customer rating data from a Delta Lake table located at
    '/mnt/wetelcodump/bronze/Customer_rating'. The raw customer rating data may include
    information about customer satisfaction scores, reviews, or feedback.

    Returns:
        DataFrame: A DataFrame containing raw customer rating information.

    Raises:
        FileNotFoundError: If the specified Delta Lake table path does not exist.

    Example:
        >>> rating_data = customer_rating_raw()
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
    Reads raw device information data from a Delta Lake table.

    This function reads device information data from a Delta Lake table located at
    '/mnt/wetelcodump/bronze/Device_Information'. The raw device information data may include details about
    various types of devices used by customers.

    Returns:
        DataFrame: A DataFrame containing raw device information.

    Raises:
        FileNotFoundError: If the specified Delta Lake table path does not exist.

    Example:
        >>> device_data = device_information_raw()
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
    Reads raw plan data from a Delta Lake table.

    This function reads plan data from a Delta Lake table located at '/mnt/wetelcodump/bronze/Plans'.
    The raw plan data may include information about different service offerings including Voice Service,
    Mobile Data, Spam Detection, and Fraud Prevention.

    Returns:
        DataFrame: A DataFrame containing raw plan information with columns renamed as follows:
            - 'Voice Service' -> 'voice_service'
            - 'Mobile Data' -> 'mobile_data'
            - 'Spam Detection' -> 'spam_detection'
            - 'Fraud Prevention' -> 'fraud_prevention'

    Raises:
        FileNotFoundError: If the specified Delta Lake table path does not exist.

    Example:
        >>> plans_data = plans_raw()
    """
    plans = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Plans')
    plans = plans.withColumnRenamed("Voice Service", "voice_service").withColumnRenamed("Mobile Data", "mobile_data").withColumnRenamed("Spam Detection", "spam_detection").withColumnRenamed("Fraud Prevention","fraud_prevention")
    return plans

# COMMAND ----------


