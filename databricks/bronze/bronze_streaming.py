# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

import dlt
@dlt.create_table(
  comment="The raw stream data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraud_raw():
    """
    Process raw streaming data and create a Delta table with bronze quality.

    This function reads raw streaming data from a specified location in the DBFS
    (Databricks File System), applies column name modifications (replacing spaces
    with underscores), and creates a Delta table with bronze quality level.

    Returns:
        DataFrame: A Delta DataFrame containing the processed raw streaming data.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        raw_data_table = fraud_raw()
    """
    fraud_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation","dbfs:/mnt/wetelcodump/raw/_schemas").load("dbfs:/mnt/wetelcodump/raw/stream/")
    column_names = fraud_df.columns
    for column_name in column_names:
        fraud_df = fraud_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))
    return fraud_df

# COMMAND ----------


