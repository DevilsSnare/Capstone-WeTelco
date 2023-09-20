# Databricks notebook source
import dlt
@dlt.create_table(
  comment="The raw stream data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def fraud_raw():
    fraud_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation","dbfs:/mnt/wetelcodump/raw/_schemas").load("dbfs:/mnt/wetelcodump/raw/stream/")
    column_names = fraud_df.columns
    for column_name in column_names:
        fraud_df = fraud_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))
    return fraud_df

# COMMAND ----------


