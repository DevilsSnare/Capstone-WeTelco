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


    """
    Reads raw fraud data from a streaming source and returns it as a DataFrame.

    This function reads raw fraud data from a streaming source using Apache Spark's
    Structured Streaming API. It assumes the data is in Parquet format and the schema
    is available at 'dbfs:/mnt/wetelcodump/raw/_schemas'.

    Parameters:
    None

    Returns:
    pyspark.sql.DataFrame:
        A DataFrame containing raw fraud data streamed from the source.

    Example Usage:
    To stream and access the raw fraud data, you can call this function as follows:

    >>> fraud_stream = fraud_raw()
    >>> query = fraud_stream.writeStream.outputMode("append").format("console").start()
    >>> query.awaitTermination()
    
    Note:
    - The function assumes that the Apache Spark session `spark` is already
      initialized and available in the calling environment.
    - It expects the streaming source to be in Parquet format and the schema to
      be located at 'dbfs:/mnt/wetelcodump/raw/_schemas'.
    - The column names in the resulting DataFrame are sanitized by replacing spaces
      with underscores for consistency.
    """
 

