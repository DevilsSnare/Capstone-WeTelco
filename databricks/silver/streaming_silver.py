# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The raw streaming data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def streaming_data_clean():
    streaming_df = dlt.readStream('streaming_data')
    streaming_df = streaming_df.select([col(column).alias(column.lower()) for column in streaming_df.columns])
    
    streaming_df = streaming_df.dropDuplicates()

    # streaming_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/streaming")

    return streaming_df
