# Databricks notebook source
# MAGIC %md
# MAGIC ##### creating delta live tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

mount_point = '/mnt/wetelcodump/bronze/stream'

# COMMAND ----------

@dlt.create_table(
  comment="The raw streaming data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def streaming_data():
    stream_df = spark.readStream \
        .format("delta") \
        .load(mount_point)
    return stream_df


# COMMAND ----------


