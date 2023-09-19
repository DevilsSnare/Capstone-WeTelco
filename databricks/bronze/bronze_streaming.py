# Databricks notebook source
# MAGIC %md
# MAGIC ##### creating delta live tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

mount_point = '/mnt/wetelcodump/raw/stream'
shemaLocation = '/mnt/wetelcodump/raw/stream'

# COMMAND ----------

from pyspark.sql.functions import col

@dlt.create_table(
  comment="The raw streaming data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def streaming_data():
    stream_df = spark.readStream \
        .format("cloudFiles") \
                 .option("cloudFiles.format", "parquet") \
                 .option("cloudFiles.schemaLocation", 
                                  f"dbfs:{shemaLocation}") \
                 .load(f"dbfs:{mount_point}")
    
    column_names = stream_df.columns
    # Create a new DataFrame with updated column names
    for column_name in column_names:
        stream_df = stream_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))
        
    return stream_df


# COMMAND ----------


