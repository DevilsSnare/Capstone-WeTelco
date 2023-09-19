# Databricks notebook source
# MAGIC %md
# MAGIC ##### creating delta live tables

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

stream_df = spark.readStream \
        .format("cloudFiles") \
                 .option("cloudFiles.format", "parquet") \
                 .option("cloudFiles.schemaLocation", 
                                  "dbfs:/mnt/wetelcodump/raw/stream") \
                 .load(f"dbfs:/mnt/wetelcodump/raw/stream")

# Loop through all columns and cast them to string
for column_name in stream_df.columns:
    stream_df = stream_df.withColumn(column_name, col(column_name).cast("string"))


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
    
    stream_data = stream_df
    column_names = stream_data.columns
    for column_name in column_names:
        stream_data = stream_data.withColumnRenamed(column_name, column_name.replace(" ", "_"))
        
    return stream_data


# COMMAND ----------


