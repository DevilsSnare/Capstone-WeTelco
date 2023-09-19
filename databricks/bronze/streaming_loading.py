# Databricks notebook source
account_name = "adlsstoragedata01"
container_name = "frauddata"
mount_point = '/mnt/wetelcodump/raw/stream'

storage_acc_key = "tBwtMqWlyr9ToC74Jxtq1UrA9aFi8fugoJo2SaKHxbwQnSIimMs6QjLW/Xw2Ujpk6M/wb9F9BXeB+AStk6vtGQ=="

dbutils.fs.mount(source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
mount_point = mount_point,
    extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_acc_key}
)

# COMMAND ----------

display(dbutils.fs.ls(mount_point))

# COMMAND ----------

shemaLocation = '/mnt/wetelcodump/raw/stream'

# COMMAND ----------

display(dbutils.fs.ls(shemaLocation))

# COMMAND ----------

stream_df = spark.readStream \
                 .format("cloudFiles") \
                 .option("cloudFiles.format", "parquet") \
                 .option("cloudFiles.schemaLocation", 
                                  f"dbfs:{shemaLocation}") \
                 .load(f"dbfs:{mount_point}")

# COMMAND ----------

display(stream_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Define a list of column names in the DataFrame
column_names = stream_df.columns

# Create a new DataFrame with updated column names
for column_name in column_names:
    stream_df = stream_df.withColumnRenamed(column_name, column_name.replace(" ", "_"))

# COMMAND ----------

display(stream_df.dtypes)

# COMMAND ----------

display(stream_df)

# COMMAND ----------

stream_df.writeStream.option("mergeSchema", "true") \
                    .format("delta") \
                    .option("checkpointLocation", "dbfs:/mnt/wetelcodump/bronze/checkpoint") \
                    .trigger(processingTime = '20 seconds') \
                    .start("dbfs:/mnt/wetelcodump/bronze/stream")

# COMMAND ----------


