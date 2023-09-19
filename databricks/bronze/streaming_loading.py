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

# stream_df.writeStream.option("mergeSchema", "true") \
#                     .format("delta") \
#                     .option("checkpointLocation", "dbfs:/mnt/wetelcodump/bronze/checkpoint") \
#                     .trigger(processingTime = '20 seconds') \
#                     .start("dbfs:/mnt/wetelcodump/bronze/stream")

# COMMAND ----------


