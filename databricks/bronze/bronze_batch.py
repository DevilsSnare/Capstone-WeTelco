# Databricks notebook source
# MAGIC %md
# MAGIC ##### downloading and unzipping batch dump to ADLS

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip

# COMMAND ----------

ozaid = '/Workspace/Repos/md_1692255888379@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'
chetan = '/Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'

# COMMAND ----------

import os
if os.path.exists(ozaid):
    my_directory = ozaid
elif os.path.exists(chetan):
    my_directory = chetan
else:
    raise ValueError("Neither 'ozaid' nor 'chetan' directory exists.")

# COMMAND ----------

from zipfile import ZipFile
import shutil

zip_file_path = f'{my_directory}/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip'
destination_folder = f'{my_directory}/dump_unzipped/'
with ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(destination_folder)
print(f"File unzipped to {destination_folder}")

# COMMAND ----------

mount_point = "/mnt/wetelcodump"

# COMMAND ----------

base_path = f"file:///{my_directory}/dump_unzipped"
def moveToADLS(folder_path):
    for item in dbutils.fs.ls(folder_path):
        if item.isDir():
            writeAsDelta(item.path)
        else:
            file_path = item.path
            dbutils.fs.cp(file_path, '/mnt/wetelcodump/raw')
moveToADLS(base_path)

# COMMAND ----------

dbutils.fs.rm(f'file:///{my_directory}/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip')
dbutils.fs.rm(f'file:///{my_directory}/dump_unzipped', True)

# COMMAND ----------

base_path = "/mnt/wetelcodump/raw"

def writeAsDelta(folder_path):
    for item in dbutils.fs.ls(folder_path):
        if item.isDir():
            writeAsDelta(item.path)
        else:
            file_path = item.path
            filename = item.name.split('.')[0]
            extension = item.name.split('.')[1]
            df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(file_path)
            save_path = '/mnt/wetelcodump/bronze/'
            df.write.format('delta').option("delta.columnMapping.mode", "name").mode("overwrite").save(save_path+filename)
writeAsDelta(base_path)

# COMMAND ----------

display(dbutils.fs.ls(mount_point))

# COMMAND ----------

display(dbutils.fs.ls(f"{mount_point}/bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating DLT

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ####Billing DLT

# COMMAND ----------

@dlt.create_table(
  comment="The raw billing, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def billing_raw():
    billing = spark.read.format("delta").load('/mnt/wetelcodump/bronze/Billing')
    return billing

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customer_Information DLT

# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_information, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_information_raw():
    customer_information = spark.read.format("delta").load(f'{mount_point}/bronze/Customer_information')
    customer_information = customer_information.withColumnRenamed("system status", "system_status")
    return customer_information

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customer_Rating DLT

# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_rating, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_raw():
    customer_rating = spark.read.format("delta").load(f'{mount_point}/bronze/Customer_rating')
    return customer_rating

# COMMAND ----------

# MAGIC %md
# MAGIC ####Device_Information DLT

# COMMAND ----------

@dlt.create_table(
  comment="The raw device_information, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def device_information_raw():
    device_information = spark.read.format("delta").load(f'{mount_point}/bronze/Device_Information')
    return device_information

# COMMAND ----------

# MAGIC %md
# MAGIC ####Plans DLT

# COMMAND ----------

@dlt.create_table(
  comment="The raw plans, batch data.",
  table_properties={
    "wetelco_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    plans = spark.read.format("delta").load(f'{mount_point}/bronze/Plans')
    # Alias the columns
    plans = plans.withColumnRenamed("Voice Service", "Voice_service") \
                 .withColumnRenamed("Mobile Data", "Mobile_data") \
                 .withColumnRenamed("Spam Detection", "Spam_detection") \
                 .withColumnRenamed("Fraud Prevention","Fraud_prevention")
    return plans

# COMMAND ----------


