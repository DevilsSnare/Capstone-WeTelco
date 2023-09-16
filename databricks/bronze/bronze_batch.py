# Databricks notebook source
# MAGIC %sh
# MAGIC wget https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip

# COMMAND ----------

from zipfile import ZipFile
import shutil
zip_file_path = '/Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip'
destination_folder = '/Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze/dump_unzipped/'
with ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(destination_folder)
print(f"File unzipped to {destination_folder}")

# COMMAND ----------

base_path = "file:///Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze/dump_unzipped"
def moveToADLS(folder_path):
    for item in dbutils.fs.ls(folder_path):
        if item.isDir():
            writeAsDelta(item.path)
        else:
            file_path = item.path
            dbutils.fs.cp(file_path, '/mnt/wetelcodump/raw')
moveToADLS(base_path)

# COMMAND ----------

dbutils.fs.rm('file:///Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip')
dbutils.fs.rm('file:///Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze/dump_unzipped', True)

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


