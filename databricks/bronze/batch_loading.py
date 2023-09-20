# Databricks notebook source
# MAGIC %md
# MAGIC ##### mounting the storage

# COMMAND ----------

container = 'wetelcodump'
storage_account = 'wetelco'
key = 'Z9G6HOyHMR7baJdrbhuE7AY7+LcquhN6SdgkUz9ggfeo4lDSQvESX/SJsVRTX+qZzEZxq+083L7z+ASt0dUiCw=='

mount_location = '/mnt/wetelcodump/'

dbutils.fs.mount(
    source = f'wasbs://{container}@{storage_account}.blob.core.windows.net',
    mount_point = f'{mount_location}',
    extra_configs = {
        f'fs.azure.account.key.{storage_account}.blob.core.windows.net': key
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### downloading and unzipping batch dump to ADLS

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/ebcca86b-6b55-48c9-8e05-4340d2dafd50_83d04ac6-cb74-4a96-a06a-e0d5442aa126_TelecomZip.zip

# COMMAND ----------

ozaid = '/Workspace/Repos/md_1692255888379@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'
chetan = '/Workspace/Repos/chetan_1692255825295@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'
rohan = '/Workspace/Repos/rohan_1692255798122@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'
shivani = '/Workspace/Repos/shivani_1692255792853@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'
saumy = '/Workspace/Repos/saumy_1692255800150@npmentorskool.onmicrosoft.com/Capstone-WeTelco/databricks/bronze'

# COMMAND ----------

import os
if os.path.exists(ozaid):
    my_directory = ozaid
elif os.path.exists(chetan):
    my_directory = chetan
elif os.path.exists(rohan):
    my_directory = rohan
elif os.path.exists(shivani):
    my_directory = shivani
elif os.path.exists(saumy):
    my_directory = saumy
else:
    raise ValeError("No directory found.")

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

#def moveToADLS(folder_path):
    """
    Recursively moves files and directories from a source location to Azure Data Lake Storage.

    This function recursively iterates through the contents of the specified `folder_path`,
    and if it encounters a file (not a directory), it copies the file to the '/mnt/wetelcodump/raw'
    directory in Azure Data Lake Storage (ADLS).

    Parameters:
    folder_path (str): The source path containing files and directories to move to ADLS.

    Returns:
    None

    Example Usage:
    To move the contents of a local directory 'data_folder' to ADLS:

    >>> moveToADLS('/dbfs/mnt/wetelcodump/raw/')
    
    Note:
    - The function assumes that the Databricks utility `dbutils` is available and configured
      to interact with ADLS.
    - It recursively processes subdirectories if present in `folder_path`.
    - Files encountered in `folder_path` are copied to the '/mnt/wetelcodump/raw/' directory
      in ADLS.
    """
   




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

#def writeAsDelta(folder_path):
    """
    Recursively converts and saves CSV files in a folder to Delta Lake format.

    This function recursively searches for CSV files in the specified `folder_path`,
    reads them as DataFrames, and saves them as Delta Lake tables in the
    '/mnt/wetelcodump/bronze/' directory with the same base filename.

    Parameters:
    folder_path (str): The path to the directory containing CSV files to convert.

    Returns:
    None

    Example Usage:
    To convert all CSV files in a directory 'data_folder' to Delta Lake format:

    >>> writeAsDelta('/dbfs/mnt/wetelcodump/bronze/')
    
    Note:
    - The function assumes that the Apache Spark session `spark` is already
      initialized and available in the calling environment.
    - The function recursively processes subdirectories if present in `folder_path`.
    - Each CSV file is converted to a Delta Lake table with the same base filename
      in the '/mnt/wetelcodump/bronze/' directory, overwriting existing files with
      the same names.
    """

    

# COMMAND ----------

display(dbutils.fs.ls(mount_point))

# COMMAND ----------

display(dbutils.fs.ls(f"{mount_point}/bronze"))
