# Databricks notebook source
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

# MAGIC %fs ls /mnt/wetelcodump/bronze/

# COMMAND ----------


