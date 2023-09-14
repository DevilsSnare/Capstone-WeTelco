# Databricks notebook source
container = 'wetelcobatch'
storage_account = 'wetelco'
key = 'r/8yMbm1tixNemxY34fJMlrpbNeWhZbpqeIPSUOoDq6AL6Crp+Dlh6AAt2mFQd2aQnZUZSZWaDws+ASt7m0F2w=='

mount_location = '/mnt/wetelco/wetelcobatch/bronze'
dbutils.fs.mount(
    source = f'wasbs://{container}@{storage_account}.blob.core.windows.net',
    mount_point = f'{mount_location}',
    extra_configs = {
        'fs.azure.account.key.foodwagondatalake.blob.core.windows.net': key
    }
)
