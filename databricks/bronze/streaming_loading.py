# Databricks notebook source
container = "frauddata"
storage_account_name = "adlsstoragedata01"
key = "tBwtMqWlyr9ToC74Jxtq1UrA9aFi8fugoJo2SaKHxbwQnSIimMs6QjLW/Xw2Ujpk6M/wb9F9BXeB+AStk6vtGQ=="

mount_location = '/mnt/wetelcodump/raw/stream/'

dbutils.fs.mount(
    source = f'wasbs://{container}@{storage_account_name}.blob.core.windows.net',
    mount_point = f'{mount_location}',
    extra_configs = {
        f'fs.azure.account.key.{storage_account_name}.blob.core.windows.net': key
    }
)
