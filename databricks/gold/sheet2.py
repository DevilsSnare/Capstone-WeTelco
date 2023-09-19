# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

#customer_information = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_information_clean")
    customer_information=dlt.read("customer_information_clean")
    #billing = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/billing_clean")
    billing = dlt.read("billing_clean")
    #customer_rating=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_rating_clean")
    customer_rating=dlt.read("customer_rating_clean")
    #device_information=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/device_information_clean")
    device_information=dlt.read("device_information_clean")
    #plans=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/plans_clean")
    plans=dlt.read("plans_clean")

# COMMAND ----------

def rev_tier1(customer_information,billing):
    joined_data = customer_information.join(billing, on="customer_id")
    revenue_by_tier = joined_data \
    .groupby("value_segment") \
    .agg(sum("bill_amount").alias("total_revenue"))
    return revenue_by_tier

    

# COMMAND ----------

customer_information = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_information_clean")
billing = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/billing_clean")
revenue=rev_tier1(customer_information,billing)
display(revenue)

# COMMAND ----------

def no_of_customers(customer_information):
    customers_per_tier = customer_information \
    .groupby("value_segment") \
    .agg(count("customer_id").alias("num_customers"))
    return customers_per_tier

# COMMAND ----------

no_of_customer=no_of_customers(customer_information)
display(no_of_customer)

# COMMAND ----------


