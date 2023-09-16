# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customers, ingested from delta and partitioned by city",
  partition_cols=["city"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_clean():
    customers_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/customers_raw")
    customers_df = customers_df.select([col(column).alias(column.lower()) for column in customers_df.columns])
    return customers_df

# COMMAND ----------

@dlt.create_view(
  comment="The cleaned and validated customers with valid customer_id and partitioned by city",
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def customers_cleaned_valid():
  customers_df = dlt.read("customers_clean")
  return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned Vendors, ingested from delta",
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def vendors_clean():
    vendors_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/vendors_raw")
    vendors_df = vendors_df.select([col(column).alias(column.lower()) for column in vendors_df.columns])
    return vendors_df

# COMMAND ----------

@dlt.create_view(
  comment="Seperating year and month for purchase date to partition",
) 
def orders_part():
    orders_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/orders_raw")
    orders_df = orders_df.withColumn("purchase_year", year("order_approved_at")) \
                                 .withColumn("purchase_month", month("order_approved_at"))
    return orders_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned orders, ingested from delta and partitioned by date",
  partition_cols=["purchase_year","purchase_month"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def orders_clean():
    orders_df = dlt.read("orders_part")
    orders_df = orders_df.select([col(column).alias(column.lower()) for column in orders_df.columns])
    return orders_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned transactions, ingested from delta ",
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def transactions_clean():
    transactions_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/transactions_raw")
    transactions_df = transactions_df.select([col(column).alias(column.lower()) for column in transactions_df.columns])
    return transactions_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned returns, ingested from delta and partitioned on reasons",
  partition_cols=["return_reason"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def returns_clean():
    returns_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/returns_raw")
    returns_df = returns_df.select([col(column).alias(column.lower()) for column in returns_df.columns])
    return returns_df

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned products, ingested from delta and partitioned on brand",
  partition_cols=["brand"],
  table_properties={
    "Globalmart_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def products_clean():
    products_df = spark.read.format("delta").load("dbfs:/pipelines/29341027-97c2-4039-889f-189f52403781/tables/products_raw")
    products_df = products_df.select([col(column).alias(column.lower()) for column in products_df.columns])
    return products_df
