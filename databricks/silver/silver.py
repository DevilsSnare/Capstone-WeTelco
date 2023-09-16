# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ####customer_information data

# COMMAND ----------

from pyspark.sql.functions import col

@dlt.create_table(
  comment="The cleaned customer_information, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_information_clean():
    customer_information_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/customer_information_raw")

    # Convert all columns into lower case
    customer_information_df = customer_information_df.select([col(column).alias(column.lower()) for column in customer_information_df.columns])

    # Convert bigint customer_phone to string and filter out rows where the number of characters is less than 10
    customer_information_df = customer_information_df.withColumn("customer_phone_str", col("customer_phone").cast("string"))
    customer_information_df = customer_information_df.filter(length(col("customer_phone_str")) >= 10)
    
    # Drop the temporary customer_phone_str column
    customer_information_df = customer_information_df.drop("customer_phone_str")
    
    # Remove duplicates
    customer_information_df = customer_information_df.dropDuplicates()
    
    return customer_information_df


# COMMAND ----------

# MAGIC %md
# MAGIC ####Billing Data

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, when, mean

# Define a UDF to calculate the mean of bill_amount for each customer_id
def calculate_mean_udf(bill_amount_col, customer_id_col):
    window_spec = Window().partitionBy(customer_id_col)
    return mean(bill_amount_col).over(window_spec)

@dlt.create_table(
  comment="The cleaned billing, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def billing_clean():
    billing_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/billing_raw")
    billing_df = billing_df.select([col(column).alias(column.lower()) for column in billing_df.columns])
    
    # Replace '?' with null and cast bill_amount to a numeric type (e.g., double)
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount") == '?', None).otherwise(col("bill_amount").cast("double")))
    
    # Calculate the mean of bill_amount for each customer_id using the UDF
    billing_df = billing_df.withColumn("mean_bill_amount", calculate_mean_udf("bill_amount", "customer_id"))
    
    # Fill null values in bill_amount with the calculated mean_bill_amount
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount").isNull(), col("mean_bill_amount")).otherwise(col("bill_amount")))
    
    # Drop the mean_bill_amount column as it's no longer needed
    billing_df = billing_df.drop("mean_bill_amount")
    
    # Remove duplicates
    billing_df = billing_df.dropDuplicates()
    
    return billing_df


# COMMAND ----------

# MAGIC %md
# MAGIC ####Plans Data

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned plans data, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_clean():
    plans_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/plans_raw")

    # Convert all columns into lower case
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    
    # Remove duplicates
    plans_df = plans_df.dropDuplicates()
    
    return plans_df


# COMMAND ----------

# MAGIC %md
# MAGIC ####Customer Rating Data

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customer_rating, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_clean():
    customer_rating_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/customer_rating_raw")

    # Convert all columns into lower case
    customer_rating_df = customer_rating_df.select([col(column).alias(column.lower()) for column in customer_rating_df.columns])
    
    # Remove duplicates
    customer_rating_df = customer_rating_df.dropDuplicates()
    
    return customer_rating_df


# COMMAND ----------

# MAGIC %md
# MAGIC ####Device information Data

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.window import Window

@dlt.create_table(
  comment="The cleaned device_information, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def device_information_clean():
    device_information_df = spark.read.format("delta").load("dbfs:/pipelines/f7c91f60-3450-426b-80d0-e890be30ed63/tables/device_information_raw")

    # Convert all columns into lower case
    device_information_df = device_information_df.select([col(column).alias(column.lower()) for column in device_information_df.columns])
    
    # Step 1: Filter out rows where brand_name or model_name is null
    device_information_df = device_information_df.filter(col("brand_name").isNotNull() & col("model_name").isNotNull())
    
    # Step 2: Replace null values in os_name with a non-null os_name for the same model_name if available, otherwise remove the row
    window_spec = Window.partitionBy("model_name")
    device_information_df = device_information_df.withColumn(
        "os_name",
        when(col("os_name").isNotNull(), col("os_name")).otherwise(
            first(col("os_name"), ignorenulls=True).over(window_spec)
        )
    )
    device_information_df = device_information_df.filter(col("os_name").isNotNull())
    
    # Step 3: Replace null values in os_vendor with a non-null os_vendor for the same os_name if available, otherwise remove the row
    window_spec = Window.partitionBy("os_name")
    device_information_df = device_information_df.withColumn(
        "os_vendor",
        when(col("os_vendor").isNotNull(), col("os_vendor")).otherwise(
            first(col("os_vendor"), ignorenulls=True).over(window_spec)
        )
    )
    device_information_df = device_information_df.filter(col("os_vendor").isNotNull())
    
    # Remove duplicates
    device_information_df = device_information_df.dropDuplicates()
    
    return device_information_df


# COMMAND ----------


