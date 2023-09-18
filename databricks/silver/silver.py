# Databricks notebook source
## necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ##### customer_information cleaning

# COMMAND ----------

# MAGIC %sql
# MAGIC use wetelco;
# MAGIC select *from customer_information_raw

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customer_information, ingested from delta",
  partition_cols=["system_status", "connection_type"],
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customer_information_clean():
    customer_information_df = dlt.read('customer_information_raw')
    #customer_information_df = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_information_raw")
    # Convert all columns into lower case
    customer_information_df = customer_information_df.select([col(column).alias(column.lower()) for column in customer_information_df.columns])

    # Convert bigint customer_phone to string and filter out rows where the number of characters is less than 10
    customer_information_df = customer_information_df.withColumn("customer_phone_str", col("customer_phone").cast("string"))
    #customer_information_df = customer_information_df.filter(length(col("customer_phone_str")) >= 10)
    
    # Drop the temporary customer_phone_str column
    customer_information_df = customer_information_df.drop("customer_phone_str")
    
    # Drop columns which are blank
    
    blank_columns = [col_name for col_name in customer_information_df.columns if customer_information_df.filter(col(col_name).isNull() | (col(col_name) == "")).count() > 0]

    # Drop the identified blank columns
    customer_information_df = customer_information_df.drop(*blank_columns)

        # Remove duplicates
    customer_information_df = customer_information_df.dropDuplicates()
    customer_information_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Customer_information")

    return customer_information_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### billing cleaning

# COMMAND ----------


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
    billing_df = dlt.read('billing_raw')
    # billing_df = spark.read.format("delta").load("dbfs:/pipelines/daa0e31b-1862-4679-9ea2-0c6cd43ac09d/tables/billing_raw")
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
    
    billing_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Billing")
    
    return billing_df


# COMMAND ----------

# MAGIC %md
# MAGIC ##### plans cleaning

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned plans data, ingested from delta",
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_clean():
    plans_df = dlt.read('plans_raw')
    # plans_df = spark.read.format("delta").load("dbfs:/pipelines/daa0e31b-1862-4679-9ea2-0c6cd43ac09d/tables/plans_raw")

    # Convert all columns into lower case
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    
    # Remove duplicates
    plans_df = plans_df.dropDuplicates()
    
    plans_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Plans")

    return plans_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### customer_rating cleaning

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned customer_rating, ingested from delta",
  partition_cols=["rating"],
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_rating_clean():
    customer_rating_df = dlt.read('customer_rating_raw')
    # customer_rating_df = spark.read.format("delta").load("dbfs:/pipelines/daa0e31b-1862-4679-9ea2-0c6cd43ac09d/tables/customer_rating_raw")

    # Convert all columns into lower case
    customer_rating_df = customer_rating_df.select([col(column).alias(column.lower()) for column in customer_rating_df.columns])
    
    # Remove duplicates
    customer_rating_df = customer_rating_df.dropDuplicates()
    
    customer_rating_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Customer_rating")

    return customer_rating_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### device_information cleaning

# COMMAND ----------

@dlt.create_table(
  comment="The cleaned device information ingested from delta and partitioned by brand name",
  partition_cols=["brand_name"],
  table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def device_information_clean():
    device_information_df = dlt.read('device_information_raw')
    # device_information_df = spark.read.format("delta").load("dbfs:/pipelines/daa0e31b-1862-4679-9ea2-0c6cd43ac09d/tables/device_information_raw")
    device_information_df = device_information_df.select([col(column).alias(column.lower()) for column in device_information_df.columns])
    
    #dropping duplicates
    device_information_df = device_information_df.dropDuplicates()

    #dropping records that have columns 3rd onwards all values as null
    device_information_df = device_information_df.dropna(thresh=3)

    #replacing null with appropriate os name for the vendor Nokia
    condition_1 = (col("os_vendor") == "NOKIA") & (col("os_name").isNull())
    device_information_df = device_information_df.withColumn("os_name", when(condition_1, 'Series 30+').otherwise(col("os_name")))

    #replacing null with appropriate os name for the vendor Mentor Graphics
    condition_2 = (col("os_vendor") == "Mentor Graphics") & (col("os_name").isNull())
    device_information_df = device_information_df.withColumn("os_name", when(condition_2, 'Android').otherwise(col("os_name")))

    device_information_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Device_information")

    return device_information_df

# COMMAND ----------


