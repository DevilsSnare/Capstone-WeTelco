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

@dlt.create_table(
  comment="The cleaned customer_information, ingested from delta",
  partition_cols=["system_status", "connection_type"],
  table_properties={
    "wetelco_deltaliv.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def customer_information_clean():
    customer_information_df = dlt.read('customer_information_raw')
    customer_information_df = customer_information_df.select([col(column).alias(column.lower()) for column in customer_information_df.columns])
    customer_information_df = customer_information_df.withColumn("customer_phone", when(length(col("customer_phone").cast("string"))<10, None).otherwise(col("customer_phone")))     
    customer_information_df = customer_information_df.dropDuplicates()
    customer_information_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Customer_information")
    return customer_information_df

    """
    This Python script defines a Delta Live table and a data cleaning function to process customer information data.

    Table Creation:
    - A Delta Live table named "customer_information_clean" is created with specific metadata and properties.

    Data Validation:
    - Data validation is performed to ensure that the "customer_id" column is not null. Rows with null "customer_id" are either expected to be valid or dropped from the DataFrame based on the "expect_or_drop" decorator.

    Data Processing Steps:
    - Data is read from the "customer_information_raw" source table.
    - All column names are converted to lowercase for consistency.
    - Phone numbers with less than 10 digits are replaced with "None" in the "customer_phone" column.
    - Duplicate rows in the DataFrame are removed.
    - The cleaned data is written back to a Delta Lake table with overwrite mode.

    Function Return:
    - The function returns the cleaned DataFrame, but this return value is typically used for further processing and not for direct table manipulation.
    """

# COMMAND ----------

# MAGIC %md
# MAGIC ##### billing cleaning

# COMMAND ----------

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
    billing_df = billing_df.select([col(column).alias(column.lower()) for column in billing_df.columns])
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount") == '?', None).otherwise(col("bill_amount").cast("double")))
    billing_df = billing_df.withColumn("mean_bill_amount", calculate_mean_udf("bill_amount", "customer_id"))
    billing_df = billing_df.withColumn("bill_amount", when(col("bill_amount").isNull(), col("mean_bill_amount")).otherwise(col("bill_amount")))
    billing_df = billing_df.drop("mean_bill_amount")
    billing_df = billing_df.dropDuplicates()
    billing_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Billing")
    return billing_df

    """
    This Python script defines a Delta Live table and a data cleaning function to process billing information data.

    User-Defined Function (UDF):
    - `calculate_mean_udf(bill_amount_col, customer_id_col)` calculates the mean of the "bill_amount_col" partitioned by "customer_id_col" using a window specification.

    Table Creation:
    - A Delta Lake table named "billing_clean" is created with specific metadata and properties.

    Data Processing Steps:
    - Data is read from the "billing_raw" source table.
    - All column names are converted to lowercase for consistency.
    - Entries with a value of '?' in the "bill_amount" column are replaced with "None" (null) and cast to double.
    - A new column "mean_bill_amount" is added to the DataFrame using the `calculate_mean_udf` function, representing the mean bill amount per customer.
    - Null values in the "bill_amount" column are replaced with their corresponding "mean_bill_amount."
    - The "mean_bill_amount" column is dropped from the DataFrame.
    - Duplicate rows in the DataFrame are removed.
    - The cleaned data is written back to a Delta Lake table with overwrite mode.

    Function Return:
    - The function returns the cleaned DataFrame, but this return value is typically used for further processing and not for direct table manipulation.
    """

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
    plans_df = plans_df.select([col(column).alias(column.lower()) for column in plans_df.columns])
    plans_df = plans_df.dropDuplicates()
    plans_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Plans")
    return plans_df

    """
    This Python script defines a Delta Live table and a data cleaning function to process plans data.

    Table Creation:
    - A Delta Lake table named "plans_clean" is created with specific metadata and properties.

    Data Processing Steps:
    - Data is read from the "plans_raw" source table.
    - All column names are converted to lowercase for consistency.
    - Duplicate rows in the DataFrame are removed.
    - The cleaned data is written back to a Delta Lake table with overwrite mode.

    Function Return:
    - The function returns the cleaned DataFrame, but this return value is typically used for further processing and not for direct table manipulation.
    """

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
@dlt.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
def customer_rating_clean():
    customer_rating_df = dlt.read('customer_rating_raw')
    customer_rating_df = customer_rating_df.select([col(column).alias(column.lower()) for column in customer_rating_df.columns])
    customer_rating_df = customer_rating_df.dropDuplicates()
    customer_rating_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Customer_rating")
    return customer_rating_df
    
    """
    This Python script defines a Delta Live table and a data cleaning function to process customer rating data.

    Table Creation:
    - A Delta Lake table named "customer_rating_clean" is created with specific metadata and properties.
    - Comment: The cleaned customer_rating, ingested from delta.
    - Partition Columns: ["rating"]
    - Table Properties:
        - wetelco_deltaliv.quality: "silver"
        - pipelines.autoOptimize.managed: "true"

    Data Validation:
    - Data validation is performed to ensure that the "customer_id" column is not null. Rows with null "customer_id" are either expected to be valid or dropped from the DataFrame based on the "expect_or_drop" decorator.

    Data Processing Steps:
    - Data is read from the "customer_rating_raw" source table.
    - All column names are converted to lowercase for consistency.
    - Duplicate rows in the DataFrame are removed.
    - The cleaned data is written back to a Delta Lake table with overwrite mode.

    Function Return:
    - The function returns the cleaned DataFrame, but this return value is typically used for further processing and not for direct table manipulation.
    """

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
    device_information_df = device_information_df.select([col(column).alias(column.lower()) for column in device_information_df.columns])
    device_information_df = device_information_df.dropDuplicates()
    device_information_df = device_information_df.dropna(thresh=3)
    condition_1 = (col("os_vendor") == "NOKIA") & (col("os_name").isNull())
    device_information_df = device_information_df.withColumn("os_name", when(condition_1, 'Series 30+').otherwise(col("os_name")))
    condition_2 = (col("os_vendor") == "Mentor Graphics") & (col("os_name").isNull())
    device_information_df = device_information_df.withColumn("os_name", when(condition_2, 'Android').otherwise(col("os_name")))
    device_information_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Device_information")
    return device_information_df

    """
    This Python script defines a Delta Live table and a data cleaning function to process device information data.

    Table Creation:
    - A Delta Lake table named "device_information_clean" is created with specific metadata and properties.

    Data Validation:
    - Data validation is performed to ensure that the "customer_id" column is not null. Rows with null "customer_id" are either expected to be valid or dropped from the DataFrame based on the "expect_or_drop" decorator.

    Data Processing Steps:
    - Data is read from the "device_information_raw" source table.
    - All column names are converted to lowercase for consistency.
    - Duplicate rows in the DataFrame are removed.
    - Rows with less than 3 non-null values are dropped from the DataFrame.
    - Conditional replacements are performed for missing values in the "os_name" column based on "os_vendor."
    - The cleaned data is written back to a Delta Lake table with overwrite mode.

    Function Return:
    - The function returns the cleaned DataFrame, but this return value is typically used for further processing and not for direct table manipulation.
    """

# COMMAND ----------

# MAGIC %md
# MAGIC #####fraud cleaning (stream data)

# COMMAND ----------

@dlt.create_table(
comment="The cleaned fraud ingested from delta.",
table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
}
)
@dlt.expect_or_drop("valid call_id", "call_id IS NOT NULL")
def fraud_clean(): 
    fraud_df = dlt.read_stream('fraud_raw')
    fraud_df = fraud_df.select([col(column).alias(column.lower()) for column in fraud_df.columns])
    # fraud_df = fraud_df.withColumn("start_time", to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss")).withColumn("end_time", to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss")).withColumn("event_timestamp", to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
    # fraud_df.write.format('delta').mode("overwrite").save("/mnt/wetelcodump/silver/Fraud")
    return fraud_df

# COMMAND ----------


