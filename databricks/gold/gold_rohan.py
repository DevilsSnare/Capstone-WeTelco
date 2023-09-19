# Databricks notebook source
# DBTITLE 1,Importing libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

# DBTITLE 1,Number of delayed Payments
def delayed_payment(billing):
    # Convert date columns to date type
    billing = billing.withColumn('due_date', F.to_date('due_date'))
    billing = billing.withColumn('payment_date', F.to_date('payment_date'))
    
    # Calculate late payment status
    billing = billing.withColumn('late_payment', when(billing['payment_date'] > billing['due_date'], 1).otherwise(0))
    
    # Group by customer_id and sum delayed payments
    delayed_payments = billing.groupBy('customer_id').agg({'late_payment': 'sum'}).withColumnRenamed('sum(late_payment)', 'total_delayed_payments')
    
    return delayed_payments

# COMMAND ----------

# DBTITLE 1,Number of on-time payments
def ontime_payment(billing):
    # Convert date columns to date type
    billing = billing.withColumn('due_date', F.to_date('due_date'))
    billing = billing.withColumn('billing_date', F.to_date('billing_date'))
    
    # Calculate on-time payment status
    billing = billing.withColumn('ontime_payment', when(billing['due_date'] > billing['billing_date'], 1).otherwise(0))
    
    # Group by customer_id and calculate total on-time payments for each customer
    ontime_payments_by_customer = billing.groupBy('customer_id').agg({'ontime_payment': 'sum'})
    
    # Rename columns for clarity
    ontime_payments_by_customer = ontime_payments_by_customer.withColumnRenamed('sum(ontime_payment)', 'total_ontime_payments')
    
    return ontime_payments_by_customer

# COMMAND ----------

# DBTITLE 1,Customer Tier
def customer_tier(customer_information):
    customer_tier=customer_information.groupBy("customer_id").agg(F.first("value_segment").alias("customer_plan"))
    return(customer_tier)


# COMMAND ----------

# DBTITLE 1,Customer_device
def customer_device(device_information):
    customer_device = device_information.groupBy("customer_id").agg(
        concat_ws(",", collect_list(concat_ws(",", "brand_name", "model_name"))).alias("devices")
    )
    return customer_device


# COMMAND ----------

# DBTITLE 1,Total customer value
def customer_value(billing):
    avg_bill = billing.groupBy("customer_id").agg(F.avg("bill_amount").alias("avg_bill_value"))

# Calculate the average frequency rate for each customer
    window_spec = Window.partitionBy("customer_id").orderBy("billing_date")
    billing = billing.withColumn("billing_date", F.to_date("billing_date"))
    billing = billing.withColumn("days_between_billings", F.datediff("billing_date", F.lag("billing_date").over(window_spec)))
    avg_frequency = billing.groupBy("customer_id").agg(F.avg(coalesce("days_between_billings", lit(1))).alias("avg_frequency_rate"))

# Join the two DataFrames and calculate the product
    total_customer_value = avg_bill.join(avg_frequency, on="customer_id", how="inner")
    total_customer_value = total_customer_value.withColumn("average_product", total_customer_value["avg_bill_value"] * total_customer_value["avg_frequency_rate"])
    return total_customer_value



# COMMAND ----------

# DBTITLE 1,latest payment month
def month(billing):

    latest_billing_date = billing.groupBy("customer_id").agg(F.max("billing_date").alias("latest_billing_date"))

 

# If you want to extract the month from the latest billing date:

    latest_month = latest_billing_date.withColumn("latest_month", F.month("latest_billing_date"))
    latest_month=latest_month.drop("latest_billing_date")

    return latest_month

# COMMAND ----------

# DBTITLE 1,latest payment year
def year(billing):

    latest_billing_date = billing.groupBy("customer_id").agg(F.max("billing_date").alias("latest_billing_year"))

    latest_year = latest_billing_date.withColumn("latest_year", F.year("latest_billing_year"))
    latest_year=latest_year.drop("latest_billing_year")

    return latest_year


# COMMAND ----------

# DBTITLE 1,CSAT Score by customer
def csat(customer_rating):
    csat_score = customer_information.groupBy("customer_id").agg(avg(col("rating")).alias("csat_score"))
    return csat_score

    

# COMMAND ----------

# DBTITLE 1,Creating Dlt pipeline
#spark = SparkSession.builder.appName("YourAppName").getOrCreate()

@dlt.create_table(
  comment="The customers aggregated facts",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_facts():
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
    
    delayed_payments=delayed_payment(billing)
    ontime_payments=ontime_payment(billing)
    customers_tier=customer_tier(customer_information)
    customers_device=customer_device(device_information)
    customers_value=customer_value(billing)
    latest_payment_month=month(billing)
    latest_payment_year=year(billing)
   
    customer_facts=delayed_payments.join(ontime_payments,"customer_id","inner")\
        .join(customers_tier,"customer_id","inner")\
        .join(customers_device,"customer_id","inner")\
        .join(customers_value,"customer_id","inner")\
        .join(latest_payment_month,"customer_id","inner")\
        .join(latest_payment_year,"customer_id","inner")

    return customer_facts

# COMMAND ----------


