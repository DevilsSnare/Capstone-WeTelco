# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------



# COMMAND ----------

spark = SparkSession.builder.appName("YourAppName").getOrCreate()

@dlt.create_table(
  comment="The customers aggregated facts",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_agg_facts():
    customer_information = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_information_clean")
    billing = spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/billing_clean")
    customer_rating=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/customer_rating_clean")
    device_information=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/device_information_clean")
    plans=spark.read.format("delta").load("dbfs:/pipelines/bdcffee6-ab29-4f45-995b-43408227fe5d/tables/plans_clean")
    
    billing = billing.withColumn('due_date', F.to_date('due_date'))
    billing = billing.withColumn('payment_date', F.to_date('payment_date'))
    billing = billing.withColumn('billing_date', F.to_date('billing_date'))
    billing = billing.withColumn('late_payment', billing['payment_date'] > billing['due_date'])
    billing = billing.withColumn('ontime_payment', billing['due_date'] > billing['billing_date'])

    # Count the number of delayed payments
    total_delayed_payments = billing.filter(billing['late_payment'] == True).count()
     # Count the number of ontime payments
    total_ontime_payments = billing.filter(billing['ontime_payment'] == True).count()
    customer_tier=customer_information.groupBy("customer_id").agg(F.first("value_segment").alias("customer_plan"))
    
    # Create a DataFrame with a single row and column to hold the total delayed payments count
    total_delayed_payments = spark.createDataFrame([(total_delayed_payments,)], ["total_delayed_payments"])
    total_ontime_payments = spark.createDataFrame([(total_ontime_payments,)], ["total_ontime_payments"])
    customer_device = device_information.groupBy("customer_id").agg(
        F.collect_list(F.struct("brand_name", "model_name")).alias("devices")
    )
    avg_bill = billing.groupBy("customer_id").agg(F.avg("bill_amount").alias("avg_bill_value"))

# Calculate the average frequency rate for each customer
    window_spec = Window.partitionBy("customer_id").orderBy("billing_date")
    billing = billing.withColumn("billing_date", F.to_date("billing_date"))
    billing = billing.withColumn("days_between_billings", F.datediff("billing_date", F.lag("billing_date").over(window_spec)))
    avg_frequency = billing.groupBy("customer_id").agg(F.avg("days_between_billings").alias("avg_frequency_rate"))

# Join the two DataFrames and calculate the product
    total_customer_value = avg_bill.join(avg_frequency, on="customer_id", how="inner")
    total_customer_value = total_customer_value.withColumn("average_product", total_customer_value["avg_bill_value"] * total_customer_value["avg_frequency_rate"])
    latest_billing_date = billing.groupBy("customer_id").agg(F.max("billing_date").alias("latest_billing_date"))

# If you want to extract the month from the latest billing date:
    latest_month = latest_billing_date.withColumn("latest_month", F.month("latest_billing_date"))
    latest_year = latest_billing_date.withColumn("latest_year", F.year("latest_billing_date"))

    #display(total_delayed_payments)
    #display(total_customer_value)
    #display(latest_year)
    #display(customer_tier)
    customer_facts=total_delayed_payments.join(total_ontime_payments,"customer_id","inner")\
        .join(customer_tier,"customer_id","inner")\
        .join(total_customer_value,"customer_id","inner")\
        .join(latest_month,"customer_id","inner")\
        .join(latest_year,"customer_id","inner")

    
    


    return customer_facts

# COMMAND ----------



# COMMAND ----------


