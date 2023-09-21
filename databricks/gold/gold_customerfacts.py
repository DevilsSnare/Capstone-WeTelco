# Databricks notebook source
# DBTITLE 1,Importing libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

# DBTITLE 1,Number of delayed Payments
def delayed_payment(billing):
    """
    Calculate the total delayed payments for each customer based on billing information.

    This function takes a DataFrame containing billing information and calculates the total
    number of delayed payments for each customer. It considers payments to be delayed if
    the payment date is greater than the due date.

    Args:
        billing (DataFrame): DataFrame containing billing information with a "customer_id",
                            "due_date," and "payment_date" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the total number of delayed payments.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        delayed_payments_df = delayed_payment(billing)
    """
    billing = billing.withColumn('due_date', F.to_date('due_date'))
    billing = billing.withColumn('payment_date', F.to_date('payment_date'))
    
    billing = billing.withColumn('late_payment', when(billing['payment_date'] > billing['due_date'], 1).otherwise(0))
    
    delayed_payments = billing.groupBy('customer_id').agg({'late_payment': 'sum'}).withColumnRenamed('sum(late_payment)', 'total_delayed_payments')
    
    return delayed_payments

# COMMAND ----------

# DBTITLE 1,Number of on-time payments
def ontime_payment(billing):
    """
    Calculate the total on-time payments for each customer based on billing information.

    This function takes a DataFrame containing billing information and calculates the total
    number of on-time payments for each customer. It considers payments to be on-time if
    the due date is greater than the billing date.

    Args:
        billing (DataFrame): DataFrame containing billing information with a "customer_id",
                            "due_date," and "billing_date" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the total number of on-time payments.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        ontime_payments_df = ontime_payment(billing)
    """

    billing = billing.withColumn('due_date', F.to_date('due_date'))
    billing = billing.withColumn('billing_date', F.to_date('billing_date'))
    
    billing = billing.withColumn('ontime_payment', when(billing['due_date'] > billing['billing_date'], 1).otherwise(0))
    
    ontime_payments_by_customer = billing.groupBy('customer_id').agg({'ontime_payment': 'sum'})
    
    ontime_payments_by_customer = ontime_payments_by_customer.withColumnRenamed('sum(ontime_payment)', 'total_ontime_payments')
    
    return ontime_payments_by_customer

# COMMAND ----------

# DBTITLE 1,Customer Tier
def customer_tier(customer_information):
    """
    Extract the customer plan information (tier) for each customer.

    This function takes a DataFrame containing customer information and extracts the
    customer plan information (tier) for each customer. It retains the first value
    of the "value_segment" column for each customer.

    Args:
        customer_information (DataFrame): DataFrame containing customer information
                                          with a "customer_id" and "value_segment" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the extracted customer plan (tier).

    Raises:
        Any exceptions raised during data processing.

    Usage:
        customer_tier_df = customer_tier(customer_information)
    """
    customer_tier=customer_information.groupBy("customer_id").agg(F.first("value_segment").alias("customer_plan"))
    return(customer_tier)

# COMMAND ----------

# DBTITLE 1,Customer_device
def customer_device(device_information):
    """
    Aggregate device information for each customer.

    This function takes a DataFrame containing device information and aggregates the
    devices (brand names and model names) for each customer into a comma-separated list.

    Args:
        device_information (DataFrame): DataFrame containing device information with a
                                        "customer_id," "brand_name," and "model_name" column.

    Returns:
        DataFrame: A DataFrame with customer_id and a comma-separated list of devices.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        customer_device_df = customer_device(device_information)
    """
    customer_device = device_information.groupBy("customer_id").agg(
        concat_ws(",", collect_list(concat_ws(",", "brand_name", "model_name"))).alias("devices")
    )
    return customer_device

# COMMAND ----------

# DBTITLE 1,Total customer value
def customer_value(billing):
    """
    Calculate the total customer value based on billing information.

    This function takes a DataFrame containing billing information and calculates the total
    customer value for each customer. It does so by computing the average bill value and
    the average frequency rate (days between billings) for each customer and then multiplying
    these values to calculate the average product.

    Args:
        billing (DataFrame): DataFrame containing billing information with a "customer_id",
                            "billing_date", and "bill_amount" column.

    Returns:
        DataFrame: A DataFrame with customer_id, average bill value, average frequency rate,
                    and the calculated total customer value.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        customer_value_df = customer_value(billing)
    """
    avg_bill = billing.groupBy("customer_id").agg(F.avg("bill_amount").alias("avg_bill_value"))

    window_spec = Window.partitionBy("customer_id").orderBy("billing_date")
    billing = billing.withColumn("billing_date", F.to_date("billing_date"))
    billing = billing.withColumn("days_between_billings", F.datediff("billing_date", F.lag("billing_date").over(window_spec)))
    avg_frequency = billing.groupBy("customer_id").agg(F.avg(coalesce("days_between_billings", lit(1))).alias("avg_frequency_rate"))

    total_customer_value = avg_bill.join(avg_frequency, on="customer_id", how="inner")
    total_customer_value = total_customer_value.withColumn("average_product", total_customer_value["avg_bill_value"] * total_customer_value["avg_frequency_rate"])
    return total_customer_value

# COMMAND ----------

# DBTITLE 1,latest payment month
def month(billing):
    """
    Extract the latest billing month from a DataFrame containing billing information.

    This function takes a DataFrame containing billing information, groups it by customer_id,
    and calculates the latest billing month for each customer.

    Args:
        billing (DataFrame): DataFrame containing billing information with a "customer_id"
                            and "billing_date" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the latest billing month.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        latest_month_df = month(billing)
    """
    latest_billing_date = billing.groupBy("customer_id").agg(F.max("billing_date").alias("latest_billing_date"))
    # If you want to extract the month from the latest billing date:
    latest_month = latest_billing_date.withColumn("latest_month", F.month("latest_billing_date"))
    latest_month=latest_month.drop("latest_billing_date")

    return latest_month

# COMMAND ----------

# DBTITLE 1,latest payment year
def year(billing):
    """
    Extract the latest billing year from a DataFrame containing billing information.

    This function takes a DataFrame containing billing information, groups it by customer_id,
    and calculates the latest billing year for each customer.

    Args:
        billing (DataFrame): DataFrame containing billing information with a "customer_id"
                            and "billing_date" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the latest billing year.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        latest_year_df = year(billing)
    """
    latest_billing_date = billing.groupBy("customer_id").agg(F.max("billing_date").alias("latest_billing_year"))
    latest_year = latest_billing_date.withColumn("latest_year", F.year("latest_billing_year"))
    latest_year=latest_year.drop("latest_billing_year")
    return latest_year

# COMMAND ----------

# DBTITLE 1,CSAT Score by customer
def csat(customer_rating):
    """
    Calculate the Customer Satisfaction (CSAT) score based on customer ratings.

    This function takes a DataFrame containing customer information, including ratings,
    and calculates the CSAT score by grouping the data by customer_id and computing
    the average rating.

    Args:
        customer_information (DataFrame): DataFrame containing customer information
                                          with a "customer_id" and "rating" column.

    Returns:
        DataFrame: A DataFrame with customer_id and the computed CSAT score.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        csat_score_df = csat(customer_information)
    """
    csat_score = customer_information.groupBy("customer_id").agg(avg(col("rating")).alias("csat_score"))
    return csat_score    

# COMMAND ----------

# DBTITLE 1,Creating Dlt pipeline
@dlt.create_table(
  comment="The customers aggregated facts",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customer_facts():
    """
    Generate aggregated customer facts based on cleaned customer-related data.

    This function reads various cleaned customer-related DataFrames such as customer_information,
    billing, customer_rating, device_information, and plans. It then calculates and aggregates
    customer facts, including delayed payments, on-time payments, customer tier, device information,
    customer value, latest payment month, and latest payment year. The aggregated customer facts
    are combined into a single DataFrame.

    Returns:
        DataFrame: A Delta DataFrame containing the aggregated customer facts.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        customer_facts_table = customer_facts()
    """
    customer_information=dlt.read("customer_information_clean")
    billing = dlt.read("billing_clean")
    customer_rating=dlt.read("customer_rating_clean")
    device_information=dlt.read("device_information_clean")
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


