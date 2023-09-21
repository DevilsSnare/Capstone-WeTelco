# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import dlt

# COMMAND ----------

def rev_tier(customer_information,billing):
    """
    Calculate the total revenue by value segment.

    This function takes two DataFrames, customer_information, and billing, and
    joins them on the "customer_id" column. It then calculates the total revenue for each
    value segment by grouping the data.

    Args:
        customer_information (DataFrame): DataFrame containing customer information with
                                          a "customer_id" and "value_segment" column.
        billing (DataFrame): DataFrame containing billing information with a "customer_id"
                            and "bill_amount" column.

    Returns:
        DataFrame: A DataFrame with "value_segment" and the computed total revenue.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        revenue_by_tier_df = rev_tier(customer_information, billing)
    """
    joined_data = customer_information.join(billing, on="customer_id")
    revenue_by_tier = joined_data.groupby("value_segment").agg(sum("bill_amount").alias("total_revenue"))
    return revenue_by_tier

# COMMAND ----------

def no_of_customers(customer_information):
    """
    Calculate the number of customers per value segment.

    This function takes a DataFrame containing customer information and calculates the number
    of customers in each value segment by grouping the data.

    Args:
        customer_information (DataFrame): DataFrame containing customer information with
                                          a "value_segment" column.

    Returns:
        DataFrame: A DataFrame with "value_segment" and the count of customers.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        customers_per_tier_df = no_of_customers(customer_information)
    """
    customers_per_tier = customer_information.groupby("value_segment").agg(count("customer_id").alias("num_customers"))
    return customers_per_tier

# COMMAND ----------

def rating_by_value_segments(customer_information,customer_rating):
    """
    Calculate the average rating by value segment.

    This function takes two DataFrames, customer_information, and customer_rating, and
    joins them on the "customer_id" column. It then calculates the average rating for each
    value segment by grouping the data.

    Args:
        customer_information (DataFrame): DataFrame containing customer information with
                                          a "customer_id" and "value_segment" column.
        customer_rating (DataFrame): DataFrame containing customer ratings with a
                                    "customer_id" and "rating" column.

    Returns:
        DataFrame: A DataFrame with "value_segment" and the computed average rating.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        rating_by_value_segments_df = rating_by_value_segments(customer_information, customer_rating)
    """
    result = customer_information.join(customer_rating, on="customer_id")
    ratings=result.groupby("value_segment").agg(avg("rating").alias("rating_by_value_segments"))
    return ratings

# COMMAND ----------

def inactive_customers(customer_information,billing):
    """
    Calculate the number of inactive customers by value segment.

    This function calculates the number of inactive customers within the last quarter by
    comparing billing data with customer information. Inactive customers are defined as
    those with missing payment dates or payments made after the last quarter end date.

    Args:
        customer_information (DataFrame): DataFrame containing customer information with
                                          a "customer_id" and "value_segment" column.
        billing (DataFrame): DataFrame containing billing information with a "customer_id"
                            and "payment_date" column.

    Returns:
        DataFrame: A DataFrame with "value_segment" and the count of inactive customers.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        inactive_customers_df = inactive_customers(customer_information, billing)
    """
    current_date = to_date(date_add(date_sub(last_day(to_date("payment_date")), 90), 1))
    last_quarter_end = last_day(date_sub(current_date, 1))
    
    filtered_billing = billing.filter((col("payment_date").isNull()) | (col("payment_date") > last_quarter_end))

    joined_df = filtered_billing.join(customer_information, "customer_id")

    customers_without_payments = joined_df.groupBy("value_segment").agg(countDistinct("customer_id").alias("inactive_customers"))
    return customers_without_payments

# COMMAND ----------

def csat(customer_information,customer_rating):
    """
    Calculate the Customer Satisfaction (CSAT) score by value segment.

    This function takes two DataFrames, customer_information, and customer_rating, and
    joins them on the "customer_id" column. It then calculates the CSAT score by grouping
    the data by "value_segment" and computing the average rating.

    Args:
        customer_information (DataFrame): DataFrame containing customer information with
                                          a "customer_id" and "value_segment" column.
        customer_rating (DataFrame): DataFrame containing customer ratings with a
                                    "customer_id" and "rating" column.

    Returns:
        DataFrame: A DataFrame with "value_segment" and the computed CSAT score.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        csat_score_df = csat(customer_information, customer_rating)
    """
    joined_df = customer_information.join(customer_rating, "customer_id")
    csat_score = joined_df.groupBy("value_segment").agg(avg(col("rating")).alias("csat_score"))
    return csat_score

# COMMAND ----------

@dlt.create_table(
  comment="Aggregated facts by value segment",
  table_properties={
    "wetelco_deltaliv.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)
def value_segment_facts():
    """
    Aggregate facts by value segment using cleaned customer-related data.

    This function reads cleaned customer-related data from different sources, including
    customer_information, billing, customer_rating, device_information, and plans.
    It then calculates various facts by value segment, such as revenue by tier,
    the number of customers per tier, rating by value segment, inactive customers by tier,
    and CSAT score by tier. These aggregated facts are combined into a single DataFrame.

    Returns:
        DataFrame: A DataFrame with value_segment as the key and aggregated facts.

    Raises:
        Any exceptions raised during data processing.

    Usage:
        value_segment_facts_df = value_segment_facts()
    """
    customer_information=dlt.read("customer_information_clean")
    billing = dlt.read("billing_clean")
    customer_rating=dlt.read("customer_rating_clean")
    device_information=dlt.read("device_information_clean")
    plans=dlt.read("plans_clean")

    revenue_by_tier=rev_tier(customer_information,billing)
    customer_per_tier=no_of_customers(customer_information)
    rating_by_value_segment=rating_by_value_segments(customer_information,customer_rating)
    inactive_customers_by_tier=inactive_customers(customer_information,billing)
    csat_score_by_tier=csat(customer_information,customer_rating)
    sheet2_fact=revenue_by_tier.join(customer_per_tier,"value_segment","inner")\
    .join(rating_by_value_segment,"value_segment","inner")\
    .join(inactive_customers_by_tier,"value_segment","inner")\
    .join(csat_score_by_tier,"value_segment","inner")
    return sheet2_fact
