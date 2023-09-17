# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q1: Find customers who have multiple devices and their total bill amount.

# COMMAND ----------

@dlt.create_table(
  comment="The ad-hoc queries, ingested from delta",
  table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

def customers_with_multiple_devices():
    billing_clean_df = dlt.read('billing_clean')
    
    # Apply the logic to calculate the summary for customers with multiple devices
    summary_df = billing_clean_df.groupBy("customer_id").agg(
        countDistinct("billing_id").alias("device_count"),
        sum("bill_amount").alias("total_bill_amount")
    ).filter("device_count > 1")

    return summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q2: generate a summary of customer billing information, including the total bill amount, the number of bills issued, and the average bill amount for each customer.

# COMMAND ----------

@dlt.create_table(
  comment="The ad-hoc queries, ingested from delta",
  table_properties={
    "wetelco.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_billing_summary():
    billing_clean_df = dlt.read('billing_clean')

    # Apply the logic to calculate the billing summary
    summary_df = billing_clean_df.groupBy("customer_id").agg(
        count("billing_id").alias("number_of_bills"),
        sum("bill_amount").alias("total_bill_amount"),
        avg("bill_amount").alias("average_bill_amount")
    )

    return summary_df


# COMMAND ----------

# MAGIC %md
# MAGIC ######Q3: analyze the distribution of device brands for each operating system (OS) and determine the count of devices for each combination.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Q4: analyze the billing history for each customer and identify patterns in their bill amounts.Calculate the average bill amount for each customer and compare it with the previous bill.

# COMMAND ----------

@dlt.create_table(
    comment="The ad-hoc queries, ingested from delta",
    table_properties={
        "wetelco.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customers_billing_history():
    billing_clean_df = dlt.read('billing_clean')

    # Register the temporary view for the CTE
    billing_clean_df.createOrReplaceTempView("billing_clean_temp_view")

    # Write the SQL query with CTE
    sql_query = """
    WITH BillHistory AS (
        SELECT
            customer_id,
            billing_date,
            bill_amount,
            LAG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date) AS previous_bill_amount
        FROM
            billing_clean_temp_view
    )
    SELECT
        bh.customer_id,
        bh.billing_date,
        bh.bill_amount,
        AVG(bh.bill_amount) OVER (PARTITION BY bh.customer_id ORDER BY bh.billing_date) AS avg_bill_amount,
        bh.previous_bill_amount
    FROM
        BillHistory bh
    ORDER BY
        bh.customer_id,
        bh.billing_date;
    """

    # Execute the SQL query
    summary_df = spark.sql(sql_query)

    return summary_df


# COMMAND ----------

# MAGIC %md
# MAGIC ######Q5: analyze customer purchase behavior and identify customers who have made more than the average number of purchases. Calculate the average purchase amount only for these
# MAGIC customers

# COMMAND ----------

@dlt.create_table(
    comment="The ad-hoc queries, ingested from delta",
    table_properties={
        "wetelco.quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customer_purchase_behavior():
    billing_clean_df = dlt.read('billing_clean')

    # Register the temporary view for the billing_clean DataFrame
    billing_clean_df.createOrReplaceTempView("billing_clean_temp_view")

    # Write and execute the SQL query
    sql_query = """
    WITH PurchaseSummary AS (
        SELECT
            customer_id,
            COUNT(billing_id) AS purchase_count,
            AVG(bill_amount) AS avg_purchase_amount
        FROM
            billing_clean_temp_view
        GROUP BY
            customer_id
    ),
    AveragePurchaseCount AS (
        SELECT
            AVG(purchase_count) AS avg_purchase_count
        FROM
            PurchaseSummary
    )
    SELECT
        ps.customer_id,
        ps.purchase_count,
        CASE
            WHEN ps.purchase_count > apc.avg_purchase_count THEN ps.avg_purchase_amount
            ELSE NULL
        END AS avg_purchase_amount
    FROM
        PurchaseSummary ps
    CROSS JOIN
        AveragePurchaseCount apc
    WHERE
        ps.purchase_count > apc.avg_purchase_count;
    """
    
    summary_df = spark.sql(sql_query)

    return summary_df


# COMMAND ----------


