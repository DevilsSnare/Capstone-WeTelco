# Databricks notebook source
# MAGIC %python
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q1: Find customers who have multiple devices and their total bill amount.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.create_table(
# MAGIC   comment="The ad-hoc queries, ingested from delta",
# MAGIC   table_properties={
# MAGIC     "wetelco.quality": "silver",
# MAGIC     "pipelines.autoOptimize.managed": "true"
# MAGIC   }
# MAGIC )
# MAGIC
# MAGIC def customers_with_multiple_devices():
# MAGIC     billing_clean_df = dlt.read('billing_clean')
# MAGIC     
# MAGIC     # Apply the logic to calculate the summary for customers with multiple devices
# MAGIC     summary_df = billing_clean_df.groupBy("customer_id").agg(
# MAGIC         countDistinct("billing_id").alias("device_count"),
# MAGIC         sum("bill_amount").alias("total_bill_amount")
# MAGIC     ).filter("device_count > 1")
# MAGIC
# MAGIC     return summary_df

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q2: generate a summary of customer billing information, including the total bill amount, the number of bills issued, and the average bill amount for each customer.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.create_table(
# MAGIC   comment="The ad-hoc queries, ingested from delta",
# MAGIC   table_properties={
# MAGIC     "wetelco.quality": "silver",
# MAGIC     "pipelines.autoOptimize.managed": "true"
# MAGIC   }
# MAGIC )
# MAGIC def customers_billing_summary():
# MAGIC     billing_clean_df = dlt.read('billing_clean')
# MAGIC
# MAGIC     # Apply the logic to calculate the billing summary
# MAGIC     summary_df = billing_clean_df.groupBy("customer_id").agg(
# MAGIC         count("billing_id").alias("number_of_bills"),
# MAGIC         sum("bill_amount").alias("total_bill_amount"),
# MAGIC         avg("bill_amount").alias("average_bill_amount")
# MAGIC     )
# MAGIC
# MAGIC     return summary_df
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q3: analyze the distribution of device brands for each operating system (OS) and determine the count of devices for each combination.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Q4: analyze the billing history for each customer and identify patterns in their bill amounts.Calculate the average bill amount for each customer and compare it with the previous bill.

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.create_table(
# MAGIC     comment="The ad-hoc queries, ingested from delta",
# MAGIC     table_properties={
# MAGIC         "wetelco.quality": "silver",
# MAGIC         "pipelines.autoOptimize.managed": "true"
# MAGIC     }
# MAGIC )
# MAGIC def customers_billing_history():
# MAGIC     billing_clean_df = dlt.read('billing_clean')
# MAGIC
# MAGIC     # Register the temporary view for the CTE
# MAGIC     billing_clean_df.createOrReplaceTempView("billing_clean_temp_view")
# MAGIC
# MAGIC     # Write the SQL query with CTE
# MAGIC     sql_query = """
# MAGIC     WITH BillHistory AS (
# MAGIC         SELECT
# MAGIC             customer_id,
# MAGIC             billing_date,
# MAGIC             bill_amount,
# MAGIC             LAG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date) AS previous_bill_amount
# MAGIC         FROM
# MAGIC             billing_clean_temp_view
# MAGIC     )
# MAGIC     SELECT
# MAGIC         bh.customer_id,
# MAGIC         bh.billing_date,
# MAGIC         bh.bill_amount,
# MAGIC         AVG(bh.bill_amount) OVER (PARTITION BY bh.customer_id ORDER BY bh.billing_date) AS avg_bill_amount,
# MAGIC         bh.previous_bill_amount
# MAGIC     FROM
# MAGIC         BillHistory bh
# MAGIC     ORDER BY
# MAGIC         bh.customer_id,
# MAGIC         bh.billing_date;
# MAGIC     """
# MAGIC
# MAGIC     # Execute the SQL query
# MAGIC     summary_df = spark.sql(sql_query)
# MAGIC
# MAGIC     return summary_df
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ######Q5: analyze customer purchase behavior and identify customers who have made more than the average number of purchases. Calculate the average purchase amount only for these
# MAGIC customers

# COMMAND ----------

# MAGIC %python
# MAGIC @dlt.create_table(
# MAGIC     comment="The ad-hoc queries, ingested from delta",
# MAGIC     table_properties={
# MAGIC         "wetelco.quality": "silver",
# MAGIC         "pipelines.autoOptimize.managed": "true"
# MAGIC     }
# MAGIC )
# MAGIC def customer_purchase_behavior():
# MAGIC     billing_clean_df = dlt.read('billing_clean')
# MAGIC
# MAGIC     # Register the temporary view for the billing_clean DataFrame
# MAGIC     billing_clean_df.createOrReplaceTempView("billing_clean_temp_view")
# MAGIC
# MAGIC     # Write and execute the SQL query
# MAGIC     sql_query = """
# MAGIC     WITH PurchaseSummary AS (
# MAGIC         SELECT
# MAGIC             customer_id,
# MAGIC             COUNT(billing_id) AS purchase_count,
# MAGIC             AVG(bill_amount) AS avg_purchase_amount
# MAGIC         FROM
# MAGIC             billing_clean_temp_view
# MAGIC         GROUP BY
# MAGIC             customer_id
# MAGIC     ),
# MAGIC     AveragePurchaseCount AS (
# MAGIC         SELECT
# MAGIC             AVG(purchase_count) AS avg_purchase_count
# MAGIC         FROM
# MAGIC             PurchaseSummary
# MAGIC     )
# MAGIC     SELECT
# MAGIC         ps.customer_id,
# MAGIC         ps.purchase_count,
# MAGIC         CASE
# MAGIC             WHEN ps.purchase_count > apc.avg_purchase_count THEN ps.avg_purchase_amount
# MAGIC             ELSE NULL
# MAGIC         END AS avg_purchase_amount
# MAGIC     FROM
# MAGIC         PurchaseSummary ps
# MAGIC     CROSS JOIN
# MAGIC         AveragePurchaseCount apc
# MAGIC     WHERE
# MAGIC         ps.purchase_count > apc.avg_purchase_count;
# MAGIC     """
# MAGIC     
# MAGIC     summary_df = spark.sql(sql_query)
# MAGIC
# MAGIC     return summary_df
# MAGIC

# COMMAND ----------


