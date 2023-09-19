# Databricks notebook source
<<<<<<< Updated upstream
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="The customers aggregated facts",
  table_properties={
    "Customer_information_.quality": "gold",
    "pipelines.autoOptimize.managed": "true"
  }
)


# COMMAND ----------

 

# COMMAND ----------

## necessary libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import dlt

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import dlt

spark = SparkSession.builder \
    .appName("Gold Layer") \
    .getOrCreate()
    
@dlt.create_table(
    comment="The cleaned customer_information, ingested from delta",
    partition_cols=["system_status", "connection_type"],
    table_properties={
        "wetelco_deltaliv.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def create_customer_segment_gold_table():
    # Read data from Silver layer
    billing_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/billing_clean")
    customer_information_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_information_clean")
    customer_rating_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_rating_clean")

# Perform SQL operations to create the fact table
fact_table = billing_data.alias("b") \
    .join(customer_info_data.alias("ci"), "customer_id") \
    .join(customer_rating_data.alias("cr"), "customer_id") \
    .join(device_info_data.alias("di"), "customer_id") \
    .groupBy("ci.customer_id") \
    .agg(
        sum(when(col("b.payment_date") > col("b.due_date"), 1).otherwise(0)).alias("Number_of_Delayed_Payments"),
        sum(when(col("b.payment_date") <= col("b.due_date"), 1).otherwise(0)).alias("Number_of_OnTime_Payments"),
        avg(col("b.bill_amount")).alias("Total_customer_value"),
        max(col("b.payment_date")).alias("latest_payment_date"),
        max(col("ci.value_segment")).alias("Customer_Tier"),
        max(col("di.brand_name")).alias("customer_device")
    ) \
    .withColumn("latest_payment_year", max(year(col("b.payment_date"))).over()) \
    .withColumn("latest_payment_month", max(month(col("b.payment_date"))).over())

# Show the fact table
create_customer_segment_gold_table.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import dlt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Gold Layer") \
    .getOrCreate()
    
@dlt.create_table(
    comment="The cleaned customer_information, ingested from delta",
    partition_cols=["system_status", "connection_type"],
    table_properties={
        "wetelco_deltaliv.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def create_customer_segment_gold_table():
    # Read data from Silver layer
    billing_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/billing_clean")
    customer_information_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_information_clean")
    customer_rating_df = spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_rating_clean")

    # 1. Revenue generated per value segment
    #revenue_per_segment = billing_df.groupBy("value_segment").agg(
    #    F.sum("bill_amount").alias("total_revenue")
    #)

    # 2. Number of customers per value segment
    customers_per_segment = customer_information_df.groupBy("value_segment").agg(
        F.count("Customer_id").alias("num_customers")
    )

    # 3. Ratings by value segment
    ratings_per_segment = customer_rating_df.groupBy("value_segment").agg(
        F.avg("rating").alias("avg_rating")
    )

    # 4. Inactive customers by value segment
    inactive_customers_per_segment = customer_information_df.filter(F.col("system_status") == "Inactive").groupBy("value_segment").agg(
        F.count("Customer_id").alias("num_inactive_customers")
    )

    # Combine all metrics into a single fact table
    fact_table = revenue_per_segment.join(
        customers_per_segment, "value_segment", "outer"
    ).join(
        ratings_per_segment, "value_segment", "outer"
    ).join(
        inactive_customers_per_segment, "value_segment", "outer"
    )

    # Write fact table to Gold layer
    fact_table.write.format("delta").mode("overwrite").saveAsTable("gold.fact_table")

# Call the function to create the table
create_customer_segment_gold_table()


# COMMAND ----------

=======
>>>>>>> Stashed changes

