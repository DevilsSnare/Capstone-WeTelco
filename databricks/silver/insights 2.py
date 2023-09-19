# Databricks notebook source
billing_df=spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/billing_clean")

customer_info_df=spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_information_clean")

customer_rating_df=spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/customer_rating_clean")

device_info_df=spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/device_information_clean")

plans_df=spark.read.format("delta").load("dbfs:/pipelines/a69a1d57-3950-43d4-ba41-a222251f0e44/tables/plans_clean")

# COMMAND ----------

billing_df.display()

# COMMAND ----------

plans_df.display()

# COMMAND ----------

device_info_df.display()

# COMMAND ----------

# DBTITLE 1,Number of repeat customers.


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col


# Group by customer_id and count distinct billing_ids
repeat_customers = billing_df.groupBy("customer_id").agg(
    countDistinct("billing_id").alias("num_purchases")
)

# Filter for customers with more than one purchase
repeat_customers = repeat_customers.filter(col("num_purchases") > 1)

# Show the list of repeat customers
repeat_customers.show()


# COMMAND ----------

# DBTITLE 1,calculate the average customer rating 


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Calculate the average customer rating
average_rating = customer_rating_df.select(avg("rating").alias("average_rating"))

# Show the average rating
average_rating.show()

# COMMAND ----------


