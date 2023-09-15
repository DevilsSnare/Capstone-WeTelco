# Databricks notebook source
mount_point="dbfs:/mnt/wetelcodump/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading data from mount point

# COMMAND ----------

billing_df=spark.read.format("csv").option("header", "true").option("inferSchema","true").load(f"{mount_point}/Billing.csv")
customer_information_df=spark.read.format("csv").option("header", "true").option("inferSchema","true").load(f"{mount_point}/Customer_information.csv")
customer_rating_df=spark.read.format("csv").option("header", "true").option("inferSchema","true").load(f"{mount_point}/Customer_rating.csv")
device_information_df=spark.read.format("csv").option("header", "true").option("inferSchema","true").load(f"{mount_point}/Device_Information.csv")
plans_df=spark.read.format("csv").option("header", "true").option("inferSchema","true").load(f"{mount_point}/Plans.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display data

# COMMAND ----------

display(billing_df)
display(customer_information_df)
display(customer_rating_df)
display(device_information_df)
display(plans_df)

# COMMAND ----------


