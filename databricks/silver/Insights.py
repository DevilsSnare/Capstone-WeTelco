# Databricks notebook source
billing_df=spark.read.format("delta").load("dbfs:/pipelines/a5ffd579-f19b-4129-aa48-06bf7dd76487/tables/billing_clean")

customer_info_df=spark.read.format("delta").load("dbfs:/pipelines/a5ffd579-f19b-4129-aa48-06bf7dd76487/tables/customer_information_clean")

customer_rating_df=spark.read.format("delta").load("dbfs:/pipelines/a5ffd579-f19b-4129-aa48-06bf7dd76487/tables/customer_rating_clean")

device_info_df=spark.read.format("delta").load("dbfs:/pipelines/a5ffd579-f19b-4129-aa48-06bf7dd76487/tables/device_information_clean")

plans_df=spark.read.format("delta").load("dbfs:/pipelines/a5ffd579-f19b-4129-aa48-06bf7dd76487/tables/plans_clean")



# COMMAND ----------

billing_df.display()

# COMMAND ----------

plans_df.display()

# COMMAND ----------

device_info_df.display()

# COMMAND ----------

# DBTITLE 1,Total Revenue


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import col
from pyspark.sql.functions import avg


# Initialize SparkSession
spark = SparkSession.builder.appName("TotalRevenueCalculation").getOrCreate()

# COMMAND ----------

total_revenue = billing_df.select(sum('bill_amount')).collect()[0][0]
# Print the total revenue
print("Total Revenue:", total_revenue)


# COMMAND ----------

# DBTITLE 1,Top 10 highest paying customers


# COMMAND ----------

top_customers = billing_df.groupBy('customer_id').agg({'bill_amount': 'sum'}).orderBy(col('sum(bill_amount)').desc()).limit(10)

# Show the top 10 customers
top_customers.show()

# COMMAND ----------

# DBTITLE 1,Top 10 lowest paying customers
lowest_customers = billing_df.groupBy('customer_id').agg({'bill_amount': 'sum'}).orderBy(col('sum(bill_amount)').asc()).limit(10)

# Show the top 10 customers
lowest_customers.show()

# COMMAND ----------

# DBTITLE 1,Average Bill Amount for each customer


# COMMAND ----------

average_bill_amount = billing_df.groupBy('customer_id').agg(avg('bill_amount').alias('average_bill_amount'))

# Show the average bill_amount for each customer
average_bill_amount.show()

# COMMAND ----------

# DBTITLE 1,Payment Status (Late or on-time)


# COMMAND ----------

from pyspark.sql.functions import when


# COMMAND ----------

payment_status = billing_df.withColumn(
    "payment_status",
    when(col("payment_date") > col("due_date"), "Late").otherwise("On-Time")
)
# Show the DataFrame with the new column
result_df = payment_status.select("billing_id", "customer_id", "payment_status")

# Show the result DataFrame
result_df.show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


