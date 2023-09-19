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

customer_info_df.display()

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

# DBTITLE 1,Revenue based on connection type


# COMMAND ----------

joined_df = billing_df.join(customer_info_df, "customer_id", "inner")

# Select connection_type and bill_amount
#joined_df.display()
#grouped_df = result_df.groupBy('customer_id').agg(sum('bill_amount').alias('total_bill_amount'))
# Show the result DataFrame
grouped_df = joined_df.groupBy('connection_type').agg(sum('bill_amount').alias('total_bill_amount'))

# Show the resulting DataFrame
grouped_df.show()

# COMMAND ----------

# DBTITLE 1,Age Distribution of customers


# COMMAND ----------

from pyspark.sql.functions import col, expr, when


# COMMAND ----------

spark = SparkSession.builder.appName("AgeGroups").getOrCreate()

# Assuming customer_info_df is your DataFrame with a column 'dob'
# If not, replace 'customer_info_df' with the actual DataFrame name

# Define age ranges
age_ranges = [
    (0, 16, '0-16'),
    (17, 25, '17-25'),
    (26, 40, '26-40'),
    (41, 60, '41-60'),
    (61, float('inf'), '60+')
]

# Calculate age based on date of birth
current_year = 2023  # Assuming the current year is 2023
customer_info_df = customer_info_df.withColumn("age", current_year - col("dob").substr(1, 4))

# Define conditions and corresponding age groups using 'when' function
condition_expr = [
    when((start <= customer_info_df['age']) & (customer_info_df['age'] <= end), group)
    for start, end, group in age_ranges
]

# Apply conditions to create the 'age_group' column
customer_info_df = customer_info_df.withColumn("age_group", *condition_expr)

# Show the DataFrame with the new 'age_group' column
customer_info_df.show()

# COMMAND ----------



# Show the resulting DataFrame
grouped_df.show()

# COMMAND ----------


