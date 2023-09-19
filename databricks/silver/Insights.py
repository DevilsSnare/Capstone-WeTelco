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
from pyspark.sql.functions import floor, current_date, datediff



# COMMAND ----------

spark = SparkSession.builder.appName("AgeGroups").getOrCreate()

current_year = 2023  # Assuming the current year is 2023
customer_info_df = customer_info_df.withColumn("dob", col("dob").cast("date"))
customer_info_df = customer_info_df.withColumn("year_of_birth", year("dob"))
customer_info_df = customer_info_df.withColumn("age", current_year - col("year_of_birth"))

# Define conditions for age groups
conditions = [
    (col("age") >= 16) & (col("age") <= 25),
    (col("age") >= 26) & (col("age") <= 40),
    (col("age") >= 41) & (col("age") <= 60),
    col("age") > 60
]

# Define labels for age groups
labels = ["16-25", "26-40", "41-60", "60+"]

# Create a new column 'age_category' based on the conditions
customer_info_df = customer_info_df.withColumn("age_category", 
                           expr(
                               "CASE "
                               "WHEN {0} THEN '{1}' "
                               "WHEN {2} THEN '{3}' "
                               "WHEN {4} THEN '{5}' "
                               "ELSE '{6}' END"
                               .format(conditions[0], labels[0], conditions[1], labels[1], conditions[2], labels[2], conditions[3], labels[3])
                           )
                         )

# Show the DataFrame with the new 'age_category' column
customer_info_df.show()

# COMMAND ----------

result_df = customer_info_df.select("customer_id", "full_name", "age_group")

# Show the result DataFrame
result_df.show()

# COMMAND ----------

# DBTITLE 1,Most Commonly used email domains
from pyspark.sql.functions import split, col
from pyspark.sql.functions import col, datediff, min



# COMMAND ----------

customer_info_df = customer_info_df.withColumn("email_domain", split("customer_email", "@").getItem(1))

# Group by email domains and count their frequency
email_domain_counts = customer_info_df.groupBy("email_domain").count()
email_domain_counts = email_domain_counts.orderBy(col("count").desc())

# Show the result
email_domain_counts.show()

# COMMAND ----------

# DBTITLE 1,Number of Active and Suspended users


# COMMAND ----------

status_counts = customer_info_df.groupBy('system_status').count()

# Show the result
status_counts.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, count, collect_set


# COMMAND ----------

status_count_by_connection_type = customer_info_df.groupBy('connection_type', 'system_status') \
    .agg(count('*').alias('count'))

# Show the result

status_count_by_connection_type.show()


# COMMAND ----------


