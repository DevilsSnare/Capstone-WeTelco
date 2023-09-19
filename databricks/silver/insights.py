# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Total Revenue generated

# COMMAND ----------

total_revenue = billing_df.select(sum('bill_amount')).collect()[0][0]
print("Total Revenue:", total_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 highest paying customers

# COMMAND ----------

top_customers = billing_df.groupBy('customer_id').agg({'bill_amount': 'sum'}).orderBy(col('sum(bill_amount)').desc()).limit(10)
top_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 lowest paying customers

# COMMAND ----------

lowest_customers = billing_df.groupBy('customer_id').agg({'bill_amount': 'sum'}).orderBy(col('sum(bill_amount)').asc()).limit(10)
lowest_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Average Bill Amount for each customer

# COMMAND ----------

average_bill_amount = billing_df.groupBy('customer_id').agg(avg('bill_amount').alias('average_bill_amount'))
average_bill_amount.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Payment Status (Late/on-time)

# COMMAND ----------

payment_status = billing_df.withColumn(
    "payment_status",
    when(col("payment_date") > col("due_date"), "Late").otherwise("On-Time")
)
result_df = payment_status.select("billing_id", "customer_id", "payment_status")
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue based on connection type

# COMMAND ----------

joined_df = billing_df.join(customer_info_df, "customer_id", "inner")
grouped_df = joined_df.groupBy('connection_type').agg(sum('bill_amount').alias('total_bill_amount'))
grouped_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Age Distribution of customers

# COMMAND ----------

current_year = current_date().substr(1, 4).cast('integer')
df_with_age = customer_info_df.withColumn("age", current_year - col("dob").substr(1, 4).cast('integer'))
df_with_age.select('full_name','customer_id', 'age').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Age Distribution of customers

# COMMAND ----------

df_with_age = df_with_age.withColumn("age_group", \
               when((col('age')>=16) & (col('age')<26), '16-25')
              .when((col('age')>=26) & (col('age')<41), '26-40')
              .when((col('age')>=41) & (col('age')<61), '41-60')
              .otherwise('60+')
              )
df_with_age.select('full_name','customer_id', 'age_group').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Most Commonly used email domains

# COMMAND ----------

customer_info_df = customer_info_df.withColumn("email_domain", split("customer_email", "@").getItem(1))
email_domain_counts = customer_info_df.groupBy("email_domain").count()
email_domain_counts = email_domain_counts.orderBy(col("count").desc())
email_domain_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Number of Active and Suspended users

# COMMAND ----------

status_counts = customer_info_df.groupBy('system_status').count()
status_counts.show()

# COMMAND ----------

status_count_by_connection_type = customer_info_df.groupBy('connection_type', 'system_status').agg(count('*').alias('count'))
status_count_by_connection_type.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Number of repeat customers.

# COMMAND ----------

repeat_customers = billing_df.groupBy("customer_id").agg(
    countDistinct("billing_id").alias("num_purchases")
)
repeat_customers = repeat_customers.filter(col("num_purchases") > 1)
repeat_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC calculate the average customer rating 

# COMMAND ----------

average_rating = customer_rating_df.select(avg("rating").alias("average_rating"))
average_rating.show()

# COMMAND ----------

# MAGIC %md
# MAGIC which value_segment has highest or lowest customer rating 

# COMMAND ----------

avg_ratings_df = customer_info_df.join(
    customer_rating_df, "customer_id", "inner"
).groupBy("value_segment").agg(avg("rating").alias("average_rating"))
best_segment = avg_ratings_df.orderBy("average_rating", ascending=False).first()
worst_segment = avg_ratings_df.orderBy("average_rating").first()
print("Best Value Segment:", best_segment["value_segment"])
print("Average Rating for Best Segment:", best_segment["average_rating"])
print("Worst Value Segment:", worst_segment["value_segment"])
print("Average Rating for Worst Segment:", worst_segment["average_rating"])

# COMMAND ----------

# MAGIC %md
# MAGIC The most popular device brands and models among customers

# COMMAND ----------

popular_brands = device_info_df.groupBy("brand_name").count()
popular_models = device_info_df.groupBy("model_name").count()
most_popular_brand = popular_brands.orderBy(desc("count")).first()
most_popular_model = popular_models.orderBy(desc("count")).first()
print("Most Popular Device Brand:", most_popular_brand["brand_name"])
print("Most Popular Device Model:", most_popular_model["model_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC obtain the relation between bills paid and the customer rating. 

# COMMAND ----------

joined_data = billing_df.join(customer_rating_df, "customer_id", "inner")
result = joined_data.groupBy("customer_id").agg(
    sum("bill_amount").alias("total_bill_amount"),
    avg("rating").alias("average_rating")
)
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC obtain the issues based on the feedback from customers 

# COMMAND ----------

split_feedback = customer_rating_df.withColumn("words", split(col("feedback"), " "))
filtered_feedback = split_feedback.filter(
    (array_contains(col("words"), "issue")) |
    (array_contains(col("words"), "Not happy")) |
    (array_contains(col("words"), "facing")) |
    (array_contains(col("words"), "trouble"))
)
filtered_feedback.select("customer_id", "feedback").show()
