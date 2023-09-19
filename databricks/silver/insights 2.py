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

# COMMAND ----------



# Calculate the average customer rating
average_rating = customer_rating_df.select(avg("rating").alias("average_rating"))

# Show the average rating
average_rating.show()

# COMMAND ----------

# DBTITLE 1,which value_segment has highest or lowest customer rating 


# COMMAND ----------


from pyspark.sql.functions import avg



# Calculate average rating for each value segment
avg_ratings_df = customer_info_df.join(
    customer_rating_df, "customer_id", "inner"
).groupBy("value_segment").agg(avg("rating").alias("average_rating"))

# Find the segment with the best and worst ratings
best_segment = avg_ratings_df.orderBy("average_rating", ascending=False).first()
worst_segment = avg_ratings_df.orderBy("average_rating").first()

print("Best Value Segment:", best_segment["value_segment"])
print("Average Rating for Best Segment:", best_segment["average_rating"])
print("Worst Value Segment:", worst_segment["value_segment"])
print("Average Rating for Worst Segment:", worst_segment["average_rating"])


# COMMAND ----------

# DBTITLE 1,The most popular device brands and models among customers. 


# COMMAND ----------

from pyspark.sql.functions import desc

# Group by brand_name and count the number of occurrences
popular_brands = device_info_df.groupBy("brand_name").count()

# Group by model_name and count the number of occurrences
popular_models = device_info_df.groupBy("model_name").count()

# Find the most popular brand
most_popular_brand = popular_brands.orderBy(desc("count")).first()

# Find the most popular model
most_popular_model = popular_models.orderBy(desc("count")).first()

# Display the results
print("Most Popular Device Brand:", most_popular_brand["brand_name"])
print("Most Popular Device Model:", most_popular_model["model_name"])

# COMMAND ----------


