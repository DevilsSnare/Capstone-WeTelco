# Databricks notebook source
# MAGIC %sql
# MAGIC show databases;
# MAGIC use capstone;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_information_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from billing_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from device_information_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_information_raw
# MAGIC WHERE model_name LIKE '%IPHONE%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from device_information_raw where model_name = "D54I"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from plans_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_rating_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from billing_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_information_clean where customer_id ="YKPQO1RIRC"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from plans_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_rating_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from device_information_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE device_information_clean1 RENAME TO device_information_clean;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from device_information_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_with_multiple_devices

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_billing_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from device_information_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     os_name,
# MAGIC     brand_name,
# MAGIC     COUNT(*) AS device_count
# MAGIC FROM
# MAGIC     device_information_clean
# MAGIC GROUP BY
# MAGIC     os_name,
# MAGIC     brand_name
# MAGIC ORDER BY
# MAGIC     os_name,
# MAGIC     brand_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH BillHistory AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         billing_date,
# MAGIC         bill_amount,
# MAGIC         LAG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date) AS previous_bill_amount
# MAGIC     FROM
# MAGIC         billing_clean
# MAGIC )
# MAGIC SELECT
# MAGIC     bh.customer_id,
# MAGIC     bh.billing_date,
# MAGIC     bh.bill_amount,
# MAGIC     AVG(bh.bill_amount) OVER (PARTITION BY bh.customer_id ORDER BY bh.billing_date) AS avg_bill_amount,
# MAGIC     bh.previous_bill_amount
# MAGIC FROM
# MAGIC     BillHistory bh
# MAGIC ORDER BY
# MAGIC     bh.customer_id,
# MAGIC     bh.billing_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH PurchaseSummary AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(billing_id) AS purchase_count,
# MAGIC         AVG(bill_amount) AS avg_purchase_amount
# MAGIC     FROM
# MAGIC         billing_clean
# MAGIC     GROUP BY
# MAGIC         customer_id
# MAGIC ),
# MAGIC AveragePurchaseCount AS (
# MAGIC     SELECT
# MAGIC         AVG(purchase_count) AS avg_purchase_count
# MAGIC     FROM
# MAGIC         PurchaseSummary
# MAGIC )
# MAGIC SELECT
# MAGIC     ps.customer_id,
# MAGIC     ps.purchase_count,
# MAGIC     CASE
# MAGIC         WHEN ps.purchase_count > apc.avg_purchase_count THEN ps.avg_purchase_amount
# MAGIC         ELSE NULL
# MAGIC     END AS avg_purchase_amount
# MAGIC FROM
# MAGIC     PurchaseSummary ps
# MAGIC CROSS JOIN
# MAGIC     AveragePurchaseCount apc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from billing_clean where customer_id = "229TP0W9RT"

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH PurchaseSummary AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(billing_id) AS purchase_count,
# MAGIC         AVG(bill_amount) AS avg_purchase_amount
# MAGIC     FROM
# MAGIC         billing_clean
# MAGIC     GROUP BY
# MAGIC         customer_id
# MAGIC ),
# MAGIC AveragePurchaseCount AS (
# MAGIC     SELECT
# MAGIC         AVG(purchase_count) AS avg_purchase_count
# MAGIC     FROM
# MAGIC         PurchaseSummary
# MAGIC )
# MAGIC SELECT
# MAGIC     ps.customer_id,
# MAGIC     ps.purchase_count,
# MAGIC     CASE
# MAGIC         WHEN ps.purchase_count > apc.avg_purchase_count THEN ps.avg_purchase_amount
# MAGIC         ELSE NULL
# MAGIC     END AS avg_purchase_amount
# MAGIC FROM
# MAGIC     PurchaseSummary ps
# MAGIC CROSS JOIN
# MAGIC     AveragePurchaseCount apc
# MAGIC WHERE
# MAGIC     ps.purchase_count > apc.avg_purchase_count;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_purchase_behavior

# COMMAND ----------


