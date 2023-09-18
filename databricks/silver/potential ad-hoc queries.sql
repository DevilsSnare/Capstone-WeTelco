-- Databricks notebook source
use capstone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q1: Find customers who have multiple devices and their total bill amount.

-- COMMAND ----------

SELECT customer_id, COUNT(DISTINCT billing_id) AS device_count, SUM(bill_amount) AS total_bill_amount
FROM billing_clean
GROUP BY customer_id
HAVING COUNT(DISTINCT billing_id) > 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q2: generate a summary of customer billing information, including the total bill amount, the number of bills issued, and the average bill amount for each customer.

-- COMMAND ----------

SELECT
    customer_id,
    COUNT(billing_id) AS number_of_bills,
    SUM(bill_amount) AS total_bill_amount,
    AVG(bill_amount) AS average_bill_amount
FROM
    billing_clean
GROUP BY
    customer_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q3: analyze the distribution of device brands for each operating system (OS) and determine the count of devices for each combination.

-- COMMAND ----------

SELECT
    os_name,
    brand_name,
    COUNT(*) AS device_count
FROM
    device_information_clean
GROUP BY
    os_name,
    brand_name
ORDER BY
    os_name,
    brand_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q4: analyze the billing history for each customer and identify patterns in their bill amounts.Calculate the average bill amount for each customer and compare it with the previous bill.

-- COMMAND ----------

WITH BillHistory AS (
    SELECT
        customer_id,
        billing_date,
        bill_amount,
        LAG(bill_amount) OVER (PARTITION BY customer_id ORDER BY billing_date) AS previous_bill_amount
    FROM
        billing_clean
)

SELECT
    bh.customer_id,
    bh.billing_date,
    bh.bill_amount,
    AVG(bh.bill_amount) OVER (PARTITION BY bh.customer_id ORDER BY bh.billing_date) AS avg_bill_amount,
    bh.previous_bill_amount
FROM
    BillHistory bh
ORDER BY
    bh.customer_id,
    bh.billing_date;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Q5: analyze customer purchase behavior and identify customers who have made more than the average number of purchases. Calculate the average purchase amount only for these
-- MAGIC customers

-- COMMAND ----------

WITH PurchaseSummary AS (
    SELECT
        customer_id,
        COUNT(billing_id) AS purchase_count,
        AVG(bill_amount) AS avg_purchase_amount
    FROM
        billing_clean
    GROUP BY
        customer_id
),
AveragePurchaseCount AS (
    SELECT
        AVG(purchase_count) AS avg_purchase_count
    FROM
        PurchaseSummary
)
SELECT
    ps.customer_id,
    ps.purchase_count,
    CASE
        WHEN ps.purchase_count > apc.avg_purchase_count THEN ps.avg_purchase_amount
        ELSE NULL
    END AS avg_purchase_amount
FROM
    PurchaseSummary ps
CROSS JOIN
    AveragePurchaseCount apc
WHERE
    ps.purchase_count > apc.avg_purchase_count;


-- COMMAND ----------


