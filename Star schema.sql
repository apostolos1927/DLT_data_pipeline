-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE Customer_Dim
(
  customer_id long GENERATED ALWAYS AS IDENTITY,
  customer_name STRING,
  customer_country STRING,
  CONSTRAINT customer_dim EXPECT (customer_name IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT DISTINCT customer_name,customer_country
FROM hive_metastore.demo.silver_layer

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Payment_Dim
(
  payment_id long GENERATED ALWAYS AS IDENTITY,
  payment_method STRING,
  payment_status STRING,
  CONSTRAINT payment_dim EXPECT (payment_status IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT DISTINCT payment_method,payment_status
FROM hive_metastore.demo.silver_layer

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Fact_Table
(
  order_id INTEGER,
  customer_id LONG,
  payment_id LONG,
  amount INTEGER
) AS
SELECT order_id,customer_id,payment_id,amount
FROM hive_metastore.demo.silver_layer sl
LEFT OUTER JOIN LIVE.Customer_Dim cd
ON sl.customer_name=cd.customer_name AND sl.customer_country=cd.customer_country
LEFT OUTER JOIN LIVE.Payment_Dim pd
ON sl.payment_method=pd.payment_method AND sl.payment_status=pd.payment_status

