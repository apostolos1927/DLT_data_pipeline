-- Databricks notebook source
SELECT customer_country,sum(amount) FROM demo.Fact_Table FT
LEFT OUTER JOIN demo.Payment_Dim PD
ON FT.payment_id=PD.payment_id
LEFT OUTER JOIN demo.Customer_Dim CD
ON FT.customer_id=CD.customer_id
WHERE payment_status='completed'
GROUP BY customer_country
