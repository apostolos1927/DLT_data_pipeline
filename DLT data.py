# Databricks notebook source
dataDictionary = [
        (1,13,'2024-03-16','John','Greece',"Method1","completed"),
        (2,6362,'2024-03-17','Jane','USA',"Method2","pending"),
        (3,211,'2024-03-18','Jack','UK',"Method3","completed"),
        (4,331,'2024-03-18','Tom','USA',"Method3","failed"),
        (5,216,'2024-03-18','Larry','Italy',"Method1","completed"),
        (5,1164,'2024-03-18','Larry','Italy',"Method1","completed"),
        (6,22,'2024-03-19','Smith','Germany',"Method1","completed"),
        (6,22,'2024-03-19','Smith','Germany',"Method1","completed"),
        (6,22,'2024-03-19','Smith','Germany',"Method1","completed"),
        (6,22,'2024-03-19','Smith','Germany',"Method1","completed"),
        (7,0,'2024-03-16','John','Greece',"Method1","completed"),
        (8,33,'2024-03-17','Jane','USA',"Method1","completed"),
        (-9,45,'2024-03-18','Jack','UK',"Method1","completed"),
        (10,23,'2024-03-18','Tom','India',"Method1","completed"),
        (11,99,'2024-03-18','Larry','Italy',"Method1","completed"),
        (12,76,'2024-03-19','Smith','Germany',"Method1","completed"),
        (13,55,'2024-03-20','Smith','Germany',"Method1","completed"),
        (14,43,'2024-03-20','Smith','Germany',"Method1","completed"),
        ]
df = spark.createDataFrame(data=dataDictionary, schema = ["order_id","amount","ship_date","customer_name","customer_country","payment_method","payment_status"])
df.display()

# COMMAND ----------

(df.selectExpr("customer_name AS key", "to_json(struct(*)) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") \
  .option("topic", "topic_test") \
  .option("kafka.security.protocol","SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="...." password="....";""") \
  .save()
)
