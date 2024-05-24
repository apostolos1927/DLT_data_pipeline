# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType,IntegerType,StringType

# COMMAND ----------

look_up_schema =  StructType([
  StructField('date', StringType(), True),
  StructField('datekey', IntegerType(),True)
])
date_lookup_dict = [
        ('2024-03-16',20240316),
        ('2024-03-17',20240317),
        ('2024-03-18',20240318),
        ('2024-03-19',20240319),
        ('2024-03-20',20240320)
        ]
date_lookup = spark.createDataFrame(data=date_lookup_dict, schema = look_up_schema)

# COMMAND ----------

@dlt.view(comment="Daily kafka daily")
def landing_view():
  return (
    spark.readStream.format("kafka") 
      .option("kafka.bootstrap.servers", "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092") 
      .option("subscribe", "topic_test") 
      .option("startingOffsets", "earliest") 
      .option("kafka.security.protocol","SASL_SSL") 
      .option("kafka.sasl.mechanism", "PLAIN") 
      .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="..." password="...";""") 
    .load()
  )

# COMMAND ----------

@dlt.table(
  name="bronze_layer",
  comment="Bronze layer",
  table_properties={"quality": "bronze"}
)
@dlt.expect("valid_topic", "topic IS NOT NULL")
def bronze_layer():
  return (
    dlt.read_stream("landing_view")
      .withColumn("processed_timestamp", F.current_timestamp())
      .select("value","topic","processed_timestamp")
)

# COMMAND ----------

json_schema = """order_id int, amount int, ship_date string, customer_name string, customer_country string, payment_method string, payment_status string"""
drop_duplicates_columns = ["order_id","amount","ship_date","customer_name","customer_country","payment_method","payment_status"]
select_columns_silver = ["order_id","amount","ship_date","customer_name","customer_country","payment_method","payment_status","datekey"]

# COMMAND ----------

@dlt.table(
        name="silver_layer",
        table_properties={"quality": "silver"},
        partition_cols=["datekey"],
        comment=f"Silver Layer"
)
@dlt.expect_or_drop("order_id_constraint", "order_id>0")
@dlt.expect_or_drop("customer_country_constraint", "customer_country IS NOT NULL")
@dlt.expect_or_drop("amount_constraint", "amount>0")
def silver_layer():
        return (
            dlt.read_stream("bronze_layer")
              .filter(f"topic = 'topic_test'")
              .withColumn("body",F.from_json(F.col("value").cast("string"), json_schema))
              .select("body.*","topic","processed_timestamp")
              .dropDuplicates(drop_duplicates_columns)
              .join(F.broadcast(date_lookup.select("date", "datekey")),
               F.to_date("ship_date") == F.to_date(F.col("date")), "left")
              .select(select_columns_silver)
              )
