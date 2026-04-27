# Databricks notebook source
spark.sql("DROP TABLE IF EXISTS bronze_nrb_forex")
import requests, json
from pyspark.sql.functions import current_timestamp, lit

url = "https://www.nrb.org.np/api/forex/v1/rates"

params = {
    "from": "2026-04-01",
    "to": "2026-04-24",
    "page": 1,
    "per_page": 100
}

response = requests.get(url, params=params)
data = response.json()
raw_json = json.dumps(data)

bronze_df = spark.createDataFrame(
    [(raw_json,)],
    ["raw_data"]
)

bronze_df = bronze_df.withColumn("ingested_at", current_timestamp()) \
                     .withColumn("source", lit("NRB Forex API"))
display(bronze_df)
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("bronze_nrb_forex")

# COMMAND ----------

