# Databricks notebook source
from pyspark.sql.functions import col, explode, from_json, to_date, current_timestamp
from pyspark.sql.types import *

spark.sql("USE nepal_forex_db")

bronze_df = spark.table("bronze_nrb_forex")

schema = StructType([
    StructField("data", StructType([
        StructField("payload", ArrayType(StructType([
            StructField("date", StringType(), True),
            StructField("rates", ArrayType(StructType([
                StructField("currency", StructType([
                    StructField("iso3", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("unit", IntegerType(), True)
                ]), True),
                StructField("buy", StringType(), True),
                StructField("sell", StringType(), True)
            ])), True)
        ])), True)
    ]), True)
])

parsed_df = bronze_df.withColumn(
    "parsed_json",
    from_json(col("raw_data"), schema)
)

silver_df = parsed_df \
    .select(explode(col("parsed_json.data.payload")).alias("day")) \
    .select(
        to_date(col("day.date")).alias("rate_date"),
        explode(col("day.rates")).alias("rate")
    ) \
    .select(
        col("rate_date"),
        col("rate.currency.iso3").alias("currency_code"),
        col("rate.currency.name").alias("currency_name"),
        col("rate.currency.unit").alias("unit"),
        col("rate.buy").cast("double").alias("buy_rate"),
        col("rate.sell").cast("double").alias("sell_rate"),
        current_timestamp().alias("processed_at")
    )

# display(silver_df)
failed_df = silver_df.filter(
    col("rate_date").isNull()|
    col("currency_code").isNull()|
    col("currency_name").isNull()|
    col("buy_rate").isNull()|
    col("sell_rate").isNull()
)
valid_df = silver_df.exceptAll(failed_df)
display(failed_df)
valid_df.write.format("delta").mode("overwrite").saveAsTable("silver_nrb_forex")

# COMMAND ----------

