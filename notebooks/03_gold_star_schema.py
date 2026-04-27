# Databricks notebook source

silver_df = spark.table("nepal_forex_db.silver_nrb_forex")
display(silver_df)

dim_currency = silver_df.select(
    "currency_code",
    "currency_name",
    "unit"
).dropDuplicates(["currency_code"])

dim_currency.write.format("delta").mode("overwrite").saveAsTable("nepal_forex_db.dim_currency")

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.functions import year,month,dayofmonth,dayofweek,date_format
date_df = silver_df.select("rate_date").dropDuplicates()
date_df = date_df.withColumn("year",year("rate_date"))\
                 .withColumn("month",month("rate_date"))\
                 .withColumn("day",dayofmonth("rate_date"))\
                 .withColumn("day_of_week",dayofweek("rate_date"))\
                 .withColumn("month_name",date_format("rate_date","MMMM"))

date_df.write.format("delta").mode("overwrite").saveAsTable("nepal_forex_db.date_dim")

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql.functions import col,round
fact_df = silver_df.select(
    "rate_date",
    "currency_code",
    "buy_rate",
    "sell_rate"
    ).withColumn("spread",round(col("sell_rate")-col("buy_rate"),2)).dropDuplicates(["rate_date","currency_code"])

fact_df.write.format("delta").mode("overwrite").saveAsTable("nepal_forex_db.fact_nrb_forex")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Daily Change
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 5

from pyspark.sql.window import Window
from pyspark.sql.functions import lag,col,round
df =spark.table("nepal_forex_db.fact_nrb_forex")

w = Window.partitionBy("currency_code").orderBy("rate_date")

change_df = df.withColumn("prev_sell_rate",lag("sell_rate").over(w))\
.withColumn("daily_changes",col("sell_rate")-col("prev_sell_rate"))\
.withColumn("daily_change_percentate",round(col("daily_changes")/col("prev_sell_rate")*100,2))
display(change_df)

change_df.write.format("delta").mode("overwrite").saveAsTable("nepal_forex_db.gold_forex_alerts")


# COMMAND ----------

