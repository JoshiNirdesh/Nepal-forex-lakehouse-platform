# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lag,col,round
df =spark.table("nepal_forex_db.fact_nrb_forex")

w = Window.partitionBy("currency_code").orderBy("rate_date")

change_df = df.withColumn("prev_sell_rate",lag("sell_rate").over(w))\
.withColumn("daily_changes",col("sell_rate")-col("prev_sell_rate"))\
.withColumn("daily_change_percentage",round(col("daily_changes")/col("prev_sell_rate")*100,2))
display(change_df)

# COMMAND ----------

from pyspark.sql.functions import avg
moving_avg =df.withColumn("moving_avg_7_day",
                round(avg("sell_rate").over(
                    Window.partitionBy("currency_code")
                    .orderBy("rate_date")
                    .rowsBetween(-6,0)),2
                ))
display(moving_avg)

# COMMAND ----------

