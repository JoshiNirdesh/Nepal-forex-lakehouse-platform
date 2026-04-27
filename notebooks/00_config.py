# Databricks notebook source
BASE_URL ="https://www.nrb.org.np/api/forex/v1/rates"
START_DATE = "2026-04-01"
END_DATE = "2026-04-24"

DATABASE = "nepal_forex_db"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

spark.sql("SELECT current_database()").show()



# COMMAND ----------

