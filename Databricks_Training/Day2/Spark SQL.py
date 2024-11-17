# Databricks notebook source
# MAGIC %md
# MAGIC %sql
# MAGIC 1. -- Querying raw files
# MAGIC 2. Select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table michelin.constructor_sql as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/sfm_databricks/michelin/raw/constructors.json`
