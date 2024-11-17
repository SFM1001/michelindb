# Databricks notebook source
# MAGIC %run /Workspace/Users/naval.datamaster@gmail.com/Michelin/includes

# COMMAND ----------

df=spark.read.json("/Volumes/datamaster/michelin/raw/constructors.json")

# COMMAND ----------

df1=add_ingestion_col(df)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("constructor")
