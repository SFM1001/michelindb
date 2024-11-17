# Databricks notebook source
# MAGIC %run /Workspace/Users/shekhfariddb10@outlook.com/Databricks_Training/includes

# COMMAND ----------

df=spark.read.json("/Volumes/sfm_databricks/michelin/raw/constructors.json")

# COMMAND ----------

df1=add_ingestion_col(df)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("constructor")
