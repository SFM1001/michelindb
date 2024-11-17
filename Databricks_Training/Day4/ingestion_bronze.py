# Databricks notebook source
# MAGIC %run /Workspace/Users/shekhfariddb10@outlook.com/Databricks_Training/Day4/include

# COMMAND ----------

dbutils.widgets.text("catalog","medallionproject")
catalog=dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("schema","bronze")
schema=dbutils.widgets.get("schema")

# COMMAND ----------

dbutils.widgets.text("table","")
source_file_name=dbutils.widgets.get("table")

# COMMAND ----------

df=spark.read.csv(f"{input_path}{source_file_name}.csv", header=True,inferSchema=True)
df_final=add_ingestion_col(df)
df_final.write.mode("overwrite").saveAsTable(f"bronze.{source_file_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `${catalog}`.`${schema}`.`${table}`
