# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Reading/Extracting
df=spark.read.csv("/Volumes/sfm_databricks/michelin/raw/circuits.csv", header=True, inferSchema=True)

# COMMAND ----------

df1=df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref", "lat":"latitude","lng":"longitude","alt":"altitude"}).withColumn("ingestion_date",current_timestamp()).drop("url")

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sfm_databricks.michelin.circuits
