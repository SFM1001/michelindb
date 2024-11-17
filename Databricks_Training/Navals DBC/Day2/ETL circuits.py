# Databricks notebook source
# MAGIC %md
# MAGIC Create Table
# MAGIC
# MAGIC 1. Pyspark (Dataframe)
# MAGIC 2. Spark SQl
# MAGIC 3. UI

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,1. Reading/Extracting
df=spark.read.csv("/Volumes/datamaster/michelin/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,2. Transformation
df1=df\
.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref","lat":"latitude","lng":"longitutde","alt":"altitude"})\
.withColumn("ingestion_date",current_timestamp())\
.drop("url")

# COMMAND ----------

# DBTITLE 1,3.Write/ Saving
df1.write.mode("overwrite").saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.michelin.circuits
