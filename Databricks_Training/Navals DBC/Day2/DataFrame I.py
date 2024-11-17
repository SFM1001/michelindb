# Databricks notebook source
# MAGIC %md
# MAGIC Create Table
# MAGIC
# MAGIC 1. Pyspark (Dataframe)
# MAGIC 2. Spark SQl
# MAGIC 3. UI

# COMMAND ----------

# DBTITLE 1,Reading/Extracting
df=spark.read.csv("/Volumes/datamaster/michelin/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

Spark is Lazy evaluated 
Transformation
dataframe functions
.select
.alias
.withColumnRenamed
.withColumn

Functions(import)
col

# COMMAND ----------

df.select("circuitId".alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("circuitId").alias("circuit_id"),"circuitRef").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

df1=df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"})

# COMMAND ----------

# DBTITLE 1,New Column
df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

# DBTITLE 1,Replace Existing Column
df.withColumn("country",upper("country")).display()

# COMMAND ----------


