# Databricks notebook source
# MAGIC %md
# MAGIC Create Table
# MAGIC 1. Pyspark (Dataframe)
# MAGIC 2. Spark SQL
# MAGIC 3. UI

# COMMAND ----------

# DBTITLE 1,Reading/Extracting
df=spark.read.csv("/Volumes/sfm_databricks/michelin/raw/circuits.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark is lazy Evaluated
# MAGIC ### Transformation
# MAGIC dataframe functions
# MAGIC .select
# MAGIC .alias
# MAGIC .withColumnRenamned
# MAGIC .withColumn
# MAGIC
# MAGIC

# COMMAND ----------

df.select("circuitId").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


df.select(col("circuitId").alias("circuit_id"),"circuitRef").display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"})

# COMMAND ----------

# DBTITLE 1,New Column
df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df.withColumn("country",upper("country")).display()
