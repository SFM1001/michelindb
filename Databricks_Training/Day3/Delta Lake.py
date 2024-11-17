# Databricks notebook source
Table
Managed Table and External Table

# COMMAND ----------

1. Pyspark Dataframe
df=spark.read.csv("path")
df.write.saveAsTable("Table")

2. Ctas
create table tablename location 'path' as
select * from file_format.`path`

3. SQL
create table tablename (col datatypes) location 'path'

# COMMAND ----------

Task:
1. create table (sql)
2. describe extended tablename (confirm: delta, managed)
3. Insert few records
4. describe detail tablename (confirm: no of parquet files)
5. describe history to check history

# COMMAND ----------

# MAGIC %sql
# MAGIC create table michlein.emp(id int, name string, age int);
# MAGIC insert into table michelin.emp
