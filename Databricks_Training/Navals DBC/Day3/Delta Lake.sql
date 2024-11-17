-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Objects in lakehouse
-- MAGIC ##L1. Catalog
-- MAGIC ###L2.Databases / Schema
-- MAGIC ####L3. Table, Views, Functions, Volumes, Model

-- COMMAND ----------

Tables (Managed Table and External Table)

-- COMMAND ----------

-- DBTITLE 1,Internal of delta lake
Delta lake: 

parquet files (data)
+
_delta_log (metadata)
  1. crc
  2. json

-- COMMAND ----------

-- DBTITLE 1,Managed
1. Pyspark Dataframe
df=spark.read.csv("path")
df.write.saveAsTable("Table")


2. Ctas
create table tblname as
select * from file_format.`path`


3. SQL
create table tblname (col datatypes)


-- COMMAND ----------

-- DBTITLE 1,External
1. Pyspark Dataframe
df=spark.read.csv("path")
df.write.option("path","adls.")saveAsTable("Table")


2. Ctas
create table tblname location 'path' as
select * from file_format.`path`


3. SQL
create table tblname (col datatypes) location 'path'

-- COMMAND ----------

Task:
1. create table (sql)
2. describe extended tablename (confirm: delta, managed)
3. Insert few records
4. describe detail tablename (confirm: no of parquet files)
5. describe history tblname (to check history )

-- COMMAND ----------

create table michelin.emp(id int, name string, age int);
insert into table michelin.emp values (1,'a',30);
insert into table michelin.emp values (2,'b',29),(3,'c',16),(4,'d',11);

-- COMMAND ----------

describe extended michelin.emp;

-- COMMAND ----------

describe detail michelin.emp

-- COMMAND ----------

describe history michelin.emp

-- COMMAND ----------

Task: continue 

1. insert few records
2. insert few records 
3. take few inserts, delete, update (check : history command)
4. optimize z order
5. vacuum


-- COMMAND ----------

select * from michelin.emp

-- COMMAND ----------

Insert into michelin.emp values (5,'e',10),(6,'d',32);
Insert into michelin.emp values (7,'e',10),(8,'d',32);
Insert into michelin.emp values (9,'e',10),(10,'d',32);
Insert into michelin.emp values (11,'e',10),(12,'d',32);
Insert into michelin.emp values (13,'e',10),(14,'d',32);

-- COMMAND ----------

describe history michelin.emp

-- COMMAND ----------

update michelin.emp 
set age= 30
where id= 5

-- COMMAND ----------

delete from michelin.emp  where name ='e'

-- COMMAND ----------


