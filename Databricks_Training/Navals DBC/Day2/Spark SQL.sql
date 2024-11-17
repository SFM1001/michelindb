-- Databricks notebook source
-- DBTITLE 1,Syntax
-- Querying Raw files
select * from file_format.`path`

-- COMMAND ----------

-- DBTITLE 1,CTAS
create table michelin.constructor_sql as 
select *, current_timestamp() as ingestion_date from json.`/Volumes/datamaster/michelin/raw/constructors.json`

-- COMMAND ----------

select * from csv.`/Volumes/datamaster/michelin/raw/circuits.csv`
