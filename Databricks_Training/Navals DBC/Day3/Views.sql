-- Databricks notebook source
Views (virtual Table)

1. Standard view (Persisted in schema) (SQL)
2. Temp View (SQL, Dataframe (Pyspark))
3. Global Temp view (Sql ,dataframe(pyspark))

-- COMMAND ----------

create view michelin.circuits_country as
(select country, count(country) as count from datamaster.michelin.circuits group by country order by count desc)

-- COMMAND ----------

create or replace temp view country_temp as
(select country, location, count(country) as count from datamaster.michelin.circuits group by all order by count desc)

-- COMMAND ----------

create or replace global temp view country_global_temp as
(select country, location, count(country) as count from datamaster.michelin.circuits group by all order by count desc)

-- COMMAND ----------

select * from country_temp limit 2

-- COMMAND ----------

select * from global_temp.country_global_temp limit 2

-- COMMAND ----------

show views in global_temp
