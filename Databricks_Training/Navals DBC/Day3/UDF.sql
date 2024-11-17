-- Databricks notebook source
create function function_name(para datatype)
returns datatype
return logic

-- COMMAND ----------

select * from datamaster.michelin.emp

-- COMMAND ----------

create function michelin.voter_eligible(age int)
returns string
return case 
  when age > 18 then 'eligible' 
  else 'You are not eligible for voting' 
  end

-- COMMAND ----------

select *, michelin.voter_eligible(age) as eligibility from datamaster.michelin.emp
