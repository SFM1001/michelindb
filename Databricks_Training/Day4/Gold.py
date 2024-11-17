# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace view medallionproject.gold.customer_total_sales as (select customer_id,customer_name, round(sum(total_amount)) as total_amount from medallionproject.silver.sales_customer group by all )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view medallionproject.gold.total_sale as (select round(sum(total_amount)) as total_sales from medallionproject.silver.sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from medallionproject.gold.customer_total_sales
