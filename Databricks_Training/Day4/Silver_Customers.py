# Databricks notebook source
# MAGIC %sql
# MAGIC select * from medallionproject.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists medallionproject.silver.customers (
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   customer_email STRING,
# MAGIC   customer_city STRING,
# MAGIC   customer_state STRING,
# MAGIC   operation STRING,
# MAGIC   sequenceNum INT,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   source_path STRING,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH source_cte AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     current_timestamp() AS start_date,
# MAGIC     NULL AS end_date,
# MAGIC     TRUE AS is_current,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY customer_id 
# MAGIC       ORDER BY sequenceNum DESC, ingestion_date DESC
# MAGIC     ) AS rn
# MAGIC   FROM  medallionproject.bronze.customers
# MAGIC )
# MAGIC MERGE INTO  medallionproject.silver.customers AS target
# MAGIC USING (
# MAGIC   SELECT *
# MAGIC   FROM source_cte
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id AND target.is_current = TRUE
# MAGIC WHEN MATCHED AND source.operation = 'UPDATE' THEN
# MAGIC   UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.is_current = FALSE
# MAGIC
# MAGIC WHEN MATCHED and source.operation='DELETE' THEN
# MAGIC  UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.is_current = FALSE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.customer_id,
# MAGIC     source.customer_name,
# MAGIC     source.customer_email,
# MAGIC     source.customer_city,
# MAGIC     source.customer_state,
# MAGIC     source.operation,
# MAGIC     source.sequenceNum,
# MAGIC     source.ingestion_date,
# MAGIC     source.source_path,
# MAGIC     source.start_date,
# MAGIC     source.end_date,
# MAGIC     source.is_current
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view medallionproject.silver.customer_current as 
# MAGIC select customer_id,customer_name, customer_email,customer_city, customer_state from medallionproject.silver.customers where is_current=true

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table medallionproject.silver.sales_customer
# MAGIC (select s.customer_id,s.product_id,s.quantity,s.discount_amount, s.total_amount,c.customer_name,c.customer_city,c.customer_state
# MAGIC from medallionproject.silver.sales s
# MAGIC inner join medallionproject.silver.customer_current c
# MAGIC on s.customer_id=c.customer_id)
