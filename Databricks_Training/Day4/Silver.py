# Databricks notebook source
# MAGIC %run /Workspace/Users/shekhfariddb10@outlook.com/Databricks_Training/Day4/include

# COMMAND ----------

df=spark.table("medallionproject.bronze.sales")
df1=df.dropDuplicates().dropna()
df1.write.saveAsTable("medallionproject.silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- create or replace table medallionproject.bronze.sales as (select distinct order_id, customer_id, transaction_id, product_id, quantity, discount_amount, total_amount, order_date from medallionproject.bronze.sales where order_id is not null)

# COMMAND ----------

# MAGIC %sql  
# MAGIC select * from medallionproject.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view products_bronze as (select * from medallionproject.bronze.products)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO medallionproject.silver.products sp
# MAGIC USING products_bronze bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND operation ='UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name=bp.product_name,
# MAGIC     product_category=bp.product_category,
# MAGIC     product_price=bp.product_price
# MAGIC WHEN MATCHED AND operation ='DELETE'
# MAGIC THEN
# MAGIC DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_bp AS (
# MAGIC   SELECT
# MAGIC     bp.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seqNum
# MAGIC DESC) AS row_num
# MAGIC   FROM
# MAGIC     products_bronze bp
# MAGIC )
# MAGIC MERGE INTO medallionproject.silver.products sp
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_bp WHERE row_num = 1
# MAGIC ) bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND bp.operation = 'UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name = bp.product_name,
# MAGIC     product_category = bp.product_category,
# MAGIC     product_price = bp.product_price
# MAGIC WHEN MATCHED AND bp.operation = 'DELETE'
# MAGIC THEN
# MAGIC   DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC THEN
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     bp.product_id,
# MAGIC     bp.product_name,
# MAGIC     bp.product_category,
# MAGIC     bp.product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table medallionproject.bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE medallionproject.silver.products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_price DOUBLE
# MAGIC   )

# COMMAND ----------


