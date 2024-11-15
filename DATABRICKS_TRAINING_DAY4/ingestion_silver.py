# Databricks notebook source
# MAGIC %run /Workspace/Users/ashwinibhide777@gmail.com/DATABRICKS_TRAINING_DAY4/include

# COMMAND ----------

df=spark.table("michelin.bronze.sales")
df1=df.dropDuplicates().dropna()
df1.write.mode("overwrite").saveAsTable("michelin.silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view 

# COMMAND ----------


