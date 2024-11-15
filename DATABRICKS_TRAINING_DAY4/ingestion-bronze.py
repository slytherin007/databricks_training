# Databricks notebook source
# MAGIC %run /Workspace/Users/ashwinibhide777@gmail.com/DATABRICKS_TRAINING_DAY4/include

# COMMAND ----------

dbutils.widgets.text("catalog","")
catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema","")
schema = dbutils.widgets.get("schema")

dbutils.widgets.text("source","")
source = dbutils.widgets.get("source")

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}{source}",header=True,inferSchema=True)
df_sales_final=add_ingestion_col(df_sales)
df_sales_final.write.mode("overwrite").saveAsTable(f"bronze.{source}")
