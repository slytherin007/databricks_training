# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extracting

# COMMAND ----------

# DBTITLE 1,Data Extraction
df=spark.read.csv(
"/Volumes/ab_databricks/michelin/raw/circuits.csv"
,header=
True
,inferSchema=
True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

df.select(col('circuitId').alias('Circuit_ID'))

# COMMAND ----------

df.withColumn(
"ingestion_date"
,current_date())

# COMMAND ----------

df.withColumn(
"country"
,upper(
"country"
))
