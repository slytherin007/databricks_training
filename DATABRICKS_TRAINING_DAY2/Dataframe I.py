# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.csv(
"/Volumes/ab_databricks/michelin/raw/circuits.csv" ,header=True,inferSchema=True)

# COMMAND ----------

df.withColumnsRenamed({'circuitId':'circuit_id','circuitRef':'circuit_ref','lat':'latitude','lng':'longitude','alt':'altitude'})\
  .withColumn('ingestion',current_timestamp())\
  .drop('url')
df1=df

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("michelin.circuits")
