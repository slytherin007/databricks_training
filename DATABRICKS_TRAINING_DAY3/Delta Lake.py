# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Objects in lake house
# MAGIC
# MAGIC 1. Every transaction creates a new json file i.e. no. of insert or create statements.
# MAGIC 2. These parquet files are immutable so either the file is created or deleted(By deleting it doesn't actually delete the file because delta lake has feature to of time travel. By time travel it means that the data can be retrived even if the files are deleted. So the file becomes stale and a new file for delete is created). If you delete a parquet file you won't be able to do the time travel.
# MAGIC 3. Json file contains the metadata which is pointing to the parquet file.

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table michelin.emp(id int)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended michelin.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into michelin.emp values (12)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail michelin.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history michelin.emp

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Optimize the file: compact small file into 1 file
# MAGIC By applying z-order by it creates a index sheet in which prevents the repeated task of searching

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into michelin.emp values(13);
# MAGIC insert into michelin.emp values(14);
# MAGIC insert into michelin.emp values(15);
# MAGIC insert into michelin.emp values(16);
# MAGIC insert into michelin.emp values(17);
# MAGIC insert into michelin.emp values(18);
# MAGIC insert into michelin.emp values(19);
# MAGIC insert into michelin.emp values(20);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from michelin.emp where id=13;

# COMMAND ----------

# MAGIC %sql
# MAGIC update michelin.emp set id=55 where id=15;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history michelin.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize michelin.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize 
