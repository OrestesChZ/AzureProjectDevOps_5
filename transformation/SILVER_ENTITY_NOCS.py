# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SILVER NOTEBOOK

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading NOCS data

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@olympicsdatalakeorestes.dfs.core.windows.net/nocs")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Dropping columns

# COMMAND ----------

df = df.drop("country")

# COMMAND ----------

df = df.withColumn('tag',split(col('tag'),'-')[0])
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").option("path","abfss://silver@olympicsdatalakeorestes.dfs.core.windows.net/nocs").saveAsTable("olympicscatalog.silver.nocs")    

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("olympicscatalog.silver.nocs_managed")    