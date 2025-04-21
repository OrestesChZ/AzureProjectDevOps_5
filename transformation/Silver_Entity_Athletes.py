# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types	 import *
from pyspark.sql.window	 import Window


# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@olympicsdatalakeorestes.dfs.core.windows.net/athletes")
display(df)

# COMMAND ----------

df = df.fillna({"birth_place": "xyz", "birth_country": "abc", "residence_place": "unknown", "residence_country": "aaa"})
df.display()

# COMMAND ----------

df_filter = df.filter((col("current") == True) & (col("name").isin("GALSTYAN Slavik", "HARUTYUNYAN Arsen", "SEHEN Sajjad")))
df_filter.display()

# COMMAND ----------

df = df.withColumn("height", col("height").cast(FloatType())) \
       .withColumn("weight", col("weight").cast(FloatType()))

df.display()


# COMMAND ----------

df_sort = df.sort("height", "weight", ascending=[0,1]).filter(col("weight") > 0)
df_sort.display()

# COMMAND ----------

df_sort = df_sort.withColumn("nationality", regexp_replace("nationality","United States", "US"))
df_sort.display()

# COMMAND ----------

df.groupBy("code").agg(count("code")\
    .alias("total_count"))\
    .filter(col("total_count") > 1).display()
    


# COMMAND ----------

df_sort = df.withColumn("occupation", split("occupation",","))
df_sort.display()

# COMMAND ----------

df_sort = df_sort.withColumnRenamed("code", "athelete_id")
df_sort.display()


# COMMAND ----------

df_sort.columns

# COMMAND ----------

df_final = df_sort.select('athelete_id',
 'current',
 'name',
 'name_short',
 'name_tv',
 'gender',
 'function',
 'country_code',
 'country',
 'country_long',
 'nationality',
 'nationality_long',
 'nationality_code',
 'height',
 'weight')
df_final.display()

# COMMAND ----------

df_final.withColumn("cum_weight", sum("weight").over(Window.partitionBy("nationality").orderBy("height").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

df_final.write.format("delta").mode("append").option("path","abfss://silver@olympicsdatalakeorestes.dfs.core.windows.net/athletes").saveAsTable("olympicscatalog.silver.athletes")