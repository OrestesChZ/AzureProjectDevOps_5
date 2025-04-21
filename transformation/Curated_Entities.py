# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Delta Live Tables - GOLD LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### COACHES DLT PIPELINE

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPECTATIONS FOR DATA QUALITY
# MAGIC

# COMMAND ----------

expec_coaches = {
        "rule1" : "code is not null",
        "rule2" : "current is True"
    }


# COMMAND ----------

expec_nocs =  {
        "rule1" : "code is not null"
    }


# COMMAND ----------

expec_events =  {
        "rule1" : "event is not null"
    }


# COMMAND ----------

@dlt.table

def source_table_coaches():
    df = spark.readStream.table("olympicscatalog.silver.coaches")
    return df

# COMMAND ----------

@dlt.view

def view_coaches():
    df = spark.readStream.table("LIVE.source_table_coaches")
    df = df.fillna("Unknown")
    return df
    


# COMMAND ----------

@dlt.table
@dlt.expect_all(expec_coaches)

def coaches():
    df = spark.readStream.table("LIVE.view_coaches")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### NOCS DLT PIPELINE

# COMMAND ----------

@dlt.view

def source_view_nocs():
    df = spark.readStream.table("olympicscatalog.silver.nocs")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all_or_drop(expec_nocs)

def nocs():
    df = spark.readStream.table("LIVE.source_view_nocs")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Events DLT Pipeline

# COMMAND ----------

@dlt.view

def source_view_events():
    df = spark.readStream.table("olympicscatalog.silver.events")
    return df

# COMMAND ----------

@dlt.table
@dlt.expect_all(expec_events)

def events():
    df = spark.readStream.table("LIVE.source_view_events")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC APPLY CHANGES (DLT)

# COMMAND ----------

@dlt.view

def source_table_athletes():
    df = spark.readStream.table("olympicscatalog.silver.athletes")
    return df

# COMMAND ----------

dlt.create_streaming_table("athletes")

# COMMAND ----------

dlt.apply_changes(
    target = "athletes",
    source = "source_table_athletes",
    keys = ["athelete_id"],
    sequence_by = col("height"),
    stored_as_scd_type = 1
    )