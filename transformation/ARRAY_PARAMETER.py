# Databricks notebook source
my_array = [
    {"source_container": "bronze",
     "sink_container": "silver",
     "folder": "events"
     },
    {"source_container": "bronze",
     "sink_container": "silver",
     "folder": "coaches"
     } 
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "my_output", value = my_array)