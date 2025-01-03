# Databricks notebook source

# COMMAND ----------

#imports
import sys
import os
import json
sys.path.append("/Workspace/cdv_big_data/")
from big_data.workflows.spark.common.extract import Extract_API
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

# COMMAND ----------

params = dbutils.widgets.getAll()
print(params)
# COMMAND ----------
# set environment
environment = params["enviroment"]

# load json env file
env_path = f"/Workspace/cdv_big_data/big_data/enviroment/env.{environment}.json"

task_name = params["task_name"]
# load json config file for area task
config_path = f"/Workspace/cdv_big_data/big_data/workflows/config/ingestion/{task_name}.json"

# COMMAND ----------
# create execution context
ec = ExecutionContext(config_path, env_path)


#test pulling api
extract = Extract_API(ec)

print(extract.get_data())


