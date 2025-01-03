# Databricks notebook source

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

final_config = ec.get_config()

# COMMAND ----------
#Extract data
extract_config = final_config["extract"]

for index, extract in enumerate(extract_config):
    collable_name = extract["collable"]
    module_name = ".".join(collable_name.split(".")[:-1])
    print(module_name)
    module = importlib.import_module(module_name)
    collable = collable_name.split(".")[-1]
    collable_class = getattr(module, collable)
    
    final_config["data"].append(collable_class(ec, index).get_api_data())

    
# COMMAND ----------
#transform data
transform_config = final_config["transform"]
for data_dict in final_config["data"]:
    try:
        collable_name = transform_config["collable"]
        module_name = ".".join(collable_name.split(".")[:-1])
        module = importlib.import_module(module_name)
        collable = collable_name.split(".")[-1]
        collable_class = getattr(module, collable)
    
        final_config["data"].append(collable_class(ec))
    except KeyError:
        print("No transform step in the config file.")


# COMMAND ----------
#load data
load_config = final_config["load"]

try:
    collable_name = load_config["collable"]
    module_name = ".".join(collable_name.split(".")[:-1])
    module = importlib.import_module(module_name)
    collable = collable_name.split(".")[-1]
    collable_class = getattr(module, collable)
        
    collable_class(ec).save_to_adls()
except KeyError:
    print("No load step in the config file.")


