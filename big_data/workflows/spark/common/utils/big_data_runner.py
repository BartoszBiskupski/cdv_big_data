# Databricks notebook source

#imports
import sys
import os
import json
sys.path.append("/Workspace/cdv_big_data/")
import importlib

from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

# COMMAND ----------

params = dbutils.widgets.getAll()
print(params)
# COMMAND ----------
# set environment
environment = params["enviroment"]
job = params["job_name"].split("_")[0]

# load json env file
env_path = f"/Workspace/cdv_big_data/big_data/enviroment/env.{environment}.json"

task_name = params["task_name"]
# load json config file for area task
config_path = f"/Workspace/cdv_big_data/big_data/workflows/config/{job}/{task_name}.json"

# COMMAND ----------
# create execution context
ec = ExecutionContext(config_path, env_path)

final_config = ec.get_config()

# COMMAND ----------
#Extract data
try:
    for index, extract in enumerate(ec.config["extract"]):
        collable_name = extract["collable"]
        module_name = ".".join(collable_name.split(".")[:-1])
        print(module_name)
        module = importlib.import_module(module_name)
        collable = collable_name.split(".")[-1]
        collable_class = getattr(module, collable)
        
        collable_class(ec, index)
except KeyError:
    print("No extract step in the config file.")

    
# COMMAND ----------
# transform data
try:
    collable_name = ec.config["transform"]["collable"]
    module_name = ".".join(collable_name.split(".")[:-1])
    print(module_name)
    module = importlib.import_module(module_name)
    collable = collable_name.split(".")[-1]
    collable_class = getattr(module, collable)

    collable_class(ec)
except KeyError:
    print("No transform step in the config file.")


# COMMAND ----------
#load data
try:
    collable_name = ec.config["load"]["collable"]
    module_name = ".".join(collable_name.split(".")[:-1])
    print(module_name)
    module = importlib.import_module(module_name)
    collable = collable_name.split(".")[-1]
    collable_class = getattr(module, collable)
        
    collable_class(ec)
except KeyError:
    print("No load step in the config file.")

