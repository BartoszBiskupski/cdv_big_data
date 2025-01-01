#imports
import sys
import os
import json
sys.path.append("Workspace/cdv_big_data/")
from big_data.workflows.spark.common.extract import Extract_API

# set environment
environment = "production"

# load json env file
env_path = f"/Users/bartoszbiskupski/Documents/git/cdv_big_data/big_data/enviroment/env.{environment}.json"


# load json config file for area task
config_path = "/Users/bartoszbiskupski/Documents/git/cdv_big_data/big_data/workflows/config/ingestion/area.json"

# create execution context
ec = ExecutionContext(config_path, env_path)


#test pulling api
extract = Extract_API(ec)

print(extract.get_data())


