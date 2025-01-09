#imports
import sys
import os
import json
from jinja2 import Template, StrictUndefined
from jinja2.exceptions import UndefinedError
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

class ExecutionContext:
    def __init__(self, config_path, env_path):
        self.config_path = config_path
        self.env_path = env_path
        self.env = self.get_env()
        self.raw_config = self.get_config()
        self.config = self.render_config()
        
    def get_env(self):
        with open(self.env_path, "r") as f:
            env = json.load(f)
        print("Environment variables loaded:", env)  # Debug: Print the environment variables    
        return env

    def get_config(self):
        with open(self.config_path, "r") as f:
            config = json.load(f)
        print("Task variables loaded:", config)  # Debug: Print the environment variables   
        return config
    
    def get_api_secret(self):
        
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        api_key = dbutils.secrets.get(scope="CDV-BIG-DATA", key="api-key")
        return api_key

    def render_config(self):
        raw_config_str = json.dumps(self.raw_config)
        # render the config template using jinja2 with StrictUndefined
        try:
            template = Template(raw_config_str, undefined=StrictUndefined)
            rendered_config = template.render(env=self.env)
            # Debug: print the rendered config before parsing
            # print("Rendered config:", rendered_config)

            api_key = self.get_api_secret() # Replace with the actual API key
        
 
            rendered_config = json.loads(rendered_config)
            rendered_config["api_key"] = api_key
            # print(rendered_config)
            self.config = rendered_config
            # print the final config with api_key
            print("Loaded final config")
            return self.config
        except UndefinedError as e:
            print(f"Template rendering error: {e}")
            return None
        
    def update_config(self, new_key, new_value):
        # Update the existing config
        self.config["data"][new_key] = new_value
        print(f"Updated key {new_key}")


