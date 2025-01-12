#imports
import sys
import os
import json
from jinja2 import Template, StrictUndefined
from jinja2.exceptions import UndefinedError
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

class ExecutionContext:
    """
    Represents the execution context for loading and rendering configurations.

    Args:
        config_path (str): The path to the configuration file.
        env_path (str): The path to the environment file.

    Attributes:
        config_path (str): The path to the configuration file.
        env_path (str): The path to the environment file.
        env (dict): The environment variables loaded from the environment file.
        raw_config (dict): The raw configuration loaded from the configuration file.
        config (dict): The rendered configuration with environment variables applied.

    Methods:
        get_env(): Loads the environment variables from the environment file.
        get_config(): Loads the configuration from the configuration file.
        get_api_secret(): Retrieves the API key from the secret store.
        render_config(): Renders the configuration template with environment variables.
        update_config(new_key, new_value): Updates the existing configuration with a new key-value pair.
    """
    
    def __init__(self, config_path, env_path):
        self.config_path = config_path
        self.env_path = env_path
        self.env = self.get_env()
        self.raw_config = self.get_config()
        self.config = self.render_config()
        
    def get_env(self):
        """
        Loads the environment variables from the environment file.

        Returns:
            dict: The environment variables.
        """
        with open(self.env_path, "r") as f:
            env = json.load(f)
        print("Environment variables loaded:", env)  # Debug: Print the environment variables    
        return env

    def get_config(self):
        """
        Loads the configuration from the configuration file.

        Returns:
            dict: The raw configuration.
        """
        with open(self.config_path, "r") as f:
            config = json.load(f)
        print("Task variables loaded:", config)  # Debug: Print the environment variables   
        return config
    
    def get_api_secret(self):
        """
        Retrieves the API key from the secret store.

        Returns:
            str: The API key.
        """
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        api_key = dbutils.secrets.get(scope="CDV-BIG-DATA", key="api-key")
        return api_key

    def render_config(self):
        """
        Renders the configuration template with environment variables.

        Returns:
            dict: The rendered configuration.
        """
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
        """
        Updates the existing configuration with a new key-value pair.

        Args:
            new_key (str): The new key to be added.
            new_value (any): The value associated with the new key.

        Returns:
            None
        """
        # Update the existing config
        self.config["data"][new_key] = new_value
        print(f"Updated key {new_key}")


