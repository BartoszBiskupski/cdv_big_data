#imports
import sys
import os
import json
from jinja2 import Template, StrictUndefined
from jinja2.exceptions import UndefinedError


class ExecutionContext:
    def __init__(self, config_path):
        self.config_path = config_path
        
    def get_env(self, env_path):
        with open(env_path, "r") as f:
            env = json.load(f)
        return env

    def get_config(self):
        with open(self.config_path, "r") as f:
            config_template = f.read()
        env = self.get_env(env_path)
        # render the config template using jinja2 with StrictUndefined
        try:
            template = Template(config_template, undefined=StrictUndefined)
            rendered_config = template.render(env=env)

            # Debug: print the rendered config before parsing
            print("Rendered config:", rendered_config)

            # Add api_key to the final config
            api_key = "1NrDfbLGxXcsUP6R87aRBrhZ/utLFVZgvsQsSnZre9w="  # Replace with the actual API key
            rendered_config["extract"]['api_key'] = api_key

            # print the final config with api_key
            print(json.dumps(rendered_config, indent=4))

            return rendered_config
        except UndefinedError as e:
            print(f"Template rendering error: {e}")
            return None


