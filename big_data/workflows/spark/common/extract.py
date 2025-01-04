import requests
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext

class Extract_API:
    def __init__(self, ec, index):
        self.ec = ec
        self.name = ec.config["extract"][index]["name"]
        self.format = ec.config["extract"][index]["source"]["params"]["format"]
        self.language = ec.config["extract"][index]["source"]["params"]["language"]
        self.version = ec.config["extract"][index]["source"]["params"]["version"]
        self.category = ec.config["extract"][index]["source"]["params"]["category"]
        self.subcategory = ec.config["extract"][index]["source"]["params"]["subcategory"]
        self.base_url = ec.config["extract"][index]["source"]["base_url"]
        self.api_key = ec.config["api_key"]
        
        self.api_url = f"{self.base_url}/{self.version}/{self.category}/{self.category}-{self.subcategory}?lang={self.language}"

    def get_api_data(self):
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        response = requests.get(self.api_url, headers=headers)
        
        if response.status_code == 200:
            json_input = response.json()
            self.ec.update_config(self.name, json_input)
        else:
            response.raise_for_status()

