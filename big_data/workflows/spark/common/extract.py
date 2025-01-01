import requests


class Extract_API(ExecutionContect: ec):
    def __init__(self):
        
        self.name = config["name"]
        self.format = config["source"]["params"]["format"]
        self.language = config["source"]["params"]["language"]
        self.version = config["source"]["params"]["version"]
        self.category = config["source"]["params"]["category"]
        self.subcategory = config["source"]["params"]["subcategory"]
        self.base_url = config["source"]["base_url"]
        self.api_key = config["source"]["api_key"]
        
        
        

    def get_data(self):
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        response = requests.get(self.api_url, headers=headers)
        
        if response.status_code == 200:
            json_input = response.json()
            return {"df_input": json_input}
        else:
            response.raise_for_status()

