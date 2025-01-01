import requests


class Extract_API:
    def __init__(self, ec):
        self.name = ec.get_config()["extract"]["name"]
        self.format = ec.get_config()["extract"]["source"]["params"]["format"]
        self.language = ec.get_config()["extract"]["source"]["params"]["language"]
        self.version = ec.get_config()["extract"]["source"]["params"]["version"]
        self.category = ec.get_config()["extract"]["source"]["params"]["category"]
        self.subcategory = ec.get_config()["extract"]["source"]["params"]["subcategory"]
        self.base_url = ec.get_config()["extract"]["source"]["base_url"]
        self.api_key = ec.get_config()["extract"]["api_key"]
        
        self.api_url = f"{self.base_url}/{self.version}/{self.category}/{self.category}-{self.subcategory}?lang={self.language}"

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

