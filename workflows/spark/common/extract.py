import requests


class Extract_API:
    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

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

