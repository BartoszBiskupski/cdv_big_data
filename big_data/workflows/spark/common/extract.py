import requests
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext
import datetime
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class Extract_API:
    def __init__(self, ec, index):
        self.ec = ec
        self.extract_kwargs = {**self.ec.config["extract"][index]}
        print(self.extract_kwargs)
        self.name = self.extract_kwargs["name"]
        self.table_name = self.ec.config["load"]["params"]["table_name"]
        self.source_name = self.ec.config["load"]["params"]["source_name"]
        self.api_key = ec.config["api_key"]
        
        self.headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "text/csv"
            }
        self.id_przekroj_list = self.extract_kwargs["source"]["params"]["id_przekroj_list"]
        self.id_rok_list = self.extract_kwargs["source"]["params"]["id_rok_list"]
        self.zone = self.ec.config["load"]["params"]["zone"]
        self.url = self.url_builder()
        self.run_time = datetime.datetime.now().strftime("%Y-%m-%d")
        print(self.url)
        self.get_api_data()
        
    
    def url_builder(self, przekroj=None, rok=None, page_no=None):
        self.params = {**self.extract_kwargs["source"]["params"]}
        self.base_url = self.params["base_url"]
        self.version = self.params["version"]
        self.category = self.params["category"]
        self.subcategory = self.params["subcategory"]
        self.data = self.params["data"]
        self.cnt_per_page = f"&page-size={self.params['cnt_per_page']}" if self.params['cnt_per_page'] else ""
        self.page_no = f"&page={page_no}" if page_no else ""
        self.id_zmienna = f"&id-zmienna={self.params['id_zmienna']}" if self.params['id_zmienna'] else ""
        self.id_przekroj = f"&id-przekroj={przekroj}" if przekroj else ""
        self.id_okres = f"&id-okres={self.params['id_okres'][0]}" if self.params['id_okres'] else ""
        self.id_rok = f"&id-rok={rok}" if rok else ""
        self.language = f"?lang={self.params['language']}"
        
        api_url = f"{self.base_url}/{self.version}/{self.category}/{self.subcategory}?{self.id_zmienna}{self.id_przekroj}{self.id_rok}{self.id_okres}{self.page_no}{self.cnt_per_page}{self.language}"             
        return api_url
    
    
    def page_turner(self, przekroj="", rok="", page_no=0): 
        page_check = True
        while page_check:
            self.url = self.url_builder(przekroj, rok, page_no)
            response = requests.get(self.url, headers=self.headers)
            # print(response.json())
            # Check the response status code and print the response
            if response.status_code == 200:
                print("Request was successful.")
                page_no += 1
                path = f"{self.zone}{self.source_name}/{self.table_name}/run_time={self.run_time}/{self.name}_{przekroj}_{rok}_{page_no}.csv"
                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                dbutils.fs.put(path, response.text, overwrite=True)
                print(f"Saved data to {path}")
                page_check = True
            else:
                print(f"end of pages")
                page_check = False
                return page_check
    
    def single_page(self, przekroj="", rok=""):
        self.url = self.url_builder(przekroj, rok)
        response = requests.get(self.url, headers=self.headers)
        # print(response.json())
        # Check the response status code and print the response
        if response.status_code == 200:
            print("Request was successful.")
            path = f"{self.zone}{self.source_name}/{self.table_name}/run_time={self.run_time}/{self.name}_{przekroj}_{rok}.csv"
            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            dbutils.fs.put(path, response.text, overwrite=True)
            print(f"Saved data to {path}")
        else:
            print(f"end of pages")
            return False

    def get_api_data(self):
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "text/csv"
        }

        try:
            if self.id_przekroj_list:
                for przekroj in self.id_przekroj_list:
                    if self.id_rok_list:
                        for rok in self.id_rok_list:
                            if self.page_no:
                                page_check = True
                                while page_check:
                                    page_check = self.page_turner(przekroj, rok)
                            else:
                                self.single_page(przekroj, rok)
                    else:
                        if self.page_no:
                            page_check = True
                            while page_check:
                                page_check = self.page_turner(przekroj)
                        else:
                            self.single_page(przekroj)
            else:
                if self.page_no:
                    page_check = True
                    while page_check:
                        page_check = self.page_turner()
                else:
                    self.single_page()
        except Exception as e:
            print(f"Error: {e}")


