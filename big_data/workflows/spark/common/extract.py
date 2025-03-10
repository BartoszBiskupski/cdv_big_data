import requests
from big_data.workflows.spark.common.utils.config_loader import ExecutionContext
import datetime
import time
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class Extract_API:
    """
    This class represents an API data extraction process from gus dbw api
    https://api-dbw.stat.gov.pl/apidocs/index.html
    
    Args:
        ec (object): An object representing the EC (execution context).
        
        index (int): The index of the extract configuration.
    """
    
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
        self.page_no = self.extract_kwargs["source"]["params"]["page_no"]
        self.id_rok_list = self.extract_kwargs["source"]["params"]["id_rok_list"]
        self.okres_list = self.extract_kwargs["source"]["params"]["id_okres_list"]
        self.zone = self.ec.config["load"]["params"]["zone"]

        self.run_time = "2025-01-09" #datetime.datetime.now().strftime("%Y-%m-%d")
        self.get_api_data()
        
    
    def url_builder(self, przekroj=None, rok=None, okres=None, page_no=None):
        self.params = {**self.extract_kwargs["source"]["params"]}
        self.base_url = self.params["base_url"]
        self.version = self.params["version"]
        self.category = self.params["category"]
        self.subcategory = self.params["subcategory"]
        self.data = self.params["data"]
        self.cnt_per_page = f"&ile-na-stronie={self.params['cnt_per_page']}" if self.params['cnt_per_page'] else ""
        self.page_no = f"&numer-strony={page_no}" if page_no else ""
        self.id_zmienna = f"&id-zmienna={self.params['id_zmienna']}" if self.params['id_zmienna'] else ""
        self.id_przekroj = f"&id-przekroj={przekroj}" if przekroj else ""
        self.id_okres = f"&id-okres={okres}" if okres else ""
        self.id_rok = f"&id-rok={rok}" if rok else ""
        self.language = f"&lang={self.params['language']}"
        
        api_url = f"{self.base_url}/{self.version}/{self.category}/{self.subcategory}?{self.id_zmienna}{self.id_przekroj}{self.id_rok}{self.id_okres}{self.cnt_per_page}{self.page_no}{self.language}"
        print(api_url)          
        return api_url
    
    
    def page_turner(self, przekroj="", rok="", okres = "", page_no=0): 
        page_check = True
        while page_check:
            self.url = self.url_builder(przekroj, rok, okres, str(page_no))
            response = requests.get(self.url, headers=self.headers)
            # print(response.json())
            # Check the response status code and print the response
            if response.status_code == 200:
                data = response.text
                # Count the number of lines in the CSV response
                print(f"Request was successful for page {page_no}.")
                page_no += 1
                path = f"{self.zone}{self.source_name}/{self.table_name}/run_time={self.run_time}/{self.name}_{przekroj}_{okres}_{rok}_{page_no}.csv"
                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
                dbutils.fs.put(path, response.text, overwrite=True)
                print(f"Saved data to {path}")
                page_check = True
                line_count = len(data.splitlines())
                if line_count < 5000:
                    print(f"Less than 5000 records received. Stopping pagination.")
                    page_check = False
                    time.sleep(10)  # Wait for 5 seconds
                    break
                time.sleep(10)  # Wait for 5 seconds
            else:
                print(f"Error {response.status_code}")
                if response.status_code == 429:
                    print("Too many requests. Waiting for 60 seconds.")
                    time.sleep(60)
                page_check = False
                return page_check
    
    def single_page(self, przekroj="", rok="", okres = ""):
        self.url = self.url_builder(przekroj, rok, okres)
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
            time.sleep(10)  # Wait for 5 seconds
        else:
            print(f"Error {response.status_code}")
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
                            if self.okres_list:
                                okres_list = self.okres_list[str(przekroj)]
                                for okres in okres_list:
                                    if self.page_no:
                                        page_check = True
                                        while page_check:
                                            page_check = self.page_turner(przekroj, rok, okres)
                                    else:
                                        self.single_page(przekroj, rok, okres)
                            else:
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


class Extract_CSV:
    """
    A class that represents the extraction of CSV data.

    Attributes:
        ec (object): An object representing the execution context.
        index (int): The index of the extract configuration.

    Methods:
        __init__(self, ec, index): Initializes the Extract_CSV object.
        get_csv_data(self): Retrieves the CSV data using SparkSession.

    """

    def __init__(self, ec, index):
        self.ec = ec
        self.extract_kwargs = {**self.ec.config["extract"][index]}
        print(self.extract_kwargs)
        self.name = self.extract_kwargs["name"]
        self.header = self.extract_kwargs["source"]["params"]["header"]
        self.landing = self.extract_kwargs["source"]["params"]["landing"]
        self.source_name = self.extract_kwargs["source"]["params"]["source_name"]
        self.file_name = self.extract_kwargs["source"]["params"]["file_name"]
        self.run_time = "*" #hardcoded for one time run
        self.get_csv_data()
        
    
    def get_csv_data(self):
        """
        Retrieves the CSV data using SparkSession.

        Returns:
            None

        """
        spark = SparkSession.builder.getOrCreate()
        try:
            df_extract = (spark.read
                            .format("csv")
                            .load(f"{self.landing}{self.source_name}/{self.file_name}/*/*.csv", header=self.header)
            ).distinct()
            self.ec.update_config(f"{self.name}", df_extract)
            print(f"Data extracted for {self.name} and added to ec")
        except Exception as e:
            print(f"Error: {e}")
            
class Extract_DataFrame:
    """
    A class that represents the extraction of data from a Spark DataFrame.

    Attributes:
        ec (object): The execution context object.
        index (int): The index of the extraction configuration.

    Methods:
        __init__(self, ec, index): Initializes the Extract_DataFrame object.
        get_df(self): Extracts the data from the specified table and updates the execution context.

    """

    def __init__(self, ec, index):
        self.ec = ec
        self.extract_kwargs = {**self.ec.config["extract"][index]}
        print(self.extract_kwargs)
        self.name = self.extract_kwargs["name"]
        self.catalog = self.extract_kwargs["source"]["params"]["catalog"]
        self.db_name = self.extract_kwargs["source"]["params"]["db_name"]
        self.table_name = self.extract_kwargs["source"]["params"]["table_name"]
        self.get_df()
        
    
    def get_df(self):
        """
        Extracts the data from the specified table and updates the execution context.

        Raises:
            Exception: If there is an error during the extraction process.

        """
        spark = SparkSession.builder.getOrCreate()
        try:
            df_extract = (spark.table(f"{self.catalog}.{self.db_name}.{self.table_name}")
            )
            self.ec.update_config(f"{self.name}", df_extract)
            print(f"Data extracted for {self.name} and added to ec")
        except Exception as e:
            print(f"Error: {e}")
        
        

