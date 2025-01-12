# cdv_big_data

## Project Description
cdv_big_data is a project focused on processing and analyzing large datasets using various big data technologies and tools.

## Prerequisites
- Python 3.x / pyspark / SQL
- Apache Spark
- Hadoop
- Databricks

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/cdv_big_data.git
    ```
2. Navigate to the project directory:
    ```sh
    cd cdv_big_data
    ```
3. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

## USAGE
- pulling data from GUS API: https://api-dbw.stat.gov.pl/apidocs/index.html
- using env & task json - source and destination can be defined either azure or AWS (running script is defined as databricks notebook)
- can be extsended to pull any other source 

## TODO
- incremental load (currnelty only historic load configured for period 2010-2022)
- consumption layer with proper dimentions
- add scd2 extract/load functionality
- logger for ingestion / ETL 
- permission groups Azure/ ADB / pipeline to deploy the permissions & add ppl


## Contributing
1. Fork the repository.
2. Create a new branch:
    ```sh
    git checkout -b feature-branch
    ```
3. Make your changes and commit them:
    ```sh
    git commit -m "Description of changes"
    ```
4. Push to the branch:
    ```sh
    git push origin feature-branch
    ```
5. Open a pull request.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.