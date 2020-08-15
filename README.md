# covid_data_enginering

# data
## covid_us_county.csv(43.5 MB) COVID-19 dataset
- fips: County code in numeric format (i.e. no leading zeros). A small number of cases have NA values here, but can still be used for state-wise aggregation. Currently, this only affect the states of Massachusetts and Missouri.

- county: Name of the US county. This is NA for the (aggregated counts of the) territories of American Samoa, Guam, Northern Mariana Islands, Puerto Rico, and Virgin Islands.

- state: Name of US state or territory.

- state_code: Two letter abbreviation of US state (e.g. "CA" for "California"). This feature has NA values for the territories listed above.

- lat and long: coordinates of the county or territory.

- date: Reporting date.

- cases & deaths: Cumulative numbers for cases & deaths.


## us_county.csv(348.43 KB) Demographic dataset 
- fips, county, state, state_code: same as above. The county names are slightly different, but mostly the difference is that this dataset has the word "County" added. I recommend to join on fips.

- male & female: Population numbers for male and female.

- population: Total population for the county. Provided as convenience feature; is always the sum of male + female.

- female_percentage: Another convenience feature: female / population in percent.

- median_age: Overall median age for the county.
data source [kaggle](https://www.kaggle.com/headsortails/covid19-us-county-jhu-data-demographics?select=us_county.csv))
## goal
Create a data dashboard to analysis covid-19 effection by demography

# Project structure
```
covid_data_enginering
│   README.md                            # Project description
│   docker-compose.yml                   # Airflow containers description   
│   requirements.txt                     # Python dependencies
│   dl.cfg                               # Config file
|
└───src
    └───airflow                          # Airflow home
    |   |               
    |   └───dags                         # DAG definitions
    |   |   │ datalake_etl_dag.py        # Optimized datalake ETL DAG
    |   |   | load_raw_datalake_dag.py   # Raw datalake DAG
    |   |
    |   └───plugins
    |       │  
    |       └───operators                # Custom operators
    |           | check_s3_file_count.py # UploadFilesToS3Operator
    |           | create_s3_bucket.py    # CreateS3BucketOperator
    |           | upload_files_to_s3.py  # UploadFilesToS3Operator
    |
    └───notebook                             # Demo files for analytics
    |   | Athena_analysis.ipynb                # Run SQL analytics queries with Athena
    |
    └───helper                           # Helper files
    |   | emr_default.json               # EMR cluster config
    |
    └───util                             # utility functions
        | check_data_quality
        | county.py
        | covidus.etl.py
        | partition_csv.py
   
```

## Requirements
-   Python3
-   Docker
-   Docker-compose (yml by [Bitnami](https://github.com/bitnami/bitnami-docker-airflow))
-   AWS account and Redshift cluster

## Overview
The idea is to use airflow to run end to end data flow. 

csv files --> S3 buckets --etl--> redshift --> Anthena (for analysis)

step1 partition csv 
    
        cd src/util
        python partition_csv.py
step2: startup airflow

        docker-compose up
        
Visit the path http://localhost:8080 in your browser. Login to Airflow.

Username: user

Password: password

step3: Connect Airflow to AWS

   - Click on the Admin tab and select Connections.
   - Under Connections, select Create.
   - On the create connection page, enter the following values:
   - - Conn Id: Enter aws_credentials.
   - - Conn Type: Enter Amazon Web Services.
   - - Login: Enter your Access key ID from the IAM User credentials.
   - - Password: Enter your Secret access key from the IAM User credentials.
   - - Extra: Add the default region name. { "region_name": "eu-west-1" }  
   
 Step5: start dags
 
 Step6: start Athena analysis    
 see [instruction](https://aws.amazon.com/blogs/machine-learning/run-sql-queries-from-your-sagemaker-notebooks-using-amazon-athena/)
 
 Start jupyter server
 Go to http://localhost:8888 and open src/notebook/ notebook.
 
# Addressing Other Scenarios
1. The data was increased by 100x
    - increase airflow container with container orchestration
    -
2. The pipeline run on  a daily bases by 7am.
    - we need to change the data schedule from monthly to daily
3. database needed to be accessed by 100+ people 
    - Athena serverless functionality helps in handling this 
 
