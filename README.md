# covid_data_enginering



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
    └───demo                             # Demo files for analytics
    |   | analysis.ipynb                # Run SQL analytics queries with Athena
    |
    └───helper                           # Helper files
    |   | emr_default.json               # EMR cluster config
    |
    └───util                             # utility functions
        | etl.py                #  data ETL
   
```