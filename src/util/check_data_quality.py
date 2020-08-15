import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def check_covid_data(spark, datalake_bucket):
    covid_df = spark.read.parquet(os.path.join(datalake_bucket, 'covidus/*.parquet'))

    if covid_df.count() == 0:
        raise AssertionError('covidus table is empty.')

def check_county_data(spark, datalake_bucket):
    uscounty = spark.read.parquet(os.path.join(datalake_bucket, 'uscounty/county=*/state=*/*.parquet'))

    if uscounty.count() == 0:
        raise AssertionError('uscounty table is empty.')


def main():
    if len(sys.argv) == 2:
        # aws cluster mode
        datalake_bucket = sys.argv[1]
    else:
        # local mode
        config = configparser.ConfigParser()
        config.read('../dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        datalake_bucket = 's3a://' + config['S3']['ACCIDENTS_DATALAKE_BUCKET'] + '/'

    spark = create_spark_session()

    check_covid_data(spark, datalake_bucket)
    check_county_data(spark, datalake_bucket)

if __name__ == "__main__":
    main()
