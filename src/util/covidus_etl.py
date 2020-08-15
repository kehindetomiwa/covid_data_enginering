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

def process_data(spark, covid_data, output_data):
    '''
    create an optimized table from multiple csv file
    :param spark:
    :param input_data:
    :param output_data:
    :return:
    '''
    covid19_data = os.path.join(covid_data, 'covidus/*.csv')
    df = spark.read.csv(covid19_data, header=True)
    print('covidus_count = ', df.count())
    df = df.withColumn('datatime', F.to_date(F.col('date'))).drop('date')
    df.write.partitionBy(['state']).parquet(os.path.join(output_data, 'covidus'), 'overwrite')



def main():
    if len(sys.argv) > 2:
        input_data = sys.argv[1]
        output_data = sys.argv[2]
    else:
        config = configparser.ConfigParser()
        config.read('../dl.cfg')
        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        input_data = 's3a://' + config['S3']['RAW_DATALAKE_BUCKET'] + '/'
        output_data = 's3a://' + config['S3']['ACCIDENTS_DATALAKE_BUCKET'] + '/'
    spark = create_spark_session()
    process_data(spark, input_data, output_data)

