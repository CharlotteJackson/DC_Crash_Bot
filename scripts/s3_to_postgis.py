import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings, create_postgres_engine
from rds_data_model import generate_table

AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket = s3.bucket(AWS_Credentials['s3_bucket'])
region=AWS_Credentials['region']
engine = create_postgres_engine("AWS_PostGIS", "postgres", "DEV")
folder_to_load = 'source-data/dc-open-data/'

# grab list of all csv files in target folder
files_to_load = [(obj.key.replace(folder_to_load,''),obj.Object().metadata['target_schema']) for obj in bucket.objects.filter(Prefix=folder_to_load, Delimiter='/') if '.csv' in obj.key]

# set table import parameters that are the same for every file
copy_parameters = '\'(format csv)\''
columns_to_copy = '\'\''
aws_credentials_param = '\'{}\', \'{}\',\'\''.format(AWS_Credentials['aws_access_key_id'],AWS_Credentials['aws_secret_access_key'])

# create file-specific table import parameters
for (file_name, target_schema) in files_to_load:
    target_table = file_name.replace('.csv','')
    destination_table = '\'{}.{}\''.format(target_schema,target_table)
    create_s3_uri_param = '\'{}\', \'{}\',\'{}\''.format(AWS_Credentials['s3_bucket'], folder_to_load+file_name, region)

    # generate table if it doesn't exist
    # or truncate and refill
    generate_table(engine, target_schema,target_table,mode='replace')

    # run import statement
    import_table_query = """
        SELECT aws_s3.table_import_from_s3({}, {},{}, aws_commons.create_s3_uri({}) ,aws_commons.create_aws_credentials({}));
        """.format(destination_table, columns_to_copy, copy_parameters, create_s3_uri_param, aws_credentials_param)
    print(import_table_query)
    engine.execute(import_table_query)

