import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings, create_postgres_engine

AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = 'dc-crash-bot-test'
engine = create_postgres_engine("AWS_PostGIS", "postgres", "DEV")

file_to_upload = 'vision_zero'

# # put in table in Raw schema in PostGIS 
# s3_uri_name = 'vision_zero_uri'
# create_s3_uri_query = 'SELECT aws_commons.create_s3_uri(\'{}\', \'{}.csv\',\'us-east-1\') AS {};'.format(bucket_name, file_to_upload, s3_uri_name)
# print(create_s3_uri_query)
# engine.execute(create_s3_uri_query)

# specify parameters
copy_parameters = '\'(format csv)\''
columns_to_copy = '\'\''
# s3_uri_param =  '\'{}\''.format(s3_uri_name)
file_to_upload_param = '\'{}\''.format(file_to_upload)
aws_credentials_param = 'aws_commons.create_aws_credentials(\'{}\', \'{}\',\'\')'.format(AWS_Credentials['aws_access_key_id'],AWS_Credentials['aws_secret_access_key'])
region_param = '\'us-east-1\''
bucket_name_param = '\'{}\''.format(bucket_name)
file_to_upload_param = '\'{}.csv\''.format(file_to_upload)

import_table_query = """
            SELECT aws_s3.table_import_from_s3({}, {},{}, {},{},{} ,{});
    """.format(file_to_upload_param, columns_to_copy, copy_parameters, bucket_name_param, file_to_upload_param, region_param, aws_credentials_param)
print(import_table_query)
