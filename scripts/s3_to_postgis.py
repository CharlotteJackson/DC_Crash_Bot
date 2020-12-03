import geopandas as gpd
import pandas as pd
import boto3
import os
from connect_to_rds import get_connection_strings, create_postgres_engine
from rds_data_model import generate_table
import subprocess
import sys

# set up S3 and RDS connections
AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket = s3.Bucket(AWS_Credentials['s3_bucket'])
region=AWS_Credentials['region']
dbname='postgres'
env="DEV"
engine = create_postgres_engine(destination="AWS_PostGIS", target_db=dbname, env=env)
db_credentials = get_connection_strings("AWS_PostGIS")
db_uid =db_credentials[env]['UID']
db_pwd = db_credentials[env]['PWD']
db_host =db_credentials[env]['HOST']
db_port = db_credentials[env]['PORT']

# add psql install location to default path
psql_path=subprocess.check_output(['which', 'psql']).strip().decode('utf-8')
sys.path.append(psql_path)

# set which S3 folder(s) to load data for
folders_to_load = ['source-data/dc-open-data/','analysis-data/']

for folder in folders_to_load:
    # grab list of all csv files in target folder
    files_to_load = [(obj.key.replace(folder,''),obj.Object().metadata['target_schema']) for obj in bucket.objects.filter(Prefix=folder, Delimiter='/') if '.csv' in obj.key]
    print(files_to_load)

    # set table import parameters that are the same for every file
    copy_parameters = '\'(format csv)\''
    columns_to_copy = '\'\''
    aws_credentials_param = '\'{}\', \'{}\',\'\''.format(AWS_Credentials['aws_access_key_id'],AWS_Credentials['aws_secret_access_key'])

    # create file-specific table import parameters
    for (file_name, target_schema) in files_to_load:
        target_table = file_name.replace('.csv','')
        destination_table = '\'{}.{}\''.format(target_schema,target_table)
        create_s3_uri_param = '\'{}\', \'{}\',\'{}\''.format(AWS_Credentials['s3_bucket'], folder+file_name, region)

        # generate table if it doesn't exist
        # if it does exist, either truncate and refill or drop and replace
        generate_table(engine=engine, target_schema=target_schema,target_table=target_table,mode='replace')

        # create import statement
        import_table_query = 'SELECT aws_s3.table_import_from_s3({}, {},{}, aws_commons.create_s3_uri({}) ,aws_commons.create_aws_credentials({}));'.format(destination_table, columns_to_copy, copy_parameters, create_s3_uri_param, aws_credentials_param)
        # create arg to pass to os.system
        os_system_arg='PGPASSWORD={} psql --host={} --port={} --username={} --dbname={}  --no-password --command=\"{}\"'.format(db_pwd,db_host, db_port, db_uid, dbname, import_table_query)
        # execute
        os.system(os_system_arg)

        
