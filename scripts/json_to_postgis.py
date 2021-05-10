import boto3
import os
from connect_to_rds import get_connection_strings, create_psycopg2_connection
import io
import sys
import argparse
import json
import datetime 
import os
import argparse
import pprint
import re 


def json_to_postGIS (folder_to_load:str, AWS_Credentials:dict, **kwargs):

    # assign optional arguments
    target_schema=kwargs.get('target_schema', None)
    if target_schema == None:
        target_schema='stg'
    print(target_schema)
    move_to_folder = kwargs.get('move_to_folder', None)
    print(move_to_folder)
    env=kwargs.get('env', None)
    if env == None:
        env='DEV'
    env=env.upper()

    # list of all loaded tables
    tables_created = []

    # set up RDS and S3 connections, engines, cursors
    s3_resource = boto3.resource('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
    bucket_name = AWS_Credentials['s3_bucket']
    bucket = s3_resource.Bucket(bucket_name)
    client=boto3.client('s3',aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
    region=AWS_Credentials['region']
    env="PROD"
    connection = create_psycopg2_connection(destination="AWS_PostGIS", env=env)

    files_to_load = [obj.key for obj in bucket.objects.filter(Prefix=folder_to_load) if '.json' in obj.key]
   
    for object_key in files_to_load:
        # get base file name to use as table name
        stg_tble = os.path.basename(object_key) 

        # load the target json into memory
        f = client.get_object(Bucket = bucket_name, Key=object_key)
        # decode it as string 
        f2 = f['Body'].read().decode('utf-8')

        # delete escaped quotes and newlines
        f2 = f2.replace('\\"','')
        f2 = f2.replace('\\n','')
        f2 = f2.replace('\\','')

        # load back into dictionary format 
        f3 = json.loads(f2)

        # add the source file name to the json
        if isinstance(f3,dict):
            f3['source_file'] = object_key
        elif isinstance(f3,list):
            f3={'data':f3, 'source_file':object_key}

        #create table shell script
        create_table_query = """
        DROP TABLE IF EXISTS {0}."{1}";
        CREATE TABLE IF NOT EXISTS {0}."{1}" (
            LOAD_DATETIME TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            ,source_file varchar DEFAULT '{2}'
            ,data JSONB NULL
            );
            """.format(target_schema, stg_tble, object_key)

        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()

        # copy the f3 file into the table
        with connection.cursor() as cursor:
            loadfile = io.StringIO(json.dumps(f3))
            cursor.copy_expert(' COPY {0}."{1}" (data) FROM STDIN; '.format(target_schema, stg_tble), loadfile)
            connection.commit()

        if move_to_folder is not None:
            s3_resource.Object(bucket_name,move_to_folder+stg_tble).copy_from(CopySource = {'Bucket': bucket_name, 'Key': object_key})
            s3_resource.Object(bucket_name, object_key).delete()

        tables_created.append((target_schema,"{}".format(stg_tble)))

    return tables_created


CLI=argparse.ArgumentParser()
CLI.add_argument(
"folders",  
nargs="*",  
type=str
)

CLI.add_argument(
"--move_to_folder",
type=str
)
CLI.add_argument(
"--target_schema",
type=str
)
CLI.add_argument(
"--env",
type=str
)

# parse the command line
args = CLI.parse_args()
folders_to_load = args.folders
move_to_folder = args.move_to_folder
target_schema = args.target_schema
env = args.env

# call function with command line arguments
if __name__ == "__main__":
    for folder in folders_to_load:
        json_to_postGIS(folder_to_load=folder, AWS_Credentials=get_connection_strings("AWS_DEV"), move_to_folder=move_to_folder, target_schema=target_schema, env=env)