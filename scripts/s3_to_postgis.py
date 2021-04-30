import boto3
import os
from connect_to_rds import get_connection_strings, create_postgres_engine
from rds_data_model import generate_table, correct_geo
import subprocess
import sys
import argparse
import urllib

def s3_to_postGIS (folder_to_load:str, AWS_Credentials:dict, header:str, mode:str,move_after_loading:str,move_to_folder:str):

    # set up S3 and RDS connections
    s3_resource = boto3.resource('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
    bucket_name = AWS_Credentials['s3_bucket']
    bucket = s3_resource.Bucket(bucket_name)
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

    # grab list of all files in target folder that have a target table
    # url encode the file key so the ones with semicolons don't throw an error
    # update january 2021: the script now throws an error if i try to feed it URL-encoded object keys, so just using the plain text one now
    files_to_load = [(urllib.parse.quote(obj.key), obj.key, obj.Object().metadata['target_schema'],obj.Object().metadata['target_table']) for obj in bucket.objects.filter(Prefix=folder_to_load) if 'target_table' in obj.Object().metadata.keys() if '.csv' in obj.key]
    # generate distinct list of target tables so they're all only dropped and recreated/truncated one time
    target_tables = [(target_schema, target_table) for (file_name, file_name_native, target_schema, target_table) in files_to_load]
    target_tables_distinct = set(target_tables)
    target_tables=list(target_tables_distinct)

    # drop and recreate and/or truncate each target table
    for (target_schema, target_table) in target_tables:
        generate_table(engine=engine, target_schema=target_schema,target_table=target_table,mode=mode)

    # set table import parameters that are the same for every file
    copy_parameters = '\'(FORMAT CSV, HEADER {})\''.format(header)
    columns_to_copy = '\'\''
    aws_credentials_param = '\'{}\', \'{}\',\'\''.format(AWS_Credentials['aws_access_key_id'],AWS_Credentials['aws_secret_access_key'])

    # create file-specific table import parameters
    for (file_name, file_name_native, target_schema, target_table) in files_to_load:
        destination_table = '\'{}.{}\''.format(target_schema,target_table)
        create_s3_uri_param = '\'{}\', \'{}\',\'{}\''.format(AWS_Credentials['s3_bucket'], file_name_native, region)
        base_file_name = os.path.basename(file_name_native) 

        # create import statement
        import_table_query = 'SELECT aws_s3.table_import_from_s3({}, {},{}, aws_commons.create_s3_uri({}) ,aws_commons.create_aws_credentials({}));'.format(destination_table, columns_to_copy, copy_parameters, create_s3_uri_param, aws_credentials_param)
        # create arg to pass to os.system
        os_system_arg='PGPASSWORD=\'{}\' psql --host={} --port={} --username={} --dbname={}  --no-password --command=\"{}\"'.format(db_pwd,db_host, db_port, db_uid, dbname, import_table_query)
        # execute
        if move_after_loading != 'yes':
            os.system(os_system_arg)
        elif move_after_loading == 'yes' and move_to_folder != '':
            os.system(os_system_arg)
            try:
                s3_resource.Object(bucket_name,move_to_folder+base_file_name).copy_from(CopySource = {'Bucket': bucket_name, 'Key': file_name_native})
                s3_resource.Object(bucket_name, file_name_native).delete()
            except:
                print(file_name_native," could not be copied and/or deleted")
                continue
        else:
            print("please provide move-to folder")
            continue

    # after data is loaded, update the geographies
    for (target_schema, target_table) in target_tables:
        correct_geo(engine=engine, target_schema=target_schema,target_table=target_table,mode=mode)
        
# set up ability to call with lists from the command line as follows:
# python s3_to_postgis.py --folders source-data/dc-open-data/crash --format csv --mode replace --header true --move_after_loading no --move_to_folder ""
CLI=argparse.ArgumentParser()
CLI.add_argument(
"folders",  
nargs="*",  
type=str,
default=['source-data/dc-open-data/crashes_raw/','source-data/dc-open-data/crash_details/'],  # default - load the two main crash datasets
)
CLI.add_argument(
"--header",
type=str, 
default='true', # default - header true
)
CLI.add_argument(
"--mode",
type=str, 
default='replace' # default is to append new records instead of dropping and reloading everything
)
CLI.add_argument(
"--move_after_loading",
type=str, 
default='no' # default is to append new records instead of dropping and reloading everything
)
CLI.add_argument(
"--move_to_folder",
type=str, 
default='' # default is to append new records instead of dropping and reloading everything
)

# parse the command line
args = CLI.parse_args()
folders_to_load = args.folders
header = args.header.lower()
mode=args.mode.lower()
move_after_loading = args.move_after_loading.lower()
move_to_folder = args.move_to_folder.lower()

# call function with command line arguments
for folder in folders_to_load:
    s3_to_postGIS(folder_to_load=folder, AWS_Credentials=get_connection_strings("AWS_DEV"), header=header, mode=mode,move_after_loading=move_after_loading,move_to_folder=move_to_folder)