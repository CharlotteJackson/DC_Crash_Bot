import boto3
import os
from connect_to_rds import get_connection_strings, create_psycopg2_connection, create_postgres_engine
import io
import sys
import argparse
import json
import datetime 
import os
import pandas as pd

def csv_to_postGIS (folder_to_load:str, AWS_Credentials:dict, **kwargs):

    # assign optional arguments
    target_schema=kwargs.get('target_schema', None)
    if target_schema == None:
        target_schema='stg'
    move_to_folder = kwargs.get('move_to_folder', None)
    clean_columns = kwargs.get('clean_columns', None)
    # if no environment is specified default to dev 
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
    connection = create_psycopg2_connection(destination="AWS_PostGIS", env=env)
    engine = create_postgres_engine(destination="AWS_PostGIS", env=env)

    files_to_load = [obj.key for obj in bucket.objects.filter(Prefix=folder_to_load) if '.csv' in obj.key]
   
    for object_key in files_to_load:
        # get base file name to use as table name
        stg_tble = os.path.basename(object_key) 

        # get the headers
        # get the headers to create columns
        sql_stmt = """SELECT S.* FROM s3object S LIMIT 1"""

        # pull the header row from target csv 
        req = client.select_object_content(
        Bucket=bucket_name,
        Key=object_key,
        ExpressionType='SQL',
        Expression=sql_stmt,
        InputSerialization = {'CSV': {'FileHeaderInfo': 'NONE', 'AllowQuotedRecordDelimiter': True}},
        OutputSerialization = {'CSV': {}},
        )

        # format csv headers into a list
        for event in req['Payload']:
            if 'Records' in event:
                file_str = ''.join(event['Records']['Payload'].decode('utf-8')).lower()
                columns_list = file_str.split(',')
                for column in columns_list:
                    print(column)
                    if column is None or column == '':
                        column = 'empty_col'


        # and then make a column create string out of them
        create_columns_statement = ''
        for column in columns_list:
            create_columns_statement+=',"'+column.replace('\n','').lower()+'" VARCHAR NULL'

        #generate create table shell script
        create_table_query = """
        DROP TABLE IF EXISTS {0}."{1}";
        CREATE TABLE IF NOT EXISTS {0}."{1}" (
            LOAD_DATETIME TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            ,source_file varchar DEFAULT '{2}'
            {3}
            );
            """.format(target_schema, stg_tble, object_key, create_columns_statement)

        # create the table
        with connection.cursor() as cursor:
            cursor.execute(create_table_query)
            connection.commit()

        # and then execute the query to fix the column names, if that parameter was passed 
        if clean_columns is not None and clean_columns.lower() == 'yes':
            fix_column_names_query = """
            SELECT replace(replace(SQL_Statement::varchar,\'";\', \';\'), \'u_"\', \'u_\') FROM (
            SELECT FORMAT(
                \'ALTER TABLE %%I.%%I.%%I RENAME %%I to u_%%I;\',
                table_catalog,
                table_schema,
                table_name,
                column_name,
                lower(
                    regexp_replace(
                    replace(replace(replace(replace(replace(replace(replace(column_name, \' \', \'_\'),\'?\',\'\'),\'/\',\'_\'),\'&\', \'and\'),\'(\',\'\'),\')\',\'\'),\'"\',\'\')
                    ,\'([[:lower:]])([[:upper:]])\',
                    \'\\1_\\2\',
                    \'xg\'
                    )
                )
                ) AS SQL_Statement
                FROM information_schema.columns
                WHERE table_name = \'{}\' and lower(column_name)!=\'load_datetime\' and lower(column_name) != \'source_file\'
                ) AS tmp;
            """.format(stg_tble)
            list_of_statements = [r for (r,) in engine.execute(fix_column_names_query).fetchall()]
            for statement in list_of_statements:
                engine.execute(statement)

            # get the updated list of corrected column names to pass to the copy columns parameter
            get_updated_columns_query = """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
            """.format(target_schema, stg_tble)

            # put column names of source table in list
            columns_list = [r for (r,) in engine.execute(get_updated_columns_query).fetchall() if 'load_datetime' not in r.lower() if 'source_file' not in r.lower()]
            file_str = ','.join(columns_list).lower()
        
        # load the target csv into memory
        f = client.get_object(Bucket = bucket_name, Key=object_key)
        # decode it as string 
        f2 = pd.read_csv(f['Body'])
        buffer = io.StringIO()
        f2.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # copy the file into the table
        with connection.cursor() as cursor:
            cursor.copy_expert(' COPY {0}."{1}" ({2}) FROM STDIN WITH CSV; '.format(target_schema, stg_tble, file_str), buffer)
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
"--clean_columns",
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
clean_columns=args.clean_columns
env=args.env

# call function with command line arguments
if __name__ == "__main__":
    for folder in folders_to_load:
        csv_to_postGIS(folder_to_load=folder, AWS_Credentials=get_connection_strings("AWS_DEV")
        , move_to_folder=move_to_folder, target_schema=target_schema, clean_columns=clean_columns
        ,env=env)