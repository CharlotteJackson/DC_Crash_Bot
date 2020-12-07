import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import json

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
prefix = 'source-data/pulsepoint/'
metadata = {'target_schema':'tmp', "dataset_info":"https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6"}
dataset = 'pulsepoint'

col_names = {
        'Status_At_Load':[]
        ,'Incident_ID':[]
        ,'Call_Received_Datetime':[]
        ,'Latitude':[]
        ,'Longitude':[]
        ,'FullDisplayAddress':[]
        ,'Incident_Type':[]
         ,'Units':[]
        ,'Unit_Status_Transport':[]
}

def unit_status_is_transport(units: list):
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ('TR', 'TA'):
            return 'YES'
            break 
        return 'NO'

raw_file='/Users/ag79732/Downloads/pulsepoint.json'
with open(raw_file, 'r') as fhand:
        data = json.load(fhand)

record_status_types = [i for i in data['incidents'].keys() if i != 'alerts']

for status_type in record_status_types:
    records_to_load = data['incidents'][status_type]
    for record in records_to_load:
        col_names['Status_At_Load'].append(status_type)
        col_names['Incident_ID'].append(record['ID'])
        try:
            col_names['Call_Received_Datetime'].append(record['CallReceivedDateTime'])
        except KeyError:
            col_names['Call_Received_Datetime'].append('')
        col_names['Latitude'].append(record['Latitude'])
        col_names['Longitude'].append(record['Longitude'])
        col_names['FullDisplayAddress'].append(record['FullDisplayAddress'])
        col_names['Incident_Type'].append(record['PulsePointIncidentCallType'])
        col_names['Units'].append(record['Unit'])
        col_names['Unit_Status_Transport'].append(unit_status_is_transport(record['Unit']))

df = pd.DataFrame.from_dict(col_names)

# client = connect_to_mongo.MongoDB_Client(destination="AWS_DocumentDB", env="DEV",dbName="pulsepoint")
# connect_to_mongo.mongo_import(destination="AWS_DocumentDB", env="DEV",db_to_use="pulsepoint",myData=active_records,collection_to_use='sample_raw_export')

# download each dataset to local hard drive, and then upload it to the S3 bucket
# in csv format
filename = Path(os.path.expanduser('~'), dataset+'.csv')
df.to_csv(filename, index=False, header=True, line_terminator='\n')
data = open(filename, 'rb')
s3.Bucket(bucket_name).put_object(Key=prefix+dataset+'.csv', Body=data, Metadata =metadata)

