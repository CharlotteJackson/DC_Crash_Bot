import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import json
from pulse import get_data
import datetime

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
prefix = 'source-data/pulsepoint/'
metadata = {'target_schema':'tmp', "dataset_info":"https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6"}
dataset = 'pulsepoint'
current_time = datetime.datetime.now().strftime("%Y:%m:%d:%H:%M:%S")

# get the data
data=get_data()
# dump it to file and upload to AWS so we have the raw data for every pull
with open('data.json', 'w+') as outfile:
        json.dump(data, outfile, indent=4)
upload = open('data.json', 'rb')
s3_resource.Bucket(bucket_name).put_object(Key=prefix+dataset+current_time+'.json', Body=upload, Metadata =metadata)

col_names = {
        'Status_At_Load':[]
        ,'Return_Datetime':[]
        ,'Incident_ID':[]
        ,'Call_Received_Datetime':[]
        ,'Latitude':[]
        ,'Longitude':[]
        ,'FullDisplayAddress':[]
        ,'Incident_Type':[]
        ,'Units':[]
        ,'Unit_Status_Transport':[]
}

# define function to get flag whether any responding units have a status of transport or transport arrived
def unit_status_is_transport(units: list):
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ('TR', 'TA'):
            return 'YES'
            break 
        return 'NO'

# define function to get first and last unit cleared datetimes
# def get_cleared_datetime(units: list,mode:str):
#     if mode.lower=='max':
#         return_datetime='0-11-18T22:08:22Z'
#     for unit in units:
#         if 'UnitClearedDateTime' in unit.keys():
#             return 'YES'
#             break 
#         return 'NO'

# get all record status types
record_status_types = [i for i in data['incidents'].keys() if i != 'alerts']

# parse into dataframe
for status_type in record_status_types:
    records_to_load = data['incidents'][status_type]
    for record in records_to_load:
        col_names['Status_At_Load'].append(status_type)
        col_names['Return_Datetime'].append(current_time)
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

# download each dataset to local hard drive, and then upload it to the S3 bucket
# in csv format
filename = Path(os.path.expanduser('~'), dataset+'.csv')
df.to_csv(filename, index=False, header=True, line_terminator='\n')
data = open(filename, 'rb')
s3_resource.Bucket(bucket_name).put_object(Key=prefix+dataset+current_time+'.csv', Body=data, Metadata =metadata)

