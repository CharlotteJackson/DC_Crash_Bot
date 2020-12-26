import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import json
from pulse import get_data
import datetime
from datetime import timezone 
import pytz

# set up S3 connection
AWS_Credentials=get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
prefix = 'source-data/pulsepoint/unparsed/'
destination = 'source-data/pulsepoint/'
metadata = {'target_schema':'tmp', 'target_table':'pulsepoint',"dataset_info":"https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6"}
current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
bucket = s3_resource.Bucket('dc-crash-bot-test')
client=boto3.client('s3')


# define function to get flag whether any responding units have a status of transport or transport arrived
def unit_status_is_transport(units: list):
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ('TR', 'TA'):
            return 'YES'
            break 
        return 'NO'

def parse_pulsepoint(file_name:str, api_response:dict):

    utctz=pytz.timezone('UTC')

    col_names = {
            'Status_At_Scrape':[]
            ,'Scrape_Datetime':[]
            ,'Incident_ID':[]
            ,'Call_Received_Datetime':[]
            ,'Call_Closed_Datetime':[]
            ,'Latitude':[]
            ,'Longitude':[]
            ,'FullDisplayAddress':[]
            ,'Incident_Type':[]
            ,'Units':[]
            ,'Unit_Status_Transport':[]
    }
    ######## start: handle the scrape datetime disaster ######## 
    if 'scrape_datetime' in api_response.keys():
        try:
            scrape_datetime=datetime.datetime.fromisoformat(api_response['scrape_datetime'])
            if scrape_datetime.tzinfo is None:
                scrape_datetime=pytz.timezone('US/Eastern').localize(scrape_datetime)
                scrape_datetime=scrape_datetime.astimezone(utctz)
        except:
            try:
                scrape_datetime=datetime.datetime.strptime(api_response['scrape_datetime']+':00',"%Y-%m-%d %H:%M:%S%z")
                # print(scrape_datetime, 'time zone ',scrape_datetime.tzinfo)
            except:
                scrape_datetime=datetime.datetime.strptime(api_response['scrape_datetime']+'+00:00',"%Y:%m:%d:%H:%M:%S%z")
    elif file_name=='pulsepoint.json':
            scrape_datetime='2020-11-19 17:30:00+00:00'
    else:
        y=file_name[10:14]
        mo=file_name[15:17]
        d=file_name[18:20]
        h=file_name[21:23]
        m=file_name[24:26]
        s=file_name[27:29]
        offset='-05:00'
        tmp_dt = y+'-'+mo+'-'+d+' '+h+':'+m+':'+s+offset
        scrape_datetime=datetime.datetime.strptime(tmp_dt,"%Y-%m-%d %H:%M:%S%z")
        scrape_datetime=scrape_datetime.astimezone(utctz)

    ######## end: handle the scrape datetime disaster ######## 

    # get all record status types
    record_status_types = [i for i in api_response['incidents'].keys() if i != 'alerts']

    # parse into dataframe
    for status_type in record_status_types:
        if api_response['incidents'][status_type] is not None:
            records_to_load = [i for i in api_response['incidents'][status_type] if "IsShareable" in i.keys() if i["IsShareable"]=="1" if "PulsePointIncidentCallType" in i.keys() if i["PulsePointIncidentCallType"] in ["TC", "TCE"]]
            if records_to_load is not None and len(records_to_load)>0:
                for record in records_to_load:
                    col_names['Status_At_Scrape'].append(status_type)
                    col_names['Scrape_Datetime'].append(scrape_datetime)
                    col_names['Incident_ID'].append(record['ID'])
                    try:
                        col_names['Call_Received_Datetime'].append(record['CallReceivedDateTime'])
                    except KeyError:
                        col_names['Call_Received_Datetime'].append('')
                    try:
                        col_names['Call_Closed_Datetime'].append(record['ClosedDateTime'])
                    except KeyError:
                        col_names['Call_Closed_Datetime'].append('')
                    col_names['Latitude'].append(record['Latitude'])
                    col_names['Longitude'].append(record['Longitude'])
                    col_names['FullDisplayAddress'].append(record['FullDisplayAddress'])
                    col_names['Incident_Type'].append(record['PulsePointIncidentCallType'])
                    try:
                        col_names['Units'].append(record['Unit'])
                    except KeyError:
                        col_names['Units'].append([])
                    try:
                        col_names['Unit_Status_Transport'].append(unit_status_is_transport(record['Unit']))
                    except KeyError:
                        col_names['Unit_Status_Transport'].append('NO')

    df = pd.DataFrame.from_dict(col_names)

    # # download each dataset to local hard drive, and then upload it to the S3 bucket
    # # in csv format
    tmp_filename = Path(os.path.expanduser('~'), 'pulsepoint.csv')
    df.to_csv(tmp_filename, index=False, header=True, line_terminator='\n')
    data = open(tmp_filename, 'rb')
    s3_resource.Bucket(bucket_name).put_object(Key=destination+file_name.replace('.json', '')+'.csv', Body=data, Metadata =metadata)


for obj in bucket.objects.filter(Prefix='source-data/pulsepoint/unparsed/', Delimiter='/'):
    file_name = obj.key.replace(prefix,'')
    with open('parse_pulsepoint.json', 'wb') as f:
        client.download_fileobj('dc-crash-bot-test', obj.key, f)
    with open('parse_pulsepoint.json', 'r') as f2:    
        f3=json.load(f2)
    parse_pulsepoint(file_name, f3)