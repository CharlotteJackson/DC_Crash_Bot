import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import json
import datetime
from datetime import timezone 
import pytz
import argparse

# set up S3 connection
AWS_Credentials=get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
metadata = {'target_schema':'source_data', 'target_table':'pulsepoint_stream',"dataset_info":"https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6"}
bucket = s3_resource.Bucket(bucket_name)
client=boto3.client('s3',aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])


# define function to get flag whether any responding units have a status of transport or transport arrived
def unit_status_is_transport(units: list):
    num_transport_units=0
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ['TR', 'TA'] :
            num_transport_units+=1
    return num_transport_units

def transport_unit_is_amr(units: list):
    num_amr_units=0
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ['TR', 'TA'] and 'AMR' in unit["UnitID"]:
            num_amr_units+=1
    return num_amr_units

def transport_unit_is_non_amr(units: list):
    num_non_amr_units=0
    for unit in units:
        if unit['PulsePointDispatchStatus'] in ['TR', 'TA'] and 'AMR' not in unit["UnitID"]:
            num_non_amr_units+=1
    return num_non_amr_units

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
            ,'Transport_Unit_Is_AMR':[]
            ,'Transport_Unit_Is_Non_AMR':[]
            ,'Agency_ID':[]
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

    # set agency id
    try:
        Agency_ID = api_response['Agency_ID']
    except:
        Agency_ID = 'EMS1205'
    # parse into dataframe
    for status_type in record_status_types:
        if api_response['incidents'][status_type] is not None:
            records_to_load = [i for i in api_response['incidents'][status_type]]
            if records_to_load is not None and len(records_to_load)>0:
                for record in records_to_load:
                    col_names['Agency_ID'].append(Agency_ID)
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
                        # print("transport status key error for record ",record['ID'])
                        col_names['Unit_Status_Transport'].append(0)
                    try:
                        col_names['Transport_Unit_Is_AMR'].append(transport_unit_is_amr(record['Unit']))
                    except KeyError:
                        col_names['Transport_Unit_Is_AMR'].append(0)
                    try:
                        col_names['Transport_Unit_Is_Non_AMR'].append(transport_unit_is_non_amr(record['Unit']))
                    except KeyError:
                        col_names['Transport_Unit_Is_Non_AMR'].append(0)

    df = pd.DataFrame.from_dict(col_names)

    return df

# set up ability to call with lists from the command line as follows:
# python parse_pulsepoint.py --items_to_parse source-data/pulsepoint/unparsed/ --move_to_folder source-data/pulsepoint/unparsed/converted/ --parsed_destination source-data/pulsepoint/
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--items_to_parse",  
nargs="*",  
type=str,
default=['source-data/pulsepoint/unparsed/'],  # default - parse everything in the unparsed folder
)
CLI.add_argument(
"--move_after_parsing",
type=str, 
default='no', 
)

CLI.add_argument(
"--move_to_folder",
type=str, 
default='source-data/pulsepoint/unparsed/converted/', 
)
CLI.add_argument(
"--parsed_destination",
type=str, 
default='source-data/pulsepoint/', 
)

# parse the command line
args = CLI.parse_args()
items_to_parse = args.items_to_parse
move_to_folder = args.move_to_folder
parsed_destination=args.parsed_destination
move_after_parsing=args.move_after_parsing

# call function with command line arguments
for item in items_to_parse:
    files_to_parse = [obj.key for obj in bucket.objects.filter(Prefix=item, Delimiter='/') if '.json' in obj.key]
    for file in files_to_parse:
        file_name = os.path.basename(file) 
        # load the json into memory
        f = client.get_object(Bucket = bucket_name, Key=file)
        # decode it as string 
        f2 = f['Body'].read().decode('utf-8')
        # load back into dictionary format 
        f3 = json.loads(f2)
        try:
            # parse the file into csv format
            df = parse_pulsepoint(file_name, f3)
            # # download each dataset to local hard drive, and then upload it to the S3 bucket
            # # in csv format
            tmp_filename = Path(os.path.expanduser('~'), 'pulsepoint.csv')
            df.to_csv(tmp_filename, index=False, header=True, line_terminator='\n')
            data = open(tmp_filename, 'rb')
            bucket.put_object(Key=parsed_destination+file_name.replace('.json', '')+'.csv', Body=data, Metadata =metadata)
            if move_after_parsing.lower() == "yes" and move_to_folder != "":
                try:
                    # move it into the converted folder
                    s3_resource.Object(bucket_name,move_to_folder+file_name).copy_from(CopySource = {'Bucket': bucket_name, 'Key': file})
                    s3_resource.Object(bucket_name, file).delete()
                except:
                    print(file," could not be copied and/or deleted")
                    continue 
        except:
            print(file, " could not be parsed")
            continue 
