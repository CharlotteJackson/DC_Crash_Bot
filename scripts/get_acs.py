import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import datetime
from datetime import timezone
import requests
import json
import sys
import csv


Census_API_Key = get_connection_strings('CENSUS')['API_Key']

AWS_Credentials = get_connection_strings('AWS_DEV')

sys.path.append(os.path.expanduser('~'))

s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
client = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])    
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']

census_endpoint = "https://api.census.gov/data/2019/acs/acs5"

metadata = {'target_schema': 'source_data', 'target_table':'acs_2019_by_tract'}
states = {'11':'DC','51':'VA','24':'MD'}
variables='NAME,B01003_001E,B08201_001E,B08201_002E,B08201_003E,B08201_004E,B08201_005E,B08201_006E'

for state_code, state_abbrev in states.items():

    census_params={"get":variables
                ,"for":"tract:*"
                ,"in":"state:{}".format(state_code)
                ,"key":Census_API_Key
                }
    response = requests.get(census_endpoint, params = census_params)

    filename = Path(os.path.expanduser('~'), 'acs_2019.csv')
    with open(filename, "w") as output:
        for line in response.text.split('\n'):
            output.write(line.replace("[","").replace("],","") + '\n')
    data = open(filename, 'rb')
    s3.Bucket(bucket_name).put_object(Key='source-data/census/acs/acs2019_{}.csv'.format(state_abbrev), Body=data, Metadata =metadata)

