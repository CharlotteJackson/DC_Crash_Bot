import json
import pulse


import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import json
from pulse import get_data
import datetime
from datetime import timezone

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
prefix = 'source-data/pulsepoint/unparsed/'
metadata = {'target_schema':'tmp', "dataset_info":"https://docs.google.com/document/pub?id=1qMdahl1E9eE4Rox52bmTA2BliR1ve1rjTYAbhtMeinI#id.q4mai5x52vi6"}
dataset = 'pulsepoint'
current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")


def main():

    data = pulse.get_data()

    data['scrape_datetime']=current_time

    # dump it to file and upload to AWS so we have the raw data for every pull
    with open('pulsepoint_data.json', 'w+') as outfile:
        json.dump(data, outfile, indent=4)
    upload = open('pulsepoint_data.json', 'rb')
    s3_resource.Bucket(bucket_name).put_object(Key=prefix+dataset+current_time+'.json', Body=upload, Metadata =metadata)

if __name__ == "__main__":
    main()
