import json
import pulse
import boto3
import os
from connect_to_rds import get_connection_strings
import datetime
from datetime import timezone
import requests
import pprint 

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region= AWS_Credentials['region']
prefix = 'source-data/citizen/unparsed/'
metadata = {'target_schema':'tmp'}
dataset = 'citizen'

def main():

    url = "https://citizen.com/api/incident/trending?lowerLatitude=38.791&lowerLongitude=-77.175&upperLatitude=38.9984&upperLongitude=-76.9078&fullResponse=true&limit=500"

    current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")

    response=requests.get(url)

    json_response=json.loads(response.text)

    # limit to just traffic incidents
    json_response['results'] = [result for result in json_response['results']  if 'categories' in result.keys() if 'Traffic Related' in result['categories']]

    json_response['scrape_datetime']=current_time

    # create file key variable
    s3_key = prefix+dataset+current_time+'.json'
    # upload to S3 so we have the raw data for every pull
    upload = json.dumps(json_response)

    s3_resource.Bucket(bucket_name).put_object(Key=s3_key, Body=upload, Metadata =metadata)

if __name__ == "__main__":
    main()

