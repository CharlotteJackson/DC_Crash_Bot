import json
import boto3
import os
from connect_to_rds import get_connection_strings
import datetime
from datetime import timezone
import requests
import pprint 
import math

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region= AWS_Credentials['region']
prefix = 'source-data/waze/unparsed/'
metadata = {'target_schema':'tmp'}
dataset = 'waze'

def main(lat_min:float, long_min:float,lat_max:float, long_max:float, num_squares:int, users:int, jams:int,alerts:int):

    types = 'alerts%2Ctraffic%2Cusers'

    num_divisions = float(math.sqrt(num_squares))
    lat_delta = (lat_max - lat_min)/num_divisions
    long_delta = (long_max - long_min)/num_divisions
    all_squares_list = []
    while lat_min < lat_max:
        loop_long_min = long_min
        while loop_long_min <long_max:
            square_coords = {}
            square_coords['bottom'] = lat_min
            square_coords['top'] = lat_min + lat_delta
            square_coords['left'] = loop_long_min
            square_coords['right'] = loop_long_min + long_delta
            loop_long_min+=long_delta
            all_squares_list.append(square_coords)
        lat_min+=lat_delta

    for square in all_squares_list:
        bottom = square['bottom']
        left = square['left']
        top = square['top']
        right = square['right']

        # f strings
        url = f"https://www.waze.com/rtserver/web/TGeoRSS?bottom={bottom}&left={left}&ma={alerts}&mj={jams}&mu={users}&right={right}&top={top}"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        json_response = json.loads(response.text)

        current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")

        json_response['scrape_datetime']=current_time

        # create file key variable
        s3_key = prefix+dataset+current_time+'_'+str(all_squares_list.index(square))+'.json'
        # upload to S3 so we have the raw data for every pull
        upload = json.dumps(json_response)

        s3_resource.Bucket(bucket_name).put_object(Key=s3_key, Body=upload, Metadata =metadata)

if __name__ == "__main__":
    main(lat_min=38.828109, long_min=-77.119350, lat_max = 38.994398, long_max =-76.896663, num_squares=36, alerts=0, jams=0, users=400)
    main(lat_min=38.828109, long_min=-77.119350, lat_max = 38.994398, long_max =-76.896663, num_squares=4, alerts=800, jams=200, users=0)

