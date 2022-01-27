import boto3
from connect_to_rds import get_connection_strings
import argparse 
import requests
import json
import pprint
import re
from tqdm import tqdm

AWS_Credentials = get_connection_strings('AWS_DEV')

s3 = boto3.resource('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
client = boto3.client('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])    
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']

resources = {
    'all311' : {
            'urls': [
                # 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                # ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/12/query?where=1%3D1&outFields=*&outSR=4326&f=json'
                ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/ServiceRequests/MapServer/14/query?where=1%3D1&outFields=*&outSR=4326&f=json'

            ]
            ,'prefix' :'source-data/dc-open-data/all311/'
        }
    ,'camera_locations' : {
            'urls': ['https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Safety_WebMercator/MapServer/16/query?where=1%3D1&outFields=*&outSR=4326&f=json']
            ,'prefix' :'source-data/dc-open-data/camera_locations/'
        }
    ,'crashes_raw' : {
            'urls': ['https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Safety_WebMercator/MapServer/24/query?where=1%3D1&outFields=*&outSR=4326&f=json']
            ,'prefix' :'source-data/dc-open-data/crashes_raw/'
        }
    ,'crash_details' : {
            'urls': ['https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Safety_WebMercator/MapServer/25/query?where=1%3D1&outFields=*&outSR=4326&f=json']
            ,'prefix' :'source-data/dc-open-data/crash_details/'
        }
    ,'moving_violations_2015':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2015/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2016':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2016/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2017':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2017/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2018':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2018/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2019':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2019/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2020':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2020/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,'moving_violations_2021':{
        'urls':[
            'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/2/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/3/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/4/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/5/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/6/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/7/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/8/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/9/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/10/query?where=1%3D1&outFields=*&outSR=4326&f=json'
            ,'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Violations_Moving_2021/MapServer/11/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        ]
        ,'prefix':'source-data/dc-open-data/moving_violations/'
    }
    ,"roadway_blocks": {
            'urls':[]
            ,'prefix':'source-data/dc-open-data/roadway_blocks/'
        }
        ,"roadway_subblocks": {
            'url':[]
            ,'prefix':'source-data/dc-open-data/roadway_subblocks/'
               }
        ,"roadway_blockface": {
            'url':[]
            ,'prefix':'source-data/dc-open-data/roadway_blockface/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_blockface','dataset_info':'https://opendata.dc.gov/datasets/roadway-blockface'}
        }
        ,"roadway_intersection_approach": {
            'url':[('complete','https://opendata.arcgis.com/datasets/a779d051865f461eb2a1f50f10940ec4_161.geojson')]
            ,'prefix':'source-data/dc-open-data/roadway_intersection_approach/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_intersection_approach','dataset_info':'https://opendata.dc.gov/datasets/roadway-intersection-approach'}
        }
        ,"sidewalks": {
            'url':[('complete','https://opendata.arcgis.com/datasets/2347fa1f3fd9412dbf11aa6441ddca8b_83.geojson')]
            ,'prefix':'source-data/dc-open-data/sidewalks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'sidewalks','dataset_info':'https://opendata.dc.gov/datasets/sidewalks/'}
        }
}

def main(dataset:str):

    urls = resources[dataset]['urls']
    prefix = resources[dataset]['prefix']

    for url in urls:
        if len(urls)>1:
            dataset_id = '_'+re.search(r'(?<=(MapServer\/)).*(?=\/)', url)[0]
        else:
            dataset_id = ''

        # first set the offset to 0, and get the total record count
        resultOffset = 0
        response = requests.get(url,params={'returnCountOnly':'true'})
        total_record_count = int(json.loads(response.text)['count'])

        # create an empty list to append records to:
        data = []
        with tqdm(total=total_record_count) as pbar: 
                
            while resultOffset < total_record_count:
                # get the data
                response=requests.get(url,params={'resultOffset':resultOffset})

                json_response=json.loads(response.text)

                # add the geometry to the attributions dictionary
                # also remove the stupid f**ing OBJECTID
                try:
                    for feature in json_response['features']:
                        feature['attributes'].pop('OBJECTID')
                        try:
                            feature['attributes'].update(feature['geometry'])
                        except KeyError:
                            continue
                    
                    # make a list of records
                    loop_data = [feature['attributes'] for feature in json_response['features']]
                    records_in_loop = len(loop_data)
                    data+=loop_data

                    # increment the resultOffset by the length returned 
                    resultOffset+=records_in_loop
                    print(resultOffset," records fetched out of ",total_record_count)
                    pbar.update(records_in_loop)
                except KeyError:
                    print('no features, only keys: ',json_response.keys())
                    continue

        # sort the list of dicts
        # data = sorted(data, key = lambda i: i['CRIMEID'])

        # preserve today's file for posterity
        # v1 and v2 should be the same!
        # with open('crashes_raw_2022_01_07_v2.json', 'w', encoding='utf-8') as f:
        #    json.dump(data, f, ensure_ascii=False, indent=4)

        # # create file key variable
        s3_key = prefix+dataset+dataset_id+'.json'
        # # upload to S3 so we have the raw data for every pull
        upload = json.dumps(data)

        s3.Bucket(bucket_name).put_object(Key=s3_key, Body=upload)

if __name__ == "__main__":
    main('camera_locations')
    # main('crashes_raw')
    # main('crash_details')
    # main('all311')
    # main('moving_violations_2015')
    # main('moving_violations_2016')
    # main('moving_violations_2017')
    # main('moving_violations_2018')
    # main('moving_violations_2019')
    # main('moving_violations_2020')
    # main('moving_violations_2021')

# CLI=argparse.ArgumentParser()
# CLI.add_argument(
# "--dataset",   
# type=str,
# default='crashes_raw'
# )
# CLI.add_argument(
# "--urls",  
# nargs="*",  
# type=str,
# default='all' 
# )
# CLI.add_argument(
# "--formats",
# nargs="*",
# type=str, 
# default=['csv'], # default is to only load csvs
# )

# # parse the command line
# args = CLI.parse_args()
# dataset = args.dataset
# formats = args.formats
# urls=args.urls
# print(dataset," ",urls)

# # call function with command line arguments
# get_dc_open_dataset(dataset=dataset, AWS_Credentials=get_connection_strings("AWS_DEV"), formats=formats, input_urls = urls)