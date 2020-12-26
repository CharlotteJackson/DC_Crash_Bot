import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import argparse 
import datetime
from datetime import timezone
import requests
import json
import sys


# function definition
def get_dc_open_dataset(dataset:str, AWS_Credentials:dict, formats:list, mode:str):

    sys.path.append(os.path.expanduser('~'))

    s3 = boto3.resource('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
    client = boto3.client('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])    
    bucket_name = AWS_Credentials['s3_bucket']
    region=AWS_Credentials['region']

    # dict of datasets to load
    resources = {
        'crashes_raw' : {
            'url': ['https://opendata.arcgis.com/datasets/70392a096a8e431381f1f692aaa06afd_24.geojson']
            ,'prefix' :'source-data/dc-open-data/crashes_raw/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'crashes_raw',"dataset_info":"https://opendata.dc.gov/datasets/crashes-in-dc"}
            ,'append':{'endpoint': 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Safety_WebMercator/MapServer/24/query?'
                , 'filters': {'where':'REPORTDATE >= CURRENT_TIMESTAMP - INTERVAL \'7\' DAY','outFields':'*','outSR':'4326','returnGeometry':'true','f':'geojson'}
                ,'metadata' :{'target_schema':'tmp', 'target_table': 'crashes_raw',"dataset_info":"https://opendata.dc.gov/datasets/crashes-in-dc"}}
        }
        ,'crash_details' : {
            'url': ['https://opendata.arcgis.com/datasets/70248b73c20f46b0a5ee895fc91d6222_25.geojson']
            ,'prefix' :'source-data/dc-open-data/crash_details/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'crash_details',"dataset_info":"https://opendata.dc.gov/datasets/crash-details-table"}
            
        }
        ,'census_blocks' : {
            'url': ['https://opendata.arcgis.com/datasets/a6f76663621548e1a039798784b64f10_0.geojson']
            ,'prefix' :'source-data/dc-open-data/census_blocks/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'census_blocks',"dataset_info":"https://opendata.dc.gov/datasets/census-blocks-2010"}
        }
    ,'vision_zero' : {
            'url': ['https://opendata.arcgis.com/datasets/3f28bc3ad77f49079efee0ac05d8464c_0.geojson']
            ,'prefix' :'source-data/dc-open-data/vision_zero/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'vision_zero',"dataset_info":"https://opendata.dc.gov/datasets/vision-zero-safety"}
            ,'append':{'endpoint': 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DDOT/VisionZero/FeatureServer/0/query?'
                , 'filters': {'where':'REQUESTDATE >= CURRENT_TIMESTAMP - INTERVAL \'1\' DAY','outFields':'*','outSR':'4326','returnGeometry':'true','f':'geojson'}}
        }
    ,'address_points' : {
            'url': ['https://opendata.arcgis.com/datasets/aa514416aaf74fdc94748f1e56e7cc8a_0.geojson']
            ,'prefix' :'source-data/dc-open-data/address_points/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'address_points', "dataset_info":"https://opendata.dc.gov/datasets/address-points"}
        }
    ,'all311' : {
            'url': ['https://opendata.arcgis.com/datasets/82b33f4833284e07997da71d1ca7b1ba_11.geojson'
                    ,'https://opendata.arcgis.com/datasets/98b7406def094fa59838f14beb1b8c81_10.geojson'
                    ,'https://opendata.arcgis.com/datasets/2a46f1f1aad04940b83e75e744eb3b09_9.geojson'
                    ,'https://opendata.arcgis.com/datasets/19905e2b0e1140ec9ce8437776feb595_8.geojson'
                    ,'https://opendata.arcgis.com/datasets/0e4b7d3a83b94a178b3d1f015db901ee_7.geojson'
                    ,'https://opendata.arcgis.com/datasets/b93ec7fc97734265a2da7da341f1bba2_6.geojson'
        ]
            ,'prefix' :'source-data/dc-open-data/all311/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'all311',"dataset_info":"https://opendata.dc.gov/datasets/311-city-service-requests-in-2020"}
            ,'append':{'endpoint': 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_APPS/SR_30days_Open/MapServer/0/'
                , 'filters': {'where':'ADDATE >= CURRENT_TIMESTAMP - INTERVAL \'1\' DAY','outFields':'*','outSR':'4326','returnGeometry':'true','f':'json'}}
            ,'filters':"['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']"
        }
        ,"roadway_blocks": {
            'url':['https://opendata.arcgis.com/datasets/6fcba8618ae744949630da3ea12d90eb_163.geojson']
            ,'prefix':'source-data/dc-open-data/roadway_blocks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_blocks','dataset_info':'https://opendata.dc.gov/datasets/roadway-block'}
        }
        ,"roadway_subblocks": {
            'url':['https://opendata.arcgis.com/datasets/df571ab7fea446e396bf2862d0ab6833_162.geojson']
            ,'prefix':'source-data/dc-open-data/roadway_subblocks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_subblocks','dataset_info':'https://opendata.dc.gov/datasets/roadway-subblock'}
        }
        ,"roadway_blockface": {
            'url':['https://opendata.arcgis.com/datasets/47945b50c4f245b58850e81d297e90b9_164.geojson']
            ,'prefix':'source-data/dc-open-data/roadway_blockface/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_blockface','dataset_info':'https://opendata.dc.gov/datasets/roadway-blockface'}
        }
        ,"roadway_intersection_approach": {
            'url':['https://opendata.arcgis.com/datasets/a779d051865f461eb2a1f50f10940ec4_161.geojson']
            ,'prefix':'source-data/dc-open-data/roadway_intersection_approach/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_intersection_approach','dataset_info':'https://opendata.dc.gov/datasets/roadway-intersection-approach'}
        }
    }

    if mode == 'replace':
        # first clean out S3 bucket
        objects_to_delete = [{'Key': obj.Object().key} for obj in s3.Bucket(bucket_name).objects.filter(Prefix=resources[dataset]['prefix']) if obj.Object().key != resources[dataset]['prefix']]
        if len(objects_to_delete)>0:
            client.delete_objects(Bucket=bucket_name,Delete={'Objects': objects_to_delete})
       
        # then load each dataset into memory, save it to disk, upload it to S3
        gdf = gpd.GeoDataFrame() 
        for i in resources[dataset]['url']:
            url = i
            gdf=gpd.read_file(url)
            if 'filters' in resources[dataset].keys():
                gdf=gdf[gdf['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']
            # 'offset' is apparently a reserved word in Postgres, so rename any columns with that label
            if 'OFFSET' in gdf.columns:
                gdf.rename(columns={"OFFSET": "_OFFSET"})
            # set S3 dataset name
            s3_dataset_name=dataset+'_'+str(resources[dataset]['url'].index(i))
            # download each dataset to local hard drive, and then upload it to the S3 bucket
            # in csv format
            if 'csv' in formats:
                filename = Path(os.path.expanduser('~'), dataset+'.csv')
                gdf.to_csv(filename, index=False, header=True, line_terminator='\n')
                data = open(filename, 'rb')
                s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.csv', Body=data, Metadata =resources[dataset]['metadata'])
            # in geojson format
            if 'geojson' in formats:
                filename = Path(os.path.expanduser('~'), dataset+'.geojson')
                gdf.to_file(filename, driver='GeoJSON')
                data = open(filename, 'rb')
                s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.geojson', Body=data, Metadata =resources[dataset]['metadata'])

    if mode == 'append':

        current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
        # if mode is append and dataset is not appendable, skip it
        if 'append' not in resources[dataset].keys():
            print('dataset {} not appendable'.format(dataset))
            exit
        gdf = gpd.GeoDataFrame() 
        # ping the endpoint
        response = requests.get(resources[dataset]['append']['endpoint'], params=resources[dataset]['append']['filters'])
        data=json.loads(response.text)
        tmpfile=Path(os.path.expanduser('~'), 'dc_open_data.json')
        with open(tmpfile, 'w+') as outfile:
            json.dump(data,outfile,indent=4)
        gdf=gpd.read_file(tmpfile,driver='GeoJSON')
        # 'offset' is apparently a reserved word in Postgres, so rename any columns with that label
        if 'OFFSET' in gdf.columns:
            gdf.rename(columns={"OFFSET": "_OFFSET"})
        # convert the date fields from miliseconds to datetime
        if 'REPORTDATE'in gdf.columns:
            gdf['REPORTDATE']=gdf['REPORTDATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"))
        if 'FROMDATE'in gdf.columns:
            gdf['FROMDATE']=gdf['FROMDATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"))
        if 'TODATE'in gdf.columns:
            gdf['TODATE']=gdf['TODATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"))
        # set S3 dataset name
        s3_dataset_name = dataset+current_time
        # download each dataset to local hard drive, and then upload it to the S3 bucket
        if 'csv' in formats:
            filename = Path(os.path.expanduser('~'), dataset+'.csv')
            gdf.to_csv(filename, index=False, header=True, line_terminator='\n')
            data = open(filename, 'rb')
            s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.csv', Body=data, Metadata =resources[dataset]['append']['metadata'])
        # in geojson format
        if 'geojson' in formats:
            filename = Path(os.path.expanduser('~'), dataset+'.geojson')
            gdf.to_file(filename, driver='GeoJSON')
            data = open(filename, 'rb')
            s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.geojson', Body=data, Metadata =resources[dataset]['append']['metadata'])


# set up ability to call with lists from the command line as follows:
# python get_all_dc_open_data.py --datasets crashes_raw crash_details vision_zero all311 --formats csv geojson --mode replace
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--datasets",  
nargs="*",  
type=str,
default=['crashes_raw','crash_details','census_blocks','address_points','all311','vision_zero'
,'roadway_blocks','roadway_subblocks','roadway_blockface','roadway_intersection_approach'],  # default - load everything
)
CLI.add_argument(
"--formats",
nargs="*",
type=str, 
default=['csv'], # default is to only load csvs
)
CLI.add_argument(
"--mode",
nargs="*",
type=str, 
default=['append'], # default is to append new records instead of dropping and reloading everything
)

# parse the command line
args = CLI.parse_args()
resources_to_load = args.datasets
formats = args.formats
mode=args.mode[0]

# call function with command line arguments
for dataset in resources_to_load:
    get_dc_open_dataset(dataset=dataset, AWS_Credentials=get_connection_strings("AWS_DEV"), formats=formats, mode=mode)
