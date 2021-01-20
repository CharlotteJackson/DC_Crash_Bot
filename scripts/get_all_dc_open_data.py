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
import csv


# function definition
def get_dc_open_dataset(dataset:str, AWS_Credentials:dict, formats:list, input_urls=['all']):

    sys.path.append(os.path.expanduser('~'))

    s3 = boto3.resource('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
    client = boto3.client('s3'
        ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
        ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])    
    bucket_name = AWS_Credentials['s3_bucket']
    region=AWS_Credentials['region']

    # dict of datasets to load - listed alphabetically
    resources = {
        'acs_housing_2011_2015' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/7db7a4684b4c41268ca7a84d4170f2d0_34.geojson')]
            ,'prefix' :'source-data/dc-open-data/acs_housing_2011_2015/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'acs_housing_2011_2015', "dataset_info":"https://opendata.dc.gov/datasets/housing-acs-characteristics-2011-to-2015"}
        }
        ,'address_points' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/aa514416aaf74fdc94748f1e56e7cc8a_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/address_points/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'address_points', "dataset_info":"https://opendata.dc.gov/datasets/address-points"}
        }
        ,'all311' : {
            'url': [('2021','https://opendata.arcgis.com/datasets/ea485dbe52ca40aaad8c7660630ec9c6_12.geojson')
                    ,('2020','https://opendata.arcgis.com/datasets/82b33f4833284e07997da71d1ca7b1ba_11.geojson')
                    ,('2019','https://opendata.arcgis.com/datasets/98b7406def094fa59838f14beb1b8c81_10.geojson')
                    ,('2018','https://opendata.arcgis.com/datasets/2a46f1f1aad04940b83e75e744eb3b09_9.geojson')
                    ,('2017','https://opendata.arcgis.com/datasets/19905e2b0e1140ec9ce8437776feb595_8.geojson')
                    ,('2016','https://opendata.arcgis.com/datasets/0e4b7d3a83b94a178b3d1f015db901ee_7.geojson')
                    ,('2015','https://opendata.arcgis.com/datasets/b93ec7fc97734265a2da7da341f1bba2_6.geojson')
                    ,('last_30_days', 'https://opendata.arcgis.com/datasets/fec196030c0b4cd9901c71e579ec7831_41.geojson')
        ]
            ,'prefix' :'source-data/dc-open-data/all311/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'all311',"dataset_info":"https://opendata.dc.gov/datasets/311-city-service-requests-in-2020"}
            ,'filters':"['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']"
        }
        ,'anc_boundaries':{
            'url': [('complete','https://opendata.arcgis.com/datasets/fcfbf29074e549d8aff9b9c708179291_1.geojson')]
            ,'prefix' :'source-data/dc-open-data/anc_boundaries/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'anc_boundaries',"dataset_info":"https://opendata.dc.gov/datasets/advisory-neighborhood-commissions-from-2013"}
        }
        ,'census_blocks' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/a6f76663621548e1a039798784b64f10_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/census_blocks/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'census_blocks',"dataset_info":"https://opendata.dc.gov/datasets/census-blocks-2010"}
        }
        ,'charter_schools' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/a3832935b1d644e48c887e6ec5a65fcd_1.geojson')]
            ,'prefix' :'source-data/dc-open-data/charter_schools/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'charter_schools', "dataset_info":"https://opendata.dc.gov/datasets/charter-schools"}
        }
        ,'cityworks_service_requests' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/96bb7f56588c4d4595933c0ba772b3cb_1.geojson')]
            ,'prefix' :'source-data/dc-open-data/cityworks_service_requests/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'cityworks_service_requests', "dataset_info":"https://opendata.dc.gov/datasets/cityworks-service-requests"}
        }
        ,'cityworks_work_orders' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/a1dd480eb86445239c8129056ab05ade_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/cityworks_work_orders/'
            ,'metadata' :{'target_schema':'source_data','target_table': 'cityworks_work_orders', "dataset_info":"https://opendata.dc.gov/datasets/cityworks-workorders"}
        }
        ,'comp_plan_areas' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/203c2342b36240949e0ad95d75a5bdca_2.geojson')]
            ,'prefix' :'source-data/dc-open-data/comp_plan_areas/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'comp_plan_areas',"dataset_info":"https://opendata.dc.gov/datasets/comprehensive-plan-planning-areas"}
            
        }
        ,'crashes_raw' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/70392a096a8e431381f1f692aaa06afd_24.geojson')]
            ,'prefix' :'source-data/dc-open-data/crashes_raw/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'crashes_raw',"dataset_info":"https://opendata.dc.gov/datasets/crashes-in-dc"}
            ,'append':{'endpoint': 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Public_Safety_WebMercator/MapServer/24/query?'
                , 'filters': {'where':'REPORTDATE >= CURRENT_TIMESTAMP - INTERVAL \'7\' DAY','outFields':'*','outSR':'4326','returnGeometry':'true','f':'geojson'}
                ,'metadata' :{'target_schema':'tmp', 'target_table': 'crashes_raw',"dataset_info":"https://opendata.dc.gov/datasets/crashes-in-dc"}}
        }
        ,'crash_details' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/70248b73c20f46b0a5ee895fc91d6222_25.geojson')]
            ,'prefix' :'source-data/dc-open-data/crash_details/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'crash_details',"dataset_info":"https://opendata.dc.gov/datasets/crash-details-table"}
            
        }
        ,'intersection_points' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/96a9bbbb475648769e311d03c78698a7_2.geojson')]
            ,'prefix' :'source-data/dc-open-data/intersection_points/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'intersection_points',"dataset_info":"https://opendata.dc.gov/datasets/intersection-points"}
            
        }

        ,'moving_violations' : {
            'url': [ #2020
                    ('2020_11','https://opendata.arcgis.com/datasets/b3a187d6f91c41c38f4a1e24f9d2cdfc_10.geojson')
                    ,('2020_10','https://opendata.arcgis.com/datasets/47c555af573646358c27fcf6cd62be65_9.geojson')
                    ,('2020_09','https://opendata.arcgis.com/datasets/81b07a3eac6541bfb87d278f895bfdeb_8.geojson')
                    ,('2020_08','https://opendata.arcgis.com/datasets/f4e2f2013a584158a37243c1da982e88_7.geojson')
                    ,('2020_07','https://opendata.arcgis.com/datasets/15c0973e9f4041bfbf69a72026723f18_6.geojson')
                    ,('2020_06','https://opendata.arcgis.com/datasets/a9edb8e5728a4e07b6d6ce38dc5b013a_5.geojson')
                    ,('2020_05','https://opendata.arcgis.com/datasets/f7159e8119cd4d3ca73b4663f76ae838_4.geojson')
                    ,('2020_04','https://opendata.arcgis.com/datasets/186e3b6cf45f44b1ac0fe750518e3cab_3.geojson')
                    ,('2020_03','https://opendata.arcgis.com/datasets/6ceb38b8e24a464a94434c7d39934ebd_2.geojson')
                    ,('2020_02','https://opendata.arcgis.com/datasets/c3e91eed970149e6a41853ddadf36394_1.geojson')
                    ,('2020_01','https://opendata.arcgis.com/datasets/295f4842e345426bb066d9df233a5203_0.geojson')
                    #2019 - february missing from dc website
                    ,('2019_12','https://opendata.arcgis.com/datasets/a1f2142a687b425ab49f5ec016f99e24_11.geojson')
                    ,('2019_11','https://opendata.arcgis.com/datasets/b7b8bea9d9fd4939a0439c7475b8a508_10.geojson')
                    ,('2019_10','https://opendata.arcgis.com/datasets/4334882edf144a36a19361fe1601edf9_9.geojson')
                    ,('2019_09','https://opendata.arcgis.com/datasets/ce5cb88d3acb412bbeebe64650f1c0cd_8.geojson')
                    ,('2019_08','https://opendata.arcgis.com/datasets/3f1f51cf63d647018250360806700ad9_7.geojson')
                    ,('2019_07','https://opendata.arcgis.com/datasets/6e8387b60cb44adc8a3b917027bc164a_6.geojson')
                    ,('2019_06','https://opendata.arcgis.com/datasets/805a9187829a4840b1094098d5c2c4bb_5.geojson')
                    ,('2019_05','https://opendata.arcgis.com/datasets/bc4a0785f2f64b979f249b18c4f3fd29_4.geojson')
                    ,('2019_04','https://opendata.arcgis.com/datasets/878e5e25b4fe47bbbbd3a37c77285a63_3.geojson')
                    ,('2019_03','https://opendata.arcgis.com/datasets/0e38e123d4414d37905d0bd64af456ad_2.geojson')
                    ,('2019_01','https://opendata.arcgis.com/datasets/0d7b690c4e874e39a6f006cc61073561_0.geojson' )
                    #2018 - july missing from dc website
                    ,('2018_12','https://opendata.arcgis.com/datasets/1bf00863fedf4d09a236e4353dba1670_11.geojson')
                    ,('2018_11','https://opendata.arcgis.com/datasets/b3aa63faaeb243ea87eb58f2eb5a1931_10.geojson')
                    ,('2018_10','https://opendata.arcgis.com/datasets/a9f464dc91d9465394a90d44c61eb06a_10.geojson')
                    ,('2018_09','https://opendata.arcgis.com/datasets/03dfb384c57d40f8b05fa5936b23fb82_8.geojson')
                    ,('2018_08','https://opendata.arcgis.com/datasets/e2b5f90018c3487ea293f53edadbd69a_7.geojson')
                    ,('2018_06','https://opendata.arcgis.com/datasets/f6cced2fa2764599af101884d8e0ade0_5.geojson')
                    ,('2018_05','https://opendata.arcgis.com/datasets/87a2d2f6a8124b1e9d7406185d8fec80_4.geojson')
                    ,('2018_04','https://opendata.arcgis.com/datasets/e70527c48ac04bf8839fec666b328b24_3.geojson')
                    ,('2018_03','https://opendata.arcgis.com/datasets/3d56bc3d7d8046c081159e57ed338f29_2.geojson')
                    ,('2018_02','https://opendata.arcgis.com/datasets/f7fb9a35ff1c43239b071709ab597ff3_1.geojson')
                    ,('2018_01','https://opendata.arcgis.com/datasets/cac773db047e478195b4a534b878f6e3_0.geojson')
        ]
            ,'prefix' :'source-data/dc-open-data/moving_violations/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'moving_violations',"dataset_info":"https://opendata.dc.gov/datasets/moving-violations-issued-in-november-2020"}
        
        }
        ,"neighborhood_clusters": {
            'url':[('complete','https://opendata.arcgis.com/datasets/f6c703ebe2534fc3800609a07bad8f5b_17.geojson')]
            ,'prefix':'source-data/dc-open-data/neighborhood_clusters/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'neighborhood_clusters','dataset_info':'https://opendata.dc.gov/datasets/neighborhood-clusters'}
        }
        ,"public_schools":{
            'url':[('complete','https://opendata.arcgis.com/datasets/4ac321b2d409438ebd76a6569ad94034_5.geojson')]
            ,'prefix':'source-data/dc-open-data/public_schools/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'public_schools','dataset_info':'https://opendata.dc.gov/datasets/public-schools'}
        }
        ,"roadway_blocks": {
            'url':[('complete','https://opendata.arcgis.com/datasets/6fcba8618ae744949630da3ea12d90eb_163.geojson')]
            ,'prefix':'source-data/dc-open-data/roadway_blocks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_blocks','dataset_info':'https://opendata.dc.gov/datasets/roadway-block'}
        }
        ,"roadway_subblocks": {
            'url':[('complete','https://opendata.arcgis.com/datasets/df571ab7fea446e396bf2862d0ab6833_162.geojson')]
            ,'prefix':'source-data/dc-open-data/roadway_subblocks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_subblocks','dataset_info':'https://opendata.dc.gov/datasets/roadway-subblock'}
        }
        ,"roadway_blockface": {
            'url':[('complete','https://opendata.arcgis.com/datasets/47945b50c4f245b58850e81d297e90b9_164.geojson')]
            ,'prefix':'source-data/dc-open-data/roadway_blockface/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_blockface','dataset_info':'https://opendata.dc.gov/datasets/roadway-blockface'}
        }
        ,"roadway_intersection_approach": {
            'url':[('complete','https://opendata.arcgis.com/datasets/a779d051865f461eb2a1f50f10940ec4_161.geojson')]
            ,'prefix':'source-data/dc-open-data/roadway_intersection_approach/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'roadway_intersection_approach','dataset_info':'https://opendata.dc.gov/datasets/roadway-intersection-approach'}
        }
        ,"smd_boundaries": {
            'url':[('complete','https://opendata.arcgis.com/datasets/890415458c4c40c3ada2a3c48e3d9e59_21.geojson')]
            ,'prefix':'source-data/dc-open-data/smd_boundaries/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'smd_boundaries','dataset_info':'https://opendata.dc.gov/datasets/single-member-district-from-2013'}
        }
        ,'vision_zero' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/3f28bc3ad77f49079efee0ac05d8464c_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/vision_zero/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'vision_zero',"dataset_info":"https://opendata.dc.gov/datasets/vision-zero-safety"}
            ,'append':{'endpoint': 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DDOT/VisionZero/FeatureServer/0/query?'
                , 'filters': {'where':'REQUESTDATE >= CURRENT_TIMESTAMP - INTERVAL \'1\' DAY','outFields':'*','outSR':'4326','returnGeometry':'true','f':'geojson'}}
        }
        ,"ward_boundaries": {
            'url':[('complete','https://opendata.arcgis.com/datasets/0ef47379cbae44e88267c01eaec2ff6e_31.geojson')]
            ,'prefix':'source-data/dc-open-data/ward_boundaries/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'ward_boundaries','dataset_info':'https://opendata.dc.gov/datasets/ward-from-2012'}
        }
    }

    
    # then load each dataset into memory, save it to disk, upload it to S3
    gdf = gpd.GeoDataFrame() 
    if len(input_urls)==1 and input_urls[0]=='all':
        urls_to_load = [(label, url) for (label,url) in resources[dataset]['url']]
    else:
        urls_to_load = [(label, url) for (label,url) in resources[dataset]['url'] if label in input_urls or input_urls[0] in label]
    for (label, url) in urls_to_load:
            try:
                gdf=gpd.read_file(url)
            except:
                print("file ",label, "could not be read")
                continue
            # if 'filters' in resources[dataset].keys():
            #     gdf=gdf[gdf['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']
            # 'offset' is apparently a reserved word in Postgres, so rename any columns with that label
            if 'OFFSET' in gdf.columns:
                gdf.rename(columns={"OFFSET": "_OFFSET"})
            # TODO: fix this hack 
            if dataset == 'all311' and label == '2018':
                print("currently dataframe has ",len(gdf)," rows")
                gdf =  gdf[~gdf["DETAILS"].str.contains(" per N. Williams on 7/27/2018. Closed by N. Whiteman on 7/30/2018.", na=False)]
                print("after removing the one record, gdf has ",len(gdf), " rows")
            # set S3 dataset name
            s3_dataset_name=dataset+'_'+label
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


# set up ability to call with lists from the command line as follows:
# python get_all_dc_open_data.py --dataset all311 --urls "2020" "2019" "2017" --formats csv geojson 
CLI=argparse.ArgumentParser()
CLI.add_argument(
"--dataset",   
type=str,
default='crashes_raw'
)
CLI.add_argument(
"--urls",  
nargs="*",  
type=str,
default='all' 
)
CLI.add_argument(
"--formats",
nargs="*",
type=str, 
default=['csv'], # default is to only load csvs
)

# parse the command line
args = CLI.parse_args()
dataset = args.dataset
formats = args.formats
urls=args.urls
print(dataset," ",urls)

# call function with command line arguments
get_dc_open_dataset(dataset=dataset, AWS_Credentials=get_connection_strings("AWS_DEV"), formats=formats, input_urls = urls)

    # if mode == 'append':

    #     current_time = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
    #     # if mode is append and dataset is not appendable, skip it
    #     if 'append' not in resources[dataset].keys():
    #         print('dataset {} not appendable'.format(dataset))
    #         exit
    #     gdf = gpd.GeoDataFrame() 
    #     # ping the endpoint
    #     response = requests.get(resources[dataset]['append']['endpoint'], params=resources[dataset]['append']['filters'])
    #     data=json.loads(response.text)
    #     tmpfile=Path(os.path.expanduser('~'), 'dc_open_data.json')
    #     with open(tmpfile, 'w+') as outfile:
    #         json.dump(data,outfile,indent=4)
    #     gdf=gpd.read_file(tmpfile,driver='GeoJSON')
    #     # 'offset' is apparently a reserved word in Postgres, so rename any columns with that label
    #     if 'OFFSET' in gdf.columns:
    #         gdf.rename(columns={"OFFSET": "_OFFSET"})
    #     # convert the date fields from miliseconds to datetime
    #     if 'REPORTDATE'in gdf.columns:
    #         gdf['REPORTDATE']=gdf['REPORTDATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00"))
    #     if 'FROMDATE'in gdf.columns:
    #         gdf['FROMDATE']=gdf['FROMDATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00"))
    #     if 'TODATE'in gdf.columns:
    #         gdf['TODATE']=gdf['TODATE'].apply(lambda x: '' if x is None else datetime.datetime.fromtimestamp(x/1e3,tz = timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00"))
    #     # set S3 dataset name
    #     s3_dataset_name = dataset+current_time
    #     # download each dataset to local hard drive, and then upload it to the S3 bucket
    #     if 'csv' in formats:
    #         filename = Path(os.path.expanduser('~'), dataset+'.csv')
    #         gdf.to_csv(filename, index=False, header=True, line_terminator='\n')
    #         data = open(filename, 'rb')
    #         s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.csv', Body=data, Metadata =resources[dataset]['append']['metadata'])
    #     # in geojson format
    #     if 'geojson' in formats:
    #         filename = Path(os.path.expanduser('~'), dataset+'.geojson')
    #         gdf.to_file(filename, driver='GeoJSON')
    #         data = open(filename, 'rb')
    #         s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+s3_dataset_name+'.geojson', Body=data, Metadata =resources[dataset]['append']['metadata'])