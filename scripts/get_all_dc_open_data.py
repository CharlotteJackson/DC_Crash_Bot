import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings, create_postgres_engine

# set up S3 connection
AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
engine = create_postgres_engine("AWS_PostGIS", "postgres", "DEV")

resources = {
    'crashes_raw' : {
        'url': 'https://opendata.arcgis.com/datasets/70392a096a8e431381f1f692aaa06afd_24.geojson'
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/crashes-in-dc"}
    }
    ,'crash_details' : {
        'url': 'https://opendata.arcgis.com/datasets/70248b73c20f46b0a5ee895fc91d6222_25.geojson'
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/crash-details-table"}
    }
    ,'census_blocks' : {
        'url': 'https://opendata.arcgis.com/datasets/a6f76663621548e1a039798784b64f10_0.geojson'
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/census-blocks-2010"}
    }
,'vision_zero' : {
        'url': 'https://opendata.arcgis.com/datasets/3f28bc3ad77f49079efee0ac05d8464c_0.geojson'
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/vision-zero-safety"}
    }
,'address_points' : {
        'url': 'https://opendata.arcgis.com/datasets/aa514416aaf74fdc94748f1e56e7cc8a_0.geojson'
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/address-points"}
    }
,'all311' : {
        'url': ['https://opendata.arcgis.com/datasets/82b33f4833284e07997da71d1ca7b1ba_11.geojson'
                ,'https://opendata.arcgis.com/datasets/98b7406def094fa59838f14beb1b8c81_10.geojson'
                ,'https://opendata.arcgis.com/datasets/2a46f1f1aad04940b83e75e744eb3b09_9.geojson'
                ,'https://opendata.arcgis.com/datasets/19905e2b0e1140ec9ce8437776feb595_8.geojson'
                ,'https://opendata.arcgis.com/datasets/0e4b7d3a83b94a178b3d1f015db901ee_7.geojson'
                ,'https://opendata.arcgis.com/datasets/b93ec7fc97734265a2da7da341f1bba2_6.geojson'
    ]
        ,'prefix' :'source-data/dc-open-data/'
        ,'metadata' :{'target_schema':'source_data', "dataset_info":"https://opendata.dc.gov/datasets/311-city-service-requests-in-2020"}
        ,'filters':"['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']"
    }
    ,"roadway_blocks": {
        'url':'https://opendata.arcgis.com/datasets/6fcba8618ae744949630da3ea12d90eb_163.geojson'
        ,'prefix':'source-data/dc-open-data/'
        ,'metadata':{'target_schema':'source_data', 'dataset_info':'https://opendata.dc.gov/datasets/roadway-block'}
    }
    ,"roadway_subblocks": {
        'url':'https://opendata.arcgis.com/datasets/df571ab7fea446e396bf2862d0ab6833_162.geojson'
        ,'prefix':'source-data/dc-open-data/'
        ,'metadata':{'target_schema':'source_data', 'dataset_info':'https://opendata.dc.gov/datasets/roadway-subblock'}
    }
    ,"roadway_blockface": {
        'url':'https://opendata.arcgis.com/datasets/47945b50c4f245b58850e81d297e90b9_164.geojson'
        ,'prefix':'source-data/dc-open-data/'
        ,'metadata':{'target_schema':'source_data', 'dataset_info':'https://opendata.dc.gov/datasets/roadway-blockface'}
    }
    ,"roadway_intersection_approach": {
        'url':'https://opendata.arcgis.com/datasets/a779d051865f461eb2a1f50f10940ec4_161.geojson'
        ,'prefix':'source-data/dc-open-data/'
        ,'metadata':{'target_schema':'source_data', 'dataset_info':'https://opendata.dc.gov/datasets/roadway-intersection-approach'}
    }
}

# specify which datasets to load
resources_to_load = ['crashes_raw','crash_details','census_blocks','address_points','all311','vision_zero','roadway_blocks','roadway_subblocks','roadway_blockface','roadway_intersection_approach']

for dataset in resources_to_load:
    gdf = gpd.GeoDataFrame() 
    # load each dataset into memory
    if isinstance(resources[dataset]['url'],list):
        for i in resources[dataset]['url']:
            url = i
            gdf=gdf.append(gpd.read_file(url), ignore_index=True)
    else:
        url = resources[dataset]['url']
        gdf = gpd.read_file(url)
    if 'filters' in resources[dataset].keys():
        gdf=gdf[gdf['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']
    # 'offset' is apparently a reserved word in Postgres, so rename any columns with that label
    if 'OFFSET' in gdf.columns:
        gdf.rename(columns={"OFFSET": "_OFFSET"})
    # download each dataset to local hard drive, and then upload it to the S3 bucket
    # in csv format
    filename = Path(os.path.expanduser('~'), dataset+'.csv')
    gdf.to_csv(filename, index=False, header=False, line_terminator='\n')
    data = open(filename, 'rb')
    s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+dataset+'.csv', Body=data, Metadata =resources[dataset]['metadata'])
    # in geojson format
    filename = Path(os.path.expanduser('~'), dataset+'.geojson')
    gdf.to_file(filename, driver='GeoJSON')
    data = open(filename, 'rb')
    s3.Bucket(bucket_name).put_object(Key=resources[dataset]['prefix']+dataset+'.geojson', Body=data, Metadata =resources[dataset]['metadata'])
