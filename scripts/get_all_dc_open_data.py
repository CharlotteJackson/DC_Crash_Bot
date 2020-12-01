import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings, create_postgres_engine

AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = 'dc-crash-bot-test'
engine = create_postgres_engine("AWS_PostGIS", "postgres", "DEV")

resources_and_urls = {
    'crashes_raw' : 'https://opendata.arcgis.com/datasets/70392a096a8e431381f1f692aaa06afd_24.geojson'
    ,'crash_details': 'https://opendata.arcgis.com/datasets/70248b73c20f46b0a5ee895fc91d6222_25.geojson'
    ,'census_blocks':'https://opendata.arcgis.com/datasets/a6f76663621548e1a039798784b64f10_0.geojson'
    ,'vision_zero':'https://opendata.arcgis.com/datasets/3f28bc3ad77f49079efee0ac05d8464c_0.geojson'
    ,'address_points':'https://opendata.arcgis.com/datasets/aa514416aaf74fdc94748f1e56e7cc8a_0.geojson'
    ,'all311': ['https://opendata.arcgis.com/datasets/82b33f4833284e07997da71d1ca7b1ba_11.geojson'
                ,'https://opendata.arcgis.com/datasets/98b7406def094fa59838f14beb1b8c81_10.geojson'
                ,'https://opendata.arcgis.com/datasets/2a46f1f1aad04940b83e75e744eb3b09_9.geojson'
                ,'https://opendata.arcgis.com/datasets/19905e2b0e1140ec9ce8437776feb595_8.geojson'
                ,'https://opendata.arcgis.com/datasets/0e4b7d3a83b94a178b3d1f015db901ee_7.geojson'
                ,'https://opendata.arcgis.com/datasets/b93ec7fc97734265a2da7da341f1bba2_6.geojson'
    ]
}

resources_to_skip = ['crashes_raw','crash_details','census_blocks','address_points','all311']

pd_filters = {"all311": "['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']"}

for key in resources_and_urls.keys():
    if key not in resources_to_skip:
        gdf = gpd.GeoDataFrame() 
        # globals()[str(key)] = gdf 
        if isinstance(resources_and_urls[key],list):
            for i in resources_and_urls[key]:
                url = i
                gdf=gdf.append(gpd.read_file(url), ignore_index=True)
        else:
            url = resources_and_urls[key]
            gdf = gpd.read_file(url)
        if key in pd_filters.keys():
            gdf=gdf[gdf['SERVICECODEDESCRIPTION'] == 'Traffic Safety Investigation']
        # fix offset 
        if 'OFFSET' in gdf.columns:
            gdf.rename(columns={"OFFSET": "_OFFSET"})
        # export to csv 
        filename = Path(os.path.expanduser('~'), key+'.csv')
        gdf.to_csv(filename, index=False, header=False, line_terminator='\n')
        data = open(filename, 'rb')
        s3.Bucket(bucket_name).put_object(Key=key+'.csv', Body=data)
        # export to geojson  
        filename = Path(os.path.expanduser('~'), key+'.geojson')
        gdf.to_file(filename, driver='GeoJSON')
        data = open(filename, 'rb')
        s3.Bucket(bucket_name).put_object(Key=key+'.geojson', Body=data)
