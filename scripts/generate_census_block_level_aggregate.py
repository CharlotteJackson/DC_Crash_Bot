import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings, create_postgres_engine

AWS_Credentials = get_connection_strings("AWS_DEV")
s3 = boto3.client('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
s3_resource = boto3.resource('s3'
    ,aws_access_key_id=AWS_Credentials['aws_access_key_id']
    ,aws_secret_access_key=AWS_Credentials['aws_secret_access_key'])
bucket_name = 'dc-crash-bot-test'
home = os.path.expanduser('~')

datasets = ['crashes_w_detail','census_blocks','address_points','all311', 'vision_zero']
geodfs = {}
current_files = [os.path.splitext(f)[0] for f in os.listdir(home) if os.path.splitext(f)[1] == '.geojson']
print(current_files)

# load datasets into memory and put them in a dict of gdf's
for dataset in datasets:
    filename = os.path.join(home,dataset+'.geojson')
    if dataset not in current_files:
        s3.download_file(bucket_name, dataset+'.geojson', filename)
    gdf=gpd.read_file(filename)
    geodfs[dataset] = gdf 
    print(dataset)
    for column in gdf.columns:
        print(column)

# process crashes
census_blocks_crashes = gpd.sjoin(geodfs['crashes_w_detail'], geodfs['census_blocks'], how="left", op='intersects')
crashes_agg = (census_blocks_crashes.groupby(['OBJECTID_right', 'YEAR'])
               .agg({'OBJECTID_left':'count'
                     , 'FATAL_BICYCLIST': 'sum', 'FATAL_DRIVER': 'sum','FATAL_PEDESTRIAN': 'sum'
                     ,'TOTAL_VEHICLES': 'sum','TOTAL_BICYCLES': 'sum'
                    ,'TOTAL_PEDESTRIANS': 'sum', 'PEDESTRIANSIMPAIRED': 'sum', 'BICYCLISTSIMPAIRED': 'sum', 
                     'DRIVERSIMPAIRED': 'sum', 'PED_INJURIES': 'sum', 'BICYCLE_INJURIES': 'sum', 
                     'VEHICLE_INJURIES': 'sum', 'TOTAL_INJURIES': 'sum', 'OOS_VEHICLES': 'sum', 
                     'DRIVERS_UNDER_25': 'sum', 'DRIVERS_OVER_80': 'sum', 'PEDS_OVER_70': 'sum', 
                     'PEDS_UNDER_12': 'sum', 'BIKERS_OVER_70': 'sum', 'BIKERS_UNDER_12': 'sum', 
                    'CARS': 'sum', 'SUVS_OR_TRUCKS': 'sum', 'DRIVER_TICKETS': 'sum', 'BICYCLE_TICKETS': 'sum'
                    , 'PED_TICKETS': 'sum', 'DRIVERS_SPEEDING': 'sum'
                    })
               .reset_index().rename(columns={'OBJECTID_left':'TOTAL_CRASHES'}))            

# process 311 
geodfs['all311']['YEAR'] = geodfs['all311'].apply(lambda x: x.ADDDATE[:4], axis=1)
census_blocks_311 = gpd.sjoin(geodfs['all311'], geodfs['census_blocks'], how="left", op='intersects')
census_blocks_311_agg = (census_blocks_311.groupby(['OBJECTID_right', 'YEAR'])
               .agg({'OBJECTID_left':'count'})
               .reset_index().rename(columns={'OBJECTID_left':'TOTAL_TSA_REQUESTS'}))

# process vision zero 
geodfs['vision_zero']['YEAR'] = geodfs['vision_zero'].apply(lambda x: x.REQUESTDATE[:4], axis=1)
census_blocks_vision_zero = gpd.sjoin(geodfs['vision_zero'], geodfs['census_blocks'], how="left", op='within')
census_blocks_vision_zero_agg = (census_blocks_vision_zero.groupby(['OBJECTID_right', 'YEAR'])
               .agg({'OBJECTID_left':'count'})
               .reset_index().rename(columns={'OBJECTID_left':'TOTAL_VISION_ZERO_REQUESTS'}))

# merge 
crashes_311 = crashes_agg.merge(census_blocks_311_agg, how = 'left', on=['OBJECTID_right', 'YEAR'])
crashes_311_vz = crashes_311.merge(census_blocks_vision_zero_agg, how = 'left', on=['OBJECTID_right', 'YEAR'])

# join back to census blocks to bring in geography
geo_info = crashes_311_vz.merge(geodfs['census_blocks'], how = 'inner', left_on = 'OBJECTID_right', right_on = 'OBJECTID')
geo_info = geo_info.set_geometry('geometry')
census_blocks_addr = geodfs['address_points'].dissolve(by='CENSUS_BLOCK', aggfunc='first')
final = gpd.sjoin(geo_info, census_blocks_addr, how="left", op='intersects')
print("Total rows: ", len(final))
# export to csv 
filename = os.path.join(home,'census_block_level_final.csv')
final.to_csv(filename, index=False, header=False)
data = open(filename, 'rb')
s3_resource.Bucket(bucket_name).put_object(Key='census_block_level_final.csv', Body=data)
# export to geojson  
filename = os.path.join(home,'census_block_level_final.geojson')
final.to_file(filename, driver='GeoJSON')
data = open(filename, 'rb')
s3_resource.Bucket(bucket_name).put_object(Key='census_block_level_final.geojson', Body=data)

# put in table in Raw schema in PostGIS 
# create_s3_uri_query = 'SELECT aws_commons.create_s3_uri(\'{}\', \'{}.csv\',\'us-east-1\') AS s3_uri;'.format(bucket_name, key)
# print(create_s3_uri_query)
# s3_uri = engine.execute(create_s3_uri_query)
# import_table_query = '''SELECT aws_s3.table_import_from_s3(\'{}\', \'\', \'(format csv)\', :\'s3_uri\',aws_commons.create_aws_credentials(\'{}\', \'{}\',''));
#     '''.format(key, AWS_Credentials['aws_access_key_id'], AWS_Credentials['aws_secret_access_key'])
# print(import_table_query)