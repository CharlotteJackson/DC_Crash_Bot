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
bucket_name = AWS_Credentials['s3_bucket']
region=AWS_Credentials['region']
home = os.path.expanduser('~')
source_datasets={'source-data/dc-open-data/':['crashes_raw', 'crash_details']}
destination_folder='analysis-data/'
destination_dataset = 'dc_crashes_w_details'

# load current files
current_files = [os.path.splitext(f)[0] for f in os.listdir(home) if os.path.splitext(f)[1] == '.geojson']

# load datasets into memory and put them in a dict of gdf's
geodfs = {}
for key in source_datasets.keys():
    for dataset in source_datasets[key]:
        filename = os.path.join(home,dataset+'.geojson')
        if dataset not in current_files:
            s3.download_file(bucket_name, key+dataset+'.geojson', filename)
        gdf=gpd.read_file(filename)
        geodfs[dataset] = gdf 

# process crash details
crash_details=geodfs['crash_details']
# first create variables that will be aggregated
# driver over 80/driver under 25 
crash_details['DRIVERS_OVER_80']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Driver' and x.AGE>=80 else 0, axis = 1)
crash_details['DRIVERS_UNDER_25']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Driver'  and x.AGE<=25 else 0, axis = 1)
# ped under 12/ped over 70 
crash_details['PEDS_OVER_70']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Pedestrian' and x.AGE>=70 else 0, axis = 1)
crash_details['PEDS_UNDER_12']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Pedestrian' and x.AGE<=12 else 0, axis = 1)
# biker under 12/biker over 70
crash_details['BIKERS_OVER_70']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Bicyclist' and x.AGE>=70 else 0, axis = 1)
crash_details['BIKERS_UNDER_12']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Bicyclist' and x.AGE<=12 else 0, axis = 1)
# out of state driver
crash_details['OOS_VEHICLES']= crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Driver' and x.LICENSEPLATESTATE != 'DC' else 0, axis = 1)
# vehicle type 
crash_details['CARS']=crash_details.apply(lambda x: 1 if x.INVEHICLETYPE=='Passenger Car/automobile' and x.PERSONTYPE=='Driver' else 0, axis = 1)
crash_details['SUVS_OR_TRUCKS']=crash_details.apply(lambda x: 1 if (x.INVEHICLETYPE=='Suv (sport Utility Vehicle)'or x.  INVEHICLETYPE== 'Pickup Truck')and x.PERSONTYPE=='Driver' else 0, axis = 1)

# injuries 
crash_details['PED_INJURIES']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Pedestrian' and (x.MAJORINJURY == 'Y' or x.MINORINJURY =='Y') else 0,axis = 1)
crash_details['BICYCLE_INJURIES']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Bicyclist' and (x.MAJORINJURY == 'Y' or x.MINORINJURY =='Y') else 0,axis = 1)
crash_details['VEHICLE_INJURIES']=crash_details.apply(lambda x: 1 if (x.PERSONTYPE=='Driver' or x.PERSONTYPE == 'Passenger')and (x.MAJORINJURY == 'Y' or x.MINORINJURY =='Y') else 0,axis = 1)
# tickets issued? 
crash_details['DRIVER_TICKETS']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Driver' and x.TICKETISSUED == 'Y' else 0,axis = 1)
crash_details['BICYCLE_TICKETS']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Bicyclist' and x.TICKETISSUED == 'Y' else 0, axis = 1)
crash_details['PED_TICKETS']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Pedestrian' and x.TICKETISSUED == 'Y' else 0, axis = 1)
# speeding? 
crash_details['DRIVERS_SPEEDING']=crash_details.apply(lambda x: 1 if x.PERSONTYPE=='Driver' and x.SPEEDING == 'Y' else 0, axis = 1)
# total injuries
crash_details['TOTAL_INJURIES']=crash_details['VEHICLE_INJURIES']+crash_details['BICYCLE_INJURIES']+crash_details['PED_INJURIES']          

# roll up by crash id
# make list fields out of the person type, vehicle type, and license plate state fields
crash_details_agg = (crash_details.groupby(['CRIMEID']).agg({
                    'PED_INJURIES': 'sum', 'BICYCLE_INJURIES': 'sum','VEHICLE_INJURIES': 'sum','TOTAL_INJURIES': 'sum', 'OOS_VEHICLES': 'sum', 'DRIVERS_UNDER_25': 'sum', 'DRIVERS_OVER_80': 'sum'
                    , 'PEDS_OVER_70':'sum', 'PEDS_UNDER_12': 'sum', 'BIKERS_OVER_70': 'sum', 'BIKERS_UNDER_12':'sum', 'OOS_VEHICLES': 'sum','CARS' : 'sum', 'SUVS_OR_TRUCKS' : 'sum', 'DRIVER_TICKETS': 'sum'
                   ,'BICYCLE_TICKETS': 'sum', 'PED_TICKETS':'sum', 'DRIVERS_SPEEDING': 'sum','PERSONTYPE': lambda x: list(x), 'INVEHICLETYPE':  lambda x: list(x), 
                   'LICENSEPLATESTATE': lambda x: list(x)
                    }).reset_index())

crashes_raw =geodfs['crashes_raw']
dc_crashes_w_details =  crashes_raw.merge(crash_details_agg, how = 'left', on='CRIMEID')
for column in dc_crashes_w_details.columns:
    print(column)


# download dataset to local hard drive, and then upload it to the S3 bucket
# in csv format
filename = Path(os.path.expanduser('~'), destination_dataset+'.csv')
gdf.to_csv(filename, index=False, header=False, line_terminator='\n')
data = open(filename, 'rb')
s3_resource.Bucket(bucket_name).put_object(Key=destination_folder+destination_dataset+'.csv', Body=data)
# in geojson format
filename = Path(os.path.expanduser('~'), destination_dataset+'.geojson')
gdf.to_file(filename, driver='GeoJSON')
data = open(filename, 'rb')
s3_resource.Bucket(bucket_name).put_object(Key=destination_folder+destination_dataset+'.geojson', Body=data)