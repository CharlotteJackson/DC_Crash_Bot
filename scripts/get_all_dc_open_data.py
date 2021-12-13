# import geopandas as gpd
import pandas as pd
import boto3
import os
from pathlib import Path
from connect_to_rds import get_connection_strings
import argparse 
import datetime
from datetime import timezone
# import requests
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
        ,'basic_business_licenses' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/85bf98d3915f412c8a4de706f2d13513_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/bbl/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'basic_business_license',"dataset_info":"https://opendata.dc.gov/datasets/basic-business-licenses/"}
        }
        ,'bike_trails' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/e8c2b7ef54fb43d9a2ed1b0b75d0a14d_4.geojson')]
            ,'prefix' :'source-data/dc-open-data/bike_trails/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'bike_trails',"dataset_info":"https://opendata.dc.gov/datasets/bike-trails/"}
        }
        
        ,'census_blocks' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/a6f76663621548e1a039798784b64f10_0.geojson')]
            ,'prefix' :'source-data/dc-open-data/census_blocks/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'census_blocks',"dataset_info":"https://opendata.dc.gov/datasets/census-blocks-2010"}
        }
        ,'census_tracts':{
            'url': [('complete','https://opendata.arcgis.com/datasets/6969dd63c5cb4d6aa32f15effb8311f3_8.geojson')]
            ,'prefix' :'source-data/dc-open-data/census_tracts/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'census_tracts',"dataset_info":"https://opendata.dc.gov/datasets/census-tracts-in-2010"}

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
        ,'dc_metro_stations' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/ab5661e1a4d74a338ee51cd9533ac787_50.geojson')]
            ,'prefix' :'source-data/dc-open-data/dc_metro_stations/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'dc_metro_stations',"dataset_info":"https://opendata.dc.gov/datasets/metro-station-entrances-in-dc"}
            
        }
        ,'intersection_points' : {
            'url': [('complete','https://opendata.arcgis.com/datasets/96a9bbbb475648769e311d03c78698a7_2.geojson')]
            ,'prefix' :'source-data/dc-open-data/intersection_points/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'intersection_points',"dataset_info":"https://opendata.dc.gov/datasets/intersection-points"}
            
        }
        ,'metro_stations_daily_ridership': {
            'url': [('complete','https://opendata.arcgis.com/datasets/3982795643fa45598a2fa472afbbea69_6.geojson')]
            ,'prefix' :'source-data/dc-open-data/metro_stations_daily_ridership/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'metro_stations_daily_ridership',"dataset_info":"https://rtdc-mwcog.opendata.arcgis.com/datasets/metro-rail-ridership-historical"}
            
        }

        ,'moving_violations' : {
            'url': [  #2021
                     ('2021_08','https://opendata.arcgis.com/datasets/49bac855d3d44ce6b01f9948c2925357_7.geojson')
                    ,('2021_07','https://opendata.arcgis.com/datasets/5dc96a2056d84d14838e4b65c06d454e_6.geojson')
                    ,('2021_06','https://opendata.arcgis.com/datasets/301dbe9548a2423d876da33ef416536f_5.geojson')
                    ,('2021_05','https://opendata.arcgis.com/datasets/4ac4f0b5379e4e02bab9d3fb198a1a44_4.geojson')
                    ,('2021_04','https://opendata.arcgis.com/datasets/055b11cac4f64a809ccca1d9ab64df6b_3.geojson')
                    ,('2021_03','https://opendata.arcgis.com/datasets/660ac078bea649e2a9a1c32abbff1059_2.geojson')
                    ,('2021_02','https://opendata.arcgis.com/datasets/74cbdef730fe481995b37abb2abbf6d0_1.geojson')
                    ,('2021_01','https://opendata.arcgis.com/datasets/1f104745cf4a459b9888831df78d58d7_0.geojson')
                    #2020
                    ,('2020_12','https://opendata.arcgis.com/datasets/0b0b44b8e92f48948b3d9be49e605b77_11.geojson')
                    ,('2020_11','https://opendata.arcgis.com/datasets/b3a187d6f91c41c38f4a1e24f9d2cdfc_10.geojson')
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
                    #2019
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
                    # Feb 2019 Added July 2021
                    ,('2019_02','https://opendata.arcgis.com/datasets/a03b8a80a06e4451951497dee78959ab_1.geojson')
                    ,('2019_01','https://opendata.arcgis.com/datasets/0d7b690c4e874e39a6f006cc61073561_0.geojson' )
                    #2018
                    ,('2018_12','https://opendata.arcgis.com/datasets/1bf00863fedf4d09a236e4353dba1670_11.geojson')
                    ,('2018_11','https://opendata.arcgis.com/datasets/b3aa63faaeb243ea87eb58f2eb5a1931_10.geojson')
                    # Oct 2018 Added Oct 2021
                    ,('2018_10_2','https://opendata.arcgis.com/datasets/a9f464dc91d9465394a90d44c61eb06a_9.geojson')
                    ,('2018_09','https://opendata.arcgis.com/datasets/03dfb384c57d40f8b05fa5936b23fb82_8.geojson')
                    ,('2018_08','https://opendata.arcgis.com/datasets/e2b5f90018c3487ea293f53edadbd69a_7.geojson')
                     # July 2018 Added July 2021
                    ,('2018_07','https://opendata.arcgis.com/datasets/a025be4db2874dadb0e6d776da356236_6.geojson')
                    ,('2018_06','https://opendata.arcgis.com/datasets/f6cced2fa2764599af101884d8e0ade0_5.geojson')
                    ,('2018_05','https://opendata.arcgis.com/datasets/87a2d2f6a8124b1e9d7406185d8fec80_4.geojson')
                    ,('2018_04','https://opendata.arcgis.com/datasets/e70527c48ac04bf8839fec666b328b24_3.geojson')
                    ,('2018_03','https://opendata.arcgis.com/datasets/3d56bc3d7d8046c081159e57ed338f29_2.geojson')
                    ,('2018_02','https://opendata.arcgis.com/datasets/f7fb9a35ff1c43239b071709ab597ff3_1.geojson')
                    ,('2018_01','https://opendata.arcgis.com/datasets/cac773db047e478195b4a534b878f6e3_0.geojson')
                    #2017
                    ,('2017_12','https://opendata.arcgis.com/datasets/21fc91d901764606b96cd99ec179f74a_11.geojson')
                    ,('2017_11','https://opendata.arcgis.com/datasets/f34868d103f04f33b400eee86aeb6aba_10.geojson')
                    ,('2017_10','https://opendata.arcgis.com/datasets/be5720b7bf69499992a7b43848d32d80_9.geojson')
                    ,('2017_09','https://opendata.arcgis.com/datasets/596df79f0c5c4c38b61d696a096b49cc_8.geojson')
                    ,('2017_08','https://opendata.arcgis.com/datasets/e9940e8cc1e3445e85d0f5325fa9161f_7.geojson')
                    ,('2017_07','https://opendata.arcgis.com/datasets/94455e9d5f42439788da06caeaaf35ac_6.geojson')
                    ,('2017_06','https://opendata.arcgis.com/datasets/3fb427effc254bfea7d31c62d3f37a14_5.geojson')
                    ,('2017_05','https://opendata.arcgis.com/datasets/a006bc75b0a24dcb908ccd7a89182534_4.geojson')
                    ,('2017_04','https://opendata.arcgis.com/datasets/0ba31144834e4241abdb3e6e99f09977_3.geojson')
                    ,('2017_03','https://opendata.arcgis.com/datasets/6a1341ffec934e6fb373f4185e3a0464_2.geojson')
                    ,('2017_02','https://opendata.arcgis.com/datasets/214a74077047456d90a646f4af02ba0a_1.geojson')
                    ,('2017_01','https://opendata.arcgis.com/datasets/0100ff1b67634fa2a7a146b4ef6994bc_0.geojson')
                    #2016
                    ,('2016_12','https://opendata.arcgis.com/datasets/fa2fcec4126c48beac9e9c91e9b32674_11.geojson')
                    ,('2016_11','https://opendata.arcgis.com/datasets/a4b123fdfd3a4e92a089b3acbd09de06_10.geojson')
                    ,('2016_10','https://opendata.arcgis.com/datasets/6bf443413ad349339b20213cc1ea4b38_9.geojson')
                    ,('2016_09','https://opendata.arcgis.com/datasets/e828e5b687ff411e81bd08d4c233604d_8.geojson')
                    ,('2016_08','https://opendata.arcgis.com/datasets/47a7b2dfa3e2469db738ca6ae68641a6_7.geojson')
                    ,('2016_07','https://opendata.arcgis.com/datasets/fd22ea109af5446b8c1b08997fc47d71_6.geojson')
                    ,('2016_06','https://opendata.arcgis.com/datasets/3f24e9e2a0fb460eb2020fae69ce573d_5.geojson')
                    ,('2016_05','https://opendata.arcgis.com/datasets/7e970163eece486298defda49a8d70eb_4.geojson')
                    ,('2016_04','https://opendata.arcgis.com/datasets/0f7d9b56f5414c39a42e341cc69b1f8a_3.geojson')
                    ,('2016_03','https://opendata.arcgis.com/datasets/eec8c2a39b2c4700a8cd4030725caf48_2.geojson')
                    ,('2016_02','https://opendata.arcgis.com/datasets/451ec584aacc4446ad1344fb748693ea_1.geojson')
                    ,('2016_01','https://opendata.arcgis.com/datasets/6e5c0a7d3cf44c21a05b8e0dd47d4937_0.geojson')
                    #2015
                    ,('2015_12','https://opendata.arcgis.com/datasets/6931f45412e14e96988fad6aeebc3798_11.geojson')
                    ,('2015_11','https://opendata.arcgis.com/datasets/f1902d8ea4c54d289398db2527080df4_10.geojson')
                    ,('2015_10','https://opendata.arcgis.com/datasets/9dd17fd4b5e7460aa88f1ff26bcdf0b4_9.geojson')
                    ,('2015_09','https://opendata.arcgis.com/datasets/0e054d69efea44239dc9c63cf59c6c7d_8.geojson')
                    ,('2015_08','https://opendata.arcgis.com/datasets/44ae90d74fb94ddb962356be7062dcde_7.geojson')
                    ,('2015_07','https://opendata.arcgis.com/datasets/8234913515674cf3ab63edc07d0d00c1_6.geojson')
                    ,('2015_06','https://opendata.arcgis.com/datasets/0964b879af2e4b50a5ddd947d94063c3_5.geojson')
                    ,('2015_05','https://opendata.arcgis.com/datasets/799ce041037144f8a6b190ef6395c058_4.geojson')
                    ,('2015_04','https://opendata.arcgis.com/datasets/ca3c52af582a40a1a74b92c8bb545a50_3.geojson')
                    ,('2015_03','https://opendata.arcgis.com/datasets/1805e977ddb34a6b93e94200887c8349_2.geojson')
                    ,('2015_02','https://opendata.arcgis.com/datasets/4c5d5e0ec6924410bd104c33fa8748b2_1.geojson')
                    ,('2015_01','https://opendata.arcgis.com/datasets/c4919319d3ce4107bc2e484811deaa4e_0.geojson')
                    #2014
                    ,('2014_12','https://opendata.arcgis.com/datasets/b06d44c1fae840aca9e8c00a9f8535bc_11.geojson')
                    ,('2014_11','https://opendata.arcgis.com/datasets/1d21b24d01344b22a8a30181a514ccb5_10.geojson')
                    ,('2014_10','https://opendata.arcgis.com/datasets/90e060f20b63426c9f9cee968c608eb9_9.geojson')
                    ,('2014_09','https://opendata.arcgis.com/datasets/43b1a0a986424d48afa3f15e53e8c804_8.geojson')
                    ,('2014_08','https://opendata.arcgis.com/datasets/ad8fc78a3157483fb8f1fcf7f20adbe5_7.geojson')
                    ,('2014_07','https://opendata.arcgis.com/datasets/b9d2163e02b143e38051c4dde5042fcd_6.geojson')
                    ,('2014_06','https://opendata.arcgis.com/datasets/1945a24707b140f7973ccdda7b391515_5.geojson')
                    ,('2014_05','https://opendata.arcgis.com/datasets/0c9a4824d0124499be1ebaf8f0201ed9_4.geojson')
                    ,('2014_04','https://opendata.arcgis.com/datasets/9f0f9c429c4043c786a4fbce7a9122d7_3.geojson')
                    ,('2014_03','https://opendata.arcgis.com/datasets/c15caa8cb5f04d2db920ffad1c8bd00f_2.geojson')
                    ,('2014_02','https://opendata.arcgis.com/datasets/30b8e32da5c44ee6b83fc3a6ad7e440c_1.geojson')
                    ,('2014_01','https://opendata.arcgis.com/datasets/3aa64d8146ad4693b2e4f98aea871e72_0.geojson')
                    #2013
                    ,('2013_12','https://opendata.arcgis.com/datasets/50aa3c7bfea345beb3607f7cccfbf1e2_11.geojson')
                    ,('2013_11','https://opendata.arcgis.com/datasets/5c8a1bd9f9974b3abdcd088f1d6a8b0f_10.geojson')
                    ,('2013_10','https://opendata.arcgis.com/datasets/b7ad765713ef41a7b51081afaf64a847_9.geojson')
                    ,('2013_09','https://opendata.arcgis.com/datasets/9940d47c534c4d33bf284f12fe9fb6a4_8.geojson')
                    ,('2013_08','https://opendata.arcgis.com/datasets/abdc0ebd21ea4593a08ea9eca7ce1242_7.geojson')
                    ,('2013_07','https://opendata.arcgis.com/datasets/ae4e814e630b450797bb3d67ec139b21_6.geojson')
                    ,('2013_06','https://opendata.arcgis.com/datasets/eece6199ffe44b14bc1677ebe0f82dd3_5.geojson')
                    ,('2013_05','https://opendata.arcgis.com/datasets/d274bfb9d9bc4ac6bdb9c220ce8bef3c_4.geojson')
                    ,('2013_04','https://opendata.arcgis.com/datasets/e64d3571cd794118bcd63dbcc80f96ef_3.geojson')
                    ,('2013_03','https://opendata.arcgis.com/datasets/cc6d32f24f3145afbecc44052267589e_2.geojson')
                    ,('2013_02','https://opendata.arcgis.com/datasets/bc28e338278b471086df10e1361ff5f2_1.geojson')
                    ,('2013_01','https://opendata.arcgis.com/datasets/d3b767116d8c43f0bfe8dc3d6142f33b_0.geojson')
                    #2012
                    ,('2012_12','https://opendata.arcgis.com/datasets/83471ebee37e43018097d7e2cf4d42ea_11.geojson')
                    ,('2012_11','https://opendata.arcgis.com/datasets/194df2068246477693ed745a1ab2cbbd_10.geojson')
                    ,('2012_10','https://opendata.arcgis.com/datasets/2891cbaec3844f91864ce1a6cb507126_9.geojson')
                    ,('2012_09','https://opendata.arcgis.com/datasets/21d511ff2e9f448592006fbd71845734_8.geojson')
                    ,('2012_08','https://opendata.arcgis.com/datasets/8dd1ac40211347e7b79dbad77af9e066_7.geojson')
                    ,('2012_07','https://opendata.arcgis.com/datasets/934f3c014a554b2cb951ea2de62a113b_6.geojson')
                    ,('2012_06','https://opendata.arcgis.com/datasets/64e2710c9db049a68376abfb13ffef8e_5.geojson')
                    ,('2012_05','https://opendata.arcgis.com/datasets/7a15410fcb714b16a7526e4c7ba931d2_4.geojson')
                    ,('2012_04','https://opendata.arcgis.com/datasets/c8cf5e6f96de4630a7e31ee07b1b4ebe_3.geojson')
                    ,('2012_03','https://opendata.arcgis.com/datasets/bbe8e0be605c43f28e62158efaa06398_2.geojson')
                    ,('2012_02','https://opendata.arcgis.com/datasets/004489beedce436ca96d423afe682cf1_1.geojson')
                    ,('2012_01','https://opendata.arcgis.com/datasets/ae73b2ff3bfe4893917465be65634f47_0.geojson')
                    #2011
                    ,('2011_12','https://opendata.arcgis.com/datasets/67d4ab93b2cf4bdcba85368ce9f11507_11.geojson')
                    ,('2011_11','https://opendata.arcgis.com/datasets/aab33e0a31ec4916ae3427c10ee050ee_10.geojson')
                    ,('2011_10','https://opendata.arcgis.com/datasets/7ef1340871a24b27bfa4f18a07b16340_9.geojson')
                    ,('2011_09','https://opendata.arcgis.com/datasets/3aaacd157831498ebbc2ee06e3907009_8.geojson')
                    ,('2011_08','https://opendata.arcgis.com/datasets/4698a7d0cb6b403da33a34af579ad6a6_7.geojson')
                    ,('2011_07','https://opendata.arcgis.com/datasets/570624ed24f5440486cd2c8249337b0f_6.geojson')
                    ,('2011_06','https://opendata.arcgis.com/datasets/ad4598e6bbca4e148c256d003c6b5197_5.geojson')
                    ,('2011_05','https://opendata.arcgis.com/datasets/cbf930e98c664e6798606284beb7bbe5_4.geojson')
                    ,('2011_04','https://opendata.arcgis.com/datasets/71195761feb84b1a8c9a0121828afa09_3.geojson')
                    ,('2011_03','https://opendata.arcgis.com/datasets/ead7d4be78bd471e942cc5ba65387de9_2.geojson')
                    ,('2011_02','https://opendata.arcgis.com/datasets/6b45df3c56594186a9e17f99d6042c77_1.geojson')
                    ,('2011_01','https://opendata.arcgis.com/datasets/fa44cbf8ebd64eaa801539bcebf31988_0.geojson')
                    #2010
                    ,('2010_12','https://opendata.arcgis.com/datasets/8ccfc339840f4acfa3bf2fc2e805afc8_11.geojson')
                    ,('2010_11','https://opendata.arcgis.com/datasets/16a5e4ba718d431c8bca6b48196dbb40_10.geojson')
                    ,('2010_10','https://opendata.arcgis.com/datasets/30d217daa0c945d49f8500a142b5dab1_9.geojson')
                    ,('2010_09','https://opendata.arcgis.com/datasets/41e4ed89c9594f80a8492181c77becc0_8.geojson')
                    ,('2010_08','https://opendata.arcgis.com/datasets/94de169c25554c87912a3415377eada8_7.geojson')
                    ,('2010_07','https://opendata.arcgis.com/datasets/f2134ae4915b40e786f0742e0b5e690c_6.geojson')
                    ,('2010_06','https://opendata.arcgis.com/datasets/9b4dee78e720429a90e48edcd7309082_5.geojson')
                    ,('2010_05','https://opendata.arcgis.com/datasets/cc5b20c194be4fdda78dc64349f0e9a4_4.geojson')
                    ,('2010_04','https://opendata.arcgis.com/datasets/497eeb0fd7e94f74b875363b53e1399c_3.geojson')
                    ,('2010_03','https://opendata.arcgis.com/datasets/5abe529357044c9d8f87c4bc8581ac54_2.geojson')
                    ,('2010_02','https://opendata.arcgis.com/datasets/8e5c785effcd4660a54ce39fb79f3aa1_1.geojson')
                    ,('2010_01','https://opendata.arcgis.com/datasets/135c67e4e3bc4e998c0312687782fe72_0.geojson')
        ]
            ,'prefix' :'source-data/dc-open-data/moving_violations/'
            ,'metadata' :{'target_schema':'source_data', 'target_table': 'moving_violations',"dataset_info":"https://opendata.dc.gov/datasets/moving-violations-issued-in-november-2020"}
        
        }
        ,"national_parks": {
            'url':[('complete','https://opendata.arcgis.com/datasets/14eb1c6b576940c7b876ebafb227febe_10.geojson')]
            ,'prefix':'source-data/dc-open-data/national_parks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'national_parks','dataset_info':'https://opendata.dc.gov/datasets/national-parks/data'}
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
        ,"sidewalks": {
            'url':[('complete','https://opendata.arcgis.com/datasets/2347fa1f3fd9412dbf11aa6441ddca8b_83.geojson')]
            ,'prefix':'source-data/dc-open-data/sidewalks/'
            ,'metadata':{'target_schema':'source_data', 'target_table': 'sidewalks','dataset_info':'https://opendata.dc.gov/datasets/sidewalks/'}
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