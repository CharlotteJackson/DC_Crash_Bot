import os
import sys 
import inspect
import subprocess 
import pandas as pd
from pathlib import Path
import datetime
import shutil
import numpy as np
import geopandas as gpd
import csv_to_gdf 


# add top level directory to sys.path
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

# specify path to data
data_path = os.path.join(sys.path[0], 'data')

# Import the street functional classifications
df_sfc = pd.read_csv(Path(os.path.join(data_path, 'street_functional_classification/DDOT_Street_Functional_Classification.csv')))

# Import addresses
df_address = pd.read_csv(Path(os.path.join(data_path, 'address_points/Address_Points.csv')))

# Import crashes
df_crashes = pd.read_csv(Path(os.path.join(data_path, 'crashes/Crashes_In_DC.csv')))

# Import VZ data
df_vz = pd.read_csv(Path(os.path.join(data_path, 'vision_zero/Vision_Zero_Safety.csv')))

# Import 311 requests
df_311 = pd.read_csv(Path(os.path.join(data_path, '311_requests/311_City_Service_Requests_in_2020.csv')))


# rename vision zero safety x = longitude, y = latitude 
df_vz = df_vz.rename(columns={"X": "LONGITUDE", "Y": "LATITUDE"})

# convert to gdfs 
gdf_vz = csv_to_gdf.df_to_gdf(df_vz)
gdf_crashes = csv_to_gdf.df_to_gdf(df_crashes)
gdf_address = csv_to_gdf.df_to_gdf(df_address)

crashes_with_address_info = gpd.sjoin(gdf_crashes, gdf_address, how="left", op = 'intersects')

print(crashes_with_address_info.head())
print(crashes_with_address_info['ANC'].isna().sum())