
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import json

# copied from https://gist.github.com/yassineAlouini/e8e20402e6b2fd2889042245d0bb7178

def df_to_gdf(input_df):
    """
    Convert a DataFrame with longitude and latitude columns
    to a GeoDataFrame.
    """
    df = input_df.copy()
    geometry = [Point(xy) for xy in zip(df.LONGITUDE, df.LATITUDE)]
    return gpd.GeoDataFrame(df, crs=4326, geometry=geometry)

def csv_to_geojson(input_fp, output_fp):
    """
    Read a CSV file, transform it into a GeoJSON and save it.
    """
    csv_data = pd.read_csv(input_fp, 
                       compression='bz2', 
                       sep=';', 
                       encoding='utf-8')
    geojson_data = (csv_data.pipe(df_to_gdf)
                            .drop(['extra'], axis=1)
                            .to_json())
    with open(output_fp, 'w') as geojson_file:
        geojson_file.write(geojson_data)