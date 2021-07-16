# Python imports
import logging
import os
from typing import Dict, Any

# postgress imports
import psycopg2
import pandas.io.sql as psql

# Project Imports
# TODO what is wrong with imports?
try:
    from .get_address import rev_geocode
except:
    from get_address import rev_geocode

# Check if google key found
try:
    GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
except Exception as error:
    logging.error("No GOOGLE API KEY detected")


# Needed to connect to database
db_host = os.environ["DB_HOST"]
db_pass = os.environ["DB_PASS"]
db_user = os.environ["DB_USER"]
db_name = os.environ["DB_NAME"]


def get_mulit_modal_data(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        get multi modal data
    Args:
        address: address to check
        gmap_data: google maps data for address
    Returns:
        N/A
    """
    conn = psycopg2.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
    )
    logging.info("connected to db")

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Distance
    # Distance
    DIST = 321.868  # .2 miles

    # Query for bikes
    bike_query = f"select name,miles FROM source_data.bike_trails bd WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),bd.geometry,{DIST})"
    bike_df = psql.read_sql(bike_query, conn)

    # Query for metro data
    metro_query = f"select station FROM analysis_data.metro_stations_ridership bd WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),bd.geography,{DIST})"
    metro_df = psql.read_sql(metro_query, conn)

    # Query for sidewalk data
    sidewalk_query = f"select description FROM source_data.sidewalks bd WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),bd.geometry,{DIST})"
    sw_df = psql.read_sql(sidewalk_query, conn)

    data_json = {}
    data_json["bike_paths"] = list(bike_df["name"])
    data_json["metro_stations"] = list(metro_df["station"])
    data_json["sidwalks"] = list(sw_df["description"])

    # try:
    #     data_json["website"] = list(df["web_url"])[0]
    #     data_json["name"] = list(df["name"])[0]
    # except:
    #     data_json["website"] = ""
    #     data_json["name"] = "ERROR NO ANC FOUND"

    text = format_mm_json(data_json)

    return text


def format_mm_json(
    data_json: Dict[str, Any],
) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        data_json: multi modal data
    Returns:
        text: formatted text
    """
    text = ""

    bike_paths = data_json["bike_paths"]
    metro_stations = data_json["metro_stations"]
    sidewalks = data_json["sidwalks"]

    if len(bike_paths) > 0:
        text += "Nearby Bike Trails:  \n"
        for bike_path in bike_paths:
            text += f"{bike_path}  \n"

    if len(metro_stations) > 0:
        text += "Nearby Metro Stations:  \n"
        for metro in metro_stations:
            text += f"{metro}  \n"

    if len(sidewalks) > 0:
        text += "Nearby Sidewalk types:  \n"

        sidewalk_counter = {}
        for sw in sidewalks:
            if not sw in sidewalk_counter:
                sidewalk_counter[sw] = 0
            sidewalk_counter[sw] += 1

        for key in list(sidewalk_counter.keys()):
            text += f"{key}: {sidewalk_counter[key]}  \n"

    return text


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    print("hello")

    # Use the test data we have
    address = "607 13th St NW, Washington, DC 20005"
    print(f"Getting multi modal info for {address}")

    text = get_mulit_modal_data(address)
    print(text)


if __name__ == "__main__":
    main()
