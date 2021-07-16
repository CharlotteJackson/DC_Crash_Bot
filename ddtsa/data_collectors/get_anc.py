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


def get_anc_info(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        get 311 requests
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
    DIST = 0  # O miles

    db_query = f"select objectid,anc_id,web_url,name from source_data.anc_boundaries anc WHERE ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),anc.geography,{DIST}) "
    df = psql.read_sql(db_query, conn)

    data_json = {}
    try:
        data_json["website"] = list(df["web_url"])[0]
        data_json["name"] = list(df["name"])[0]
    except:
        data_json["website"] = ""
        data_json["name"] = "ERROR NO ANC FOUND"

    return data_json


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
    address = "600 Farragut St. NW"
    print(f"Getting anc info for {address}")

    text = get_anc_info(address)
    print(text)


if __name__ == "__main__":
    main()
