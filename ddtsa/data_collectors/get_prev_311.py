# Python imports
import logging
import os
from typing import Tuple, Union, Optional, Dict, Any, List

# 3rd Party Imports
# from datetime import datetime
# import datetime
# from geopy.distance import geodesic
import pandas as pd

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


def get_prev_requests(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        get 311 requests
    Args:
        address: address to check
        gmap_data: google maps data for address
    Returns:
        N/A
    """
    json_results = {}

    conn = psycopg2.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
    )
    logging.info("connected to db")

    # year_ago_date_time = datetime.datetime.now() - datetime.timedelta(days=365)
    # date_format = "%Y-%m-%d %H:%M:%S"
    # year_ago_date_time_string = year_ago_date_time.strftime(date_format)

    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Distance
    DIST = 321.868  # .2 miles

    db_query = f"select adddate, details,servicerequestid, streetaddress,latitude,longitude from analysis_data.all311 ad WHERE adddate > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year' AND ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),ad.geography,{DIST}) order by adddate desc"
    df = psql.read_sql(db_query, conn)

    # df_subset = df.loc[(df["adddate"] >= year_ago_date_time_string)]

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    try:
        json_results = find_requests_in_past_12_months(df, gmap_data)
        text_string = format_results(json_results)
    except Exception as error:
        json_results = {"error": error}
        text_string = error

    return text_string


def format_results(json_results: List) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        json_results: crash data
    Returns:
        text_string: formatted text
    """
    text = ""

    if len(json_results) == 0:
        text += "No 311 requests in the past year"

    for item in json_results:

        text += f"On {str(item['adddate']).replace('+00:00','')} request {item['servicerequestid']} was put in at {item['streetaddress']}\n\n"

        if item["details"]:
            text += f" Here are the details: {item['details']}\n"

    return text


def fill_reqs_list(row: pd.Series, requests_list: List, lat_long: Tuple) -> None:
    """
    Purpose:
        Fill out reqs list
    Args:
        row: Row to check
        crash_json: has crash details
        lat_long: lat long of address to check
    Returns:
        N/A
    """
    try:
        # row_lat_long = (row["latitude"], row["longitude"])

        # Did req happen less than .2 miles from spot?
        # if geodesic(row_lat_long, lat_long) < 0.2:

        req_json = {}
        req_json["streetaddress"] = row["streetaddress"]
        req_json["servicerequestid"] = row["servicerequestid"]
        req_json["adddate"] = row["adddate"]
        req_json["details"] = row["details"]
        requests_list.append(req_json)

    except Exception as error:
        print(row)
        logging.error(error)
        return


def find_requests_in_past_12_months(df: pd.DataFrame, gmap_data: Dict[str, Any]):
    """
    Purpose:
        Find 311 requests in past 12 months
    Args:
        df: data to check
        gmap_data: gmap data of current address
    Returns:
        N/A
    """
    requests_list = []

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Create tuple of lat long
    lat_long = (curr_lat, curr_lng)
    # print(lat_long)

    # Find all roadway blocks within .1 miles of address
    df.apply(lambda row: fill_reqs_list(row, requests_list, lat_long), axis=1)

    return requests_list


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
    print(f"Getting 311 requests for {address}")

    text = get_prev_requests(address)
    print(text)


if __name__ == "__main__":
    main()
