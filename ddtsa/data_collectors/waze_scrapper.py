# Python imports
# import datetime
import logging
import os
from typing import Union, Optional, Dict, Any, Tuple, List

# from geopy.distance import geodesic

# 3rd Party Imports
import pandas as pd
import psycopg2
import pandas.io.sql as psql


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


def get_waze_data(address: str, gmap_data: Dict[str, Any] = None) -> str:
    """
    Purpose:
        Get waze alerts near address
    Args:
        address: current address
        gmap_data: google maps data
    Returns:
        text_string: formatted text with traffic calming info
    """
    # TODO
    json_results = {}

    # Connect to postgress db
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

    # Create query
    db_query = f"select alert_street, alert_long, alert_lat,alert_subtype,alert_reportdescription,pub_datetime from source_data.waze_alerts_stream wd WHERE pub_datetime > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year' AND ST_DWithin(ST_MakePoint({curr_lng},{curr_lat}),wd.geography,{DIST}) order by pub_datetime desc"
    df = psql.read_sql(db_query, conn)

    # df_subset = df.loc[(df["pub_datetime"] >= year_ago_date_time_string)]

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    try:
        json_results = find_waze_alerts_features(df, gmap_data)

        text_string = format_waze_json(json_results)
    except Exception as error:
        json_results = {"error": error}
        text_string = error

    return text_string


def format_waze_json(
    waze_json: List[Dict[str, Any]],
) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        waze_json: traffic calmaing data
    Returns:
        text: formatted text
    """
    text = ""

    if len(waze_json) == 0:
        text += "No Waze alerts the past year"

    for item in waze_json:
        if item["alert_subtype"]:
            text += f"On {str(item['date']).replace('+00:00','')} a request was put in at {item['alert_street']}  \n"

            if (
                item["alert_subtype"]
                and item["alert_subtype"] != ""
                and len(item["alert_subtype"]) != 0
            ):
                text += f" Here are the details: {item['alert_subtype']}"
                if item["alert_reportdescription"]:
                    text += f" - {item['alert_reportdescription']}"
                text += "\n\n"

    return text


def fill_waze_json(row: pd.Series, waze_list: Dict[str, Any], lat_long: Tuple) -> None:
    """
    Purpose:
        Fill out unsafe times json
    Args:
        row: Row to check
        crash_json: has crash details
        lat_long: lat long of address to check
    Returns:
        N/A
    """

    try:
        # row_lat_long = (row["alert_lat"], row["alert_long"])
        # print(row_lat_long)

        # Did crash happen less than .2 miles from spot?
        # if geodesic(row_lat_long, lat_long) < 0.2:

        waze_json = {}

        waze_json["date"] = row["pub_datetime"]
        waze_json["alert_subtype"] = row["alert_subtype"]
        waze_json["alert_reportdescription"] = row["alert_reportdescription"]
        waze_json["alert_street"] = row["alert_street"]

        waze_list.append(waze_json)
    except Exception as error:
        print(row)
        logging.error(error)
        return


def find_waze_alerts_features(
    df: pd.DataFrame, gmap_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Purpose:
        Find traffic calming features given an address
    Args:
        df: Dataframe of our data
        address: adress to find features for
    Returns:
        waze_json: JSON of traffic calming data
    """

    # hmm road_way_blocks dont have latlong this will be annoying to convert everything..

    # we would use the geodesic function to see if latlong withint .2 miles
    # for now we will hard code to row 0

    # speedlimit
    waze_list = []

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # Create tuple of lat long
    lat_long = (curr_lat, curr_lng)
    # print(lat_long)

    # Find all roadway blocks within .1 miles of address
    df.apply(lambda row: fill_waze_json(row, waze_list, lat_long), axis=1)

    return waze_list


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    # Use the test data we have
    # df = pd.read_csv("../../data/analysis_data_roadway_blocks.csv")

    # 1400  - 1413 BLOCK OF SPRING ROAD NW

    # sample address
    address = "Potomac Ave SW"
    print(f"Getting traffic calming features for {address}")

    text = get_waze_data(address)
    print(text)


if __name__ == "__main__":
    main()
