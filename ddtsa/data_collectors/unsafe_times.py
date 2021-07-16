# Python imports
import logging
import os
from typing import Tuple, Union, Optional, Dict, Any

# 3rd Party Imports
# from datetime import datetime
import datetime

from geopy.distance import geodesic
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


def get_unsafe_times(address: str, gmap_data: Dict[str, Any] = None) -> str:

    # TODO connect to live database
    # For now use the test data we have
    # df = pd.read_csv("../data/analysis_data_dc_crashes_w_details.csv")

    conn = psycopg2.connect(
        f"dbname='{db_name}' user='{db_user}' host='{db_host}' password='{db_pass}'"
    )
    logging.info("connected to db")

    # year_ago_date_time = datetime.datetime.now() - datetime.timedelta(days=365)
    # date_format = "%Y-%m-%d %H:%M:%S"
    # year_ago_date_time_string = year_ago_date_time.strftime(date_format)

    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # # Get the latitude and longitude
    # curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    # curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    # # Distance
    # DIST = 321.868  # .2 miles
    db_query = f"select mpdlatitude, mpdlongitude,reportdate, objectid from analysis_data.dc_crashes_w_details ad WHERE reportdate > date_trunc('month', CURRENT_DATE) - INTERVAL '1 year' order by reportdate desc"
    df = psql.read_sql(db_query, conn)

    try:
        json_results = find_unsafe_times(df, address, gmap_data)
        text_string = format_results(json_results)
    except Exception as error:
        json_results = {"error": error}
        text_string = error

    return text_string


def format_results(json_results: Dict[str, Any]) -> str:
    """
    Purpose:
        Format the json to a text response
    Args:
        json_results: crash data
    Returns:
        text_string: formatted text
    """

    early_morning_crashes = len(json_results["early_morning"])
    morning_crashes = len(json_results["morning"])
    afternoon_crashes = len(json_results["afternoon"])
    night_crashes = len(json_results["night"])
    weekday_crashes = len(json_results["weekdays"])
    weekend_crashes = len(json_results["weekends"])

    total_crashes = weekday_crashes + weekend_crashes
    # TODO massage text
    text_string = f"There have been {total_crashes} crashes in the past year.  "

    if early_morning_crashes > 0:
        text_string += f"\n {early_morning_crashes} happened overnight  "

    if morning_crashes > 0:
        text_string += f"\n {morning_crashes} happened in the morning  "

    if afternoon_crashes > 0:
        text_string += f"\n {afternoon_crashes} happened in the afternoon  "

    if night_crashes > 0:
        text_string += f"\n {night_crashes} happened at night  "

    if weekday_crashes > 0:
        text_string += f"\n {weekday_crashes} happened during the weekdays  "

    if weekend_crashes > 0:
        text_string += f"\n {weekend_crashes} happened during the weekend  "

    return text_string


def fill_crash_json(
    row: pd.Series, crash_json: Dict[str, Any], lat_long: Tuple
) -> None:
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
        row_lat_long = (row["mpdlatitude"], row["mpdlongitude"])

        # Did crash happen less than .2 miles from spot?
        if geodesic(row_lat_long, lat_long) < 0.2:

            time_string = str(row["reportdate"]).replace("+00:00", "")
            date_format = "%Y-%m-%d %H:%M:%S"

            curr_date_time = datetime.datetime.strptime(time_string, date_format)
            obj_id = row["objectid"]

            # check hour of crash
            # early_morning 12am - 6am
            # moring 6am - 12pn
            # afternoon 12pm - 6pm
            # night 6pm - 12am
            crash_hour = curr_date_time.hour
            if crash_hour <= 6:
                crash_json["early_morning"].append(obj_id)
            elif crash_hour <= 12:
                crash_json["morning"].append(obj_id)
            elif crash_hour <= 18:
                crash_json["afternoon"].append(obj_id)
            else:
                crash_json["night"].append(obj_id)

            # check if weekday or weekend
            weekday = curr_date_time.weekday()

            if weekday <= 4:
                crash_json["weekdays"].append(obj_id)
            else:
                crash_json["weekends"].append(obj_id)
    except Exception as error:
        print(row)
        logging.error(error)
        return


def find_unsafe_times(
    df: pd.DataFrame, address: str, gmap_data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Purpose:
        Find unsafe times given an address
    Args:
        df: Data to use
        address: address to use
    Returns:
        unsafe_times_json - > JSON with unsafe times
    """

    # geocode service - using google maps
    if not gmap_data:
        gmap_data = rev_geocode(address, GOOGLE_API_KEY)

    # Get the latitude and longitude
    curr_lat = gmap_data[0]["geometry"]["location"]["lat"]
    curr_lng = gmap_data[0]["geometry"]["location"]["lng"]

    lat_long = (curr_lat, curr_lng)

    crash_json = {}
    crash_json["early_morning"] = []
    crash_json["morning"] = []
    crash_json["afternoon"] = []
    crash_json["night"] = []
    crash_json["weekdays"] = []
    crash_json["weekends"] = []

    df.apply(lambda row: fill_crash_json(row, crash_json, lat_long), axis=1)

    return crash_json


def main():
    """
    Purpose:
        Test the function
    Args:
        N/A
    Returns:
        N/A
    """
    # sample address
    address = "R Street Northwest & 19th Street Northwest, Washington, DC"
    print(f"Testing using address: {address}")

    crash_data = get_unsafe_times(address)
    print(crash_data)


if __name__ == "__main__":
    main()
